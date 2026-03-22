//! The consumer is our LPS2 proxy.
//!
//! It is an ampq client that consumes events from RabbitMQ and sends work requests to the server.
//!
//! ```not_rust
//!
//! cargo run -r --bin consumer -- --config-file consumer-config.yaml
//!
//! ```

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::*,
    types::{FieldTable, ShortString},
};
use lps_rust::Event;
use serde::Deserialize;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "consumer-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

fn default_server_push_url() -> String {
    String::from("http://localhost:8080/work")
}

fn default_rabbit_mq_url() -> String {
    String::from("amqp://127.0.0.1:5672")
}

fn default_reconnect_duration() -> Duration {
    Duration::from_secs(5)
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    #[serde(default = "default_rabbit_mq_url")]
    rabbit_mq_url: String,
    #[serde(default = "default_server_push_url")]
    server_push_url: String,
    #[serde(default = "default_reconnect_duration", with = "humantime_serde")]
    reconnect_duration: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rabbit_mq_url: default_rabbit_mq_url(),
            server_push_url: default_server_push_url(),
            reconnect_duration: default_reconnect_duration(),
        }
    }
}

impl Config {
    async fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(
            &tokio::fs::read(path)
                .await
                .context("Failed to read config file")?,
        )
        .context("Failed to deserialize config file")
    }
}

// XXX: We will have duplicated events if the connection to RabbitMQ is interrupted while the server is processing our sent event.
async fn connection(config: Config) -> Result<(), anyhow::Error> {
    let queues = (0..10).map(|i| format!("events-{i}"));

    let conn = Arc::new(
        Connection::connect(&config.rabbit_mq_url, ConnectionProperties::default())
            .await
            .context("Failed to connect")?,
    );

    tracing::info!("Connected");

    let tasks: Vec<_> = queues
        .into_iter()
        .map(|queue| {
            let push_url = config.server_push_url.clone();
            let conn = conn.clone();

            tokio::spawn(async move {
                'channel: loop {
                    let channel = conn
                        .create_channel()
                        .await
                        .context("Failed to create channel")?;

                    channel
                        .basic_qos(1, BasicQosOptions::default())
                        .await
                        .context("Failed to create basic qos")?;

                    let mut consumer = channel
                        .basic_consume(
                            ShortString::from(queue.clone()),
                            ShortString::from(format!("consumer-{queue}")),
                            BasicConsumeOptions::default(),
                            FieldTable::default(),
                        )
                        .await
                        .context("Failed to create basic consume")?;

                    let client = reqwest::Client::new();

                    while let Some(delivery) = consumer.next().await {
                        let delivery = delivery.context("Failed to consume message")?;

                        tracing::debug!(%queue, ?delivery, "Received");
                        tracing::debug!(%queue, data=?delivery.data, "Received");

                        let event: Event = serde_json::from_slice(&delivery.data)
                            .context("Failed to deserialize event")?;

                        tracing::info!(%queue, ?event, "Received");

                        tracing::info!(%queue, ?event, "Sending work request");

                        let response = client.post(&push_url).json(&event.original).send().await;

                        match response {
                            Ok(response) if response.status().is_success() => {
                                tracing::info!(%queue, ?event, "Work succeeded");

                                delivery
                                    .ack(BasicAckOptions::default())
                                    .await
                                    .context(format!("Failed to basic ack (work): {event:?}"))?;

                                tracing::info!(%queue, ?event, "Acknowledged");
                            }
                            _ => {
                                tracing::warn!(%queue, ?event, "Work failed");

                                delivery
                                    .nack(BasicNackOptions {
                                        requeue: true,
                                        ..Default::default()
                                    })
                                    .await
                                    .context(format!("Failed to basic nack: {event:?}"))?;

                                channel
                                    .close(0, Default::default())
                                    .await
                                    .context(format!("Failed to close channel: {event:?}"))?;

                                continue 'channel;
                            }
                        }
                    }
                }

                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            })
        })
        .collect();

    let mut result = Result::Ok(());

    for task in tasks {
        if let Err(err) = task.await.context("Task panicked")? {
            tracing::error!(?err, "Task failed");

            result = Err(anyhow::anyhow!("One or more tasks failed"));
        }
    }

    result
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("consumer=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            Config::default()
        });

    tracing::info!(?config, "Starting");

    loop {
        tracing::info!("Connecting to RabbitMQ");

        match connection(config.clone()).await {
            Ok(()) => {
                return Ok(());
            }
            Err(err) => {
                tracing::error!(%err, "Connection error");

                tracing::info!(
                    "Reconnecting in {} seconds...",
                    config.reconnect_duration.as_secs()
                );

                tokio::time::sleep(config.reconnect_duration).await;
            }
        }
    }
}
