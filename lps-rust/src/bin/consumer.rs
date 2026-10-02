//! The consumer is our LPS2 proxy.
//!
//! It is an amqp client that consumes events from RabbitMQ and sends work requests to the server.
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
use lps_rust::{
    Event,
    consumer::{Config, Job},
    shutdown_signal,
};
use tokio::sync::Mutex;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "consumer-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Clone)]
struct JobsSate {
    jobs: Arc<Mutex<Vec<Job>>>,
    jobs_file: PathBuf,
}

impl JobsSate {
    fn new(jobs_file: PathBuf) -> Self {
        Self {
            jobs: Arc::new(Mutex::new(Vec::new())),
            jobs_file,
        }
    }

    async fn add_job(&self, job: Job) {
        let mut jobs = self.jobs.lock().await;

        jobs.push(job);
    }

    async fn save_jobs(&self) -> anyhow::Result<()> {
        let data =
            serde_json::to_vec(&*self.jobs.lock().await).context("Failed to serialize results")?;

        tokio::fs::write(&self.jobs_file, data)
            .await
            .context("Failed to write results file")
    }
}

// XXX: We will have duplicated events if the connection to RabbitMQ is interrupted while the server is processing our sent event.
async fn connection(config: Config, state: JobsSate) -> Result<(), anyhow::Error> {
    let queues = (0..config.queues_count).map(|i| format!("events-{i}"));

    let conn = Arc::new(
        tokio::time::timeout(
            Duration::from_secs(1),
            Connection::connect(&config.rabbit_mq_url, ConnectionProperties::default()),
        )
        .await
        .context("Connection timed out")?
        .context("Failed to connect")?,
    );

    tracing::info!("Connected");

    let tasks: Vec<_> = queues
        .into_iter()
        .map(|queue| {
            let push_url = config.server_push_url.clone();
            let conn = conn.clone();
            let state = state.clone();

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

                        let job = Job::new_received_and_sent_and_waiting_for_response_now(
                            event.clone(),
                            queue.clone(),
                        );

                        state.add_job(job.clone()).await;

                        let response = client.post(&push_url).json(&event.original).send().await;

                        match response {
                            Ok(response) if response.status().is_success() => {
                                tracing::info!(%queue, ?event, "Work succeeded");

                                state.add_job(job.clone().success_before_ack_now()).await;

                                delivery
                                    .ack(BasicAckOptions::default())
                                    .await
                                    .context(format!("Failed to basic ack (work): {event:?}"))?;

                                state.add_job(job.clone().success_after_ack_now()).await;

                                tracing::info!(%queue, ?event, "Acknowledged");
                            }
                            _ => {
                                tracing::warn!(%queue, ?event, ?response, "Work failed");

                                state.add_job(job.clone().fail_before_nack_now()).await;

                                delivery
                                    .nack(BasicNackOptions {
                                        requeue: true,
                                        ..Default::default()
                                    })
                                    .await
                                    .context(format!("Failed to basic nack: {event:?}"))?;

                                state.add_job(job.fail_after_nack_now()).await;

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

    let (result, _, _) = futures::future::select_all(tasks).await;

    match result.context("Task panicked")? {
        Ok(()) => {
            tracing::info!("A task finished successfully, shutting down");

            Ok(())
        }
        Err(err) => {
            tracing::error!(?err, "Task failed, shutting down");

            Err(anyhow::anyhow!("Task failed: {err}"))
        }
    }
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

    let state = JobsSate::new(config.results_file.clone());
    let state_c = state.clone();

    tokio::select! {
        _ = shutdown_signal() => {
            tracing::info!("Received shutdown signal, shutting down");
        }
        _ = async move {


            loop {
                tracing::info!("Connecting to RabbitMQ");

                match connection(config.clone(), state.clone()).await {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::error!(%err, "Connection error");
                    }
                }

                tracing::info!(
                    "Reconnecting in {}...",
                    humantime::Duration::from(config.reconnect_duration)
                );

                tokio::time::sleep(config.reconnect_duration).await;
            }
        } => {}
    }

    tracing::info!("Saving results");

    state_c
        .save_jobs()
        .await
        .context("Failed to save results")?;

    Ok(())
}
