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

/// The request body ars-comp-2-proxy accepts on `/ID_REQ_KC_STORE7D3BPACKET`
/// (its pydantic `Message` model: `id: str`, `body: str`).
#[derive(Debug, serde::Serialize)]
struct SimpleCall {
    id: String,
    /// The original event, serialized as a JSON string
    body: String,
}

impl SimpleCall {
    /// `id` is taken from the original event's `id` field if it has one,
    /// otherwise `fallback_id` is used.
    fn new<T: serde::Serialize>(original: &T, fallback_id: String) -> anyhow::Result<Self> {
        let value = serde_json::to_value(original).context("Failed to serialize event")?;

        let id = match value.get("id") {
            Some(serde_json::Value::String(id)) => id.clone(),
            Some(id) if !id.is_null() => id.to_string(),
            _ => fallback_id,
        };

        let body = serde_json::to_string(&value).context("Failed to serialize event body")?;

        Ok(Self { id, body })
    }
}

#[derive(Clone)]
struct JobsState {
    client: reqwest::Client,
    jobs: Arc<Mutex<Vec<Job>>>,
    jobs_file: PathBuf,
}

impl JobsState {
    fn new(jobs_file: PathBuf, http_timeout: Duration) -> Self {
        Self {
            client: reqwest::ClientBuilder::new()
                .http1_only()
                .pool_max_idle_per_host(0)
                .timeout(http_timeout)
                .build()
                .expect("Failed to build http client"),
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
async fn connection(config: Config, state: JobsState) -> Result<(), anyhow::Error> {
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
            let use_simple_call_format = config.use_simple_call_format;
            let conn = conn.clone();
            let state = state.clone();
            let prefetch_count = config.prefetch_count;

            tokio::spawn(async move {
                'channel: loop {
                    let channel = conn
                        .create_channel()
                        .await
                        .context("Failed to create channel")?;

                    channel
                        .basic_qos(prefetch_count, BasicQosOptions::default())
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

                        let request = state.client.post(&push_url);

                        let request = if use_simple_call_format {
                            let simple_call = SimpleCall::new(
                                &event.original,
                                format!("{queue}-{}", delivery.delivery_tag),
                            )
                            .context(format!("Failed to build simple call: {event:?}"))?;

                            tracing::debug!(%queue, ?simple_call, "Using simple call format");

                            request.json(&simple_call)
                        } else {
                            request.json(&event.original)
                        };

                        let response = request.send().await;

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

    let state = JobsState::new(config.results_file.clone(), config.http_timeout);
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
