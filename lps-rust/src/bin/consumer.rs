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

/// Target size, in bytes, of every request we send.
const TARGET_SIZE: usize = 1024;
/// Maximum length of a single padding header's value, in bytes.
const MAX_PADDING_VALUE_LEN: usize = 100;

/// Padding headers (`x-pad-0`, `x-pad-1`, ...) that fill a message of `current_size` bytes up to `TARGET_SIZE`.
///
/// Each header costs `"x-pad-N: " + value + "\r\n"` bytes on the wire, and the last one is
/// shortened so the total lands exactly on the target.
fn padding_headers(current_size: usize) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    let mut headers = HeaderMap::new();
    let mut remaining = TARGET_SIZE.saturating_sub(current_size);

    for i in 0usize.. {
        let name = format!("x-pad-{i}");
        // "name: value\r\n"
        let overhead = name.len() + 2 + 2;

        if remaining <= overhead {
            break;
        }

        let value_len = (remaining - overhead).min(MAX_PADDING_VALUE_LEN);

        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("padding header name is valid"),
            HeaderValue::from_str(&"x".repeat(value_len)).expect("padding value is valid ASCII"),
        );

        remaining -= overhead + value_len;
    }

    headers
}

/// Size on the wire of the request we send before any padding is added:
/// request line, the headers reqwest sends by default, and the body.
fn request_base_size(url: &reqwest::Url, body_len: usize) -> usize {
    let target = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    };

    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_string(),
    };

    format!("POST {target} HTTP/1.1\r\n").len()
        + "content-type: application/json\r\n".len()
        + "accept: */*\r\n".len()
        + format!("host: {host}\r\n").len()
        + format!("content-length: {body_len}\r\n").len()
        + "\r\n".len()
        + body_len
}

/// Builds a JSON POST request to `url`, padded with headers to `TARGET_SIZE` bytes.
fn padded_request(
    client: &reqwest::Client,
    url: &str,
    body: Vec<u8>,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let parsed_url = reqwest::Url::parse(url).context("Failed to parse push url")?;

    let padding = padding_headers(request_base_size(&parsed_url, body.len()));

    Ok(client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .headers(padding)
        .body(body))
}

#[derive(Clone)]
struct JobsSate {
    client: reqwest::Client,
    jobs: Arc<Mutex<Vec<Job>>>,
    jobs_file: PathBuf,
}

impl JobsSate {
    fn new(jobs_file: PathBuf, tcp_keepalive: Option<Duration>, http_timeout: Duration) -> Self {
        Self {
            client: reqwest::ClientBuilder::new()
                .tcp_keepalive(tcp_keepalive)
                .read_timeout(http_timeout)
                .timeout(http_timeout)
                .http1_only()
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

                        let body = if use_simple_call_format {
                            let simple_call = SimpleCall::new(
                                &event.original,
                                format!("{queue}-{}", delivery.delivery_tag),
                            )
                            .context(format!("Failed to build simple call: {event:?}"))?;

                            tracing::debug!(%queue, ?simple_call, "Using simple call format");

                            serde_json::to_vec(&simple_call)
                        } else {
                            serde_json::to_vec(&event.original)
                        }
                        .context(format!("Failed to serialize request body: {event:?}"))?;

                        let request = padded_request(&state.client, &push_url, body)
                            .context(format!("Failed to build request: {event:?}"))?;

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

    let state = JobsSate::new(
        config.results_file.clone(),
        config.tcp_keepalive,
        config.http_timeout,
    );
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
