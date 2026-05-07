//! This is a device simulator that acts like a device.
//!
//! The device generates events at a fixed interval and pushes them to the server. If the push fails, it will retry after a fixed duration.
//!
//! ```not_rust
//!
//! cargo run -r --bin device -- --config-file device-config.yaml
//!
//! ```

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use lps_rust::{Event, SimpleCall, device::Config};
use rand::RngExt;

use clap::Parser;
use tokio::sync::Mutex;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "device-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Debug)]
struct State {
    events: Mutex<Vec<Event>>,
    results_file: PathBuf,
}

impl State {
    fn new(results_file: PathBuf) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            results_file,
        }
    }

    async fn push(&self, event: Event) {
        let count = {
            let mut events = self.events.lock().await;

            events.push(event);

            events.len()
        };

        tracing::debug!(count, "Events count");
    }

    async fn save(&self) -> anyhow::Result<()> {
        let data = serde_json::to_vec(&*self.events.lock().await)
            .context("Failed to serialize results")?;

        tokio::fs::write(&self.results_file, data)
            .await
            .context("Failed to write results file")
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("device=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            Config::default()
        });

    if let Some(addr) = config.wait_for_start_socket_addr {
        tracing::info!(?addr, "Waiting for start signal");

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context("Failed to bind start signal socket")?;

        tokio::select! {
            _ = lps_rust::shutdown_signal() => {
                tracing::info!("Received shutdown signal while waiting for start signal, shutting down");

                return Ok(());
            }
            result = listener.accept() => {
                let _ = result.context("Failed to accept start signal connection")?;
            }
        }

        tracing::info!(?addr, "Received start signal");
    }

    tracing::info!(?config, "Starting");

    let state = Arc::new(State::new(config.results_file.clone()));

    let tasks: Vec<_> = (0..config.devices_count)
        .map(|id| {
            let config = config.clone();
            let state = state.clone();

            tokio::spawn(async move {
                tokio::select! {
                    _ = async move {
                        let client = reqwest::Client::new();

                        for i in config.iter() {
                            let max_ms = config.max_event_interval.as_millis();

                            let duration_ms = if max_ms == 0 {
                                0
                            } else {
                                rand::rng().random_range(0..max_ms) as u64
                            };

                            tokio::time::sleep(Duration::from_millis(duration_ms)).await;

                            let call = SimpleCall::new_now(id.to_string());

                            let mut try_index = 0;

                            let mut server_push_urls = config.server_push_urls.clone().into_iter().cycle();

                            'work: loop {
                                try_index += 1;

                                // The request id is not used in our system
                                let request_id = format!("device-{}-event-{}-try-{}", id, i, try_index);

                                tracing::info!(request_id, ?call, "Sending work request");

                                let server_push_url = server_push_urls.next().expect("server_push_urls should not be empty");

                                let response = client
                                    .post(&server_push_url)
                                    .header("request-id", request_id.clone())
                                    .json(&call)
                                    .send()
                                    .await;

                                match response {
                                    Ok(response) if response.status().is_success() => {
                                        tracing::info!(request_id, ?call, "Work succeeded");

                                        state.push(Event::from(call)).await;

                                        break 'work;
                                    }
                                    Ok(response) => {
                                        tracing::warn!(request_id, status=%response.status(), ?call, "Work failed");

                                        tokio::time::sleep(config.retry_duration).await;

                                        continue 'work;
                                    }
                                    Err(err) => {
                                        tracing::warn!(request_id, %err, ?call, "Work failed");

                                        tokio::time::sleep(config.retry_duration).await;

                                        continue 'work;
                                    }
                                }
                            }
                        }
                    } => {},
                    _ = lps_rust::shutdown_signal() => {}
                }
            })
        })
        .collect();

    for task in tasks {
        task.await?;
    }

    tracing::info!("Saving results");

    state.save().await?;

    Ok(())
}
