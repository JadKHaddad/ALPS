#![allow(clippy::obfuscated_if_else)]

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
use rand::{RngExt, seq::SliceRandom};

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

/// Maps an id in `0..1_000_000` to a unique, deterministic, random-looking 6-digit string.
fn six_digit_device_id(id: usize) -> String {
    const HALF: u32 = 1000;
    const ROUND_KEYS: [u32; 4] = [0x9E37_79B9, 0x85EB_CA6B, 0xC2B2_AE35, 0x27D4_EB2F];

    assert!(id < 1_000_000, "id must be < 1_000_000 to fit in 6 digits");

    let id = id as u32;
    let (mut left, mut right) = (id / HALF, id % HALF);

    for key in ROUND_KEYS {
        let f = mix(right, key) % HALF;
        (left, right) = (right, (left + f) % HALF);
    }

    format!("{:06}", left * HALF + right)
}

/// Deterministic integer mixer (no std hasher, so it's stable across Rust versions).
fn mix(x: u32, key: u32) -> u32 {
    let mut h = x.wrapping_mul(0x9E37_79B1) ^ key;

    h ^= h >> 15;

    h = h.wrapping_mul(0x2C1B_3C6D);

    h ^= h >> 12;

    h
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("device=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let mut config = Config::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            Config::default()
        });

    if config
        .min_event_interval
        .map(|d| d.as_millis())
        .unwrap_or(0)
        >= config.max_event_interval.as_millis()
    {
        tracing::warn!(
            "min_event_interval is greater than or equal to max_event_interval, setting min_event_interval to None"
        );

        config.min_event_interval = None;
    }

    if config.use_six_digit_device_id && config.devices_count >= 1_000_000 {
        tracing::warn!(
            "use_six_digit_device_id is set to true, but devices_count is greater than or equal to 1_000_000. Setting devices_count to 999_999"
        );

        config.devices_count = 999_999;
    }

    if let Some(wait_before_start) = config.wait_before_start {
        tracing::info!(wait_before_start = ?wait_before_start, "Waiting before starting to generate events");

        tokio::time::sleep(wait_before_start).await;
    }

    tracing::info!(?config, "Starting");

    let state = Arc::new(State::new(config.results_file.clone()));

    let tasks: Vec<_> = (0..config.devices_count)
        .map(|id| 
            config.use_six_digit_device_id
                .then_some(six_digit_device_id(id as usize))
                .unwrap_or(id.to_string()))
        .map(|id| {
            let config = config.clone();
            let state = state.clone();

            tokio::spawn(async move {
                tokio::select! {
                    _ = async move {
                        let client = reqwest::Client::new();

                        for i in config.iter() {
                            let min_ms = config.min_event_interval.map(|d| d.as_millis()).unwrap_or(0);

                            // max_ms is always > min_ms. See check above.
                            let max_ms = config.max_event_interval.as_millis();

                            let duration_ms = if max_ms == 0 {
                                0
                            } else {
                                rand::rng().random_range(min_ms..max_ms) as u64
                            };

                            tokio::time::sleep(Duration::from_millis(duration_ms)).await;

                            let call = SimpleCall::new_now(id.clone()).with_incremental_id(i);

                            let mut try_index = 0;

                            let mut server_push_urls = config.server_push_urls.clone();

                            server_push_urls.shuffle(&mut rand::rng());

                            let mut server_push_urls = server_push_urls.into_iter().cycle();

                            'work: loop {
                                try_index += 1;

                                // The request id is not used in our system
                                let request_id = format!("device-{}-event-{}-try-{}", id, i, try_index);

                                let server_push_url = server_push_urls.next().expect("server_push_urls should not be empty");

                                tracing::info!(request_id, url=%server_push_url, ?call, "Sending work request");

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

#[test]
fn device_code_is_collision_free() {
    let mut seen = std::collections::HashSet::new();

    for id in 0..1_000_000 {
        let code = six_digit_device_id(id);

        assert_eq!(code.len(), 6);
        assert!(seen.insert(code), "collision at id {id}");
    }
}
