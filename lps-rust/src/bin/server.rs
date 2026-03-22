//! This is the real server that receives the calls from a devices and saves them in the database.
//!
//! The api is designed to accept the calls sent from devices.
//!
//! The server then converts these calls into events and saves them in the database.
//! We convert the calls into events to make it easier for use to analyze the ordering and duplications.
//! The real server would not save the calls as events.
//!
//! ```not_rust
//!
//! cargo run -r --bin server
//!
//! ```

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::post};
use lps_rust::{Event, SimpleCall};
use rand::RngExt;
use reqwest::StatusCode;
use serde::Deserialize;

use clap::Parser;
use tokio::{net::TcpListener, sync::Mutex};

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "server-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

fn default_max_work_duration() -> Duration {
    Duration::from_secs(1)
}

fn default_max_response_duration() -> Duration {
    Duration::from_secs(1)
}

fn default_results_file() -> PathBuf {
    PathBuf::from("server-results.json")
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_socket_addr")]
    socket_addr: SocketAddr,
    /// Failure rate of the server, between 0 and 1
    #[serde(
        default = "lps_rust::default_failure_rate",
        deserialize_with = "lps_rust::deserialize_failure_rate"
    )]
    failure_rate: f64,
    #[serde(default = "default_max_work_duration", with = "humantime_serde")]
    max_work_duration: Duration,
    #[serde(default = "default_max_response_duration", with = "humantime_serde")]
    max_response_duration: Duration,
    #[serde(default = "default_results_file")]
    results_file: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            failure_rate: lps_rust::default_failure_rate(),
            max_work_duration: default_max_work_duration(),
            max_response_duration: default_max_response_duration(),
            results_file: default_results_file(),
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

#[derive(Debug)]
struct AppState {
    events: Mutex<Vec<Event>>,
    max_work_duration: Duration,
    max_response_duration: Duration,
    failure_rate: f64,
    results_file: PathBuf,
}

impl AppState {
    fn new(
        max_work_duration: Duration,
        max_response_duration: Duration,
        failure_rate: f64,
        results_file: PathBuf,
    ) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            max_work_duration,
            max_response_duration,
            failure_rate,
            results_file,
        }
    }

    async fn work(&self, call: SimpleCall) -> StatusCode {
        let max_ms = self.max_work_duration.as_millis();

        let work_duration_ms = if max_ms == 0 {
            0
        } else {
            rand::rng().random_range(0..max_ms) as u64
        };

        tracing::info!(ms = work_duration_ms, ?call, "Working");

        tokio::time::sleep(Duration::from_millis(work_duration_ms)).await;

        let fail = rand::rng().random_bool(self.failure_rate);

        let max_ms = self.max_response_duration.as_millis();

        let response_duration_ms = if max_ms == 0 {
            0
        } else {
            rand::rng().random_range(0..max_ms) as u64
        };

        match fail {
            true => {
                tracing::warn!(?call, "Work failed");

                tracing::info!(ms = response_duration_ms, ?call, "Responding");

                tokio::time::sleep(Duration::from_millis(response_duration_ms)).await;

                StatusCode::INTERNAL_SERVER_ERROR
            }
            false => {
                tracing::info!(?call, "Work succeeded");

                self.push(Event::from(call.clone())).await;

                tracing::info!(ms = response_duration_ms, ?call, "Responding");

                tokio::time::sleep(Duration::from_millis(response_duration_ms)).await;

                StatusCode::OK
            }
        }
    }

    async fn push(&self, event: Event) {
        self.events.lock().await.push(event);
    }

    async fn save(&self) -> anyhow::Result<()> {
        let data = serde_json::to_vec(&*self.events.lock().await)
            .context("Failed to serialize results")?;

        tokio::fs::write(&self.results_file, data)
            .await
            .context("Failed to write results file")
    }

    async fn validate(&self) -> anyhow::Result<()> {
        let events = self.events.lock().await;

        let (unique, duplicates) = Event::remove_duplication(events.iter());

        let duplicates: Vec<_> = duplicates.collect();

        if !duplicates.is_empty() {
            tracing::warn!(
                count = duplicates.len(),
                ?duplicates,
                "Found duplicated events"
            );
        }

        Event::validate_order(unique).context("Validation error")
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("server=info"))?;

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

    let app_state = Arc::new(AppState::new(
        config.max_work_duration,
        config.max_response_duration,
        config.failure_rate,
        config.results_file,
    ));

    let app = Router::new()
        .route(
            "/work",
            post(
                |State(state): State<Arc<AppState>>, Json(call): Json<SimpleCall>| async move {
                    state.work(call).await
                },
            ),
        )
        .with_state(app_state.clone());

    let listener = TcpListener::bind(config.socket_addr)
        .await
        .context("Failed to bind to socket")?;

    _ = axum::serve(listener, app)
        .with_graceful_shutdown(lps_rust::shutdown_signal())
        .await
        .inspect_err(|err| tracing::error!(%err, "Server stopped with error"));

    tracing::info!("Saving results");

    app_state.save().await.context("Failed to save results")?;

    tracing::info!("Validation results");

    app_state
        .validate()
        .await
        .context("Validation error")
        .inspect(|_| tracing::info!("Validation succeeded"))
}
