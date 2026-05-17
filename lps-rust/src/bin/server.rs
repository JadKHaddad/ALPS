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
//! cargo run -r --bin server -- --config-file server-config.yaml
//!
//! ```

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use lps_rust::{Event, Failure, SimpleCall, server::Config};
use rand::RngExt;
use reqwest::StatusCode;

use clap::Parser;
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "server-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Debug)]
struct AppState {
    events: Mutex<Vec<Event>>,
    failures: Mutex<Vec<Failure>>,
    max_work_duration: Duration,
    max_response_duration: Duration,
    failure_rate: f64,
    results_file: PathBuf,
    failures_file: PathBuf,
    last_event: RwLock<Option<Event>>,
}

impl AppState {
    fn new(
        max_work_duration: Duration,
        max_response_duration: Duration,
        failure_rate: f64,
        results_file: PathBuf,
        failures_file: PathBuf,
    ) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            max_work_duration,
            max_response_duration,
            failure_rate,
            results_file,
            failures_file,
            last_event: RwLock::new(None),
        }
    }

    async fn set_last_event(&self, event: Event) {
        *self.last_event.write().await = Some(event);
    }

    /// We check the arrived_at of the last event periodically to make sure that the server is still working and receiving calls.
    ///
    /// If the server did not receive any calls for a while, we can shutdown the simulation.
    async fn get_last_event(&self) -> Option<Event> {
        self.last_event.read().await.clone()
    }

    async fn work(&self, call: SimpleCall) -> StatusCode {
        let event = Event::from(call.clone()).with_arrived_at_now();

        self.set_last_event(event.clone()).await;

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

                self.push_failure(Failure::new_now(event)).await;

                tracing::info!(ms = response_duration_ms, ?call, "Responding");

                tokio::time::sleep(Duration::from_millis(response_duration_ms)).await;

                StatusCode::INTERNAL_SERVER_ERROR
            }
            false => {
                tracing::info!(?call, "Work succeeded");

                self.push_event(event).await;

                tracing::info!(ms = response_duration_ms, ?call, "Responding");

                tokio::time::sleep(Duration::from_millis(response_duration_ms)).await;

                StatusCode::OK
            }
        }
    }

    async fn push_event(&self, event: Event) {
        let count = {
            let mut events = self.events.lock().await;

            events.push(event);

            events.len()
        };

        tracing::info!(count, "Events count");
    }

    async fn push_failure(&self, failure: Failure) {
        let count = {
            let mut failures = self.failures.lock().await;

            failures.push(failure);

            failures.len()
        };

        tracing::debug!(count, "Failures count");
    }

    async fn save_events(&self) -> anyhow::Result<()> {
        let data = serde_json::to_vec(&*self.events.lock().await)
            .context("Failed to serialize results")?;

        tokio::fs::write(&self.results_file, data)
            .await
            .context("Failed to write results file")
    }

    async fn save_failures(&self) -> anyhow::Result<()> {
        let data = serde_json::to_vec(&*self.failures.lock().await)
            .context("Failed to serialize failures")?;

        tokio::fs::write(&self.failures_file, data)
            .await
            .context("Failed to write failures file")
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
        config.failures_file,
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
        .route(
            "/last",
            get(|State(state): State<Arc<AppState>>| async move {
                let event = state.get_last_event().await;

                Json(event)
            }),
        )
        .route("/health", get(|| async move { StatusCode::OK }))
        .with_state(app_state.clone());

    let listener = TcpListener::bind(config.socket_addr)
        .await
        .context("Failed to bind to socket")?;

    let cancellation_token = Arc::new(CancellationToken::new());

    if let Some(idle_duration) = config.shutdown_after_idle_duration {
        let cancellation_token = cancellation_token.clone();
        let app_state = app_state.clone();

        tokio::spawn(async move {
            tracing::info!(?idle_duration, "Idle shutdown monitor started");

            let monitor_start_time = chrono::Utc::now();
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                let last_activity = if let Some(event) = app_state.get_last_event().await {
                    event.arrived_at.unwrap_or(monitor_start_time)
                } else {
                    monitor_start_time
                };

                let now = chrono::Utc::now();
                let duration_since = now.signed_duration_since(last_activity);

                if let Ok(elapsed_std) = duration_since.to_std()
                    && elapsed_std > idle_duration
                {
                    tracing::warn!(
                        ?elapsed_std,
                        "Server idle threshold reached (no recent activity). Shutting down."
                    );
                    cancellation_token.cancel();
                    break;
                }
            }
        });
    }

    let shutdown_signal = async move {
        tokio::select! {
            _ = lps_rust::shutdown_signal() => {
                tracing::info!("Received shutdown signal, shutting down");
            }
            _ = cancellation_token.cancelled() => {
                tracing::info!("Idle shutdown triggered, shutting down");
            }
        }
    };

    _ = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .inspect_err(|err| tracing::error!(%err, "Server stopped with error"));

    tracing::info!("Saving results");

    app_state
        .save_events()
        .await
        .context("Failed to save results")?;

    app_state
        .save_failures()
        .await
        .context("Failed to save failures")?;

    tracing::info!("Validation results");

    app_state
        .validate()
        .await
        .context("Validation error")
        .inspect(|_| tracing::info!("Validation succeeded"))
}
