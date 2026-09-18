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
//! cargo run -r --bin gs-prod-workload-server -- --config-file server-config.yaml
//!
//! ```

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::post};
use lps_rust::{Event, SimpleCall, server::GsProdConfig};
use pyo3::{
    Py, PyAny, PyResult, Python,
    types::{PyAnyMethods, PyModule},
};
use rand::seq::IndexedRandom;
use reqwest::StatusCode;

use clap::Parser;
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "gs-prod-server-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

/// The mappings define 347 commands as String:number pairs. We only care
/// about the number part, since we use it in the prediction model.
static COMMANDS: LazyLock<[u16; 347]> = LazyLock::new(|| std::array::from_fn(|i| i as u16));

#[derive(Debug, Default)]
struct ActiveRequest {
    parallel_commands_finished: u64,
}

#[derive(Debug)]
struct AppState {
    events: Mutex<Vec<Event>>,
    results_file: PathBuf,
    failures_file: PathBuf,
    last_event: RwLock<Option<Event>>,
    model: Py<PyAny>,
    number_of_parallel_requests_pending: AtomicI64,
    next_request_id: AtomicU64,
    active_requests: Mutex<HashMap<u64, ActiveRequest>>,
}

impl AppState {
    fn new(py: Python<'_>, results_file: PathBuf, failures_file: PathBuf) -> PyResult<Self> {
        let model_bytes = include_bytes!("../../models/gs_model_prod_workload.pmml");

        let io = PyModule::import_bound(py, "io")?;
        let model_file = io.call_method1("BytesIO", (model_bytes.as_ref(),))?;

        let module = PyModule::import_bound(py, "sklearn_pmml_model.tree")?;
        let cls = module.getattr("PMMLTreeRegressor")?;
        let model = cls.call1((model_file,))?;

        Ok(Self {
            events: Mutex::new(Vec::new()),
            results_file,
            failures_file,
            last_event: RwLock::new(None),
            model: model.into(),
            number_of_parallel_requests_pending: AtomicI64::new(0),
            next_request_id: AtomicU64::new(1),
            active_requests: Mutex::new(HashMap::new()),
        })
    }

    async fn start_request(&self) -> (u64, f32) {
        let req_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);

        let pr_1 = self
            .number_of_parallel_requests_pending
            .fetch_add(1, Ordering::SeqCst) as f32;

        let mut active = self.active_requests.lock().await;

        active.insert(
            req_id,
            ActiveRequest {
                parallel_commands_finished: 0,
            },
        );

        (req_id, pr_1)
    }

    async fn get_pr_3(&self, req_id: u64) -> f32 {
        let active = self.active_requests.lock().await;

        active
            .get(&req_id)
            .map(|r| r.parallel_commands_finished as f32)
            .unwrap_or(0.0)
    }

    async fn finish_request(&self, req_id: u64) {
        self.number_of_parallel_requests_pending
            .fetch_sub(1, Ordering::SeqCst);

        let mut active = self.active_requests.lock().await;

        active.remove(&req_id);

        // Increment finished count for all other concurrently running requests
        for entry in active.values_mut() {
            entry.parallel_commands_finished += 1;
        }
    }

    fn run_model_predict(&self, pr_1: f32, pr_3: f32, request_type: f32) -> PyResult<f32> {
        Python::with_gil(|py| {
            let model = self.model.bind(py);
            let np = PyModule::import_bound(py, "numpy")?;

            let input: Vec<Vec<f32>> = vec![vec![pr_1, pr_3, request_type]];
            let x = np.call_method1("array", (input,))?;

            let prediction = model.call_method1("predict", (x,))?;
            let value: f64 = prediction.get_item(0)?.extract()?;

            Ok(value.max(0.0) as f32)
        })
    }

    async fn simulate_workload(&self, req_id: u64, pr_1: f32) -> PyResult<Duration> {
        let request_type = *COMMANDS
            .choose(&mut rand::rng())
            .expect("COMMANDS is non-empty") as f32;

        let start_time = Instant::now();

        // 1. Initial prediction
        let pr_3 = self.get_pr_3(req_id).await;
        let predicted_duration_secs = self.run_model_predict(pr_1, pr_3, request_type)?;

        let mut elapsed = start_time.elapsed().as_secs_f32();
        let sleep_time = (predicted_duration_secs - elapsed).max(0.0);

        if sleep_time > 0.0 {
            tokio::time::sleep(Duration::from_secs_f32(sleep_time)).await;

            // 2. Re-evaluate iteration (mirrors Python's for i in range(1) loop)
            elapsed = start_time.elapsed().as_secs_f32();
            let current_pr_3 = self.get_pr_3(req_id).await;
            let re_predicted_duration = self.run_model_predict(pr_1, current_pr_3, request_type)?;

            let remaining_sleep = (re_predicted_duration - elapsed).max(0.0);

            if remaining_sleep > 0.0 {
                tokio::time::sleep(Duration::from_secs_f32(remaining_sleep)).await;
            }
        }

        Ok(start_time.elapsed())
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

        let (req_id, pr_1) = self.start_request().await;

        let total_duration = match self.simulate_workload(req_id, pr_1).await {
            Ok(duration) => duration,
            Err(err) => {
                tracing::error!(%err, "Prediction failed, fallback to zero delay");
                Duration::from_secs(0)
            }
        };

        tracing::info!(ms = total_duration.as_millis(), ?call, "Working");

        self.push_event(event).await;

        tracing::info!(?call, "Work succeeded. Responding");

        self.finish_request(req_id).await;

        StatusCode::OK
    }

    async fn push_event(&self, event: Event) {
        let count = {
            let mut events = self.events.lock().await;
            events.push(event);
            events.len()
        };

        tracing::info!(count, "Events count");
    }

    async fn save_events(&self) -> anyhow::Result<()> {
        let data = serde_json::to_vec(&*self.events.lock().await)
            .context("Failed to serialize results")?;

        tokio::fs::write(&self.results_file, data)
            .await
            .context("Failed to write results file")
    }

    /// This does not save anything but is used to mirror `server.rs` so that the plotter finds the failures file.
    async fn save_failures(&self) -> anyhow::Result<()> {
        let data =
            serde_json::to_vec(&[serde_json::json!({})]).context("Failed to serialize failures")?;

        tokio::fs::write(&self.failures_file, data)
            .await
            .context("Failed to write failures file")
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pyo3::prepare_freethreaded_python();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("gs_prod_workload_server=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = GsProdConfig::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            GsProdConfig::default()
        });

    tracing::info!(?config, "Starting");

    let app_state =
        Python::with_gil(|py| AppState::new(py, config.results_file, config.failures_file))
            .context("Failed to create state")?;

    let app_state = Arc::new(app_state);

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

    Ok(())
}
