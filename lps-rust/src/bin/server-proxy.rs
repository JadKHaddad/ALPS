//! One instance of this proxies requests to multiple servers, and multiple instances of this proxies requests to one server.
//!
//! This is used to let the consumers talk to multiple servers.
//!
//! When we run the simulation with multiple servers, we deploy multiple instances of this,
//! One of them proxies to all other instances, and the others proxy to one server.
//!
//! This is used so that we have once central server that validates results. And we can multiple deduplication instances that all talk to the same server.
//!
//! ```not_rust
//!
//! cargo run -r --bin server-proxy -- --config-file server-proxy-config.yaml
//!
//! ```

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::post};
use lps_rust::{SimpleCall, server_proxy::Config};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reqwest::StatusCode;

use clap::Parser;
use tokio::{net::TcpListener, sync::Mutex};

const SEED_SALT: u64 = 0x9A1C_0000_0000_0000;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "server-proxy-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

struct AppState {
    client: reqwest::Client,
    server_push_urls: Vec<String>,
    rng: Mutex<StdRng>,
}

impl AppState {
    fn new(server_push_urls: Vec<String>, rng: StdRng) -> Self {
        Self {
            client: reqwest::Client::new(),
            server_push_urls,
            rng: Mutex::new(rng),
        }
    }

    async fn next_server_push_url(&self) -> String {
        let index = self
            .rng
            .lock()
            .await
            .random_range(0..self.server_push_urls.len());

        self.server_push_urls[index].clone()
    }

    async fn work(&self, call: SimpleCall) -> StatusCode {
        let url = self.next_server_push_url().await;

        let status = self
            .client
            .post(&url)
            .json(&call)
            .send()
            .await
            .map(|resp| resp.status())
            .unwrap_or_else(|err| {
                tracing::error!(%err, "Failed to send request to server");
                StatusCode::INTERNAL_SERVER_ERROR
            });

        tracing::debug!(%status, "Received response from server");

        status
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("server_proxy=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            Config::default()
        });

    (!config.server_push_urls.is_empty())
        .then_some(())
        .ok_or(anyhow::anyhow!("server_push_urls should not be empty"))?;

    let base_seed = config.seed.unwrap_or_else(|| rand::rng().random());

    tracing::info!(seed = base_seed, "Using RNG seed");

    tracing::info!(?config, "Starting");

    let rng = StdRng::seed_from_u64(base_seed ^ SEED_SALT);

    let app_state = Arc::new(AppState::new(config.server_push_urls, rng));

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

    axum::serve(listener, app)
        .with_graceful_shutdown(lps_rust::shutdown_signal())
        .await
        .inspect_err(|err| tracing::error!(%err, "Server stopped with error"))?;

    Ok(())
}
