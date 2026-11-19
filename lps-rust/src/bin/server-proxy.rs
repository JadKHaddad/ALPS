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
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue},
    routing::post,
};
use lps_rust::{SimpleCall, server_proxy::Config};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reqwest::StatusCode;

use clap::Parser;
use tokio::{net::TcpListener, sync::Mutex};

const SEED_SALT: u64 = 0x9A1C_0000_0000_0000;

/// Target size, in bytes, of every request we send and every response we return.
const TARGET_SIZE: usize = 1024;
/// Maximum length of a single padding header's value, in bytes.
const MAX_PADDING_VALUE_LEN: usize = 100;

/// Padding headers (`x-pad-0`, `x-pad-1`, ...) that fill a message of `current_size` bytes up to `TARGET_SIZE`.
///
/// Each header costs `"x-pad-N: " + value + "\r\n"` bytes on the wire, and the last one is
/// shortened so the total lands exactly on the target.
fn padding_headers(current_size: usize) -> HeaderMap {
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

/// Size on the wire of our empty response before any padding is added:
/// status line, `content-length: 0`, `date` and the blank line.
fn response_base_size(status: StatusCode) -> usize {
    format!(
        "HTTP/1.1 {} {}\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or_default()
    )
    .len()
        + "content-length: 0\r\n".len()
        + "date: Thu, 19 Nov 2026 09:05:28 GMT\r\n".len()
        + "\r\n".len()
}

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

    /// Builds the request to the server, padded to `TARGET_SIZE` bytes.
    fn build_request(
        &self,
        url: &str,
        call: &SimpleCall,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let body = serde_json::to_vec(call).context("Failed to serialize call")?;
        let parsed_url = reqwest::Url::parse(url).context("Failed to parse server push url")?;

        let padding = padding_headers(request_base_size(&parsed_url, body.len()));

        Ok(self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .headers(padding)
            .body(body))
    }

    async fn work(&self, call: SimpleCall) -> (StatusCode, HeaderMap) {
        let url = self.next_server_push_url().await;

        let status = match self.build_request(&url, &call) {
            Ok(request) => request
                .send()
                .await
                .map(|resp| resp.status())
                .unwrap_or_else(|err| {
                    tracing::error!(%err, "Failed to send request to server");
                    StatusCode::INTERNAL_SERVER_ERROR
                }),
            Err(err) => {
                tracing::error!(%err, "Failed to build request to server");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        tracing::debug!(%status, "Received response from server");

        (status, padding_headers(response_base_size(status)))
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
