//!
//! ```not_rust
//! cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml
//! ```

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use lps_rust::{
    TcpConnectionDrop, shutdown_signal,
    tcp_proxy::{Config, Connection, Latency},
};
use rand::{RngExt, SeedableRng, rngs::StdRng};

use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Mutex,
};

const SEED_SALT: u64 = 0x9A0C_0000_0000_0000;
const LATENCY_SEED_SALT: u64 = 0x1A7E_0000_0000_0000;
const DOWNSTREAM_SEED_SALT: u64 = 0x0000_D0D0_0000_0000;

/// Maximum number of bytes forwarded per socket read.
const READ_BUF_SIZE: usize = 64 * 1024;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "tcp-proxy-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Debug)]
struct State {
    drops: Mutex<Vec<TcpConnectionDrop>>,
    results_file: PathBuf,
}

impl State {
    fn new(results_file: PathBuf) -> Self {
        Self {
            drops: Mutex::new(Vec::new()),
            results_file,
        }
    }

    async fn push(&self, drop: TcpConnectionDrop) {
        let count = {
            let mut drops = self.drops.lock().await;

            drops.push(drop);

            drops.len()
        };

        tracing::debug!(count, "Drops count");
    }

    async fn save(&self) -> anyhow::Result<()> {
        let data =
            serde_json::to_vec(&*self.drops.lock().await).context("Failed to serialize results")?;

        tokio::fs::write(&self.results_file, data)
            .await
            .context("Failed to write results file")
    }
}

/// Resolves an optional minimum against a maximum, following the `min_up_duration` rules:
/// a missing `min` means `0`, and a `min` greater than `max` is ignored with a warning.
///
/// `source` names the config entry (e.g. `connection` or `connections[2]`) for the warning.
fn resolve_min(source: &str, name: &str, min: Option<Duration>, max: Duration) -> Duration {
    match min {
        Some(min) if min > max => {
            tracing::warn!(
                source,
                "{source}: min_{name} is greater than max_{name}, setting min_{name} to None"
            );

            Duration::ZERO
        }
        Some(min) => min,
        None => Duration::ZERO,
    }
}

/// Per-byte latency in one direction.
///
/// For each read, a per-byte latency is drawn uniformly from `min..=max` and multiplied by
/// the number of bytes read; the data is forwarded after sleeping that long.
#[derive(Debug, Clone, Copy)]
struct ByteLatency {
    min: Duration,
    max: Duration,
}

impl ByteLatency {
    fn is_zero(&self) -> bool {
        self.max.is_zero()
    }

    /// Delay for `bytes` bytes.
    fn sample(&self, rng: &mut StdRng, bytes: usize) -> Duration {
        let per_byte = if self.min == self.max {
            self.max
        } else {
            let ns = rng.random_range(self.min.as_nanos() as u64..=self.max.as_nanos() as u64);

            Duration::from_nanos(ns)
        };

        per_byte.saturating_mul(bytes as u32)
    }
}

/// Latency settings for both directions of a proxied connection.
#[derive(Debug, Clone, Copy)]
struct ProxyLatency {
    /// client -> server
    upstream: ByteLatency,
    /// server -> client
    downstream: ByteLatency,
}

impl ProxyLatency {
    fn new(source: &str, latency: &Latency) -> Self {
        Self {
            upstream: ByteLatency {
                min: resolve_min(
                    source,
                    "upstream_latency",
                    latency.min_upstream_latency,
                    latency.max_upstream_latency,
                ),
                max: latency.max_upstream_latency,
            },
            downstream: ByteLatency {
                min: resolve_min(
                    source,
                    "downstream_latency",
                    latency.min_downstream_latency,
                    latency.max_downstream_latency,
                ),
                max: latency.max_downstream_latency,
            },
        }
    }
}

/// A validated [`Connection`] config, ready to use for an accepted connection.
#[derive(Debug, Clone, Copy)]
struct ConnectionProfile {
    failure_rate: f64,
    min_up_ms: u128,
    max_up_ms: u128,
    latency: ProxyLatency,
}

impl ConnectionProfile {
    fn new(source: &str, connection: &Connection) -> Self {
        let min_up = resolve_min(
            source,
            "up_duration",
            connection.min_up_duration,
            connection.max_up_duration,
        );

        Self {
            failure_rate: connection.failure_rate,
            min_up_ms: min_up.as_millis(),
            max_up_ms: connection.max_up_duration.as_millis(),
            latency: ProxyLatency::new(source, &connection.latency),
        }
    }
}

/// Copies `reader` -> `writer`, delaying every read by `per-byte latency * bytes read`.
///
/// On EOF the write side is shut down, so the FIN reaches the peer after the last byte.
async fn delayed_copy(
    direction: &'static str,
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    latency: ByteLatency,
    mut rng: StdRng,
) -> std::io::Result<()> {
    let mut buf = vec![0u8; READ_BUF_SIZE];

    let mut total_bytes: u64 = 0;
    let mut total_delay = Duration::ZERO;

    loop {
        let n = match reader.read(&mut buf).await {
            Ok(n) => {
                tracing::debug!(
                    direction,
                    bytes = n,
                    ?total_delay,
                    total_bytes,
                    "Read bytes"
                );

                n
            }
            Err(err) => {
                tracing::error!(direction, %err, total_bytes, ?total_delay, "Read error");

                return Err(err);
            }
        };

        if n == 0 {
            tracing::info!(direction, total_bytes, ?total_delay, "Copy finished (EOF)");

            break;
        }

        if !latency.is_zero() {
            let delay = latency.sample(&mut rng, n);

            tracing::info!(direction, ?delay, bytes = n, "Delaying bytes");

            tokio::time::sleep(delay).await;

            total_delay += delay;
        }

        let bytes_string = String::from_utf8_lossy(&buf[..n]);

        tracing::info!(direction, bytes = n, ?bytes_string, "Writing bytes");

        writer.write_all(&buf[..n]).await?;
        writer.flush().await?;

        tracing::info!(direction, bytes = n, "Wrote bytes");

        total_bytes += n as u64;
    }

    writer.shutdown().await
}

/// Proxies both directions, each with its own latency and deterministic RNG.
async fn proxy(
    client: TcpStream,
    upstream: TcpStream,
    latency: ProxyLatency,
    conn_seed: u64,
) -> std::io::Result<()> {
    let (client_r, client_w) = client.into_split();
    let (upstream_r, upstream_w) = upstream.into_split();

    let upstream_rng = StdRng::seed_from_u64(conn_seed);
    let downstream_rng = StdRng::seed_from_u64(conn_seed ^ DOWNSTREAM_SEED_SALT);

    tokio::try_join!(
        delayed_copy(
            "upstream",
            client_r,
            upstream_w,
            latency.upstream,
            upstream_rng
        ),
        delayed_copy(
            "downstream",
            upstream_r,
            client_w,
            latency.downstream,
            downstream_rng
        ),
    )?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("tcp_proxy=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            Config::default()
        });

    let base_seed = config.seed.unwrap_or_else(|| rand::rng().random());

    tracing::info!(seed = base_seed, "Using RNG seed");

    tracing::info!(?config, "Starting");

    let state = Arc::new(State::new(config.results_file.clone()));

    let tasks: Vec<_> = config
        .tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| {
            let state = state.clone();

            let mut rng = StdRng::seed_from_u64(base_seed ^ SEED_SALT ^ index as u64);

            let latency_task_seed = base_seed ^ LATENCY_SEED_SALT ^ ((index as u64) << 40);

            // Fallback for connections without an entry in `connections`.
            let default_profile =
                ConnectionProfile::new(&format!("tasks[{index}].connection"), &task.connection);

            // Connection `i` (in accept order) uses `connections[i]`.
            let profiles: Vec<ConnectionProfile> = task
                .connections
                .iter()
                .enumerate()
                .map(|(i, connection)| {
                    ConnectionProfile::new(
                        &format!("tasks[{index}].connections[{i}]"),
                        connection,
                    )
                })
                .collect();

            tokio::spawn(async move {
                let listener = TcpListener::bind(task.socket_addr)
                    .await
                    .context("Failed to bind to socket")?;

                let resolver = hickory_resolver::TokioResolver::builder_tokio()
                    .and_then(|builder| builder.build())
                    .context("Failed to create dns resolver")?;

                let ip_addr = resolver
                    .lookup_ip(task.upstream_domain)
                    .await
                    .context("Failed to lookup IP")?
                    .iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("No IP found"))?;

                let upstream_addr = SocketAddr::new(ip_addr, task.upstream_port);

                // Index of the accepted connection, counting failed ones too, so the
                // mapping to `connections` depends only on accept order.
                let mut conn_counter: usize = 0;

                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(accepted) => accepted,
                        Err(err) => {
                            tracing::error!(%err, "Failed to accept connection");

                            continue;
                        }
                    };

                    let conn_index = conn_counter;
                    conn_counter += 1;

                    let profile = profiles
                        .get(conn_index)
                        .copied()
                        .unwrap_or(default_profile);

                    // Always exactly two draws per accepted connection, in accept order,
                    // so the sequence depends only on the seed.
                    let fail = rng.random_bool(profile.failure_rate);

                    let duration_ms = if profile.max_up_ms == 0 {
                        0
                    } else {
                        rng.random_range(profile.min_up_ms..=profile.max_up_ms) as u64
                    };

                    // Per-connection latency seed: derived from the accept order rather than
                    // drawn from `rng`, so the fail/duration sequence above is unaffected.
                    let conn_seed = latency_task_seed ^ conn_index as u64;

                    if fail {
                        tracing::warn!(conn_index, "Connection failed");

                        state.push(TcpConnectionDrop::new_now()).await;

                        continue;
                    }

                    tracing::info!(conn_index, addr=%task.socket_addr, "Connection accepted");

                    let state = state.clone();

                    tokio::spawn(async move {
                        tracing::debug!(addr=%task.socket_addr, upstream=%upstream_addr, "Connecting to upstream");

                        let upstream = match TcpStream::connect(upstream_addr).await {
                            Ok(upstream) => upstream,
                            Err(err) => {
                                tracing::error!(addr=%task.socket_addr, upstream=%upstream_addr, %err, "Failed to connect to upstream");

                                return;
                            }
                        };

                        tracing::info!(conn_index, duration_ms, latency=?profile.latency, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying connection");

                        match tokio::time::timeout(
                            Duration::from_millis(duration_ms),
                            proxy(stream, upstream, profile.latency, conn_seed),
                        )
                        .await
                        {
                            Ok(_) => {
                                tracing::info!(conn_index, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying completed");
                            }
                            Err(_) => {
                                state.push(TcpConnectionDrop::new_now()).await;

                                tracing::warn!(conn_index, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying timed out");
                            }
                        }
                    });
                }

                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            })
        })
        .collect();

    shutdown_signal().await;

    tracing::info!("Shutdown signal received, shutting down");

    for task in tasks {
        task.abort();
        task.await.ok();
    }

    tracing::info!("Saving results");

    state.save().await?;

    Ok(())
}
