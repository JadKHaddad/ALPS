//!
//! ```not_rust
//! cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml
//! ```

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use lps_rust::{
    TcpConnectionEvent, shutdown_signal,
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
    sync::{Mutex, mpsc},
};
use tokio_timerfd::Delay;

const SEED_SALT: u64 = 0x9A0C_0000_0000_0000;
const LATENCY_SEED_SALT: u64 = 0x1A7E_0000_0000_0000;
const DOWNSTREAM_SEED_SALT: u64 = 0x0000_D0D0_0000_0000;

/// Maximum number of bytes forwarded per socket read.
const READ_BUF_SIZE: usize = 1024;

/// Number of read chunks buffered between the reader and the delayed writer.
const CHUNK_CHANNEL_CAPACITY: usize = 64;

/// How long to wait before retrying a failed upstream DNS lookup.
const UPSTREAM_LOOKUP_RETRY: Duration = Duration::from_millis(250);

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "tcp-proxy-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Debug)]
struct State {
    drops: Mutex<Vec<TcpConnectionEvent>>,
    results_file: PathBuf,
}

impl State {
    fn new(results_file: PathBuf) -> Self {
        Self {
            drops: Mutex::new(Vec::new()),
            results_file,
        }
    }

    async fn push(&self, drop: TcpConnectionEvent) {
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

/// Latency in one direction.
///
/// A new delay is drawn uniformly from `min..=max` for every chunk read from the socket,
/// from the connection's seeded RNG, and the chunk is forwarded at `arrival + delay`.
#[derive(Debug, Clone, Copy)]
struct DirectionLatency {
    min: Duration,
    max: Duration,
}

impl DirectionLatency {
    fn is_zero(&self) -> bool {
        self.max.is_zero()
    }

    /// Draws one delay.
    fn sample_delay(&self, rng: &mut StdRng) -> Duration {
        if self.min == self.max {
            self.max
        } else {
            let ns = rng.random_range(self.min.as_nanos() as u64..=self.max.as_nanos() as u64);

            Duration::from_nanos(ns)
        }
    }
}

/// Latency settings for both directions of a proxied connection.
#[derive(Debug, Clone, Copy)]
struct ProxyLatency {
    /// client -> server
    upstream: DirectionLatency,
    /// server -> client
    downstream: DirectionLatency,
}

impl ProxyLatency {
    fn new(source: &str, latency: &Latency) -> Self {
        Self {
            upstream: DirectionLatency {
                min: resolve_min(
                    source,
                    "upstream_latency",
                    latency.min_upstream_latency,
                    latency.max_upstream_latency,
                ),
                max: latency.max_upstream_latency,
            },
            downstream: DirectionLatency {
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

/// Copies `reader` -> `writer`, delaying each chunk read by a freshly drawn random delay.
///
/// On EOF the write side is shut down, so the FIN reaches the peer after the last byte.
async fn delayed_copy(
    direction: &'static str,
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    latency: DirectionLatency,
    mut rng: StdRng,
) -> std::io::Result<()> {
    // The reader timestamps chunks as they arrive, so delays are measured from the
    // read, not skewed by the writer sleeping. The bounded channel keeps TCP backpressure.
    //
    // `std::time::Instant` (CLOCK_MONOTONIC) is used on purpose: it is the clock the
    // timerfd below is armed against.
    let (tx, mut rx) = mpsc::channel::<(Instant, Vec<u8>)>(CHUNK_CHANNEL_CAPACITY);

    let read = async move {
        let mut buf = vec![0u8; READ_BUF_SIZE];
        let mut total_bytes: u64 = 0;

        loop {
            let n = match reader.read(&mut buf).await {
                Ok(n) => n,
                Err(err) => {
                    tracing::error!(direction, %err, total_bytes, "Read error");

                    return Err(err);
                }
            };

            let arrived = Instant::now();

            if n == 0 {
                tracing::debug!(direction, total_bytes, "Read finished (EOF)");

                return Ok::<(), std::io::Error>(());
            }

            tracing::debug!(direction, bytes = n, total_bytes, "Read bytes");

            total_bytes += n as u64;

            if tx.send((arrived, buf[..n].to_vec())).await.is_err() {
                // The writer is gone
                return Ok(());
            }
        }
    };

    let write = async move {
        let mut total_bytes: u64 = 0;
        let mut total_delay = Duration::ZERO;

        // One timerfd per direction, re-armed for every chunk. Unlike `tokio::time::sleep`
        // (1 ms wheel granularity), timerfd has nanosecond resolution.
        let mut timer = Delay::new(Instant::now())?;

        while let Some((arrived, data)) = rx.recv().await {
            if !latency.is_zero() {
                let latency_delay = latency
                    .sample_delay(&mut rng)
                    .saturating_mul(data.len() as u32);
                let release = arrived + latency_delay;
                let delay = release.saturating_duration_since(Instant::now());

                tracing::info!(
                    direction,
                    ?latency_delay,
                    ?delay,
                    bytes = data.len(),
                    "Delaying bytes"
                );

                if delay > Duration::ZERO {
                    timer.reset(release);
                    (&mut timer).await?;
                }

                total_delay += delay;
            }

            let bytes_string = String::from_utf8_lossy(&data);

            tracing::info!(
                direction,
                bytes = data.len(),
                ?bytes_string,
                "Writing bytes"
            );

            writer.write_all(&data).await?;
            writer.flush().await?;

            tracing::info!(direction, bytes = data.len(), "Wrote bytes");

            total_bytes += data.len() as u64;
        }

        tracing::info!(direction, total_bytes, ?total_delay, "Copy finished (EOF)");

        writer.shutdown().await
    };

    tokio::try_join!(read, write)?;

    Ok(())
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
                // Run the task in an inner block so an error is logged right away instead of
                // only surfacing (and being discarded) when the task is awaited at shutdown.
                let result: anyhow::Result<()> = async move {
                let listener = TcpListener::bind(task.socket_addr)
                    .await
                    .context("Failed to bind to socket")?;

                let resolver = hickory_resolver::TokioResolver::builder_tokio()
                    .and_then(|builder| builder.build())
                    .context("Failed to create dns resolver")?;

                // The upstream container may not be registered in Docker's DNS yet when this
                // proxy starts, so retry instead of giving up. The listener is already bound,
                // so clients connecting meanwhile wait in the accept backlog instead of
                // getting "connection refused".
                let ip_addr = loop {
                    match resolver.lookup_ip(task.upstream_domain.as_str()).await {
                        Ok(lookup) => match lookup.iter().next() {
                            Some(ip) => break ip,
                            None => tracing::warn!(
                                upstream = %task.upstream_domain,
                                "No IP found for upstream, retrying"
                            ),
                        },
                        Err(err) => tracing::warn!(
                            upstream = %task.upstream_domain,
                            %err,
                            "Failed to look up upstream, retrying"
                        ),
                    }

                    tokio::time::sleep(UPSTREAM_LOOKUP_RETRY).await;
                };

                tracing::info!(upstream = %task.upstream_domain, %ip_addr, "Resolved upstream");

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

                        state.push(TcpConnectionEvent::new_refused_now()).await;

                        continue;
                    }

                    tracing::info!(conn_index, addr=%task.socket_addr, "Connection accepted");

                    state.push(TcpConnectionEvent::new_accepted_now()).await;

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
                                state.push(TcpConnectionEvent::new_completed_now()).await;

                                tracing::info!(conn_index, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying completed");
                            }
                            Err(_) => {
                                state.push(TcpConnectionEvent::new_dropped_now()).await;

                                tracing::warn!(conn_index, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying timed out");
                            }
                        }
                    });
                }

                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
                }
                .await;

                if let Err(err) = &result {
                    tracing::error!("Task {index} failed: {err:#}");
                }

                result
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
