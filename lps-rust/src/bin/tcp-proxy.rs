//!
//! ```not_rust
//! cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml
//! ```

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use lps_rust::{TcpConnectionDrop, shutdown_signal, tcp_proxy::Config};
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

/// Latency applied to every chunk of up to `chunk_size` bytes in one direction.
///
/// Each chunk waits a uniformly random duration in `min..=max` before it is sent on.
#[derive(Debug, Clone, Copy)]
struct ChunkLatency {
    min: Duration,
    max: Duration,
    chunk_size: NonZeroUsize,
}

impl ChunkLatency {
    /// Same rules as `min_up_duration`: a missing `min` means `0`, and a `min` greater
    /// than `max` is ignored with a warning.
    fn new(
        direction: &str,
        min: Option<Duration>,
        max: Duration,
        chunk_size: NonZeroUsize,
    ) -> Self {
        let min = match min {
            Some(min) if min > max => {
                tracing::warn!(
                    direction,
                    "min_{direction}_latency is greater than max_{direction}_latency, setting min_{direction}_latency to None"
                );

                Duration::ZERO
            }
            Some(min) => min,
            None => Duration::ZERO,
        };

        Self {
            min,
            max,
            chunk_size,
        }
    }

    fn sample(&self, rng: &mut StdRng) -> Duration {
        if self.min == self.max {
            return self.max;
        }

        let us = rng.random_range(self.min.as_micros() as u64..=self.max.as_micros() as u64);

        Duration::from_micros(us)
    }
}

/// Copies `reader` -> `writer` one chunk at a time:
/// read up to `chunk_size` bytes, wait the chunk latency, write, repeat.
///
/// On EOF the write side is shut down, so the FIN reaches the peer after the last chunk.
async fn chunked_copy(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    profile: ChunkLatency,
    mut rng: StdRng,
) -> std::io::Result<()> {
    let mut buf = vec![0u8; profile.chunk_size.get()];

    loop {
        let n = reader.read(&mut buf).await?;

        if n == 0 {
            break;
        }

        let delay = profile.sample(&mut rng);

        tracing::info!(delay = ?delay, bytes = n, "Chunk copied");

        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        writer.write_all(&buf[..n]).await?;
    }

    writer.shutdown().await
}

/// Latency settings for both directions of a proxied connection.
#[derive(Debug, Clone, Copy)]
struct ProxyLatency {
    /// client -> server
    upstream: ChunkLatency,
    /// server -> client
    downstream: ChunkLatency,
}

/// Proxies both directions, each with its own latency and deterministic jitter sequence.
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
        // client -> server
        chunked_copy(client_r, upstream_w, latency.upstream, upstream_rng),
        // server -> client
        chunked_copy(upstream_r, client_w, latency.downstream, downstream_rng),
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
        .map(|mut task| {
            if task
                .min_up_duration
                .map(|d| d.as_millis())
                .unwrap_or(0)
                > task.max_up_duration.as_millis()
            {
                tracing::warn!(
                    "min_up_duration is greater than max_up_duration, setting min_up_duration to None"
                );

                task.min_up_duration = None;
            };

            task
        })
        .enumerate()
        .map(|(index, task)| {
            let state = state.clone();

            let mut rng = StdRng::seed_from_u64(base_seed ^ SEED_SALT ^ index as u64);

            let profile = ProxyLatency {
                upstream: ChunkLatency::new(
                    "upstream",
                    task.min_upstream_latency,
                    task.max_upstream_latency,
                    task.chunk_size,
                ),
                downstream: ChunkLatency::new(
                    "downstream",
                    task.min_downstream_latency,
                    task.max_downstream_latency,
                    task.chunk_size,
                ),
            };

            let latency_task_seed = base_seed ^ LATENCY_SEED_SALT ^ ((index as u64) << 40);

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

                let min_ms = task.min_up_duration.map(|d| d.as_millis()).unwrap_or(0);

                // max_ms is always >= min_ms. See check above.
                let max_ms = task.max_up_duration.as_millis();

                let mut conn_counter: u64 = 0;

                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(accepted) => accepted,
                        Err(err) => {
                            tracing::error!(%err, "Failed to accept connection");

                            continue;
                        }
                    };

                    // Always exactly two draws per accepted connection, in accept order,
                    // so the sequence depends only on the seed.
                    let fail = rng.random_bool(task.failure_rate);

                    let duration_ms = if max_ms == 0 {
                        0
                    } else {
                        rng.random_range(min_ms..=max_ms) as u64
                    };

                    // Per-connection latency seed: derived from the accept order rather than
                    // drawn from `rng`, so the fail/duration sequence above is unaffected.
                    let conn_seed = latency_task_seed ^ conn_counter;
                    conn_counter += 1;

                    if fail {
                        tracing::warn!("Connection failed");

                        state.push(TcpConnectionDrop::new_now()).await;

                        continue;
                    }

                    tracing::info!(addr=%task.socket_addr, "Connection accepted");

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

                        tracing::info!(duration_ms, ?profile, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying connection");

                        match tokio::time::timeout(
                            Duration::from_millis(duration_ms),
                            proxy(stream, upstream, profile, conn_seed),
                        )
                        .await
                        {
                            Ok(_) => {
                                tracing::info!(addr=%task.socket_addr, upstream=%upstream_addr, "Proxying completed");
                            }
                            Err(_) => {
                                state.push(TcpConnectionDrop::new_now()).await;

                                tracing::warn!(addr=%task.socket_addr, upstream=%upstream_addr, "Proxying timed out");
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
