//!
//! ```not_rust
//! cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml
//! ```

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

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
    time::Instant,
};

const SEED_SALT: u64 = 0x9A0C_0000_0000_0000;
const LATENCY_SEED_SALT: u64 = 0x1A7E_0000_0000_0000;
const DOWNSTREAM_SEED_SALT: u64 = 0x0000_D0D0_0000_0000;

const READ_BUF_SIZE: usize = 16 * 1024;
const CHANNEL_CAPACITY: usize = 1024;

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

/// One-way latency applied to each forwarded chunk: `base..=base + jitter`, uniformly distributed.
#[derive(Debug, Clone, Copy)]
struct LatencyProfile {
    base: Duration,
    jitter: Duration,
}

impl LatencyProfile {
    fn is_zero(&self) -> bool {
        self.base.is_zero() && self.jitter.is_zero()
    }

    fn sample(&self, rng: &mut StdRng) -> Duration {
        if self.jitter.is_zero() {
            return self.base;
        }

        let jitter_us = rng.random_range(0..=self.jitter.as_micros() as u64);

        self.base + Duration::from_micros(jitter_us)
    }
}

/// Copies `reader` -> `writer`, delaying every chunk by `profile.sample()` without ever
/// reordering. Propagates EOF (FIN) once the last chunk has been delivered.
async fn delayed_copy(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    profile: LatencyProfile,
    mut rng: StdRng,
) -> std::io::Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Instant, Vec<u8>)>(CHANNEL_CAPACITY);

    let read_side = async move {
        let mut buf = vec![0u8; READ_BUF_SIZE];
        let mut last_deliver_at = Instant::now();

        loop {
            let n = reader.read(&mut buf).await?;

            if n == 0 {
                // EOF: dropping `tx` lets the writer drain the queue and shut down.
                break;
            }

            // TCP never reorders, so a chunk can't overtake the one ahead of it.
            let deliver_at = (Instant::now() + profile.sample(&mut rng)).max(last_deliver_at);
            last_deliver_at = deliver_at;

            if tx.send((deliver_at, buf[..n].to_vec())).await.is_err() {
                // Writer side is gone.
                break;
            }
        }

        Ok::<_, std::io::Error>(())
    };

    let write_side = async move {
        while let Some((deliver_at, chunk)) = rx.recv().await {
            tokio::time::sleep_until(deliver_at).await;

            writer.write_all(&chunk).await?;
        }

        writer.shutdown().await
    };

    tokio::try_join!(read_side, write_side).map(|_| ())
}

/// Proxies both directions, each with its own deterministic jitter sequence.
async fn proxy_with_latency(
    client: TcpStream,
    upstream: TcpStream,
    profile: LatencyProfile,
    conn_seed: u64,
) -> std::io::Result<()> {
    if profile.is_zero() {
        let (mut client, mut upstream) = (client, upstream);

        return tokio::io::copy_bidirectional(&mut client, &mut upstream)
            .await
            .map(|_| ());
    }

    let (client_r, client_w) = client.into_split();
    let (upstream_r, upstream_w) = upstream.into_split();

    let upstream_rng = StdRng::seed_from_u64(conn_seed);
    let downstream_rng = StdRng::seed_from_u64(conn_seed ^ DOWNSTREAM_SEED_SALT);

    tokio::try_join!(
        // client -> server
        delayed_copy(client_r, upstream_w, profile, upstream_rng),
        // server -> client
        delayed_copy(upstream_r, client_w, profile, downstream_rng),
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

            let latency = LatencyProfile {
                base: task.latency,
                jitter: task.jitter,
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

                        tracing::info!(duration_ms, ?latency, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying connection");

                        match tokio::time::timeout(
                            Duration::from_millis(duration_ms),
                            proxy_with_latency(stream, upstream, latency, conn_seed),
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
