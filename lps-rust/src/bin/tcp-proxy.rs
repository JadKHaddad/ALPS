//!
//! ```not_rust
//! cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml
//! ```

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use lps_rust::{TcpConnectionDrop, shutdown_signal, tcp_proxy::Config};
use rand::{RngExt, SeedableRng, rngs::StdRng};

use clap::Parser;
use tokio::{net::TcpListener, sync::Mutex};

const SEED_SALT: u64 = 0x9A0C_0000_0000_0000;

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
                >= task.max_up_duration.as_millis()
            {
                tracing::warn!(
                    "min_up_duration is greater than or equal to max_up_duration, setting min_up_duration to None"
                );

                task.min_up_duration = None;
            };

            task
        })
        .enumerate()
        .map(|(index, task)| {
            let state = state.clone();

            let mut rng = StdRng::seed_from_u64(base_seed ^ SEED_SALT ^ index as u64);

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

                // max_ms is always > min_ms. See check above.
                let max_ms = task.max_up_duration.as_millis();

                loop {
                    let (mut stream, _) = match listener.accept().await {
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
                        rng.random_range(min_ms..max_ms) as u64
                    };

                    if fail {
                        tracing::warn!("Connection failed");

                        state.push(TcpConnectionDrop::new_now()).await;

                        continue;
                    }

                    tracing::info!(addr=%task.socket_addr, "Connection accepted");

                    let state = state.clone();

                    tokio::spawn(async move {
                        tracing::debug!(addr=%task.socket_addr, upstream=%upstream_addr, "Connecting to upstream");

                        let mut upstream = match tokio::net::TcpStream::connect(upstream_addr).await {
                            Ok(upstream) => upstream,
                            Err(err) => {
                                tracing::error!(addr=%task.socket_addr, upstream=%upstream_addr, %err, "Failed to connect to upstream");

                                return;
                            }
                        };

                        tracing::info!(duration_ms, addr=%task.socket_addr, upstream=%upstream_addr, "Proxying connection");

                        match tokio::time::timeout(
                            Duration::from_millis(duration_ms),
                            tokio::io::copy_bidirectional(&mut stream, &mut upstream),
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
