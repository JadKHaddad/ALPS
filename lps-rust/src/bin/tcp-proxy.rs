//!
//! ```not_rust
//! cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml
//! ```

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::Context;
use rand::RngExt;
use serde::Deserialize;

use clap::Parser;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "tcp-proxy-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090)
}

fn default_upstream_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5672)
}

fn default_failure_rate() -> f64 {
    0.1
}

fn deserialize_failure_rate<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let rate = f64::deserialize(deserializer)?;

    if !(0.0..=1.0).contains(&rate) {
        return Err(serde::de::Error::custom(format!(
            "failure rate must be between 0 and 1, got {rate}"
        )));
    }

    Ok(rate)
}

fn default_max_up_duration() -> Duration {
    Duration::from_secs(60)
}

#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    tasks: Vec<Task>,
}

#[derive(Debug, Deserialize)]
struct Task {
    #[serde(default = "default_socket_addr")]
    socket_addr: SocketAddr,
    #[serde(default = "default_upstream_addr")]
    upstream_addr: SocketAddr,
    /// Failure rate of accepting tcp connections, between 0 and 1
    #[serde(
        default = "default_failure_rate",
        deserialize_with = "deserialize_failure_rate"
    )]
    failure_rate: f64,
    /// Maximum duration of proxying tcp connections before a disconnection
    #[serde(default = "default_max_up_duration", with = "humantime_serde")]
    max_up_duration: Duration,
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

    tracing::info!(?config, "Starting");

    let tasks: Vec<_> = config
        .tasks
        .into_iter()
        .map(|task| {
            tokio::spawn(async move {
                let listener = TcpListener::bind(task.socket_addr)
                    .await
                    .context("Failed to bind to socket")?;

                loop {
                    let stream = listener.accept().await;

                    match stream {
                        Ok((mut stream, _)) => {
                            let fail = rand::rng().random_bool(task.failure_rate);

                            match fail {
                                true => {
                                    tracing::warn!("Connection failed");

                                    continue;
                                }
                                false => {
                                    tracing::info!("Connection accepted");

                                    tokio::spawn(async move {
                                        tracing::debug!("Connecting to upstream");

                                        match tokio::net::TcpStream::connect(task.upstream_addr).await {
                                            Ok(mut upstream) => {
                                                tracing::info!("Connected to upstream");

                                                let max_ms = task.max_up_duration.as_millis();

                                                let duration_ms = if max_ms == 0 {
                                                    0
                                                } else {
                                                    rand::rng().random_range(0..max_ms) as u64
                                                };

                                                tracing::info!(duration_ms, "Proxying connection");

                                                match tokio::time::timeout(
                                                    Duration::from_millis(duration_ms),
                                                    tokio::io::copy_bidirectional(&mut stream, &mut upstream),
                                                )
                                                .await
                                                {
                                                    Ok(_) => {
                                                        tracing::info!("Proxying completed");
                                                    }
                                                    Err(_) => {
                                                        tracing::warn!("Proxying timed out");
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                tracing::error!(%err, "Failed to connect to upstream");
                                            }
                                        }
                                    });
                                }
                            }
                        }

                        Err(err) => {
                            tracing::error!(%err, "Failed to accept connection");

                            continue;
                        }
                    }
                }

                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            })
        })
        .collect();

    for task in tasks {
        task.await??;
    }

    Ok(())
}
