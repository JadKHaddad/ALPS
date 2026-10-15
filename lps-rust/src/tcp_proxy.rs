use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090)
}

fn default_upstream_domain() -> String {
    String::from("localhost")
}

fn default_upstream_port() -> u16 {
    5672
}

fn default_max_up_duration() -> Duration {
    Duration::from_secs(60)
}

fn default_results_file() -> PathBuf {
    PathBuf::from("tcp-proxy-results.json")
}

pub fn default_chunk_size() -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(16 * 1024).expect("Valid NonZeroUsize")
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub tasks: Vec<Task>,
    #[serde(default = "default_results_file")]
    pub results_file: PathBuf,
    /// Seed for the random number generators. If omitted, a random seed is chosen and logged.
    #[serde(default)]
    pub seed: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tasks: Vec::new(),
            results_file: default_results_file(),
            seed: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Task {
    #[serde(default = "default_socket_addr")]
    pub socket_addr: SocketAddr,
    #[serde(default = "default_upstream_domain")]
    pub upstream_domain: String,
    #[serde(default = "default_upstream_port")]
    pub upstream_port: u16,
    #[serde(default)]
    pub connection: Connection,
    /// A per connection config.
    ///
    /// Each connection reads its config values according to its index and applies it.
    ///
    /// If the index is not found the connection uses the [`latency`](Self::connection) values.
    #[serde(default)]
    pub connections: Vec<Connection>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Connection {
    /// Failure rate of accepting tcp connections, between 0 and 1
    #[serde(
        default = "crate::default_failure_rate",
        deserialize_with = "crate::deserialize_failure_rate"
    )]
    pub failure_rate: f64,
    /// Maximum duration of proxying tcp connections before a disconnection
    #[serde(default = "default_max_up_duration", with = "humantime_serde")]
    pub max_up_duration: Duration,
    /// The minimum duration of proxying tcp connections before a disconnection. The actual interval will be a random value between `min_up_duration` and `max_up_duration.
    ///
    /// If not set, the minimum duration will be `0`.
    ///
    /// If set and greater than `max_up_duration`, the minimum duration will be `0`.
    #[serde(default, with = "humantime_serde")]
    pub min_up_duration: Option<Duration>,
    #[serde(default)]
    pub latency: Latency,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            failure_rate: crate::default_failure_rate(),
            max_up_duration: default_max_up_duration(),
            min_up_duration: None,
            latency: Latency::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Latency {
    /// Maximum latency per byte for client -> server traffic.
    #[serde(default, with = "humantime_serde")]
    pub max_upstream_latency: Duration,
    /// Minimum latency per byte for client -> server traffic. Each read waits
    /// `bytes * random(min_upstream_latency..=max_upstream_latency)`.
    ///
    /// If not set, or greater than `max_upstream_latency`, the minimum is `0`.
    #[serde(default, with = "humantime_serde")]
    pub min_upstream_latency: Option<Duration>,
    /// Maximum latency per byte for server -> client traffic.
    #[serde(default, with = "humantime_serde")]
    pub max_downstream_latency: Duration,
    /// Minimum latency per byte for server -> client traffic. Each read waits
    /// `bytes * random(min_downstream_latency..=max_downstream_latency)`.
    ///
    /// If not set, or greater than `max_downstream_latency`, the minimum is `0`.
    #[serde(default, with = "humantime_serde")]
    pub min_downstream_latency: Option<Duration>,
}

impl Default for Latency {
    fn default() -> Self {
        Self {
            max_upstream_latency: Duration::ZERO,
            min_upstream_latency: None,
            max_downstream_latency: Duration::ZERO,
            min_downstream_latency: None,
        }
    }
}

impl Default for Task {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            upstream_domain: default_upstream_domain(),
            upstream_port: default_upstream_port(),
            connection: Connection::default(),
            connections: Vec::new(),
        }
    }
}

impl Config {
    pub async fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(
            &tokio::fs::read(path)
                .await
                .context("Failed to read config file")?,
        )
        .context("Failed to deserialize config file")
    }
}
