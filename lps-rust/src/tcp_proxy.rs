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

fn default_upstream_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5672)
}

fn default_max_up_duration() -> Duration {
    Duration::from_secs(60)
}

fn default_results_file() -> PathBuf {
    PathBuf::from("tcp-proxy-results.json")
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub tasks: Vec<Task>,
    #[serde(default = "default_results_file")]
    pub results_file: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tasks: Vec::new(),
            results_file: default_results_file(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Task {
    #[serde(default = "default_socket_addr")]
    pub socket_addr: SocketAddr,
    #[serde(default = "default_upstream_addr")]
    pub upstream_addr: SocketAddr,
    /// Failure rate of accepting tcp connections, between 0 and 1
    #[serde(
        default = "crate::default_failure_rate",
        deserialize_with = "crate::deserialize_failure_rate"
    )]
    pub failure_rate: f64,
    /// Maximum duration of proxying tcp connections before a disconnection
    #[serde(default = "default_max_up_duration", with = "humantime_serde")]
    pub max_up_duration: Duration,
}

impl Default for Task {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            upstream_addr: default_upstream_addr(),
            failure_rate: crate::default_failure_rate(),
            max_up_duration: default_max_up_duration(),
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
