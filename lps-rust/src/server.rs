use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

fn default_max_work_duration() -> Duration {
    Duration::from_secs(1)
}

fn default_max_response_duration() -> Duration {
    Duration::from_secs(1)
}

fn default_results_file() -> PathBuf {
    PathBuf::from("server-results.json")
}

fn default_failures_file() -> PathBuf {
    PathBuf::from("server-failures.json")
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_socket_addr")]
    pub socket_addr: SocketAddr,
    /// Failure rate of the server, between 0 and 1
    #[serde(
        default = "crate::default_failure_rate",
        deserialize_with = "crate::deserialize_failure_rate"
    )]
    pub failure_rate: f64,
    #[serde(default = "default_max_work_duration", with = "humantime_serde")]
    pub max_work_duration: Duration,
    #[serde(default, with = "humantime_serde")]
    pub min_work_duration: Option<Duration>,
    #[serde(default = "default_max_response_duration", with = "humantime_serde")]
    pub max_response_duration: Duration,
    #[serde(default, with = "humantime_serde")]
    pub min_response_duration: Option<Duration>,
    #[serde(default = "default_results_file")]
    pub results_file: PathBuf,
    #[serde(default = "default_failures_file")]
    pub failures_file: PathBuf,
    #[serde(default, with = "humantime_serde")]
    pub shutdown_after_idle_duration: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            failure_rate: crate::default_failure_rate(),
            max_work_duration: default_max_work_duration(),
            min_work_duration: None,
            max_response_duration: default_max_response_duration(),
            min_response_duration: None,
            results_file: default_results_file(),
            failures_file: default_failures_file(),
            shutdown_after_idle_duration: None,
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
