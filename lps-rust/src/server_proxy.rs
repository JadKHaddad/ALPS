use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_socket_addr")]
    pub socket_addr: SocketAddr,
    #[serde(default)]
    pub server_push_urls: Vec<String>,
    /// Seed for the random number generators. If omitted, a random seed is chosen and logged.
    #[serde(default)]
    pub seed: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            server_push_urls: Vec::new(),
            seed: None,
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
