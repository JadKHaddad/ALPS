use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_socket_addr")]
    pub socket_addr: SocketAddr,
    #[serde(default = "default_rabbit_mq_url")]
    pub rabbit_mq_url: String,
    /// The number of queues to declare and bind to the exchange.
    #[serde(default = "default_queues_count")]
    pub queues_count: u32,
    /// Whether to purge the queues on start.
    #[serde(default)]
    pub purge_queues_on_start: bool,
}

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

fn default_rabbit_mq_url() -> String {
    String::from("amqp://127.0.0.1:5672")
}

fn default_queues_count() -> u32 {
    10
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            rabbit_mq_url: default_rabbit_mq_url(),
            queues_count: default_queues_count(),
            purge_queues_on_start: false,
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
