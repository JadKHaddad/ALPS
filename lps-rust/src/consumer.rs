use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use serde::Deserialize;

fn default_server_push_url() -> String {
    String::from("http://localhost:8080/work")
}

fn default_rabbit_mq_url() -> String {
    String::from("amqp://127.0.0.1:5672")
}

fn default_reconnect_duration() -> Duration {
    Duration::from_secs(5)
}

fn default_queues_count() -> u32 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_rabbit_mq_url")]
    pub rabbit_mq_url: String,
    #[serde(default = "default_server_push_url")]
    pub server_push_url: String,
    #[serde(default = "default_reconnect_duration", with = "humantime_serde")]
    pub reconnect_duration: Duration,
    /// The number of queues to consume from.
    #[serde(default = "default_queues_count")]
    pub queues_count: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rabbit_mq_url: default_rabbit_mq_url(),
            server_push_url: default_server_push_url(),
            reconnect_duration: default_reconnect_duration(),
            queues_count: default_queues_count(),
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
