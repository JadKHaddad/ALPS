use std::path::PathBuf;

use anyhow::Context;
use serde::{Deserialize, Serialize};

fn default_server_results_file() -> PathBuf {
    PathBuf::from("server-results.json")
}

fn default_device_results_file() -> PathBuf {
    PathBuf::from("device-results.json")
}

fn default_duplicate_events_file() -> PathBuf {
    PathBuf::from("duplicate-events.json")
}

fn default_out_of_order_events_file() -> PathBuf {
    PathBuf::from("out-of-order-events.json")
}

fn default_rates_file() -> PathBuf {
    PathBuf::from("rates-results.json")
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_server_results_file")]
    pub server_results_file: PathBuf,
    #[serde(default = "default_device_results_file")]
    pub device_results_file: PathBuf,
    #[serde(default = "default_duplicate_events_file")]
    pub duplicate_events_file: PathBuf,
    #[serde(default = "default_out_of_order_events_file")]
    pub out_of_order_events_file: PathBuf,
    #[serde(default = "default_rates_file")]
    pub rates_file: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_results_file: default_server_results_file(),
            device_results_file: default_device_results_file(),
            duplicate_events_file: default_duplicate_events_file(),
            out_of_order_events_file: default_out_of_order_events_file(),
            rates_file: default_rates_file(),
        }
    }
}

impl Config {
    pub fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(&std::fs::read(path).context("Failed to read config file")?)
            .context("Failed to deserialize config file")
    }
}
