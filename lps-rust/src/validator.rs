use std::path::PathBuf;

use anyhow::Context;
use serde::{Deserialize, Serialize};

fn default_server_results_file() -> PathBuf {
    PathBuf::from("server-results.json")
}

fn default_device_results_file() -> PathBuf {
    PathBuf::from("device-results.json")
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_server_results_file")]
    pub server_results_file: PathBuf,
    #[serde(default = "default_device_results_file")]
    pub device_results_file: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_results_file: default_server_results_file(),
            device_results_file: default_device_results_file(),
        }
    }
}

impl Config {
    pub fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(&std::fs::read(path).context("Failed to read config file")?)
            .context("Failed to deserialize config file")
    }
}
