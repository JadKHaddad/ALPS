//!
//! ```not_rust
//!
//! cargo run -r --bin validator
//!
//! ```

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use lps_rust::Event;
use serde::Deserialize;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "validator-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

fn default_results_file() -> PathBuf {
    PathBuf::from("server-results.json")
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_results_file")]
    results_file: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            results_file: default_results_file(),
        }
    }
}

impl Config {
    fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(&std::fs::read(path).context("Failed to read config file")?)
            .context("Failed to deserialize config file")
    }
}

fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("validator=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file).unwrap_or_else(|err| {
        tracing::error!("Failed to load config: {}", err);
        tracing::warn!("Using default configuration");

        Config::default()
    });

    std::fs::read(&config.results_file)
        .context("Failed to open results file")
        .and_then(|bytes| {
            serde_json::from_slice::<Vec<Event>>(&bytes).context("Failed to deserialize events")
        })
        .and_then(|events| {
            let (unique, duplicates) = Event::remove_duplication(events.iter());

            let duplicates: Vec<_> = duplicates.collect();

            if !duplicates.is_empty() {
                tracing::warn!(
                    count = duplicates.len(),
                    ?duplicates,
                    "Found duplicated events"
                );
            }

            Event::validate_order(unique).context("Validation error")
        })
        .inspect(|_| tracing::info!("Validation succeeded"))
}
