//!
//! ```not_rust
//!
//! cargo run -r --bin validator -- --config-file validator-config.yaml
//!
//! ```

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use lps_rust::{Event, validator::Config};

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "validator-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
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

    tracing::info!(?config, "Starting");

    let server_events = std::fs::read(&config.server_results_file)
        .context("Failed to open server results file")
        .and_then(|bytes| {
            serde_json::from_slice::<Vec<Event>>(&bytes)
                .context("Failed to deserialize server events")
        })
        .context("Failed to load server events")?;

    let device_events = std::fs::read(&config.device_results_file)
        .context("Failed to open device results file")
        .and_then(|bytes| {
            serde_json::from_slice::<Vec<Event>>(&bytes)
                .context("Failed to deserialize device events")
        })
        .context("Failed to load device events")?;

    tracing::info!(
        server_count = server_events.len(),
        device_count = device_events.len(),
        "Loaded events"
    );

    tracing::info!("Checking for duplicates");

    let (unique, duplicates) = Event::remove_duplication(server_events.iter());

    let duplicates: Vec<_> = duplicates.collect();

    if !duplicates.is_empty() {
        tracing::warn!(
            count = duplicates.len(),
            ?duplicates,
            "Found duplicated events"
        );
    }

    tracing::info!("Validating order");

    let order_result = Event::validate_order(unique).context("Order Validation error");

    if let Err(order_result) = &order_result {
        tracing::error!("Order validation failed: {:#}", order_result);
    }

    tracing::info!("Validating completeness");

    let completeness_result =
        Event::validate_completeness(device_events.iter(), server_events.iter())
            .context("Completeness Validation error");

    if let Err(completeness_result) = &completeness_result {
        tracing::error!("Completeness validation failed: {:#}", completeness_result);
    }

    completeness_result
        .and(order_result)
        .map_err(|_| anyhow::anyhow!("Validation failed. Check logs for details."))
}
