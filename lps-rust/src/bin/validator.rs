//!
//! ```not_rust
//!
//! cargo run -r --bin validator -- --config-file validator-config.yaml
//!
//! ```

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use lps_rust::{Event, Rates, validator::Config};

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
    let unique: Vec<_> = unique.collect();
    let unique_count = unique.len();

    let duplicates: Vec<_> = duplicates.collect();

    std::fs::write(
        &config.duplicate_events_file,
        serde_json::to_string_pretty(&duplicates)
            .context("Failed to serialize duplicate events")?,
    )
    .context("Failed to write duplicate events file")?;

    if !duplicates.is_empty() {
        tracing::warn!(
            count = duplicates.len(),
            ?duplicates,
            "Found duplicated events"
        );
    }

    tracing::info!("Validating order");

    let mut order_violations = Vec::new();
    let order_result = Event::validate_order(unique.into_iter());

    if let Err(order_result) = &order_result {
        tracing::error!("Order validation failed: {:#}", order_result);

        order_violations = order_result.violations.clone();
    }

    std::fs::write(
        &config.out_of_order_events_file,
        serde_json::to_string_pretty(&order_violations)
            .context("Failed to serialize out-of-order events")?,
    )
    .context("Failed to write out-of-order events file")?;

    tracing::info!("Validating completeness");

    let completeness_result =
        Event::validate_completeness(device_events.iter(), server_events.iter());

    if let Err(completeness_result) = &completeness_result {
        tracing::error!("Completeness validation failed: {:#}", completeness_result);
    }

    let median_latency = Event::calculate_median_latency_ms(server_events.iter())
        .context("Failed to calculate median latency")?;

    let duplication_rate = duplicates.len() as f64 / server_events.len() as f64;
    let out_of_order_rate = order_violations.len() as f64 / unique_count as f64;
    let missing_rate = match completeness_result {
        Ok(_) => 0.0,
        Err(ref err) => err.missing.len() as f64 / device_events.len() as f64,
    };

    let results = Rates {
        median_latency_ms: median_latency,
        duplication_rate,
        out_of_order_rate,
        missing_rate,
    };

    std::fs::write(
        &config.rates_file,
        serde_json::to_string_pretty(&results).context("Failed to serialize validation results")?,
    )
    .context("Failed to write validation results file")?;

    completeness_result
        .context("Completeness Validation error")
        .and(order_result.context("Order Validation error"))
        .map_err(|_| anyhow::anyhow!("Validation failed. Check logs for details."))
}
