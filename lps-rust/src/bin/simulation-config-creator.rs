//! ```not_rust
//!
//! cargo run -r --bin simulation-config-creator -- --config-file simulation-config.yaml
//!
//! ```

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use lps_rust::simulation::config::Config;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "device-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .context("Failed to load config file")?;

    config
        .write_files()
        .await
        .context("Failed to write simulation config files")?;

    Ok(())
}
