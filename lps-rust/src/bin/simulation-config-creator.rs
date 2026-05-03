//! ```not_rust
//!
//! cargo run -r --bin simulation-config-creator -- --config-file simulation-config.yaml --out-dir ./simulation-configs
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
    #[clap(long, default_value = "./simulation-configs")]
    /// Output directory: The directory to save the generated simulation config files
    out_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .context("Failed to load config file")?;

    tokio::fs::create_dir_all(&args.out_dir)
        .await
        .context("Failed to create output directory")?;

    let config_files = config.to_config_files();

    for config_file in config_files {
        let path = args.out_dir.join(&config_file.name);

        let data =
            serde_yaml::to_string(&config_file.ty).context("Failed to serialize config file")?;

        tokio::fs::write(path, data)
            .await
            .context("Failed to write config file")?;
    }

    Ok(())
}
