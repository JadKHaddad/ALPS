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
    #[clap(long, default_value = "simulation-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
    #[clap(long)]
    /// Root absolute path: The absolute path to the root directory
    ///
    /// Important if you are running this file in a docker container to run the simulation.
    ///
    /// The value must be: --root-abs-path $(pwd)
    root_abs_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let mut config = Config::load_from_file(&args.config_file)
        .await
        .context("Failed to load config file")?;

    if let Some(root_abs_path) = args.root_abs_path {
        config = config.with_root_abs_path(root_abs_path);
    }

    config
        .amqp_write_files()
        .await
        .context("Failed to write simulation config files")?;

    Ok(())
}
