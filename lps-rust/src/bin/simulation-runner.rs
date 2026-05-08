//! ```not_rust
//!
//! cargo run -r --bin simulation-runner -- --config-file simulation-config.yaml
//!
//! ```

use std::{path::PathBuf, process::Stdio, sync::Arc};

use anyhow::Context;
use clap::Parser;
use lps_rust::simulation::config::Config;
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "device-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
    #[clap(long, default_value = "false")]
    /// If set, the simulation runner will not create the simulation config files and will instead expect them to already exist in the output directory.
    no_create: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("simulation_runner=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let token = Arc::new(CancellationToken::new());
    let token_c = token.clone();

    tokio::spawn(async move {
        lps_rust::shutdown_signal().await;

        tracing::info!("Shutdown signal received, shutting down.");

        token_c.cancel();
    });

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .context("Failed to load config file")?;

    let simulation_dir = config.relative_simulation_dir();

    if !args.no_create {
        config
            .write_files()
            .await
            .context("Failed to write simulation config files")?;
    }

    select(&token, async {
        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("up")
            .arg("rabbitmq")
            .arg("-d")
            .current_dir(&simulation_dir)
            .status()
            .await
            .map(|_| ())
            .context("Failed to run docker compose up rabbitmq")
    })
    .await?;

    let mut docker_compose_logs = tokio::process::Command::new("docker")
        .arg("compose")
        .arg("logs")
        .arg("-f")
        .current_dir(&simulation_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to run docker compose logs")?;

    let logs = docker_compose_logs
        .stdout
        .take()
        .context("Failed to take docker compose logs stdout")?;

    tracing::info!("Waiting for RabbitMQ to be ready...");

    let mut reader = tokio::io::BufReader::new(logs).lines();

    select(&token, async {
        while let Ok(Some(line)) = reader.next_line().await {
            println!("{line}");

            if line.contains("Time to start RabbitMQ") {
                tracing::info!("RabbitMQ is ready, starting simulation");

                break;
            }
        }

        Ok(())
    })
    .await?;

    tokio::spawn(async move {
        while let Ok(Some(line)) = reader.next_line().await {
            println!("{line}");
        }
    });

    select(&token, async {
        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("up")
            .arg("-d")
            .current_dir(&simulation_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .map(|_| ())
            .context("Failed to run docker compose up")
    })
    .await?;

    tracing::info!("Monitoring server status...");

    select(&token, async {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let output = tokio::process::Command::new("docker")
                .arg("compose")
                .arg("ps")
                .arg("server")
                .arg("--format")
                .arg("json") // JSON is easier to parse reliably than plain text
                .current_dir(&simulation_dir)
                .output()
                .await
                .context("Failed to check server status")?;

            let stdout = String::from_utf8_lossy(&output.stdout);

            // If the output is empty, the container might have been removed or stopped
            if stdout.trim().is_empty() || !stdout.contains("running") {
                tracing::info!("Server is no longer running. Moving to shutdown.");
                break;
            }
        }
        Ok(())
    })
    .await?;

    tracing::info!("Shutting down simulation runner.");

    let _ = docker_compose_logs.kill().await;
    let _ = docker_compose_logs
        .wait()
        .await
        .context("Failed to wait for docker compose logs")?;

    tokio::process::Command::new("docker")
        .arg("compose")
        .arg("down")
        .current_dir(&simulation_dir)
        .status()
        .await
        .context("Failed to run docker compose down")?;

    select(&token, async {
        tracing::info!("Running validator");

        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("up")
            .arg("validator")
            .current_dir(&simulation_dir)
            .status()
            .await
            .map(|_| ())
            .context("Failed to run docker compose up for validator")
    })
    .await?;

    select(&token, async {
        tracing::info!("Running scripts");

        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("up")
            .arg("scripts")
            .current_dir(&simulation_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .map(|_| ())
            .context("Failed to run docker compose up for scripts")
    })
    .await?;

    Ok(())
}

async fn select<T, F: Future<Output = anyhow::Result<T>>>(
    token: &CancellationToken,
    f: F,
) -> anyhow::Result<Option<T>> {
    if token.is_cancelled() {
        return Ok(None);
    }

    tokio::select! {
        _ = token.cancelled() => Ok(None),
        res = f => res.map(Some),
    }
}
