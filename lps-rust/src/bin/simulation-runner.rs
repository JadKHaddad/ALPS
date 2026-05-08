//! ```not_rust
//!
//! cargo run -r --bin simulation-runner -- --config-file simulation-config.yaml --out-dir ./simulation-configs
//!
//! ```

use std::{path::PathBuf, process::Stdio, sync::Arc};

use anyhow::Context;
use clap::Parser;
use lps_rust::simulation::config::Config;
use reqwest::StatusCode;
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;

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

    config
        .write_files(&args.out_dir)
        .await
        .context("Failed to write simulation config files")?;

    let device_exposed_host_port = config
        .device_docker_exposed_socket_addr_and_container_port()
        .map(|(socket_addr, _)| socket_addr.port());

    let device_addr = device_exposed_host_port.map(|port| format!("127.0.0.1:{}", port));

    let server_exposed_host_port = config
        .server_docker_exposed_socket_addr_and_container_port()
        .0
        .port();

    let server_addr = format!("127.0.0.1:{}", server_exposed_host_port);

    select(&token, async {
        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("up")
            .arg("-d")
            .current_dir(&args.out_dir)
            .status()
            .await
            .map(|_| ())
            .context("Failed to run docker compose up")
    })
    .await?;

    let mut docker_compose_logs = tokio::process::Command::new("docker")
        .arg("compose")
        .arg("logs")
        .arg("-f")
        .current_dir(&args.out_dir)
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

    if let Some(addr) = device_addr {
        select(&token, async {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            tracing::info!("Starting devices");

            tokio::net::TcpStream::connect(addr)
                .await
                .map(|_| ())
                .context("Failed to connect to device socket address")
        })
        .await?;
    }

    let client = reqwest::Client::new();

    select(&token, async {
        loop {
            let res = client
                .get(format!("http://{server_addr}/health"))
                .send()
                .await;

            match res {
                Ok(response) if response.status() == StatusCode::OK => {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                _ => {
                    tracing::info!(?res, "Server is no longer reachable. Simulation complete.");

                    break;
                }
            }
        }

        Ok(())
    })
    .await?;

    tracing::info!("Shutting down simulation runner.");

    let _ = docker_compose_logs.kill().await;

    tokio::process::Command::new("docker")
        .arg("compose")
        .arg("down")
        .current_dir(&args.out_dir)
        .status()
        .await
        .context("Failed to run docker compose down")?;

    let _ = docker_compose_logs
        .wait()
        .await
        .context("Failed to wait for docker compose logs")?;

    select(&token, async {
        tracing::info!("Running validator");

        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("up")
            .arg("validator")
            .current_dir(&args.out_dir)
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
            .current_dir(&args.out_dir)
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

async fn select<F: Future<Output = anyhow::Result<()>>>(
    token: &CancellationToken,
    f: F,
) -> anyhow::Result<()> {
    if token.is_cancelled() {
        return Ok(());
    }

    tokio::select! {
        _ = token.cancelled() => Ok(()),
        res = f => res,
    }
}
