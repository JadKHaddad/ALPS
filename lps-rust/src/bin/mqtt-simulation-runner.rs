//! ```not_rust
//!
//! cargo run -r --bin mqtt-simulation-runner -- --config-file mqtt-simulation-config.yaml
//!
//! ```

use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use lps_rust::simulation::config::Config;
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;

/// Must match the default `MQTT_TOPIC` in `legacy_proxy_2.py`.
const MQTT_CONSUMER_SHARED_TOPIC: &str = "$share/legacy_proxy/+/message";

/// How long to wait for each consumer to subscribe before starting the next one anyway.
const MQTT_CONSUMER_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "mqtt-simulation-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
    #[clap(long)]
    /// Root absolute path: The absolute path to the root directory
    ///
    /// Important if you are running this file in a docker container to run the simulation.
    ///
    /// The value must be: --root-abs-path $(pwd)
    root_abs_path: Option<PathBuf>,
    #[clap(long, default_value = "false")]
    /// If set, the simulation runner will not create the simulation config files and will instead expect them to already exist in the output directory.
    no_create: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("mqtt_simulation_runner=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let token = Arc::new(CancellationToken::new());
    let token_c = token.clone();

    tokio::spawn(async move {
        lps_rust::shutdown_signal().await;

        tracing::info!("Shutdown signal received, shutting down.");

        token_c.cancel();
    });

    let args = Args::parse();

    let mut config = Config::load_from_file(&args.config_file)
        .await
        .context("Failed to load config file")?;

    if let Some(root_abs_path) = args.root_abs_path {
        config = config.with_root_abs_path(root_abs_path);
    }

    let simulation_dir = config.simulation_dir.clone();

    if !args.no_create {
        config
            .mqtt_write_files()
            .await
            .context("Failed to write simulation config files")?;
    }

    select(&token, async {
        tokio::process::Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg("lps-scripts")
            .arg("lps-validator")
            .status()
            .await
            .map(|_| ())
            .context("Failed to remove existing lps-scripts and lps-validator containers")
    })
    .await?;

    if config.server.deduplication {
        let config_file = config.first_deduplication_mounted_config_file();

        // docker compose --project-directory simulation run deduplication run --config-file /app/deduplication-config.yaml --bootstrap
        select(&token, async {
            tokio::process::Command::new("docker")
                .arg("compose")
                .arg("--project-directory")
                .arg(&simulation_dir)
                .arg("run")
                .arg("--rm")
                .arg("deduplication-0")
                .arg("run")
                .arg("--config-file")
                .arg(config_file)
                .arg("--bootstrap")
                .status()
                .await
                .map(|_| ())
                .context("Failed to run docker compose up deduplication")
        })
        .await?;

        select(&token, async {
            tokio::process::Command::new("docker")
                .arg("compose")
                .arg("--project-directory")
                .arg(&simulation_dir)
                .arg("up")
                .arg("deduplication-0")
                .arg("-d")
                .status()
                .await
                .map(|_| ())
                .context("Failed to run docker compose up deduplication")
        })
        .await?;
    }

    select(&token, async {
        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("--project-directory")
            .arg(&simulation_dir)
            .arg("up")
            .arg("hivemq")
            .arg("-d")
            .status()
            .await
            .map(|_| ())
            .context("Failed to run docker compose up hivemq")
    })
    .await?;

    let mut docker_compose_logs = tokio::process::Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(&simulation_dir)
        .arg("logs")
        .arg("-f")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to run docker compose logs")?;

    let logs = docker_compose_logs
        .stdout
        .take()
        .context("Failed to take docker compose logs stdout")?;

    tracing::info!("Waiting for broker to be ready...");

    if config.server.deduplication {
        tracing::info!("Waiting for deduplication to be ready...");
    }

    let mut reader = tokio::io::BufReader::new(logs).lines();

    select(&token, async {
        // deduplication is ready if we are not using it or when deduplication-0 is ready
        let mut deduplication_ready = !config.server.deduplication;
        let mut broker_ready = false;

        while let Ok(Some(line)) = reader.next_line().await {
            println!("{line}");

            if line.contains("Started HiveMQ") || line.contains("mosquitto version 2.1.2 running") {
                tracing::info!("Broker is ready");

                broker_ready = true;

                if deduplication_ready && broker_ready {
                    break;
                }
            }

            if line.contains("lps-deduplication-0")
                && (
                    // First run, no schema was created on disk
                    line.contains("Starting server") ||
                    // Subsequent runs, if the schema dir was not deleted
                    line.contains("Table discovered in schema")
                )
            {
                tracing::info!("Deduplication is ready");

                deduplication_ready = true;

                if deduplication_ready && broker_ready {
                    break;
                }
            }
        }

        Ok(())
    })
    .await?;

    tracing::info!("Starting simulation...");

    // Containers started by `up -d` print their first lines before an already running
    // `logs -f` attaches to them, so those lines would be lost. Instead, remember the
    // current time, stop the first log stream, start everything, and then follow the logs
    // again with `--since`, which replays every line from that moment on.
    let since = now_since()?;

    drop(reader);
    let _ = docker_compose_logs.kill().await;
    let _ = docker_compose_logs.wait().await;

    // Start everything except the consumers (and the manual-profile services)
    select(
        &token,
        compose_up_detached(&simulation_dir, &config.mqtt_pre_consumer_service_keys()),
    )
    .await?;

    if let Some(wait) = config.mqtt_consumers_wait_before_start() {
        tracing::info!("Waiting {wait:?} before starting consumers...");

        select(&token, async {
            tokio::time::sleep(wait).await;
            Ok(())
        })
        .await?;
    }

    tracing::info!("Starting consumers...");

    // The broker hands the first message of a shared subscription to whoever subscribed
    // first, so start the consumers one at a time and wait until each has subscribed.
    // That makes the message assignment the same on every run.
    for key in config.mqtt_consumer_service_keys() {
        let consumer_since = now_since()?;

        select(
            &token,
            compose_up_detached(&simulation_dir, std::slice::from_ref(&key)),
        )
        .await?;

        match select(
            &token,
            wait_for_subscription(
                &simulation_dir,
                &consumer_since,
                &key,
                MQTT_CONSUMER_SUBSCRIBE_TIMEOUT,
            ),
        )
        .await?
        {
            Some(true) => tracing::info!("{key} subscribed"),
            Some(false) => tracing::warn!(
                "{key} did not subscribe within {MQTT_CONSUMER_SUBSCRIBE_TIMEOUT:?}; starting the next consumer anyway"
            ),
            None => break, // cancelled
        }
    }

    let mut docker_compose_logs = tokio::process::Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(&simulation_dir)
        .arg("logs")
        .arg("-f")
        .arg("--since")
        .arg(&since)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to run docker compose logs")?;

    let logs = docker_compose_logs
        .stdout
        .take()
        .context("Failed to take docker compose logs stdout")?;

    let mut reader = tokio::io::BufReader::new(logs).lines();

    tokio::spawn(async move {
        while let Ok(Some(line)) = reader.next_line().await {
            println!("{line}");
        }
    });

    tracing::info!("Monitoring server status...");

    select(&token, async {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let output = tokio::process::Command::new("docker")
                .arg("compose")
                .arg("--project-directory")
                .arg(&simulation_dir)
                .arg("ps")
                .arg("server")
                .arg("--format")
                .arg("json")
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
        .arg("--project-directory")
        .arg(&simulation_dir)
        .arg("down")
        .arg("--remove-orphans")
        .status()
        .await
        .context("Failed to run docker compose down")?;

    select(&token, async {
        tracing::info!("Running validator");

        tokio::process::Command::new("docker")
            .arg("compose")
            .arg("--project-directory")
            .arg(&simulation_dir)
            .arg("up")
            .arg("validator")
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
            .arg("--project-directory")
            .arg(&simulation_dir)
            .arg("up")
            .arg("scripts")
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

/// `docker compose up -d [services...]`. An empty `services` slice starts every
/// (non-profiled) service.
async fn compose_up_detached(simulation_dir: &str, services: &[String]) -> anyhow::Result<()> {
    tokio::process::Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(simulation_dir)
        .arg("up")
        .arg("-d")
        .args(services)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .map(|_| ())
        .context("Failed to run docker compose up")
}

/// Current time in the format `docker compose logs --since` accepts.
fn now_since() -> anyhow::Result<String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("System time is before the Unix epoch")?;

    Ok(format!("{}.{:09}", now.as_secs(), now.subsec_nanos()))
}

/// Follows `service`'s logs from `since` until legacy_proxy_2.py logs a granted SUBACK
/// for its shared subscription. Works with both brokers, because it reads the consumer's
/// own log (streamed to stdout by the `tail -F` in its compose command).
///
/// Returns `false` on timeout or if the broker rejected the subscription.
async fn wait_for_subscription(
    simulation_dir: &str,
    since: &str,
    service: &str,
    timeout: Duration,
) -> anyhow::Result<bool> {
    let mut child = tokio::process::Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(simulation_dir)
        .arg("logs")
        .arg("-f")
        .arg("--no-log-prefix")
        .arg("--since")
        .arg(since)
        .arg(service)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("Failed to follow {service} logs"))?;

    let stdout = child
        .stdout
        .take()
        .context("Failed to take consumer logs stdout")?;

    let mut lines = tokio::io::BufReader::new(stdout).lines();

    // on_connect logs: "Subscribed to topic: <topic> with QoS 2, result: ..., mid: <mid>"
    // on_subscribe logs: "SUBACK received for mid <mid>, reason_codes: ['Granted QoS 2']"
    let subscribed = format!("Subscribed to topic: {MQTT_CONSUMER_SHARED_TOPIC} ");

    let granted = tokio::time::timeout(timeout, async {
        let mut suback: Option<String> = None;

        while let Some(line) = lines.next_line().await? {
            match &suback {
                None if line.contains(&subscribed) => {
                    let mid = line.rsplit("mid: ").next().unwrap_or_default().trim();
                    suback = Some(format!("SUBACK received for mid {mid},"));
                }
                Some(pattern) if line.contains(pattern.as_str()) => {
                    return Ok::<_, anyhow::Error>(line.contains("Granted"));
                }
                _ => {}
            }
        }

        Ok(false)
    })
    .await
    .unwrap_or(Ok(false))?;

    let _ = child.kill().await;
    let _ = child.wait().await;

    Ok(granted)
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
