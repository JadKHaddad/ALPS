//! The producer is our LPS1 proxy.
//!
//! It is a server that receives calls from the devices and maps them to events and publishes them to RabbitMQ.
//!
//! ```not_rust
//!
//! cargo run -r --bin producer
//!
//! ```

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::post};
use clap::Parser;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::*,
    types::{AMQPValue, FieldTable, ShortString},
};
use lps_rust::{Event, SimpleCall};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "producer-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_socket_addr")]
    socket_addr: SocketAddr,
    #[serde(default = "default_rabbit_mq_url")]
    rabbit_mq_url: String,
    /// The number of queues to declare and bind to the exchange.
    #[serde(default = "default_queues_count")]
    queues_count: u32,
    /// Whether to purge the queues on start.
    #[serde(default)]
    purge_queues_on_start: bool,
}

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

fn default_rabbit_mq_url() -> String {
    String::from("amqp://127.0.0.1:5672")
}

fn default_queues_count() -> u32 {
    10
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_addr: default_socket_addr(),
            rabbit_mq_url: default_rabbit_mq_url(),
            queues_count: default_queues_count(),
            purge_queues_on_start: false,
        }
    }
}

impl Config {
    async fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(
            &tokio::fs::read(path)
                .await
                .context("Failed to read config file")?,
        )
        .context("Failed to deserialize config file")
    }
}

#[derive(Debug)]
struct AppState {
    channel: Channel,
}

impl AppState {
    fn new(channel: Channel) -> Self {
        Self { channel }
    }

    async fn publish(&self, call: SimpleCall) -> anyhow::Result<()> {
        tracing::info!(?call, "Received call");

        let event = Event::from(call);

        let payload = serde_json::to_vec(&event).context("Failed to serialize event")?;

        self.channel
            .basic_publish(
                ShortString::from("events.hash"),
                ShortString::from(event.device_id), // routing key = event_id
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .context("Failed to publish event")?
            .await
            .context("Failed to confirm publish")?;

        Ok(())
    }

    async fn publish_mapped(&self, call: SimpleCall) -> Result<StatusCode, StatusCode> {
        self.publish(call)
            .await
            .map(|_| StatusCode::OK)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("producer=info"))?;

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let config = Config::load_from_file(&args.config_file)
        .await
        .unwrap_or_else(|err| {
            tracing::error!("Failed to load config: {}", err);
            tracing::warn!("Using default configuration");

            Config::default()
        });

    tracing::info!(?config, "Starting");

    tracing::info!("Connecting to RabbitMQ");

    let conn = Connection::connect(
        &config.rabbit_mq_url,
        ConnectionProperties::default().enable_auto_recover(),
    )
    .await
    .context("Failed to connect to RabbitMQ")?;

    let channel = conn
        .create_channel()
        .await
        .context("Failed to create channel")?;

    channel
        .exchange_declare(
            ShortString::from(String::from("events.hash")),
            ExchangeKind::Custom(String::from("x-consistent-hash")),
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("Failed to declare exchange")?;

    tracing::info!("Declaring queues and bindings");

    let queues = (0..config.queues_count).map(|i| format!("events-{i}"));

    for queue_name in queues {
        let queue = channel
            .queue_declare(
                ShortString::from(queue_name),
                QueueDeclareOptions {
                    durable: true, // persist across broker restarts
                    ..Default::default()
                },
                FieldTable::from(BTreeMap::<_, _>::from([(
                    ShortString::from("x-single-active-consumer"),
                    AMQPValue::Boolean(true),
                )])),
            )
            .await
            .context("Failed to declare queue")?;

        if config.purge_queues_on_start {
            tracing::warn!(queue = ?queue.name(), "Purging queue");

            channel
                .queue_purge(queue.name().clone(), QueuePurgeOptions::default())
                .await
                .context("Failed to purge queue")?;
        }

        channel
            .queue_bind(
                queue.name().clone(),
                ShortString::from("events.hash"),
                ShortString::from("1"), // weight = 1
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("Failed to bind queue")?;
    }

    tracing::info!("Starting server");

    let app_state = Arc::new(AppState::new(channel));

    let app = Router::new()
        .route(
            "/work",
            post(
                |State(state): State<Arc<AppState>>, Json(call): Json<SimpleCall>| async move {
                    state.publish_mapped(call).await
                },
            ),
        )
        .with_state(app_state);

    let listener = TcpListener::bind(config.socket_addr)
        .await
        .context("Failed to bind to socket")?;

    axum::serve(listener, app)
        .with_graceful_shutdown(lps_rust::shutdown_signal())
        .await
        .context("Server failed")?;

    Ok(())
}
