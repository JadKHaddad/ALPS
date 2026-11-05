//! The producer is our LPS1 proxy.
//!
//! It is a server that receives calls from the devices and maps them to events and publishes them to RabbitMQ.
//!
//! ```not_rust
//!
//! cargo run -r --bin producer -- --config-file producer-config.yaml
//!
//! ```

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::post};
use clap::Parser;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::*,
    types::{AMQPValue, FieldTable, ShortString},
};
use lps_rust::{Event, SimpleCall, producer::Config};
use reqwest::StatusCode;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long, default_value = "producer-config.yaml")]
    /// Config file: The path to the configuration file
    config_file: PathBuf,
}

#[derive(Debug)]
struct AppState {
    channel: Channel,
    use_features: bool,
}

impl AppState {
    fn new(channel: Channel, use_features: bool) -> Self {
        Self {
            channel,
            use_features,
        }
    }

    async fn publish(&self, call: SimpleCall) -> anyhow::Result<()> {
        tracing::info!(?call, "Received call");

        let event = Event::from(call);

        let payload = serde_json::to_vec(&event).context("Failed to serialize event")?;

        let (exchange, routing_key) = if self.use_features {
            (
                ShortString::from("events.hash"),
                ShortString::from(event.device_id),
            )
        } else {
            (ShortString::from(""), ShortString::from(""))
        };

        self.channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .context("Failed to publish event")?
            .await
            .context("Failed to confirm publish")
            .and_then(|confirm| {
                confirm
                    .is_ack()
                    .then_some(())
                    .ok_or(anyhow::Error::msg("Message was not acknowledged"))
            })?;

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

    let channel = tokio::select! {
        _ = lps_rust::shutdown_signal() => {
            anyhow::bail!("Shutdown signal received while connecting to RabbitMQ")
        },
        channel = async {
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

            if config.use_features {
                channel
                    .exchange_declare(
                        ShortString::from(String::from("events.hash")),
                        ExchangeKind::Custom(String::from("x-consistent-hash")),
                        ExchangeDeclareOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .context("Failed to declare exchange")?;
            }

            tracing::info!("Declaring queues and bindings");

            let queues = (0..config.queues_count).map(|i| format!("events-{i}"));

            for queue_name in queues {
                let field_table = if config.use_features {
                    FieldTable::from(BTreeMap::<_, _>::from([(
                        ShortString::from("x-single-active-consumer"),
                        AMQPValue::Boolean(true),
                    )]))
                } else {
                    FieldTable::default()
                };

                let queue = channel
                    .queue_declare(
                        ShortString::from(queue_name),
                        QueueDeclareOptions {
                            durable: true, // persist across broker restarts
                            ..Default::default()
                        },
                        field_table,
                    )
                    .await
                    .context("Failed to declare queue")?;

                if config.purge_queues_on_start {
                    tracing::warn!(queue = ?queue.name().as_str(), "Purging queue");

                    channel
                        .queue_purge(queue.name().clone(), QueuePurgeOptions::default())
                        .await
                        .context("Failed to purge queue")?;
                }

                let (exchange, routing_key) = if config.use_features {
                    (
                        ShortString::from("events.hash"),
                        ShortString::from("1")
                    )
                } else {
                    (ShortString::from(""), ShortString::from(""))
                };

                channel
                    .queue_bind(
                        queue.name().clone(),
                        exchange,
                        routing_key,
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .context("Failed to bind queue")?;

                channel.confirm_select(ConfirmSelectOptions { nowait: false })
                    .await
                    .context("Failed to enable publisher confirms")?;
            }

            anyhow::Ok(channel)
        } => channel
    }?;

    tracing::info!("Starting server");

    let app_state = Arc::new(AppState::new(channel, config.use_features));

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
