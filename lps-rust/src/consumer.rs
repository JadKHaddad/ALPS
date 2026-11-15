use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::Event;

fn default_server_push_url() -> String {
    String::from("http://localhost:8080/work")
}

fn default_rabbit_mq_url() -> String {
    String::from("amqp://127.0.0.1:5672")
}

fn default_reconnect_duration() -> Duration {
    Duration::from_secs(5)
}

fn default_queues_count() -> u32 {
    10
}

fn default_results_file() -> PathBuf {
    PathBuf::from("consumer-results.json")
}

pub fn default_prefetch_count() -> u16 {
    1
}

pub fn default_http_timeout() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_rabbit_mq_url")]
    pub rabbit_mq_url: String,
    #[serde(default = "default_server_push_url")]
    pub server_push_url: String,
    #[serde(default = "default_reconnect_duration", with = "humantime_serde")]
    pub reconnect_duration: Duration,
    /// The number of queues to consume from.
    #[serde(default = "default_queues_count")]
    pub queues_count: u32,
    #[serde(default = "default_results_file")]
    pub results_file: PathBuf,
    /// If set, work requests are sent in the ars-comp-2 simple call format: `{ "id": ..., "body": ... }`
    #[serde(default)]
    pub use_simple_call_format: bool,
    /// `0` for unlimited. Default `1`
    #[serde(default = "default_prefetch_count")]
    pub prefetch_count: u16,
    #[serde(default = "default_http_timeout", with = "humantime_serde")]
    pub http_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rabbit_mq_url: default_rabbit_mq_url(),
            server_push_url: default_server_push_url(),
            reconnect_duration: default_reconnect_duration(),
            queues_count: default_queues_count(),
            results_file: default_results_file(),
            use_simple_call_format: false,
            prefetch_count: default_prefetch_count(),
            http_timeout: default_http_timeout(),
        }
    }
}

impl Config {
    pub async fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(
            &tokio::fs::read(path)
                .await
                .context("Failed to read config file")?,
        )
        .context("Failed to deserialize config file")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum JobStatus {
    /// The Consumer Consumer has received the job and sent it to the server and is waiting for a response.
    ReceivedAndSentAndWaitingForResponse,
    /// The Consumer has received a positive response from the server and not yet acknowledged it.
    SuccessBeforeAck,
    /// The Consumer has received a positive response from the server and has acknowledged it.
    SuccessAfterAck,
    /// The Consumer has received a negative response from the server and has not yet n-acknowledged it.
    FailBeforeNack,
    /// The Consumer has received a negative response from the server and has n-acknowledged it.
    FailAfterNack,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Job {
    pub status: JobStatus,
    pub event: Event,
    pub happened_at: DateTime<Utc>,
    pub queue: String,
}

impl Job {
    pub fn new_received_and_sent_and_waiting_for_response_now(event: Event, queue: String) -> Self {
        Self {
            status: JobStatus::ReceivedAndSentAndWaitingForResponse,
            event,
            happened_at: Utc::now(),
            queue,
        }
    }

    pub fn success_before_ack_now(mut self) -> Self {
        self.status = JobStatus::SuccessBeforeAck;
        self.happened_at = Utc::now();
        self
    }

    pub fn success_after_ack_now(mut self) -> Self {
        self.status = JobStatus::SuccessAfterAck;
        self.happened_at = Utc::now();
        self
    }

    pub fn fail_before_nack_now(mut self) -> Self {
        self.status = JobStatus::FailBeforeNack;
        self.happened_at = Utc::now();
        self
    }

    pub fn fail_after_nack_now(mut self) -> Self {
        self.status = JobStatus::FailAfterNack;
        self.happened_at = Utc::now();
        self
    }
}
