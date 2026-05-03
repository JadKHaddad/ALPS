use std::time::Duration;
use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    consumer::Config as CrateConsumerConfig,
    device::Config as CrateDeviceConfig,
    producer::Config as CrateProducerConfig,
    server::Config as CrateServerConfig,
    tcp_proxy::{Config as CrateTcpProxyConfig, Task as CrateTcpProxyTask},
};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub devices: DeviceConfig,
    pub server: ServerConfig,
    pub amqp: AmqpConfig,
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

    pub fn to_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        let server_config_file = ConfigFile {
            name: "server-config.yaml".to_string(),
            ty: ConfigType::Server(CrateServerConfig {
                failure_rate: self.server.failure_rate,
                max_work_duration: self.server.max_work_duration,
                max_response_duration: self.server.max_response_duration,
                socket_addr: self.server.socket_addr,
                ..Default::default()
            }),
        };

        config_files.push(server_config_file);

        let producer_amqp_proxy_config =
            self.amqp
                .producers
                .amqp_proxy
                .as_ref()
                .map(|proxy| CrateTcpProxyConfig {
                    tasks: vec![CrateTcpProxyTask {
                        failure_rate: proxy.failure_rate,
                        max_up_duration: proxy.max_up_duration,
                        socket_addr: proxy.socket_addr,
                        ..Default::default() // TODO: we need to make the upstream addr an upstream domain instead
                    }],
                    results_file: PathBuf::from("producer-amqp-proxy-results.json"),
                });

        let producer_amqp_proxy_config_file = producer_amqp_proxy_config.map(|config| ConfigFile {
            name: "producer-amqp-proxy-config.yaml".to_string(),
            ty: ConfigType::TcpProxy(config),
        });

        if let Some(config) = producer_amqp_proxy_config_file {
            config_files.push(config);
        }

        for i in 0..self.amqp.producers_count() {
            let broker_domain = self
                .amqp
                .producers
                .amqp_proxy
                .as_ref()
                .map(|_| String::from("producer-amqp-proxy"))
                .unwrap_or(self.amqp.broker_domain.clone());

            let producer_config_file = ConfigFile {
                name: format!("producer-{}.yaml", i),
                ty: ConfigType::Producer(CrateProducerConfig {
                    queues_count: self.amqp.queues_count,
                    purge_queues_on_start: true,
                    rabbit_mq_url: format!("amqp://{}:5672", broker_domain),
                    socket_addr: self.amqp.producers.socket_addr,
                }),
            };

            config_files.push(producer_config_file);
        }

        // ------

        let consumer_amqp_proxy_config =
            self.amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|proxy| CrateTcpProxyConfig {
                    tasks: vec![CrateTcpProxyTask {
                        failure_rate: proxy.failure_rate,
                        max_up_duration: proxy.max_up_duration,
                        socket_addr: proxy.socket_addr,
                        ..Default::default() // TODO: we need to make the upstream addr an upstream domain instead
                    }],
                    results_file: PathBuf::from("consumer-amqp-proxy-results.json"),
                });

        let consumer_amqp_proxy_config_file = consumer_amqp_proxy_config.map(|config| ConfigFile {
            name: "consumer-amqp-proxy-config.yaml".to_string(),
            ty: ConfigType::TcpProxy(config),
        });

        if let Some(config) = consumer_amqp_proxy_config_file {
            config_files.push(config);
        }

        // ------

        let consumer_server_proxy_config =
            self.amqp
                .consumers
                .server_proxy
                .as_ref()
                .map(|proxy| CrateTcpProxyConfig {
                    tasks: vec![CrateTcpProxyTask {
                        failure_rate: proxy.failure_rate,
                        max_up_duration: proxy.max_up_duration,
                        socket_addr: proxy.socket_addr,
                        ..Default::default() // TODO: we need to make the upstream addr an upstream domain instead
                    }],
                    results_file: PathBuf::from("consumer-server-proxy-results.json"),
                });

        let consumer_server_proxy_config_file =
            consumer_server_proxy_config.map(|config| ConfigFile {
                name: "consumer-server-proxy-config.yaml".to_string(),
                ty: ConfigType::TcpProxy(config),
            });

        if let Some(config) = consumer_server_proxy_config_file {
            config_files.push(config);
        }

        for i in 0..self.amqp.consumers_count() {
            let broker_domain = self
                .amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|_| String::from("consumer-amqp-proxy"))
                .unwrap_or(self.amqp.broker_domain.clone());

            let server_domain = self
                .amqp
                .consumers
                .server_proxy
                .as_ref()
                .map(|_| String::from("consumer-server-proxy"))
                .unwrap_or(self.server.server_domain.clone());

            let consumer_config_file = ConfigFile {
                name: format!("consumer-{}.yaml", i),
                ty: ConfigType::Consumer(CrateConsumerConfig {
                    queues_count: self.amqp.queues_count,
                    reconnect_duration: self.amqp.consumers.reconnect_duration,
                    rabbit_mq_url: format!("amqp://{}:5672", broker_domain),
                    server_push_url: format!("http://{}:8080/work", server_domain),
                }),
            };

            config_files.push(consumer_config_file);
        }

        config_files
    }
}

#[derive(Debug, Deserialize)]
pub struct DeviceConfig {
    pub devices_count: u32,
    #[serde(with = "humantime_serde")]
    pub max_event_interval: Duration,
    pub max_events: Option<u64>,
    #[serde(with = "humantime_serde")]
    pub retry_duration: Duration,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub server_domain: String,
    pub socket_addr: SocketAddr,
    #[serde(deserialize_with = "crate::deserialize_failure_rate")]
    pub failure_rate: f64,
    #[serde(with = "humantime_serde")]
    pub max_work_duration: Duration,
    #[serde(with = "humantime_serde")]
    pub max_response_duration: Duration,
}

#[derive(Debug, Deserialize)]
pub struct AmqpConfig {
    pub broker_domain: String,
    pub queues_count: u32,
    pub producers: ProducersConfig,
    pub consumers: ConsumersConfig,
}

impl AmqpConfig {
    /// A non-zero number
    fn producers_count(&self) -> u32 {
        self.producers.producers_count.max(1)
    }

    /// A non-zero number
    fn consumers_count(&self) -> u32 {
        self.consumers.consumers_count.max(1)
    }
}

#[derive(Debug, Deserialize)]
pub struct ProducersConfig {
    pub producers_count: u32,
    pub socket_addr: SocketAddr,
    pub amqp_proxy: Option<ProxyConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ConsumersConfig {
    pub consumers_count: u32,
    #[serde(with = "humantime_serde")]
    pub reconnect_duration: Duration,
    pub amqp_proxy: Option<ProxyConfig>,
    pub server_proxy: Option<ProxyConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ProxyConfig {
    pub socket_addr: SocketAddr,
    #[serde(deserialize_with = "crate::deserialize_failure_rate")]
    pub failure_rate: f64,
    #[serde(with = "humantime_serde")]
    pub max_up_duration: Duration,
}

#[derive(Debug, Serialize)]
pub struct ConfigFile {
    pub name: String,
    pub ty: ConfigType,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ConfigType {
    Device(CrateDeviceConfig),
    Server(CrateServerConfig),
    Producer(CrateProducerConfig),
    Consumer(CrateConsumerConfig),
    TcpProxy(CrateTcpProxyConfig),
}
