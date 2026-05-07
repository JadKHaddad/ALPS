use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::time::Duration;
use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::simulation::docker::{DockerCompose, DockerService};
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
    fn server_config_file(&self) -> String {
        String::from("server-config.yaml")
    }

    fn server_failures_file(&self) -> String {
        String::from("server-failures.json")
    }

    fn server_results_file(&self) -> String {
        String::from("server-results.json")
    }

    fn device_config_file(&self) -> String {
        String::from("device-config.yaml")
    }

    fn device_results_file(&self) -> String {
        String::from("device-results.json")
    }

    pub fn host_volume(&self) -> String {
        String::from("./mount")
    }

    fn volume_mount(&self) -> String {
        format!("{}:{}", self.host_volume(), self.container_work_dir())
    }

    fn container_work_dir(&self) -> String {
        String::from("/app")
    }

    fn lps_rust_image_name(&self) -> String {
        String::from("lps-rs:latest")
    }

    pub async fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        serde_yaml::from_slice(
            &tokio::fs::read(path)
                .await
                .context("Failed to read config file")?,
        )
        .context("Failed to deserialize config file")
    }

    fn server_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.server_config_file(),
            ty: ConfigType::Server(CrateServerConfig {
                failure_rate: self.server.failure_rate,
                max_work_duration: self.server.max_work_duration,
                max_response_duration: self.server.max_response_duration,
                socket_addr: self.server.socket_addr,
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.server_results_file()),
                failures_file: PathBuf::from(self.container_work_dir())
                    .join(self.server_failures_file()),
                shutdown_after_idle_duration: self.server.shutdown_after_idle_duration,
            }),
        }]
    }

    fn producers_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

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
                        upstream_domain: self.amqp.broker_domain.clone(),
                        upstream_port: self.amqp.broker_port,
                    }],
                    results_file: PathBuf::from(self.container_work_dir())
                        .join("producer-amqp-proxy-results.json"),
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

            let broker_port = self
                .amqp
                .producers
                .amqp_proxy
                .as_ref()
                .map(|proxy| proxy.socket_addr.port())
                .unwrap_or(self.amqp.broker_port);

            let producer_config_file = ConfigFile {
                name: format!("producer-{}.yaml", i),
                ty: ConfigType::Producer(CrateProducerConfig {
                    queues_count: self.amqp.queues_count,
                    purge_queues_on_start: true,
                    rabbit_mq_url: format!("amqp://{}:{}", broker_domain, broker_port),
                    socket_addr: self.amqp.producers.socket_addr,
                }),
            };

            config_files.push(producer_config_file);
        }

        config_files
    }

    fn consumers_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

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
                        upstream_domain: self.amqp.broker_domain.clone(),
                        upstream_port: self.amqp.broker_port,
                    }],
                    results_file: PathBuf::from(self.container_work_dir())
                        .join("consumer-amqp-proxy-results.json"),
                });

        let consumer_amqp_proxy_config_file = consumer_amqp_proxy_config.map(|config| ConfigFile {
            name: "consumer-amqp-proxy-config.yaml".to_string(),
            ty: ConfigType::TcpProxy(config),
        });

        if let Some(config) = consumer_amqp_proxy_config_file {
            config_files.push(config);
        }

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
                        upstream_domain: self.server.server_domain.clone(),
                        upstream_port: self.server.socket_addr.port(),
                    }],
                    results_file: PathBuf::from(self.container_work_dir())
                        .join("consumer-server-proxy-results.json"),
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

            let broker_port = self
                .amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|proxy| proxy.socket_addr.port())
                .unwrap_or(self.amqp.broker_port);

            let server_domain = self
                .amqp
                .consumers
                .server_proxy
                .as_ref()
                .map(|_| String::from("consumer-server-proxy"))
                .unwrap_or(self.server.server_domain.clone());

            let server_port = self
                .amqp
                .consumers
                .server_proxy
                .as_ref()
                .map(|proxy| proxy.socket_addr.port())
                .unwrap_or(self.server.socket_addr.port());

            let consumer_config_file = ConfigFile {
                name: format!("consumer-{}.yaml", i),
                ty: ConfigType::Consumer(CrateConsumerConfig {
                    queues_count: self.amqp.queues_count,
                    reconnect_duration: self.amqp.consumers.reconnect_duration,
                    rabbit_mq_url: format!("amqp://{}:{}", broker_domain, broker_port),
                    server_push_url: format!("http://{}:{}/work", server_domain, server_port),
                }),
            };

            config_files.push(consumer_config_file);
        }

        config_files
    }

    fn devices_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.device_config_file(),
            ty: ConfigType::Device(CrateDeviceConfig {
                devices_count: self.devices.devices_count,
                max_event_interval: self.devices.max_event_interval,
                max_events: self.devices.max_events,
                retry_duration: self.devices.retry_duration,
                wait_for_start_socket_addr: self.devices.wait_for_start_socket_addr,
                server_push_urls: (0..self.amqp.producers_count())
                    .map(|i| {
                        format!(
                            "http://producer-{i}:{}/work",
                            self.amqp.producers.socket_addr.port()
                        )
                    })
                    .collect(),
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.device_results_file()),
            }),
        }]
    }

    pub fn config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        config_files.extend(self.server_config_files());
        config_files.extend(self.producers_config_files());
        config_files.extend(self.consumers_config_files());
        config_files.extend(self.devices_config_files());

        config_files
    }

    fn rabbit_mq_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_container_name(String::from("lps-rabbitmq"))
            .with_image(String::from("rabbitmq:4-management"))
            .with_command(String::from(
                "bash -c \" rabbitmq-plugins enable --offline rabbitmq_consistent_hash_exchange && rabbitmq-server \"",
            ))
            .with_unless_stopped()
    }

    pub fn server_docker_exposed_socket_addr_and_container_port(&self) -> (SocketAddr, u16) {
        self.server
            .expose_socket_addr
            .map(|addr| (addr, self.server.socket_addr.port()))
            .unwrap_or_else(|| {
                let addr = SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                    self.server.socket_addr.port(),
                );

                (addr, self.server.socket_addr.port())
            })
    }

    fn server_docker_service(&self) -> DockerService {
        let config_file = PathBuf::from(self.container_work_dir())
            .join(self.server_config_file())
            .display()
            .to_string();

        let (exposed_addr, container_port) =
            self.server_docker_exposed_socket_addr_and_container_port();

        let ports = vec![format!("{exposed_addr}:{container_port}")];

        DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(String::from("lps-server"))
            .with_command(format!("server --config-file {config_file}"))
            .with_volumes(vec![self.volume_mount()])
            .with_ports(ports)
    }

    pub fn device_docker_exposed_socket_addr_and_container_port(
        &self,
    ) -> Option<(SocketAddr, u16)> {
        self.devices.wait_for_start_socket_addr.map(|addr| {
            let exposed_addr = self.devices.expose_socket_addr.unwrap_or_else(|| {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), addr.port())
            });

            (exposed_addr, addr.port())
        })
    }

    fn device_docker_service(&self) -> DockerService {
        let config_file = PathBuf::from(self.container_work_dir())
            .join(self.device_config_file())
            .display()
            .to_string();

        let ports = self
            .device_docker_exposed_socket_addr_and_container_port()
            .map(|(exposed_addr, container_port)| format!("{}:{}", exposed_addr, container_port))
            .map(|port| vec![port])
            .unwrap_or_default();

        DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(String::from("lps-device"))
            .with_command(format!("device --config-file {config_file}"))
            .with_volumes(vec![self.volume_mount()])
            .with_ports(ports)
    }

    fn producers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        if self.amqp.producers.amqp_proxy.is_some() {
            services.insert(
                String::from("producer-amqp-proxy"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(String::from("producer-amqp-proxy"))
                    .with_command(format!(
                        "tcp-proxy --config-file {}",
                        PathBuf::from(self.container_work_dir())
                            .join("producer-amqp-proxy-config.yaml")
                            .display()
                    ))
                    .with_volumes(vec![self.volume_mount()])
                    .with_unless_stopped(),
            );
        }

        for i in 0..self.amqp.producers_count() {
            let config_file = PathBuf::from(self.container_work_dir())
                .join(format!("producer-{i}.yaml"))
                .display()
                .to_string();

            services.insert(
                format!("producer-{i}"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(format!("producer-{i}"))
                    .with_command(format!("producer --config-file {config_file}"))
                    .with_volumes(vec![self.volume_mount()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    fn consumers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        if self.amqp.consumers.amqp_proxy.is_some() {
            services.insert(
                String::from("consumer-amqp-proxy"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(String::from("consumer-amqp-proxy"))
                    .with_command(format!(
                        "tcp-proxy --config-file {}",
                        PathBuf::from(self.container_work_dir())
                            .join("consumer-amqp-proxy-config.yaml")
                            .display()
                    ))
                    .with_volumes(vec![self.volume_mount()])
                    .with_unless_stopped(),
            );
        }

        if self.amqp.consumers.server_proxy.is_some() {
            services.insert(
                String::from("consumer-server-proxy"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(String::from("consumer-server-proxy"))
                    .with_command(format!(
                        "tcp-proxy --config-file {}",
                        PathBuf::from(self.container_work_dir())
                            .join("consumer-server-proxy-config.yaml")
                            .display()
                    ))
                    .with_volumes(vec![self.volume_mount()])
                    .with_unless_stopped(),
            );
        }

        for i in 0..self.amqp.consumers_count() {
            let config_file = PathBuf::from(self.container_work_dir())
                .join(format!("consumer-{i}.yaml"))
                .display()
                .to_string();

            services.insert(
                format!("consumer-{i}"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(format!("consumer-{i}"))
                    .with_command(format!("consumer --config-file {config_file}"))
                    .with_volumes(vec![self.volume_mount()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    pub fn docker_compose(&self) -> DockerCompose {
        let mut services = BTreeMap::new();

        services.insert(String::from("rabbitmq"), self.rabbit_mq_docker_service());
        services.insert(String::from("server"), self.server_docker_service());
        services.insert(String::from("device"), self.device_docker_service());
        services.extend(self.producers_docker_service());
        services.extend(self.consumers_docker_service());

        DockerCompose { services }
    }

    pub async fn write_files(&self, out_dir: &Path) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(out_dir)
            .await
            .context("Failed to create output directory")?;

        let host_volume = self.host_volume();

        tokio::fs::create_dir_all(out_dir.join(&host_volume))
            .await
            .context("Failed to create host volume directory")?;

        let config_out_dir = out_dir.join(&host_volume);

        let config_files = self.config_files();

        for config_file in config_files {
            let path = config_out_dir.join(&config_file.name);

            let data = serde_yaml::to_string(&config_file.ty)
                .context("Failed to serialize config file")?;

            tokio::fs::write(path, data)
                .await
                .context("Failed to write config file")?;
        }

        let docker_compose = self.docker_compose();

        let docker_compose_data =
            serde_yaml::to_string(&docker_compose).context("Failed to serialize docker compose")?;

        tokio::fs::write(out_dir.join("docker-compose.yaml"), docker_compose_data)
            .await
            .context("Failed to write docker compose file")?;

        Ok(())
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
    pub wait_for_start_socket_addr: Option<SocketAddr>,
    pub expose_socket_addr: Option<SocketAddr>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub server_domain: String,
    pub socket_addr: SocketAddr,
    pub expose_socket_addr: Option<SocketAddr>,
    #[serde(deserialize_with = "crate::deserialize_failure_rate")]
    pub failure_rate: f64,
    #[serde(with = "humantime_serde")]
    pub max_work_duration: Duration,
    #[serde(with = "humantime_serde")]
    pub max_response_duration: Duration,
    #[serde(default, with = "humantime_serde")]
    pub shutdown_after_idle_duration: Option<Duration>,
}

#[derive(Debug, Deserialize)]
pub struct AmqpConfig {
    pub broker_domain: String,
    pub broker_port: u16,
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
