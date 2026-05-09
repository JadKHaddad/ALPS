use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::simulation::docker::{DockerCompose, DockerNetwork, DockerService};
use crate::{
    consumer::Config as CrateConsumerConfig,
    device::Config as CrateDeviceConfig,
    producer::Config as CrateProducerConfig,
    server::Config as CrateServerConfig,
    tcp_proxy::{Config as CrateTcpProxyConfig, Task as CrateTcpProxyTask},
    validator::Config as CrateValidatorConfig,
};

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub root_abs_path: Option<PathBuf>,
    pub lps_rust_image: String,
    pub lps_scripts_image: String,
    pub simulation_dir: String,
    pub simulation_docker_network: String,
    pub devices: DeviceConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub amqp: AmqpConfig,
    #[serde(default)]
    pub mqtt: MqttConfig,
}

impl Config {
    pub fn with_root_abs_path(mut self, root_abs_path: PathBuf) -> Self {
        self.root_abs_path = Some(root_abs_path);
        self
    }

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

    fn validator_config_file(&self) -> String {
        String::from("validator-config.yaml")
    }

    pub fn host_volume(&self) -> String {
        String::from("mount")
    }

    fn volume_mount(&self) -> String {
        let host_volume = self
            .root_abs_path
            .as_ref()
            .map(|path| {
                path.join(&self.simulation_dir)
                    .join(self.host_volume())
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| format!("./{}", self.host_volume()));

        format!("{}:{}", host_volume, self.container_work_dir())
    }

    fn container_work_dir(&self) -> String {
        String::from("/app")
    }

    fn service_network_name(&self) -> String {
        String::from("lps-net")
    }

    fn docker_compose_network(&self) -> DockerNetwork {
        DockerNetwork {
            external: true,
            name: self.simulation_docker_network.clone(),
        }
    }

    fn lps_rust_image_name(&self) -> String {
        self.lps_rust_image.clone()
    }

    fn lps_scripts_image_name(&self) -> String {
        self.lps_scripts_image.clone()
    }

    fn server_container_name(&self) -> String {
        String::from("lps-server")
    }

    fn rabbit_mq_container_name(&self) -> String {
        String::from("lps-rabbitmq")
    }

    fn rabbit_mq_container_port(&self) -> u16 {
        5672
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

    fn amqp_producers_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        for i in 0..self.amqp.producers_count() {
            let producer_config_file = ConfigFile {
                name: format!("producer-{}.yaml", i),
                ty: ConfigType::Producer(CrateProducerConfig {
                    queues_count: self.amqp.queues_count,
                    purge_queues_on_start: true,
                    rabbit_mq_url: format!(
                        "amqp://{}:{}",
                        self.rabbit_mq_container_name(),
                        self.rabbit_mq_container_port()
                    ),
                    socket_addr: self.amqp.producers.socket_addr,
                }),
            };

            config_files.push(producer_config_file);
        }

        config_files
    }

    fn amqp_consumers_config_files(&self) -> Vec<ConfigFile> {
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
                        upstream_domain: self.rabbit_mq_container_name(),
                        upstream_port: self.rabbit_mq_container_port(),
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

        for i in 0..self.amqp.consumers_count() {
            let broker_domain = self
                .amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|_| String::from("lps-consumer-amqp-proxy"))
                .unwrap_or(self.rabbit_mq_container_name());

            let broker_port = self
                .amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|proxy| proxy.socket_addr.port())
                .unwrap_or(self.rabbit_mq_container_port());

            let consumer_config_file = ConfigFile {
                name: format!("consumer-{}.yaml", i),
                ty: ConfigType::Consumer(CrateConsumerConfig {
                    queues_count: self.amqp.queues_count,
                    reconnect_duration: self.amqp.consumers.reconnect_duration,
                    rabbit_mq_url: format!("amqp://{}:{}", broker_domain, broker_port),
                    server_push_url: format!(
                        "http://{}:{}/work",
                        self.server_container_name(),
                        self.server.socket_addr.port()
                    ),
                }),
            };

            config_files.push(consumer_config_file);
        }

        config_files
    }

    fn amqp_devices_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.device_config_file(),
            ty: ConfigType::Device(CrateDeviceConfig {
                devices_count: self.devices.devices_count,
                max_event_interval: self.devices.max_event_interval,
                max_events: self.devices.max_events,
                retry_duration: self.devices.retry_duration,
                server_push_urls: (0..self.amqp.producers_count())
                    .map(|i| {
                        format!(
                            "http://lps-producer-{i}:{}/work",
                            self.amqp.producers.socket_addr.port()
                        )
                    })
                    .collect(),
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.device_results_file()),
            }),
        }]
    }

    fn validator_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.validator_config_file(),
            ty: ConfigType::Validator(CrateValidatorConfig {
                server_results_file: self.server_results_file().into(),
                device_results_file: self.device_results_file().into(),
            }),
        }]
    }

    pub fn amqp_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        config_files.extend(self.server_config_files());
        config_files.extend(self.amqp_producers_config_files());
        config_files.extend(self.amqp_consumers_config_files());
        config_files.extend(self.amqp_devices_config_files());
        config_files.extend(self.validator_config_files());

        config_files
    }

    fn rabbit_mq_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_container_name(self.rabbit_mq_container_name())
            .with_image(String::from("rabbitmq:4-management"))
            .with_command(String::from(
                "bash -c \" rabbitmq-plugins enable --offline rabbitmq_consistent_hash_exchange && rabbitmq-server \"",
            ))
            .with_unless_stopped()
            .with_networks(vec![self.service_network_name()])
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
            .with_container_name(self.server_container_name())
            .with_command(format!("server --config-file {config_file}"))
            .with_volumes(vec![self.volume_mount()])
            .with_ports(ports)
            .with_networks(vec![self.service_network_name()])
    }

    fn device_docker_service(&self) -> DockerService {
        let config_file = PathBuf::from(self.container_work_dir())
            .join(self.device_config_file())
            .display()
            .to_string();

        DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(String::from("lps-device"))
            .with_command(format!("device --config-file {config_file}"))
            .with_volumes(vec![self.volume_mount()])
            .with_networks(vec![self.service_network_name()])
    }

    fn amqp_producers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..self.amqp.producers_count() {
            let config_file = PathBuf::from(self.container_work_dir())
                .join(format!("producer-{i}.yaml"))
                .display()
                .to_string();

            services.insert(
                format!("producer-{i}"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(format!("lps-producer-{i}"))
                    .with_command(format!("producer --config-file {config_file}"))
                    .with_volumes(vec![self.volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    fn amqp_consumers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        if self.amqp.consumers.amqp_proxy.is_some() {
            services.insert(
                String::from("consumer-amqp-proxy"),
                DockerService::default()
                    .with_image(self.lps_rust_image_name())
                    .with_container_name(String::from("lps-consumer-amqp-proxy"))
                    .with_command(format!(
                        "tcp-proxy --config-file {}",
                        PathBuf::from(self.container_work_dir())
                            .join("consumer-amqp-proxy-config.yaml")
                            .display()
                    ))
                    .with_volumes(vec![self.volume_mount()])
                    .with_networks(vec![self.service_network_name()])
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
                    .with_container_name(format!("lps-consumer-{i}"))
                    .with_command(format!("consumer --config-file {config_file}"))
                    .with_volumes(vec![self.volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    fn validator_docker_service(&self) -> DockerService {
        let config_file = PathBuf::from(self.container_work_dir())
            .join(self.validator_config_file())
            .display()
            .to_string();

        DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(String::from("lps-validator"))
            .with_command(format!(
                "sh -c \"validator --config-file {config_file} > /app/validator.log 2>&1\""
            ))
            .with_volumes(vec![self.volume_mount()])
            .with_networks(vec![self.service_network_name()])
            .with_manual_profile()
    }

    fn scripts_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_image(self.lps_scripts_image_name())
            .with_container_name(String::from("lps-scripts"))
            .with_command(format!(
                "python /scripts/latency.py {} latency.png",
                self.server_results_file()
            ))
            .with_volumes(vec![self.volume_mount()])
            .with_networks(vec![self.service_network_name()])
            .with_manual_profile()
    }

    pub fn amqp_docker_compose(&self) -> DockerCompose {
        let networks = [(self.service_network_name(), self.docker_compose_network())]
            .into_iter()
            .collect();

        let mut services = BTreeMap::new();

        services.insert(String::from("rabbitmq"), self.rabbit_mq_docker_service());
        services.insert(String::from("server"), self.server_docker_service());
        services.insert(String::from("device"), self.device_docker_service());
        services.extend(self.amqp_producers_docker_service());
        services.extend(self.amqp_consumers_docker_service());
        services.insert(String::from("validator"), self.validator_docker_service());
        services.insert(String::from("scripts"), self.scripts_docker_service());

        DockerCompose { networks, services }
    }

    pub async fn amqp_write_files(&self) -> anyhow::Result<()> {
        let simulation_dir = PathBuf::from(&self.simulation_dir);

        tokio::fs::create_dir_all(&simulation_dir)
            .await
            .context("Failed to create output directory")?;

        let host_volume = simulation_dir.join(self.host_volume());

        tokio::fs::create_dir_all(&host_volume)
            .await
            .context("Failed to create host volume directory")?;

        let config_files = self.amqp_config_files();

        for config_file in config_files {
            let path = host_volume.join(&config_file.name);

            let data = serde_yaml::to_string(&config_file.ty)
                .context("Failed to serialize config file")?;

            tokio::fs::write(path, data)
                .await
                .context("Failed to write config file")?;
        }

        let docker_compose = self.amqp_docker_compose();

        let docker_compose_data =
            serde_yaml::to_string(&docker_compose).context("Failed to serialize docker compose")?;

        tokio::fs::write(
            simulation_dir.join("docker-compose.yaml"),
            docker_compose_data,
        )
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
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
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

#[derive(Debug, Deserialize, Default)]
pub struct AmqpConfig {
    pub queues_count: u32,
    pub producers: AmqpProducersConfig,
    pub consumers: AmqpConsumersConfig,
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
pub struct AmqpProducersConfig {
    pub producers_count: u32,
    pub socket_addr: SocketAddr,
}

impl Default for AmqpProducersConfig {
    fn default() -> Self {
        Self {
            producers_count: 0,
            socket_addr: default_socket_addr(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct AmqpConsumersConfig {
    pub consumers_count: u32,
    #[serde(with = "humantime_serde")]
    pub reconnect_duration: Duration,
    pub amqp_proxy: Option<ProxyConfig>,
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
    Validator(CrateValidatorConfig),
}

#[derive(Debug, Deserialize, Default)]
pub struct MqttConfig {
    pub producers: MqttProducersConfig,
    pub consumers: MqttConsumersConfig,
}

impl MqttConfig {
    /// A non-zero number
    fn producers_count(&self) -> u32 {
        self.producers.producers_count.max(1)
    }

    /// A non-zero number
    fn consumers_count(&self) -> u32 {
        self.consumers.consumers_count.max(1)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct MqttProducersConfig {
    pub producers_count: u32,
}

#[derive(Debug, Deserialize, Default)]
pub struct MqttConsumersConfig {
    pub consumers_count: u32,
    pub mqtt_proxy: Option<ProxyConfig>,
}

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

// Mqtt config

impl Config {
    /// Used for all mqtt http components as HTTP_PORT
    fn default_http_port(&self) -> String {
        String::from("8080")
    }

    // TODO
    fn lps_mqtt_image_name(&self) -> String {
        String::from("lps-mqtt")
    }

    fn hive_mq_container_name(&self) -> String {
        String::from("lps-hivemq")
    }

    fn hive_mq_container_port(&self) -> String {
        String::from("1883")
    }

    fn mqtt_devices_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.device_config_file(),
            ty: ConfigType::Device(CrateDeviceConfig {
                devices_count: self.devices.devices_count,
                max_event_interval: self.devices.max_event_interval,
                max_events: self.devices.max_events,
                retry_duration: self.devices.retry_duration,
                server_push_urls: (0..self.mqtt.producers_count())
                    .map(|i| {
                        format!(
                            "http://ars-comp-1-proxy-{i}:{}/api/v1/simple",
                            self.default_http_port()
                        )
                    })
                    .collect(),
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.device_results_file()),
            }),
        }]
    }

    fn mqtt_producers_and_ars_comp_1_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..self.mqtt.producers_count() {
            services.insert(
                format!("ars-comp-1-proxy-{i}"),
                DockerService::default()
                    .with_image(self.lps_mqtt_image_name())
                    .with_container_name(format!("lps-ars-comp-1-proxy-{i}"))
                    .with_command(String::from("python3 ars_comp_1_proxy.py"))
                    .with_environment(
                        [
                            (
                                String::from("SERVICE_NAME"),
                                format!("ars-comp-1-proxy-{i}"),
                            ),
                            (String::from("HTTP_PORT"), self.default_http_port()),
                            (
                                String::from("TARGET_URL"),
                                format!(
                                    "http://lps-legacy-proxy-1-{i}:{}/ID_REQ_KC_STORE7D3BPACKET",
                                    self.default_http_port()
                                ),
                            ),
                        ]
                        .into(),
                    )
                    .with_volumes(vec![self.volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );

            services.insert(
                format!("legacy-proxy-1-{i}"),
                DockerService::default()
                    .with_image(self.lps_mqtt_image_name())
                    .with_container_name(format!("lps-legacy-proxy-1-{i}"))
                    .with_command(String::from("python3 legacy_proxy_1.py"))
                    .with_environment(
                        [
                            (String::from("SERVICE_NAME"), format!("legacy-proxy-1-{i}")),
                            (String::from("MQTT_TOPIC"), String::from("proxy1/message")),
                            (String::from("HTTP_PORT"), self.default_http_port()),
                            (String::from("MQTT_BROKER"), self.hive_mq_container_name()),
                            (String::from("MQTT_PORT"), self.hive_mq_container_port()),
                        ]
                        .into(),
                    )
                    .with_volumes(vec![self.volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    fn mqtt_ars_comp_2_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        services.insert(
            String::from("ars-comp-2-proxy"),
            DockerService::default()
                .with_image(self.lps_mqtt_image_name())
                .with_container_name(String::from("lps-ars-comp-2-proxy"))
                .with_command(String::from("python3 ars_comp_2_proxy.py"))
                .with_environment(
                    [
                        (
                            String::from("SERVICE_NAME"),
                            String::from("ars-comp-2-proxy"),
                        ),
                        (String::from("HTTP_PORT"), self.default_http_port()),
                        (
                            String::from("TARGET_URL"),
                            format!(
                                "http://{}:{}/work",
                                self.server_container_name(),
                                self.server.socket_addr.port()
                            ),
                        ),
                    ]
                    .into(),
                )
                .with_volumes(vec![self.volume_mount()])
                .with_networks(vec![self.service_network_name()])
                .with_unless_stopped(),
        );

        services
    }

    fn mqtt_consumers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..self.mqtt.consumers_count() {
            services.insert(
                format!("legacy-proxy-2-{i}"),
                DockerService::default()
                    .with_image(self.lps_mqtt_image_name())
                    .with_container_name(format!("lps-legacy-proxy-2-{i}"))
                    .with_command(String::from("python3 legacy_proxy_2.py"))
                    .with_environment(
                        [
                            (String::from("SERVICE_NAME"), format!("legacy-proxy-2-{i}")),
                            (
                                String::from("TARGET_URL"),
                                format!(
                                    "http://lps-ars-comp-2-proxy:{}/ID_REQ_KC_STORE7D3BPACKET",
                                    self.default_http_port()
                                ),
                            ),
                            (String::from("MQTT_HOST"), self.hive_mq_container_name()),
                            (String::from("MQTT_PORT"), self.hive_mq_container_port()),
                        ]
                        .into(),
                    )
                    .with_volumes(vec![self.volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    pub fn mqtt_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        config_files.extend(self.server_config_files());
        config_files.extend(self.mqtt_devices_config_files());
        config_files.extend(self.validator_config_files());

        config_files
    }

    pub fn mqtt_docker_compose(&self) -> DockerCompose {
        let networks = [(self.service_network_name(), self.docker_compose_network())]
            .into_iter()
            .collect();

        let mut services = BTreeMap::new();

        services.insert(String::from("server"), self.server_docker_service());
        services.insert(String::from("device"), self.device_docker_service());
        services.extend(self.mqtt_producers_and_ars_comp_1_docker_service());
        services.extend(self.mqtt_ars_comp_2_docker_service());
        services.extend(self.mqtt_consumers_docker_service());
        services.insert(String::from("validator"), self.validator_docker_service());
        services.insert(String::from("scripts"), self.scripts_docker_service());

        DockerCompose { networks, services }
    }

    pub async fn mqtt_write_files(&self) -> anyhow::Result<()> {
        let simulation_dir = PathBuf::from(&self.simulation_dir);

        tokio::fs::create_dir_all(&simulation_dir)
            .await
            .context("Failed to create output directory")?;

        let host_volume = simulation_dir.join(self.host_volume());

        tokio::fs::create_dir_all(&host_volume)
            .await
            .context("Failed to create host volume directory")?;

        let config_files = self.mqtt_config_files();

        for config_file in config_files {
            let path = host_volume.join(&config_file.name);

            let data = serde_yaml::to_string(&config_file.ty)
                .context("Failed to serialize config file")?;

            tokio::fs::write(path, data)
                .await
                .context("Failed to write config file")?;
        }

        let docker_compose = self.mqtt_docker_compose();

        let docker_compose_data =
            serde_yaml::to_string(&docker_compose).context("Failed to serialize docker compose")?;

        tokio::fs::write(
            simulation_dir.join("docker-compose.yaml"),
            docker_compose_data,
        )
        .await
        .context("Failed to write docker compose file")?;

        Ok(())
    }
}
