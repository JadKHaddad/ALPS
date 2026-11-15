use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::simulation::docker::{DockerCompose, DockerNetwork, DockerService};
use crate::tcp_proxy::Connection;
use crate::{
    consumer::Config as CrateConsumerConfig,
    device::Config as CrateDeviceConfig,
    producer::Config as CrateProducerConfig,
    server::Config as CrateServerConfig,
    server_proxy::Config as CrateServerProxyConfig,
    tcp_proxy::{Config as CrateTcpProxyConfig, Task as CrateTcpProxyTask},
    validator::Config as CrateValidatorConfig,
};

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub root_abs_path: Option<PathBuf>,
    #[serde(default = "default_lps_rust_image")]
    pub lps_rust_image: String,
    #[serde(default = "default_lps_scripts_image")]
    pub lps_scripts_image: String,
    #[serde(default = "default_lps_gleam_image")]
    pub lps_gleam_image: String,
    /// Python image for the ars proxies (amqp and mqtt) and the mqtt legacy proxies.
    #[serde(default = "default_lps_mqtt_python_image")]
    pub lps_mqtt_python_image: String,
    #[serde(default = "default_lps_mqtt_hivemq_image")]
    pub lps_mqtt_hivemq_image: String,
    pub simulation_dir: String,
    pub simulation_docker_network: String,
    #[serde(default)]
    pub seed: Option<u64>,
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

    fn consumers_server_proxy_config_file(&self) -> String {
        String::from("consumers-server-proxy-config.yaml")
    }

    fn server_failures_file(&self) -> String {
        String::from("server-failures.json")
    }

    fn server_results_file(&self) -> String {
        String::from("server-results.json")
    }

    fn server_push_url(&self) -> String {
        format!(
            "http://{}:{}/work",
            self.server_container_name(),
            self.server.socket_addr.port()
        )
    }

    fn consumers_server_push_url(&self) -> String {
        format!(
            "http://{}:{}/work",
            self.consumers_server_proxy_container_name(),
            self.server_proxy_default_socket_addr().port()
        )
    }

    fn deduplication_http_port_u16(&self) -> u16 {
        4000
    }

    fn device_config_file(&self) -> String {
        String::from("device-config.yaml")
    }

    fn duplicate_events_file(&self) -> String {
        String::from("duplicate-events.json")
    }

    fn out_of_order_events_file(&self) -> String {
        String::from("out-of-order-events.json")
    }

    fn rates_file(&self) -> String {
        String::from("rates-results.json")
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

    fn abs_host_volume(&self) -> String {
        self.root_abs_path
            .as_ref()
            .map(|path| {
                path.join(&self.simulation_dir)
                    .join(self.host_volume())
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| format!("./{}", self.host_volume()))
    }

    /// Mounts the `host-volume:/app`
    fn container_work_dir_volume_mount(&self) -> String {
        let host_volume = self.abs_host_volume();

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

    fn lps_gleam_image_name(&self) -> String {
        self.lps_gleam_image.clone()
    }

    fn server_container_name(&self) -> String {
        String::from("lps-server")
    }

    fn consumers_server_proxy_container_name(&self) -> String {
        String::from("lps-consumers-server-proxy")
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

    /// One for each server -> server-server-proxy -> server
    fn deduplication_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        if self.server.deduplication {
            for i in 0..self.server.server_count() {
                config_files.push(ConfigFile {
                    name: format!("deduplication-{}.yaml", i),
                    ty: ConfigType::Deduplication(DeduplicationConfig {
                        port: self.deduplication_http_port_u16(),
                        server_push_url: format!(
                            "http://lps-server-server-proxy-{i}:{}/work",
                            self.server_proxy_default_socket_addr().port()
                        ),
                        nodes: (0..self.server.server_count())
                            .map(|id| format!("node@lps-deduplication-{}", id))
                            .collect(),
                    }),
                });
            }
        }

        config_files
    }

    fn server_proxy_default_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080)
    }

    /// What sits directly behind the consumers-server-proxy (or the server-tcp-proxy) for slot `i`:
    /// deduplication-{i} if enabled, otherwise server-server-proxy-{i}
    fn server_entry_domain_and_port(&self, i: u32) -> (String, u16) {
        if self.server.deduplication {
            (
                format!("lps-deduplication-{i}"),
                self.deduplication_http_port_u16(),
            )
        } else {
            (
                format!("lps-server-server-proxy-{i}"),
                self.server_proxy_default_socket_addr().port(),
            )
        }
    }

    fn server_tcp_proxy_service_key(&self, i: u32) -> String {
        format!("server-tcp-proxy-{i}")
    }

    fn server_tcp_proxy_container_name(&self, i: u32) -> String {
        format!("lps-server-tcp-proxy-{i}")
    }

    fn server_tcp_proxy_config_file(&self, i: u32) -> String {
        format!("server-tcp-proxy-{i}-config.yaml")
    }

    /// consumers-server-proxy -> (server-tcp-proxy-{i}) -> deduplication-{i} | server-server-proxy-{i}
    fn deduplication_or_server_proxy_push_urls(&self) -> Vec<String> {
        (0..self.server.server_count())
            .map(|i| {
                let (domain, port) = match self.server.tcp_proxy.as_ref() {
                    Some(proxy) => (
                        self.server_tcp_proxy_container_name(i),
                        proxy.socket_addr.port(),
                    ),
                    None => self.server_entry_domain_and_port(i),
                };

                format!("http://{domain}:{port}/work")
            })
            .collect()
    }

    /// One consumers-server-proxy -> (server-tcp-proxies) -> server-server-proxies
    ///   or (if deduplication) deduplication -> server-server-proxies
    /// Multiple server-server-proxies according to server count -> server
    /// One server
    fn server_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        for i in 0..self.server.server_count() {
            config_files.push(ConfigFile {
                name: format!("server-server-proxy-{}.yaml", i),
                ty: ConfigType::ServerProxy(CrateServerProxyConfig {
                    socket_addr: self.server_proxy_default_socket_addr(),
                    server_push_urls: vec![self.server_push_url()],
                    seed: self.seed,
                }),
            });
        }

        config_files.push(ConfigFile {
            name: self.consumers_server_proxy_config_file(),
            ty: ConfigType::ServerProxy(CrateServerProxyConfig {
                socket_addr: self.server_proxy_default_socket_addr(),
                server_push_urls: self.deduplication_or_server_proxy_push_urls(),
                seed: self.seed,
            }),
        });

        // consumers-server-proxy -> server-tcp-proxy-{i} -> deduplication-{i} | server-server-proxy-{i}
        if let Some(proxy) = self.server.tcp_proxy.as_ref() {
            for i in 0..self.server.server_count() {
                let (upstream_domain, upstream_port) = self.server_entry_domain_and_port(i);

                config_files.push(ConfigFile {
                    name: self.server_tcp_proxy_config_file(i),
                    ty: ConfigType::TcpProxy(CrateTcpProxyConfig {
                        tasks: vec![CrateTcpProxyTask {
                            socket_addr: proxy.socket_addr,
                            upstream_domain,
                            upstream_port,
                            connection: proxy.connection,
                            connections: proxy.connections.clone(),
                        }],
                        results_file: PathBuf::from(self.container_work_dir())
                            .join(format!("server-tcp-proxy-{i}-results.json")),
                        seed: self.seed.map(|seed| seed + i as u64),
                    }),
                });
            }
        }

        config_files.push(ConfigFile {
            name: self.server_config_file(),
            ty: ConfigType::Server(CrateServerConfig {
                failure_rate: self.server.failure_rate,
                max_work_duration: self
                    .server
                    .max_work_duration
                    .unwrap_or(crate::server::default_max_work_duration()),
                min_work_duration: self.server.min_work_duration,
                max_response_duration: self
                    .server
                    .max_response_duration
                    .unwrap_or(crate::server::default_max_response_duration()),
                min_response_duration: self.server.min_response_duration,
                socket_addr: self.server.socket_addr,
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.server_results_file()),
                failures_file: PathBuf::from(self.container_work_dir())
                    .join(self.server_failures_file()),
                shutdown_after_idle_duration: self.server.shutdown_after_idle_duration,
                seed: self.seed,
                background_load_per_second: self.server.background_load_per_second,
            }),
        });

        config_files
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
                    use_features: self.amqp.use_features,
                }),
            };

            config_files.push(producer_config_file);
        }

        config_files
    }

    /// The URL ars-comp-1-proxy-{i} forwards to: the amqp producer
    fn amqp_producer_push_url(&self, i: u32) -> String {
        format!(
            "http://lps-producer-{i}:{}/work",
            self.amqp.producers.socket_addr.port()
        )
    }

    fn amqp_consumers_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        for i in 0..self.amqp.consumers_count() {
            if let Some(proxy) = self.amqp.consumers.amqp_proxy.as_ref() {
                let consumer_amqp_proxy_config = CrateTcpProxyConfig {
                    tasks: vec![CrateTcpProxyTask {
                        socket_addr: proxy.socket_addr,
                        upstream_domain: self.rabbit_mq_container_name(),
                        upstream_port: self.rabbit_mq_container_port(),
                        connection: proxy.connection,
                        connections: proxy.connections.clone(),
                    }],
                    seed: self.seed.map(|seed| seed + i as u64),
                    results_file: PathBuf::from(self.container_work_dir())
                        .join(format!("consumer-amqp-proxy-{i}-results.json")),
                };

                config_files.push(ConfigFile {
                    name: format!("consumer-amqp-proxy-{i}-config.yaml"),
                    ty: ConfigType::TcpProxy(consumer_amqp_proxy_config),
                });
            }

            // consumer-{i} -> consumer-server-proxy-{i} -> ars-comp-2-proxy-{i}
            if let Some(proxy) = self.amqp.consumers.server_proxy.as_ref() {
                config_files.push(self.consumer_server_proxy_config_file(i, proxy));
            }

            let broker_domain = self
                .amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|_| format!("lps-consumer-amqp-proxy-{i}"))
                .unwrap_or_else(|| self.rabbit_mq_container_name());

            let broker_port = self
                .amqp
                .consumers
                .amqp_proxy
                .as_ref()
                .map(|proxy| proxy.socket_addr.port())
                .unwrap_or_else(|| self.rabbit_mq_container_port());

            let server_push_url =
                self.consumer_target_url(i, self.amqp.consumers.server_proxy.as_ref());

            let consumer_config_file = ConfigFile {
                name: format!("consumer-{i}.yaml"),
                ty: ConfigType::Consumer(CrateConsumerConfig {
                    queues_count: self.amqp.queues_count,
                    reconnect_duration: self.amqp.consumers.reconnect_duration,
                    rabbit_mq_url: format!("amqp://{broker_domain}:{broker_port}"),
                    server_push_url,
                    results_file: PathBuf::from(self.container_work_dir())
                        .join(format!("consumer-{i}-results.json")),
                    use_simple_call_format: true,
                    prefetch_count: self.amqp.consumers.prefetch_count,
                }),
            };

            config_files.push(consumer_config_file);
        }

        config_files
    }

    /// device -> ars-comp-1-proxy-{i} -> producer-{i}
    fn amqp_devices_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.device_config_file(),
            ty: ConfigType::Device(CrateDeviceConfig {
                devices_count: self.devices.devices_count,
                max_event_interval: self.devices.max_event_interval,
                min_event_interval: self.devices.min_event_interval,
                max_events: Some(self.devices.max_events),
                retry_duration: self.devices.retry_duration,
                server_push_urls: (0..self.amqp.producers_count())
                    .map(|i| self.ars_comp_1_proxy_push_url(i))
                    .collect(),
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.device_results_file()),
                use_six_digit_device_id: self.devices.use_six_digit_device_id,
                wait_before_start: self.devices.wait_before_start,
                seed: self.seed,
            }),
        }]
    }

    fn validator_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.validator_config_file(),
            ty: ConfigType::Validator(CrateValidatorConfig {
                server_results_file: self.server_results_file().into(),
                device_results_file: self.device_results_file().into(),
                duplicate_events_file: self.duplicate_events_file().into(),
                out_of_order_events_file: self.out_of_order_events_file().into(),
                rates_file: self.rates_file().into(),
            }),
        }]
    }

    pub fn amqp_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        config_files.extend(self.server_config_files());
        config_files.extend(self.deduplication_config_files());
        config_files.extend(self.amqp_producers_config_files());
        config_files.extend(self.amqp_consumers_config_files());
        config_files.extend(self.amqp_devices_config_files());
        config_files.extend(self.validator_config_files());

        config_files
    }

    fn rabbit_mq_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_container_name(self.rabbit_mq_container_name())
            .with_image(String::from("rabbitmq:4.3.6-management"))
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

    fn server_docker_services(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        let config_file = PathBuf::from(self.container_work_dir())
            .join(self.server_config_file())
            .display()
            .to_string();

        let (exposed_addr, container_port) =
            self.server_docker_exposed_socket_addr_and_container_port();

        let ports = vec![format!("{exposed_addr}:{container_port}")];

        let server_exe = if self.server.use_gs_prod_workload {
            String::from("gs-prod-workload-server")
        } else {
            String::from("server")
        };

        let tracing_target = if self.server.use_gs_prod_workload {
            String::from("gs_prod_workload_server")
        } else {
            String::from("server")
        };

        let service = DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(self.server_container_name())
            .with_command(format!("{server_exe} --config-file {config_file}"))
            .with_volumes(vec![self.container_work_dir_volume_mount()])
            .with_ports(ports)
            .with_networks(vec![self.service_network_name()])
            .with_environment(
                [(String::from("RUST_LOG"), format!("{tracing_target}=debug"))].into(),
            );

        services.insert(String::from("server"), service);

        // The consumers-server-proxy
        let config_file = PathBuf::from(self.container_work_dir())
            .join(self.consumers_server_proxy_config_file())
            .display()
            .to_string();

        let service = DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(self.consumers_server_proxy_container_name())
            .with_command(format!("server-proxy --config-file {config_file}"))
            .with_volumes(vec![self.container_work_dir_volume_mount()])
            .with_networks(vec![self.service_network_name()]);

        services.insert(String::from("consumers-server-proxy"), service);

        // The server-tcp-proxies: consumers-server-proxy -> server-tcp-proxy-{i}
        //   -> deduplication-{i} | server-server-proxy-{i}
        if self.server.tcp_proxy.is_some() {
            for i in 0..self.server.server_count() {
                let config_file = PathBuf::from(self.container_work_dir())
                    .join(self.server_tcp_proxy_config_file(i))
                    .display()
                    .to_string();

                services.insert(
                    self.server_tcp_proxy_service_key(i),
                    DockerService::default()
                        .with_image(self.lps_rust_image_name())
                        .with_container_name(self.server_tcp_proxy_container_name(i))
                        .with_command(format!("tcp-proxy --config-file {config_file}"))
                        .with_volumes(vec![self.container_work_dir_volume_mount()])
                        .with_networks(vec![self.service_network_name()])
                        .with_unless_stopped(),
                );
            }
        }

        // The server-server-proxies
        for i in 0..self.server.server_count() {
            let config_file = PathBuf::from(self.container_work_dir())
                .join(format!("server-server-proxy-{}.yaml", i))
                .display()
                .to_string();

            let service = DockerService::default()
                .with_image(self.lps_rust_image_name())
                .with_container_name(format!("lps-server-server-proxy-{i}"))
                .with_command(format!("server-proxy --config-file {config_file}"))
                .with_volumes(vec![self.container_work_dir_volume_mount()])
                .with_networks(vec![self.service_network_name()]);

            services.insert(format!("server-server-proxy-{i}"), service);
        }

        services
    }

    /// This is used from the runner to run the service in bootstrap mode.
    ///
    /// ```not_rust
    /// run --config-file file.yaml --bootstrap
    /// ```
    pub fn first_deduplication_mounted_config_file(&self) -> String {
        PathBuf::from(self.container_work_dir())
            .join("deduplication-0.yaml")
            .display()
            .to_string()
    }

    fn deduplication_docker_services(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        if self.server.deduplication {
            for i in 0..self.server.server_count() {
                let config_file = PathBuf::from(self.container_work_dir())
                    .join(format!("deduplication-{}.yaml", i))
                    .display()
                    .to_string();

                let service = DockerService::default()
                    .with_image(self.lps_gleam_image_name())
                    .with_container_name(format!("lps-deduplication-{i}"))
                    .with_command(format!("run --config-file {config_file}",))
                    .with_environment(
                        [(
                            String::from("ERL_FLAGS"),
                            format!("-sname node@lps-deduplication-{i} -setcookie secret",),
                        )]
                        .into(),
                    )
                    .with_volumes(vec![self.container_work_dir_volume_mount()])
                    .with_networks(vec![self.service_network_name()]);

                services.insert(format!("deduplication-{i}"), service);
            }
        }

        services
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
            .with_volumes(vec![self.container_work_dir_volume_mount()])
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
                    .with_volumes(vec![self.container_work_dir_volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    /// One ars-comp-1-proxy-{i} in front of every producer-{i}
    fn amqp_ars_comp_1_docker_services(&self) -> BTreeMap<String, DockerService> {
        (0..self.amqp.producers_count())
            .map(|i| {
                (
                    self.ars_comp_1_proxy_service_key(i),
                    self.ars_comp_1_proxy_docker_service(i, self.amqp_producer_push_url(i)),
                )
            })
            .collect()
    }

    fn amqp_consumers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..self.amqp.consumers_count() {
            if self.amqp.consumers.amqp_proxy.is_some() {
                let config_file = PathBuf::from(self.container_work_dir())
                    .join(format!("consumer-amqp-proxy-{i}-config.yaml"))
                    .display()
                    .to_string();

                services.insert(
                    format!("consumer-amqp-proxy-{i}"),
                    DockerService::default()
                        .with_image(self.lps_rust_image_name())
                        .with_container_name(format!("lps-consumer-amqp-proxy-{i}"))
                        .with_command(format!("tcp-proxy --config-file {config_file}"))
                        .with_volumes(vec![self.container_work_dir_volume_mount()])
                        .with_networks(vec![self.service_network_name()])
                        .with_unless_stopped(),
                );
            }

            if self.amqp.consumers.server_proxy.is_some() {
                services.insert(
                    format!("consumer-server-proxy-{i}"),
                    self.consumer_server_proxy_docker_service(i),
                );
            }

            let config_file = PathBuf::from(self.container_work_dir())
                .join(format!("consumer-{i}.yaml"))
                .display()
                .to_string();

            let mut service = DockerService::default()
                .with_image(self.lps_rust_image_name())
                .with_container_name(format!("lps-consumer-{i}"))
                .with_command(format!("consumer --config-file {config_file}"))
                .with_volumes(vec![self.container_work_dir_volume_mount()])
                .with_networks(vec![self.service_network_name()])
                .with_unless_stopped();

            // consumer-{i} starts after consumer-{i-1}; consumer-0 has no dependency
            if i > 0 {
                service = service.with_depends_on_started(self.amqp_consumer_service_key(i - 1));
            }

            services.insert(self.amqp_consumer_service_key(i), service);
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
            .with_volumes(vec![self.container_work_dir_volume_mount()])
            .with_networks(vec![self.service_network_name()])
            .with_manual_profile()
    }

    fn scripts_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_image(self.lps_scripts_image_name())
            .with_container_name(String::from("lps-scripts"))
            .with_command(format!(
                "sh -c 'python3 /scripts/latency.py {server} latency-line.png && python3 /scripts/failure.py {device} {server} failure.png'",
                server = self.server_results_file(),
                device = self.device_results_file()
            ))
            .with_volumes(vec![self.container_work_dir_volume_mount()])
            .with_networks(vec![self.service_network_name()])
            .with_manual_profile()
    }

    /// device -> ars-comp-1-proxy-{i} -> producer-{i} -> rabbitmq
    ///   -> (consumer-amqp-proxy-{i}) -> consumer-{i}
    ///   -> (consumer-server-proxy-{i}) -> ars-comp-2-proxy-{i} -> consumers-server-proxy
    ///   -> (server-tcp-proxy-{i}) -> (deduplication-{i}) -> server-server-proxy-{i} -> server
    pub fn amqp_docker_compose(&self) -> DockerCompose {
        let networks = [(self.service_network_name(), self.docker_compose_network())]
            .into_iter()
            .collect();

        let mut services = BTreeMap::new();

        services.insert(String::from("rabbitmq"), self.rabbit_mq_docker_service());
        services.extend(self.server_docker_services());
        services.extend(self.deduplication_docker_services());
        services.insert(String::from("device"), self.device_docker_service());
        services.extend(self.amqp_ars_comp_1_docker_services());
        services.extend(self.amqp_producers_docker_service());
        services.extend(self.amqp_consumers_docker_service());
        services.extend(self.ars_comp_2_docker_services(self.amqp.consumers_count()));
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

    // Compose service key of the amqp consumer
    fn amqp_consumer_service_key(&self, i: u32) -> String {
        format!("consumer-{i}")
    }

    pub fn amqp_consumers_wait_before_start(&self) -> Option<Duration> {
        self.amqp.consumers.wait_before_start
    }

    /// Compose service keys of the amqp consumers, started after `wait_before_start`.
    pub fn amqp_consumer_service_keys(&self) -> Vec<String> {
        (0..self.amqp.consumers_count())
            .map(|i| self.amqp_consumer_service_key(i))
            .collect()
    }

    /// Compose service keys started as soon as the broker is ready.
    ///
    /// Excludes the consumers and the manual-profile services (validator, scripts),
    /// because naming a profiled service explicitly in `up` would activate it.
    pub fn amqp_pre_consumer_service_keys(&self) -> Vec<String> {
        let consumers = self.amqp_consumer_service_keys();

        self.amqp_docker_compose()
            .services
            .into_keys()
            .filter(|key| !consumers.contains(key) && key != "validator" && key != "scripts")
            .collect()
    }
}

#[derive(Debug, Deserialize)]
pub struct DeviceConfig {
    pub devices_count: u32,
    #[serde(with = "humantime_serde")]
    pub max_event_interval: Duration,
    #[serde(default, with = "humantime_serde")]
    pub min_event_interval: Option<Duration>,
    pub max_events: u64,
    #[serde(with = "humantime_serde")]
    pub retry_duration: Duration,
    #[serde(default)]
    pub use_six_digit_device_id: bool,
    #[serde(default, with = "humantime_serde")]
    pub wait_before_start: Option<Duration>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub socket_addr: SocketAddr,
    pub expose_socket_addr: Option<SocketAddr>,
    #[serde(deserialize_with = "crate::deserialize_failure_rate")]
    pub failure_rate: f64,
    #[serde(default, with = "humantime_serde")]
    pub max_work_duration: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub min_work_duration: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub max_response_duration: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub min_response_duration: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub shutdown_after_idle_duration: Option<Duration>,
    pub server_count: u32,
    #[serde(default)]
    pub deduplication: bool,
    /// If set gs-prod-workload-server will be used
    #[serde(default)]
    pub use_gs_prod_workload: bool,
    /// See [`CrateServerConfig::background_load_per_second`]
    #[serde(default)]
    background_load_per_second: Option<f64>,
    /// TCP proxy between the consumers-server-proxy and deduplication-{i}
    /// (or server-server-proxy-{i} without deduplication), one per server slot
    #[serde(default)]
    pub tcp_proxy: Option<ProxyConfig>,
}

impl ServerConfig {
    /// A non-zero number
    fn server_count(&self) -> u32 {
        self.server_count.max(1)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct AmqpConfig {
    /// Wether to use `x-consistent-hash` and `x-single-active-consumer`.
    #[serde(default = "AmqpConfig::default_use_features")]
    pub use_features: bool,
    pub queues_count: u32,
    pub producers: AmqpProducersConfig,
    pub consumers: AmqpConsumersConfig,
}

impl AmqpConfig {
    fn default_use_features() -> bool {
        true
    }

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
    /// TCP proxy between the broker and consumer-{i}
    pub amqp_proxy: Option<ProxyConfig>,
    /// TCP proxy between consumer-{i} and ars-comp-2-proxy-{i}
    pub server_proxy: Option<ProxyConfig>,
    /// If set, the runner starts every other service once the broker is ready,
    /// waits this long, and only then starts the consumers (consumer-{i}).
    #[serde(default, with = "humantime_serde")]
    pub wait_before_start: Option<Duration>,
    #[serde(default = "crate::consumer::default_prefetch_count")]
    pub prefetch_count: u16,
}

#[derive(Debug, Deserialize)]
pub struct ProxyConfig {
    pub socket_addr: SocketAddr,
    #[serde(default)]
    pub connection: Connection,
    #[serde(default)]
    pub connections: Vec<Connection>,
}

#[derive(Debug, Serialize)]
pub struct ConfigFile {
    pub name: String,
    pub ty: ConfigType,
}

#[derive(Debug, Serialize)]
pub struct DeduplicationConfig {
    pub port: u16,
    pub server_push_url: String,
    pub nodes: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ConfigType {
    Device(CrateDeviceConfig),
    Server(CrateServerConfig),
    ServerProxy(CrateServerProxyConfig),
    Deduplication(DeduplicationConfig),
    Producer(CrateProducerConfig),
    Consumer(CrateConsumerConfig),
    TcpProxy(CrateTcpProxyConfig),
    Validator(CrateValidatorConfig),
}

#[derive(Debug, Deserialize, Default)]
pub struct MqttConfig {
    pub producers: MqttProducersConfig,
    pub consumers: MqttConsumersConfig,
    #[serde(default)]
    pub use_mosquitto: bool,
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
    /// TCP proxy between the broker and legacy-proxy-2-{i}
    pub mqtt_proxy: Option<ProxyConfig>,
    /// TCP proxy between legacy-proxy-2-{i} and ars-comp-2-proxy-{i}
    pub server_proxy: Option<ProxyConfig>,
    /// If set, the runner starts every other service once the broker is ready,
    /// waits this long, and only then starts the consumers (legacy-proxy-2-{i}).
    #[serde(default, with = "humantime_serde")]
    pub wait_before_start: Option<Duration>,
}

fn default_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

fn default_lps_rust_image() -> String {
    String::from("jadkhaddad/lps-rs:0.1.0")
}

fn default_lps_scripts_image() -> String {
    String::from("jadkhaddad/lps-scripts:0.1.0")
}

fn default_lps_gleam_image() -> String {
    String::from("jadkhaddad/lps-gleam:0.1.0")
}

fn default_lps_mqtt_python_image() -> String {
    String::from("jadkhaddad/lps-mqtt-python:0.1.0")
}

fn default_lps_mqtt_hivemq_image() -> String {
    String::from("jadkhaddad/lps-mqtt-hivemq:0.1.0")
}

// ARS proxies and consumer server proxy (shared by amqp and mqtt)

impl Config {
    /// Used for all python http components (ars proxies, legacy proxies) as HTTP_PORT
    fn default_http_port(&self) -> String {
        self.default_http_port_u16().to_string()
    }

    fn default_http_port_u16(&self) -> u16 {
        8080
    }

    fn lps_mqtt_python_image_name(&self) -> String {
        self.lps_mqtt_python_image.clone()
    }

    /// Runs a python script from the image's `/scripts`.
    ///
    /// The python services share the `/app` mount and each script creates `logs`
    /// with a non-atomic exists-check + `os.makedirs`, so services starting at the
    /// same time race and crash with `FileExistsError`. Creating it in the shell
    /// first (`mkdir -p` is idempotent) means the script always finds it.
    fn python_script_command(&self, script: &str) -> String {
        format!("sh -c \"mkdir -p logs && exec python3 /scripts/{script}\"")
    }

    fn ars_comp_1_proxy_service_key(&self, i: u32) -> String {
        format!("ars-comp-1-proxy-{i}")
    }

    fn ars_comp_1_proxy_container_name(&self, i: u32) -> String {
        format!("lps-ars-comp-1-proxy-{i}")
    }

    fn ars_comp_2_proxy_service_key(&self, i: u32) -> String {
        format!("ars-comp-2-proxy-{i}")
    }

    fn ars_comp_2_proxy_container_name(&self, i: u32) -> String {
        format!("lps-ars-comp-2-proxy-{i}")
    }

    fn consumer_server_proxy_container_name(&self, i: u32) -> String {
        format!("lps-consumer-server-proxy-{i}")
    }

    /// The URL the device pushes to for producer slot `i`
    fn ars_comp_1_proxy_push_url(&self, i: u32) -> String {
        format!(
            "http://{}:{}/api/v1/simple",
            self.ars_comp_1_proxy_service_key(i),
            self.default_http_port()
        )
    }

    /// The URL a consumer pushes to:
    /// consumer-{i} -> (consumer-server-proxy-{i} if configured) -> ars-comp-2-proxy-{i}
    fn consumer_target_url(&self, i: u32, server_proxy: Option<&ProxyConfig>) -> String {
        let (domain, port) = server_proxy
            .map(|proxy| {
                (
                    self.consumer_server_proxy_container_name(i),
                    proxy.socket_addr.port(),
                )
            })
            .unwrap_or_else(|| {
                (
                    self.ars_comp_2_proxy_container_name(i),
                    self.default_http_port_u16(),
                )
            });

        format!("http://{domain}:{port}/ID_REQ_KC_STORE7D3BPACKET")
    }

    /// TCP proxy config: consumer-{i} -> consumer-server-proxy-{i} -> ars-comp-2-proxy-{i}
    fn consumer_server_proxy_config_file(&self, i: u32, proxy: &ProxyConfig) -> ConfigFile {
        ConfigFile {
            name: format!("consumer-server-proxy-{i}-config.yaml"),
            ty: ConfigType::TcpProxy(CrateTcpProxyConfig {
                tasks: vec![CrateTcpProxyTask {
                    socket_addr: proxy.socket_addr,
                    upstream_domain: self.ars_comp_2_proxy_container_name(i),
                    upstream_port: self.default_http_port_u16(),
                    connection: proxy.connection,
                    connections: proxy.connections.clone(),
                }],
                results_file: PathBuf::from(self.container_work_dir())
                    .join(format!("consumer-server-proxy-{i}-results.json")),
                seed: self.seed.map(|seed| seed + i as u64),
            }),
        }
    }

    fn consumer_server_proxy_docker_service(&self, i: u32) -> DockerService {
        let config_file = PathBuf::from(self.container_work_dir())
            .join(format!("consumer-server-proxy-{i}-config.yaml"))
            .display()
            .to_string();

        DockerService::default()
            .with_image(self.lps_rust_image_name())
            .with_container_name(self.consumer_server_proxy_container_name(i))
            .with_command(format!("tcp-proxy --config-file {config_file}"))
            .with_volumes(vec![self.container_work_dir_volume_mount()])
            .with_networks(vec![self.service_network_name()])
            .with_unless_stopped()
    }

    /// ars-comp-1-proxy-{i} -> `target_url` (amqp producer or mqtt legacy-proxy-1)
    fn ars_comp_1_proxy_docker_service(&self, i: u32, target_url: String) -> DockerService {
        DockerService::default()
            .with_image(self.lps_mqtt_python_image_name())
            .with_container_name(self.ars_comp_1_proxy_container_name(i))
            .with_command(self.python_script_command("ars_comp_1_proxy.py"))
            .with_environment(
                [
                    (
                        String::from("SERVICE_NAME"),
                        self.ars_comp_1_proxy_service_key(i),
                    ),
                    (String::from("HTTP_PORT"), self.default_http_port()),
                    (String::from("TARGET_URL"), target_url),
                ]
                .into(),
            )
            .with_volumes(vec![self.container_work_dir_volume_mount()])
            .with_networks(vec![self.service_network_name()])
            .with_unless_stopped()
    }

    /// ars-comp-2-proxy-{i} -> consumers-server-proxy, one per consumer
    fn ars_comp_2_docker_services(&self, count: u32) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..count {
            services.insert(
                self.ars_comp_2_proxy_service_key(i),
                DockerService::default()
                    .with_image(self.lps_mqtt_python_image_name())
                    .with_container_name(self.ars_comp_2_proxy_container_name(i))
                    .with_command(self.python_script_command("ars_comp_2_proxy.py"))
                    .with_environment(
                        [
                            (
                                String::from("SERVICE_NAME"),
                                self.ars_comp_2_proxy_service_key(i),
                            ),
                            (String::from("HTTP_PORT"), self.default_http_port()),
                            (String::from("TARGET_URL"), self.consumers_server_push_url()),
                        ]
                        .into(),
                    )
                    .with_volumes(vec![self.container_work_dir_volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }
}

// Mqtt config

impl Config {
    fn lps_mqtt_hivemq_image_name(&self) -> String {
        self.lps_mqtt_hivemq_image.clone()
    }

    fn hive_mq_container_name(&self) -> String {
        String::from("lps-hivemq")
    }

    fn hive_mq_container_port_u16(&self) -> u16 {
        1883
    }

    fn hive_mq_container_port(&self) -> String {
        self.hive_mq_container_port_u16().to_string()
    }

    /// Mounts the `host-volume/hivemq/log:/opt/hivemq/log`
    fn hive_mq_log_dir_volume_mount(&self) -> String {
        let host_volume = self.abs_host_volume();

        format!("{}/hivemq/log:{}", host_volume, self.hive_mq_log_dir())
    }

    fn hive_mq_log_dir(&self) -> String {
        String::from("/opt/hivemq/log")
    }

    fn hive_mq_log_init_container_name(&self) -> String {
        String::from("lps-hivemq-log-init")
    }

    fn hive_mq_log_init_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_image(String::from("busybox:latest"))
            .with_container_name(self.hive_mq_log_init_container_name())
            .with_command(format!(
                "sh -c \"mkdir -p {} && chmod -R 777 {}\"",
                self.hive_mq_log_dir(),
                self.hive_mq_log_dir()
            ))
            .with_volumes(vec![self.hive_mq_log_dir_volume_mount()])
            .with_networks(vec![self.service_network_name()])
    }

    fn hive_mq_docker_service(&self) -> DockerService {
        DockerService::default()
            .with_image(self.lps_mqtt_hivemq_image_name())
            .with_container_name(self.hive_mq_container_name())
            .with_unless_stopped()
            .with_networks(vec![self.service_network_name()])
            .with_volumes(vec![self.hive_mq_log_dir_volume_mount()])
            .with_depends_on_completed(self.hive_mq_log_init_container_name_key())
    }

    // Compose service *keys* (map keys), not container names, are what depends_on references.
    fn hive_mq_log_init_container_name_key(&self) -> String {
        String::from("hivemq-log-init")
    }

    fn mosquitto_config_dir(&self) -> String {
        String::from("/mosquitto/config")
    }

    /// Mounts the `host-volume/mosquitto/config:/mosquitto/config`
    fn mosquitto_config_dir_volume_mount(&self) -> String {
        let host_volume = self.abs_host_volume();

        format!(
            "{}/mosquitto/config:{}",
            host_volume,
            self.mosquitto_config_dir()
        )
    }

    fn mosquitto_init_container_name(&self) -> String {
        String::from("lps-mosquitto-init")
    }

    // Compose service *keys* (map keys), not container names, are what depends_on references.
    fn mosquitto_init_container_name_key(&self) -> String {
        String::from("mosquitto-init")
    }

    fn mosquitto_init_docker_service(&self) -> DockerService {
        DockerService::default()
        .with_image(String::from("busybox:latest"))
        .with_container_name(self.mosquitto_init_container_name())
        .with_command(format!(
            "sh -c \"mkdir -p {dir} && echo 'listener {port}' > {dir}/mosquitto.conf && echo 'allow_anonymous true' >> {dir}/mosquitto.conf && echo 'log_type all' >> {dir}/mosquitto.conf\"",
            dir = self.mosquitto_config_dir(),
            port = self.hive_mq_container_port_u16(),
        ))
        .with_volumes(vec![self.mosquitto_config_dir_volume_mount()])
        .with_networks(vec![self.service_network_name()])
    }

    fn mosquitto_docker_service(&self) -> DockerService {
        let port = self.hive_mq_container_port_u16();

        DockerService::default()
            .with_image(String::from("eclipse-mosquitto:2.1.2-alpine"))
            .with_container_name(self.hive_mq_container_name())
            .with_unless_stopped()
            .with_networks(vec![self.service_network_name()])
            .with_ports(vec![format!("{port}:{port}")])
            .with_volumes(vec![self.mosquitto_config_dir_volume_mount()])
            .with_depends_on_completed(self.mosquitto_init_container_name_key())
    }

    fn mqtt_devices_config_files(&self) -> Vec<ConfigFile> {
        vec![ConfigFile {
            name: self.device_config_file(),
            ty: ConfigType::Device(CrateDeviceConfig {
                devices_count: self.devices.devices_count,
                max_event_interval: self.devices.max_event_interval,
                min_event_interval: self.devices.min_event_interval,
                max_events: Some(self.devices.max_events),
                retry_duration: self.devices.retry_duration,
                server_push_urls: (0..self.mqtt.producers_count())
                    .map(|i| self.ars_comp_1_proxy_push_url(i))
                    .collect(),
                results_file: PathBuf::from(self.container_work_dir())
                    .join(self.device_results_file()),
                use_six_digit_device_id: self.devices.use_six_digit_device_id,
                wait_before_start: self.devices.wait_before_start,
                seed: self.seed,
            }),
        }]
    }

    fn mqtt_producers_and_ars_comp_1_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..self.mqtt.producers_count() {
            services.insert(
                self.ars_comp_1_proxy_service_key(i),
                self.ars_comp_1_proxy_docker_service(
                    i,
                    format!(
                        "http://lps-legacy-proxy-1-{i}:{}/ID_REQ_KC_STORE7D3BPACKET",
                        self.default_http_port()
                    ),
                ),
            );

            services.insert(
                format!("legacy-proxy-1-{i}"),
                DockerService::default()
                    .with_image(self.lps_mqtt_python_image_name())
                    .with_container_name(format!("lps-legacy-proxy-1-{i}"))
                    .with_command(self.python_script_command("legacy_proxy_1.py"))
                    .with_environment(
                        [
                            (String::from("SERVICE_NAME"), format!("legacy-proxy-1-{i}")),
                            (
                                String::from("MQTT_TOPIC"),
                                format!("legacy-proxy-1-{i}/message"),
                            ),
                            (String::from("HTTP_PORT"), self.default_http_port()),
                            (String::from("MQTT_BROKER"), self.hive_mq_container_name()),
                            (String::from("MQTT_PORT"), self.hive_mq_container_port()),
                        ]
                        .into(),
                    )
                    .with_volumes(vec![self.container_work_dir_volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    fn mqtt_consumers_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        for i in 0..self.mqtt.consumers_count() {
            if let Some(proxy) = self.mqtt.consumers.mqtt_proxy.as_ref() {
                let consumer_mqtt_proxy_config = CrateTcpProxyConfig {
                    tasks: vec![CrateTcpProxyTask {
                        socket_addr: proxy.socket_addr,
                        upstream_domain: self.hive_mq_container_name(),
                        upstream_port: self.hive_mq_container_port_u16(),
                        connection: proxy.connection,
                        connections: proxy.connections.clone(),
                    }],
                    results_file: PathBuf::from(self.container_work_dir())
                        .join(format!("consumer-mqtt-proxy-{i}-results.json")),
                    seed: self.seed.map(|seed| seed + i as u64),
                };

                config_files.push(ConfigFile {
                    name: format!("consumer-mqtt-proxy-{i}-config.yaml"),
                    ty: ConfigType::TcpProxy(consumer_mqtt_proxy_config),
                });
            }

            // legacy-proxy-2-{i} -> consumer-server-proxy-{i} -> ars-comp-2-proxy-{i}
            if let Some(proxy) = self.mqtt.consumers.server_proxy.as_ref() {
                config_files.push(self.consumer_server_proxy_config_file(i, proxy));
            }
        }

        config_files
    }

    fn mqtt_consumers_docker_service(&self) -> BTreeMap<String, DockerService> {
        let mut services = BTreeMap::new();

        for i in 0..self.mqtt.consumers_count() {
            if self.mqtt.consumers.mqtt_proxy.is_some() {
                let config_file = PathBuf::from(self.container_work_dir())
                    .join(format!("consumer-mqtt-proxy-{i}-config.yaml"))
                    .display()
                    .to_string();

                services.insert(
                    format!("consumer-mqtt-proxy-{i}"),
                    DockerService::default()
                        .with_image(self.lps_rust_image_name())
                        .with_container_name(format!("lps-consumer-mqtt-proxy-{i}"))
                        .with_command(format!("tcp-proxy --config-file {config_file}"))
                        .with_volumes(vec![self.container_work_dir_volume_mount()])
                        .with_networks(vec![self.service_network_name()])
                        .with_unless_stopped(),
                );
            }

            if self.mqtt.consumers.server_proxy.is_some() {
                services.insert(
                    format!("consumer-server-proxy-{i}"),
                    self.consumer_server_proxy_docker_service(i),
                );
            }

            let broker_domain = self
                .mqtt
                .consumers
                .mqtt_proxy
                .as_ref()
                .map(|_| format!("lps-consumer-mqtt-proxy-{i}"))
                .unwrap_or_else(|| self.hive_mq_container_name());

            let broker_port = self
                .mqtt
                .consumers
                .mqtt_proxy
                .as_ref()
                .map(|proxy| proxy.socket_addr.port())
                .unwrap_or_else(|| self.hive_mq_container_port_u16());

            let service_key = self.mqtt_legacy_proxy_2_service_key(i);
            let log_file = format!("logs/{service_key}.log");

            services.insert(
                self.mqtt_legacy_proxy_2_service_key(i),
                DockerService::default()
                    .with_image(self.lps_mqtt_python_image_name())
                    .with_container_name(format!("lps-legacy-proxy-2-{i}"))
                    .with_command(format!(
                        "sh -c \"mkdir -p logs; touch {log_file}; tail -n 0 -F {log_file} & exec python3 /scripts/legacy_proxy_2.py\""
                    ))
                    .with_environment(
                        [
                            (
                                String::from("SERVICE_NAME"),
                                self.mqtt_legacy_proxy_2_service_key(i),
                            ),
                            (
                                String::from("TARGET_URL"),
                                self.consumer_target_url(
                                    i,
                                    self.mqtt.consumers.server_proxy.as_ref(),
                                ),
                            ),
                            (String::from("MQTT_HOST"), broker_domain),
                            (String::from("MQTT_PORT"), broker_port.to_string()),
                        ]
                        .into(),
                    )
                    .with_volumes(vec![self.container_work_dir_volume_mount()])
                    .with_networks(vec![self.service_network_name()])
                    .with_unless_stopped(),
            );
        }

        services
    }

    pub fn mqtt_config_files(&self) -> Vec<ConfigFile> {
        let mut config_files = Vec::new();

        config_files.extend(self.server_config_files());
        config_files.extend(self.deduplication_config_files());
        config_files.extend(self.mqtt_devices_config_files());
        config_files.extend(self.mqtt_consumers_config_files());
        config_files.extend(self.validator_config_files());

        config_files
    }

    pub fn mqtt_docker_compose(&self) -> DockerCompose {
        let networks = [(self.service_network_name(), self.docker_compose_network())]
            .into_iter()
            .collect();

        let mut services = BTreeMap::new();

        if self.mqtt.use_mosquitto {
            services.insert(
                self.mosquitto_init_container_name_key(),
                self.mosquitto_init_docker_service(),
            );
            services.insert(String::from("hivemq"), self.mosquitto_docker_service());
        } else {
            services.insert(
                self.hive_mq_log_init_container_name_key(),
                self.hive_mq_log_init_docker_service(),
            );
            services.insert(String::from("hivemq"), self.hive_mq_docker_service());
        }

        services.extend(self.server_docker_services());
        services.extend(self.deduplication_docker_services());
        services.insert(String::from("device"), self.device_docker_service());
        services.extend(self.mqtt_producers_and_ars_comp_1_docker_service());
        services.extend(self.ars_comp_2_docker_services(self.mqtt.consumers_count()));
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

    // Compose service key of the mqtt consumer
    fn mqtt_legacy_proxy_2_service_key(&self, i: u32) -> String {
        format!("legacy-proxy-2-{i}")
    }

    pub fn mqtt_consumers_wait_before_start(&self) -> Option<Duration> {
        self.mqtt.consumers.wait_before_start
    }

    /// Compose service keys of the mqtt consumers, started after `wait_before_start`.
    pub fn mqtt_consumer_service_keys(&self) -> Vec<String> {
        (0..self.mqtt.consumers_count())
            .map(|i| self.mqtt_legacy_proxy_2_service_key(i))
            .collect()
    }

    /// Compose service keys started as soon as the broker is ready.
    ///
    /// Excludes the consumers and the manual-profile services (validator, scripts),
    /// because naming a profiled service explicitly in `up` would activate it.
    pub fn mqtt_pre_consumer_service_keys(&self) -> Vec<String> {
        let consumers = self.mqtt_consumer_service_keys();

        self.mqtt_docker_compose()
            .services
            .into_keys()
            .filter(|key| !consumers.contains(key) && key != "validator" && key != "scripts")
            .collect()
    }
}
