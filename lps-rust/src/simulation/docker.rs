use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct DockerCompose {
    pub services: BTreeMap<String, DockerService>,
}

#[derive(Default, Debug, Serialize)]
pub struct DockerService {
    pub image: String,
    pub container_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart: Option<String>,
}

impl DockerService {
    pub fn with_image(mut self, image: String) -> Self {
        self.image = image;
        self
    }

    pub fn with_container_name(mut self, container_name: String) -> Self {
        self.container_name = container_name;
        self
    }

    pub fn with_command(mut self, command: String) -> Self {
        self.command = Some(command);
        self
    }

    pub fn with_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_ports(mut self, ports: Vec<String>) -> Self {
        self.ports = ports;
        self
    }

    pub fn with_volumes(mut self, volumes: Vec<String>) -> Self {
        self.volumes = volumes;
        self
    }

    pub fn with_profiles(mut self, profiles: Vec<String>) -> Self {
        self.profiles = profiles;
        self
    }

    pub fn with_manual_profile(mut self) -> Self {
        self.profiles.push(String::from("manual"));
        self
    }

    pub fn with_unless_stopped(mut self) -> Self {
        self.restart = Some(String::from("unless-stopped"));
        self
    }
}
