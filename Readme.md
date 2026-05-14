
# LPS

```bash
docker build -t jadkhaddad/lps-rs:0.1.0 -f lps-rust/Dockerfile lps-rust
docker build -t jadkhaddad/lps-scripts:0.1.0 -f scripts/Dockerfile scripts
docker build -t jadkhaddad/lps-mqtt-python:0.1.0 -f legacy-proxy-system-docker/Dockerfile.python legacy-proxy-system
docker build -t jadkhaddad/lps-mqtt-hivemq:0.1.0 -f legacy-proxy-system-docker/Dockerfile.hivemq legacy-proxy-system

docker push jadkhaddad/lps-rs:0.1.0
docker push jadkhaddad/lps-scripts:0.1.0
docker push jadkhaddad/lps-mqtt-python:0.1.0
docker push jadkhaddad/lps-mqtt-hivemq:0.1.0
```

## Pull the images

```bash
docker pull rabbitmq:4-management

# Contains the server, devices, consumers, producers, the validator and the simulation runners
docker pull jadkhaddad/lps-rs:0.1.0

# Contains the python scripts to generate plots
docker pull jadkhaddad/lps-scripts:0.1.0

# Contains lps python scripts: ars_comp_1_proxy, ars_comp_2_proxy, legacy_proxy_1 and legacy_proxy_2
docker pull jadkhaddad/lps-mqtt-python:0.1.0

# Contains the hivemq broker with the lps plugins
docker pull jadkhaddad/lps-mqtt-hivemq:0.1.0
```

## Create a docker network

```bash
docker network create lps-simulation-net
```

## Run the amqp simulation

```bash
# These containers may still be around because of the manual profile
docker rm -f lps-scripts lps-validator || true

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  jadkhaddad/lps-rs:0.1.0 \
  simulation-runner --config-file simulation-config.yaml --root-abs-path $(pwd)
```

## Run the mqtt simulation

```bash
# These containers may still be around because of the manual profile
docker rm -f lps-scripts lps-validator || true

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  jadkhaddad/lps-rs:0.1.0 \
  mqtt-simulation-runner --config-file mqtt-simulation-config.yaml --root-abs-path $(pwd)
```

## Run the validator and the plots generator manually

```bash
docker compose --project-directory simulation up validator scripts
```
