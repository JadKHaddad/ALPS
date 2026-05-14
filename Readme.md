
# LPS

```bash
docker build -t jadkhaddad/lps-rs:0.0.1 -f lps-rust/Dockerfile lps-rust
docker build -t jadkhaddad/lps-scripts:0.0.1 -f scripts/Dockerfile scripts
docker build -t jadkhaddad/lps-mqtt-python:0.0.1 -f legacy-proxy-system-docker/Dockerfile.python legacy-proxy-system
docker build -t jadkhaddad/lps-scripts:0.0.1 -f legacy-proxy-system-docker/Dockerfile.hivemq legacy-proxy-system
```

```bash
docker pull jadkhaddad/lps-rs:0.0.1
docker pull jadkhaddad/lps-scripts:0.0.1
docker pull jadkhaddad/lps-mqtt-python:0.0.1
docker pull jadkhaddad/lps-mqtt-hivemq:0.0.1

docker network create lps-simulation-net

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  jadkhaddad/lps-rs:0.0.1 \
  simulation-runner --config-file simulation-config.yaml --root-abs-path $(pwd)

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  jadkhaddad/lps-rs:0.0.1 \
  mqtt-simulation-runner --config-file mqtt-simulation-config.yaml --root-abs-path $(pwd)
```
