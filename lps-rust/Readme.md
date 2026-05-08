# LPS

## Dev

```bash
docker compose up 
```

## Run

```bash
cargo run -r --bin tcp-proxy -- --config-file tcp-proxy-config.yaml

RUST_LOG="server=debug" cargo run -r --bin server -- --config-file server-config.yaml

cargo run -r --bin producer -- --config-file producer-config.yaml

cargo run -r --bin consumer -- --config-file consumer-config.yaml # many instances of this

RUST_LOG="device=debug" cargo run -r --bin device -- --config-file device-config.yaml

```

```bash
cargo run -r --bin validator -- --config-file validator-config.yaml
```

```bash
docker pull jadkhaddad/lps-rs:0.0.1
docker pull jadkhaddad/lps-scripts:0.0.1

docker network create lps-simulation-net

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  jadkhaddad/lps-rs:0.0.1 \
  simulation-runner --config-file simulation-config.yaml --root-abs-path $(pwd)
```
