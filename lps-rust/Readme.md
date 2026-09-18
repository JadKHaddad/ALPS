# LPS

## Pyo3

```bash
sudo apt install python3.13 python3.13-venv python3.13-dev

python3.13 -m venv pyo3venv

source pyo3venv/bin/activate

pip install -r pyo3-requirements.txt
```

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
