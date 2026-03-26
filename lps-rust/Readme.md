# LPS

## Dev

```bash
docker compose up 
```

## Run

```bash
cargo run -r --bin server -- --config-file server-config.yaml

cargo run -r --bin producer -- --config-file producer-config.yaml

cargo run -r --bin consumer -- --config-file consumer-config.yaml # many instances of this

cargo run -r --bin device -- --config-file device-config.yaml

```

```bash
cargo run -r --bin validator -- --config-file validator-config.yaml
```
