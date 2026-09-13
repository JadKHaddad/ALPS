# Run with Mosquitto broker

```bash

## Run the mqtt simulation without building the config because we do not want to override the mosquitto settings

```bash
# These containers may still be around because of the manual profile
docker rm -f lps-scripts lps-validator || true

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  jadkhaddad/lps-rs:0.1.0 \
  mqtt-simulation-runner --no-create --config-file mqtt-simulation-config.yaml --root-abs-path $(pwd)
```
