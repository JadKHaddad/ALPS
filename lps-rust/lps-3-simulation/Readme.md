# Simulation

This simulation shows how mnesia nodes behave, if they lose connection with each other.

```bash
# The config file is not important since we use --no-create.
cargo run -r --bin simulation-runner -- --config-file lps-3-simulation/config.yaml --no-create

# Now drop the connection between node 0 and node 1

docker exec -it lps-deduplication-0 iptables -A OUTPUT -d lps-deduplication-1 -j DROP

# Now restore the connection between node 0 and node 1

docker exec -it lps-deduplication-0 iptables -D OUTPUT -d lps-deduplication-1 -j DROP

# Now when the nodes reconnect, we get inconsistent state.
```
