# Simulation

This simulation shows how mnesia nodes behave, if they lose connection with each other.

```bash
# The config file is not important since we use --no-create.
cargo run -r --bin simulation-runner -- --config-file lps-3-simulation/config.yaml --no-create

# Send something to node 2

curl -X POST http://localhost:4002/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "1",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Ok

# Now drop the connection between node 0 and node 1

docker exec -it lps-deduplication-0 iptables -A OUTPUT -d lps-deduplication-1 -j DROP

# Send something to node 2

curl -X POST http://localhost:4002/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "2",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Ok

# Now send something to node 0 or node 1. It will block until the connection is restored.

curl -X POST http://localhost:4001/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "3",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Now restore the connection between node 0 and node 1

docker exec -it lps-deduplication-0 iptables -D OUTPUT -d lps-deduplication-1 -j DROP

# Send the same thing sent to node 0 or node 1 to node 2. It will be will be shown as a duplicate and will be ignored.

curl -X POST http://localhost:4002/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "3",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'
```
