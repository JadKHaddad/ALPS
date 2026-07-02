# Simulation

This simulation shows how mnesia nodes behave, if they lose connection with each other.

```bash
# The config file is not important since we use --no-create.
cargo run -r --bin simulation-runner -- --config-file lps-3-simulation/config.yaml --no-create

# Send a message to node 0
curl -i -X POST http://localhost:4000/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "0",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Send the same message to node 1, should get a duplicate
curl -i -X POST http://localhost:4001/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "0",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Now drop the connection between node 0 and node 1
docker exec -it lps-deduplication-0 iptables -A OUTPUT -d lps-deduplication-1 -j DROP
docker exec -it lps-deduplication-1 iptables -A OUTPUT -d lps-deduplication-0 -j DROP

# Send a message to node 0, this should fail
curl -i -X POST http://localhost:4000/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "00",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Send another message to node 1, this should fail
curl -i -X POST http://localhost:4001/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "11",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Send another message to node 2, this should not fail
curl -i -X POST http://localhost:4002/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "22",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Now restore the connection between node 0 and node 1
docker exec -it lps-deduplication-0 iptables -D OUTPUT -d lps-deduplication-1 -j DROP
docker exec -it lps-deduplication-1 iptables -D OUTPUT -d lps-deduplication-0 -j DROP

# Send the same message that was sent to node 1 in the split brain to node 0, this should not fail
curl -i -X POST http://localhost:4000/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "11",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Send the same message that was sent to node 0 in the split brain to node 1, this should not fail
curl -i -X POST http://localhost:4001/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "00",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'

# Send the same message that was sent to node 2 in the split brain to node 0, this should not fail, and we should get a duplicate
curl -i -X POST http://localhost:4000/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "22",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'
```
