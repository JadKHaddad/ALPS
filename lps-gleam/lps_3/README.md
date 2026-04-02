# LPS 3

```bash
ERL_FLAGS="-sname node_1@localhost -setcookie secret" gleam run -- --config-file lps3-config-1.yaml

ERL_FLAGS="-sname node_2@localhost -setcookie secret" gleam run -- --config-file lps3-config-2.yaml
```

```bash
curl -X POST http://localhost:4000/work \
  -H "Content-Type: application/json" \
  -d '{
    "branch": "Branch",
    "headnumber": "Headnumber",
    "phone": "9",
    "triggertime": "2026-03-28T09:27:11.926751726Z"
  }'
```
