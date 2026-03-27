# LPS 3

```bash
ERL_FLAGS="-sname node_1@localhost -setcookie secret" gleam run -- --config-file lps3-config-1.yaml

ERL_FLAGS="-sname node_2@localhost -setcookie secret" gleam run -- --config-file lps3-config-2.yaml
```
