# LPS

## Dev

```bash
docker compose up 
```

## Run

```bash
source venv-3.13/bin/activate

HTTP_PORT=7070 \
TARGET_URL=http://localhost:7072/ID_REQ_KC_STORE7D3BPACKET \
python3 ars_comp_1_proxy.py

HTTP_PORT=7071 \
TARGET_URL=http://localhost:8080/work \
python3 ars_comp_2_proxy.py

HTTP_PORT=7072 MQTT_TOPIC="proxy1/message" python3 legacy_proxy_1.py

HTTP_PORT=7073 \
TARGET_URL=http://localhost:7071/ID_REQ_KC_STORE7D3BPACKET \
python3 legacy_proxy_2.py

```
