# Phase 1

# AMQP
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-1/1" --base-config "simulations/amqp/phase-1/1/simulation-config.yaml" --log-dir "simulations/amqp/phase-1/1/logs"
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-1/2" --base-config "simulations/amqp/phase-1/2/simulation-config.yaml" --log-dir "simulations/amqp/phase-1/2/logs"

# MQTT
python3 runner.py --protocol "mqtt" --max-runs 10 --base-dir "simulations/mqtt/phase-1" --base-config "simulations/mqtt/phase-1/simulation-config.yaml" --log-dir "simulations/mqtt/phase-1/logs"

# Phase 2

# AMQP
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-2/1" --base-config "simulations/amqp/phase-2/1/simulation-config.yaml" --log-dir "simulations/amqp/phase-2/1/logs"
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-2/2" --base-config "simulations/amqp/phase-2/2/simulation-config.yaml" --log-dir "simulations/amqp/phase-2/2/logs"

# MQTT
python3 runner.py --protocol "mqtt" --max-runs 10 --base-dir "simulations/mqtt/phase-2/1" --base-config "simulations/mqtt/phase-2/1/simulation-config.yaml" --log-dir "simulations/mqtt/phase-2/1/logs"
python3 runner.py --protocol "mqtt" --max-runs 10 --base-dir "simulations/mqtt/phase-2/2" --base-config "simulations/mqtt/phase-2/2/simulation-config.yaml" --log-dir "simulations/mqtt/phase-2/2/logs"

# Phase 3

# AMQP
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-3/1" --base-config "simulations/amqp/phase-3/1/simulation-config.yaml" --log-dir "simulations/amqp/phase-3/1/logs"
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-3/2" --base-config "simulations/amqp/phase-3/2/simulation-config.yaml" --log-dir "simulations/amqp/phase-3/2/logs"
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-3/3" --base-config "simulations/amqp/phase-3/3/simulation-config.yaml" --log-dir "simulations/amqp/phase-3/3/logs"
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-3/4" --base-config "simulations/amqp/phase-3/4/simulation-config.yaml" --log-dir "simulations/amqp/phase-3/4/logs"
python3 runner.py --protocol "amqp" --max-runs 10 --base-dir "simulations/amqp/phase-3/5" --base-config "simulations/amqp/phase-3/5/simulation-config.yaml" --log-dir "simulations/amqp/phase-3/5/logs"

# MQTT
python3 runner.py --protocol "mqtt" --max-runs 10 --base-dir "simulations/mqtt/phase-3/1" --base-config "simulations/mqtt/phase-3/1/simulation-config.yaml" --log-dir "simulations/mqtt/phase-3/1/logs"
python3 runner.py --protocol "mqtt" --max-runs 10 --base-dir "simulations/mqtt/phase-3/2" --base-config "simulations/mqtt/phase-3/2/simulation-config.yaml" --log-dir "simulations/mqtt/phase-3/2/logs"
python3 runner.py --protocol "mqtt" --max-runs 10 --base-dir "simulations/mqtt/phase-3/3" --base-config "simulations/mqtt/phase-3/3/simulation-config.yaml" --log-dir "simulations/mqtt/phase-3/3/logs"
