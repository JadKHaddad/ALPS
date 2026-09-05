python3 runner.py --protocol "amqp" --base-dir "simulations/amqp/phase-1" --base-config "simulations/amqp/phase-1/simulation-config.yaml" --summary-file "simulations/amqp/phase-1/simulation-summary.json"

python3 runner.py --protocol "amqp" --base-dir "simulations/amqp/phase-2/1" --base-config "simulations/amqp/phase-2/1/simulation-config.yaml" --summary-file "simulations/amqp/phase-2/1/simulation-summary.json"
python3 runner.py --protocol "amqp" --base-dir "simulations/amqp/phase-2/2" --base-config "simulations/amqp/phase-2/2/simulation-config.yaml" --summary-file "simulations/amqp/phase-2/2/simulation-summary.json"
python3 runner.py --protocol "amqp" --base-dir "simulations/amqp/phase-2/3" --base-config "simulations/amqp/phase-2/3/simulation-config.yaml" --summary-file "simulations/amqp/phase-2/3/simulation-summary.json"
python3 runner.py --protocol "amqp" --base-dir "simulations/amqp/phase-2/4" --base-config "simulations/amqp/phase-2/4/simulation-config.yaml" --summary-file "simulations/amqp/phase-2/4/simulation-summary.json"
python3 runner.py --protocol "amqp" --base-dir "simulations/amqp/phase-2/5" --base-config "simulations/amqp/phase-2/5/simulation-config.yaml" --summary-file "simulations/amqp/phase-2/5/simulation-summary.json"

python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-1" --base-config "simulations/mqtt/phase-1/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-1/simulation-summary.json"

python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-3/1" --base-config "simulations/mqtt/phase-3/1/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-3/1/simulation-summary.json"
python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-3/2" --base-config "simulations/mqtt/phase-3/2/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-3/2/simulation-summary.json"
python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-3/3" --base-config "simulations/mqtt/phase-3/3/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-3/3/simulation-summary.json"
python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-3/4" --base-config "simulations/mqtt/phase-3/4/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-3/4/simulation-summary.json"
python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-3/5" --base-config "simulations/mqtt/phase-3/5/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-3/5/simulation-summary.json"

python3 runner.py --protocol "mqtt" --base-dir "simulations/mqtt/phase-2" --base-config "simulations/mqtt/phase-2/mqtt-simulation-config.yaml" --summary-file "simulations/mqtt/phase-2/simulation-summary.json" --start-index "17" --max-runs "100"
