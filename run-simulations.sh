source runner/venv/bin/activate

# python3 runner/runner.py --protocol "mqtt" --runs 60 --base-dir "simulations/lps/lps-eod" --base-config "simulations/lps/lps-eod/simulation-config.yaml" --log-dir "simulations/lps/lps-eod/logs"
# python3 runner/runner.py --protocol "mqtt" --runs 60 --base-dir "simulations/lps/lps-iod" --base-config "simulations/lps/lps-iod/simulation-config.yaml" --log-dir "simulations/lps/lps-iod/logs"
# python3 runner/runner.py --protocol "amqp" --runs 60 --base-dir "simulations/alps/alps-eod" --base-config "simulations/alps/alps-eod/simulation-config.yaml" --log-dir "simulations/alps/alps-eod/logs"
# python3 runner/runner.py --protocol "amqp" --runs 60 --base-dir "simulations/alps/alps-iod" --base-config "simulations/alps/alps-iod/simulation-config.yaml" --log-dir "simulations/alps/alps-iod/logs"
# python3 runner/runner.py --protocol "amqp" --runs 60 --base-dir "simulations/alps/alps-real-normal" --base-config "simulations/alps/alps-real-normal/simulation-config.yaml" --log-dir "simulations/alps/alps-real-normal/logs"
python3 runner/runner.py --protocol "amqp" --runs 60 --base-dir "simulations/alps/alps-real" --base-config "simulations/alps/alps-real/simulation-config.yaml" --log-dir "simulations/alps/alps-real/logs"
