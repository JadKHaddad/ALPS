SCRIPT_DIR="/home/jadkhaddad/Desktop/LPS/scripts"
VENV_DIR="$SCRIPT_DIR/venv"

python $SCRIPT_DIR/failure_order.py device-results.json server-results.json server-failures.json failure-order.pdf
python $SCRIPT_DIR/failure_duplicate.py device-results.json server-results.json server-failures.json failure-duplicate.pdf