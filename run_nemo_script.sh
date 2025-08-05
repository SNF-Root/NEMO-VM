#!/bin/bash
cd ~/nemo_automation

echo "[run_nemo_script.sh] Script started at $(date)"

# Load environment variables from .env file
if [ -f .env ]; then
    set -a  # automatically export all variables
    source .env
    set +a  # turn off automatic export
fi

# Activate virtual environment
source .venv/bin/activate

python nemo_to_drive.py >> nemo_log.txt 2>&1 