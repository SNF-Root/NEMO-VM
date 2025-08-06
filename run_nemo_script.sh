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

# Debug: Check if token file exists
if [ -f token.pickle ]; then
    echo "[run_nemo_script.sh] Token file exists, size: $(ls -lh token.pickle | awk '{print $5}')"
else
    echo "[run_nemo_script.sh] No token file found - will need authentication"
fi

# Run the script and capture exit code
python nemo_to_drive.py >> nemo_log.txt 2>&1
EXIT_CODE=$?

echo "[run_nemo_script.sh] Python script exited with code: $EXIT_CODE"
echo "[run_nemo_script.sh] completed at $(date)"

# If the script failed, log the last few lines of output
if [ $EXIT_CODE -ne 0 ]; then
    echo "[run_nemo_script.sh] ERROR: Script failed. Last 10 lines of log:"
    tail -10 nemo_log.txt
fi 

