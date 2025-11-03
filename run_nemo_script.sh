#!/bin/bash
# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "[run_nemo_script.sh] Script started at $(date)"
echo "[run_nemo_script.sh] Working directory: $(pwd)"

# Load environment variables from .env file
if [ -f .env ]; then
    echo "[run_nemo_script.sh] Loading environment variables from .env file"
    # Source the .env file using set -a to auto-export variables
    set -a
    source .env
    set +a
else
    echo "[run_nemo_script.sh] WARNING: .env file not found"
    echo "[run_nemo_script.sh] Please create a .env file with NEMO_TOKEN and GDRIVE_PARENT_ID"
fi

# Deactivate any existing conda environment first
if [ -n "$CONDA_DEFAULT_ENV" ]; then
    echo "[run_nemo_script.sh] Deactivating conda environment: $CONDA_DEFAULT_ENV"
    conda deactivate 2>/dev/null || true
fi

# Activate virtual environment
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
    echo "[run_nemo_script.sh] Virtual environment activated"
else
    echo "[run_nemo_script.sh] ERROR: Virtual environment not found at .venv/bin/activate"
    exit 1
fi

# Verify virtual environment is active
if [ -z "$VIRTUAL_ENV" ]; then
    echo "[run_nemo_script.sh] ERROR: Virtual environment not activated"
    exit 1
fi

echo "[run_nemo_script.sh] Using Python: $(which python)"
echo "[run_nemo_script.sh] Virtual environment: $VIRTUAL_ENV"
echo "[run_nemo_script.sh] Python version: $(python --version)"

# Debug: Check if token file exists
if [ -f token.pickle ]; then
    echo "[run_nemo_script.sh] Token file exists, size: $(ls -lh token.pickle | awk '{print $5}')"
else
    echo "[run_nemo_script.sh] No token file found - will need authentication"
fi

# Run the billing script and capture exit code
echo "[run_nemo_script.sh] Starting billing data script..."
python nemo_billing_to_drive.py
BILLING_EXIT_CODE=$?

echo "[run_nemo_script.sh] Billing script exited with code: $BILLING_EXIT_CODE"

# If the billing script failed, log the error
if [ $BILLING_EXIT_CODE -ne 0 ]; then
    echo "[run_nemo_script.sh] ERROR: Billing script failed. Last 10 lines of log:"
    tail -10 nemo_log.txt
fi

# Final summary
echo "[run_nemo_script.sh] completed at $(date)"
echo "[run_nemo_script.sh] Summary:"
echo "  - Billing script exit code: $BILLING_EXIT_CODE"

# Exit with error if script failed
if [ $BILLING_EXIT_CODE -ne 0 ]; then
    echo "[run_nemo_script.sh] ERROR: Script failed"
    exit 1
fi

echo "[run_nemo_script.sh] All scripts completed successfully!"

