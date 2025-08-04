#!/bin/bash
cd ~/nemo_automation

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Activate virtual environment
source .venv/bin/activate

python nemo_to_drive.py >> nemo_log.txt 2>&1 