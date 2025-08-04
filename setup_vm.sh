#!/bin/bash

# Setup script for Nemo to Drive automation on VM

echo "Setting up Nemo to Drive automation..."
echo ""

# Store the original directory where the script is run from
ORIGINAL_DIR=$(pwd)

# Check if .env file exists in current directory
if [ ! -f .env ]; then
    echo "Error: .env file not found in current directory"
    echo "Please ensure you have a .env file with NEMO_TOKEN and GDRIVE_PARENT_ID"
    exit 1
fi

echo "Found .env file in current directory"
echo ""

# Update system
echo "=== System Setup ==="
sudo apt-get update

# Install Python and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas requests python-dotenv google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

# Create directory for the script
mkdir -p ~/nemo_automation
cd ~/nemo_automation

# Create virtual environment
python3 -m venv .venv
echo "Created virtual environment"

# Activate virtual environment and install packages
source .venv/bin/activate
if [ -f requirements.txt ]; then
    pip install -r requirements.txt
    echo "Installed Python packages from requirements.txt"
else
    echo "Warning: requirements.txt not found, installing default packages"
    pip install pandas requests python-dotenv google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
fi

# Copy .env file from original directory
cp "$ORIGINAL_DIR/.env" .env
echo "Copied .env file to ~/nemo_automation/"

# Copy script files from original directory
if [ -f "$ORIGINAL_DIR/nemo_to_drive.py" ]; then
    cp "$ORIGINAL_DIR/nemo_to_drive.py" .
    echo "Copied nemo_to_drive.py to ~/nemo_automation/"
else
    echo "Warning: nemo_to_drive.py not found in current directory"
    echo "Please upload nemo_to_drive.py to ~/nemo_automation/ manually"
fi

if [ -f "$ORIGINAL_DIR/credentials.json" ]; then
    cp "$ORIGINAL_DIR/credentials.json" .
    echo "Copied credentials.json to ~/nemo_automation/"
else
    echo "Warning: credentials.json not found in current directory"
    echo "Please upload credentials.json to ~/nemo_automation/ manually"
fi

if [ -f "$ORIGINAL_DIR/requirements.txt" ]; then
    cp "$ORIGINAL_DIR/requirements.txt" .
    echo "Copied requirements.txt to ~/nemo_automation/"
else
    echo "Warning: requirements.txt not found in current directory"
    echo "Please upload requirements.txt to ~/nemo_automation/ manually"
fi

# Copy your script files here (you'll need to upload them)
# nemo_to_drive.py
# credentials.json

# Make the script executable
chmod +x nemo_to_drive.py

# Create a wrapper script for cron that loads environment variables
cat > run_nemo_script.sh << 'EOF'
#!/bin/bash
cd ~/nemo_automation

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

python3 nemo_to_drive.py >> nemo_log.txt 2>&1
EOF

chmod +x run_nemo_script.sh

# Set up cron job to run twice a day (8 AM and 8 PM)
(crontab -l 2>/dev/null; echo "0 8,20 * * * ~/nemo_automation/run_nemo_script.sh") | crontab -

echo ""
echo "=== Setup Complete! ==="
echo "Cron job scheduled to run at 8 AM and 8 PM daily"
echo "Logs will be saved to ~/nemo_automation/nemo_log.txt"
echo ""
echo "Environment variables loaded from existing .env file"
echo ""
echo "Next steps:"
echo "1. Upload your nemo_to_drive.py file to ~/nemo_automation/"
echo "2. Upload your credentials.json file to ~/nemo_automation/"
echo "3. Test the script manually: python3 nemo_to_drive.py"
echo ""
echo "To verify setup, run: ./check_status.sh" 