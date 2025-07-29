#!/bin/bash

# Setup script for Nemo to Drive automation on VM

echo "Setting up Nemo to Drive automation..."

# Update system
sudo apt-get update

# Install Python and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas requests python-dotenv google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

# Create directory for the script
mkdir -p ~/nemo_automation
cd ~/nemo_automation

# Copy your script files here (you'll need to upload them)
# nemo_to_drive.py
# credentials.json
# .env file with your tokens

# Make the script executable
chmod +x nemo_to_drive.py

# Create a wrapper script for cron
cat > run_nemo_script.sh << 'EOF'
#!/bin/bash
cd ~/nemo_automation
python3 nemo_to_drive.py >> nemo_log.txt 2>&1
EOF

chmod +x run_nemo_script.sh

# Set up cron job to run twice a day (8 AM and 8 PM)
(crontab -l 2>/dev/null; echo "0 8,20 * * * ~/nemo_automation/run_nemo_script.sh") | crontab -

echo "Setup complete!"
echo "Cron job scheduled to run at 8 AM and 8 PM daily"
echo "Logs will be saved to ~/nemo_automation/nemo_log.txt"
echo ""
echo "Next steps:"
echo "1. Upload your nemo_to_drive.py file to ~/nemo_automation/"
echo "2. Upload your credentials.json file to ~/nemo_automation/"
echo "3. Create a .env file with your NEMO_TOKEN and GDRIVE_PARENT_ID"
echo "4. Test the script manually: python3 nemo_to_drive.py" 