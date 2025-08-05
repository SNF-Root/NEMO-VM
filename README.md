# Nemo to Google Drive Billing Data Automation

This project automatically fetches billing data from the Nemo API and uploads it to Google Drive with monthly folder organization. It includes automated VM setup scripts for easy deployment.

## Features

- Fetches billing data from Nemo API
- Saves data to CSV format with monthly organization (YYYY_MM format)
- Uploads to Google Drive with automatic folder creation
- Automated VM setup with virtual environment isolation
- Cron job scheduling for hands-off operation
- Comprehensive error handling and logging
- Automatic cleanup of local files after upload

## Quick Start (VM Deployment)

### Prerequisites
- Ubuntu/Debian VM with SSH access
- Your Nemo API token
- Google Drive API credentials
- Google Drive parent folder ID

### 1. Prepare Your Files Locally

Ensure you have these files in your local directory:
```
Sanity-Check/
├── setup_vm.sh
├── nemo_to_drive.py
├── credentials.json
├── .env
└── requirements.txt
```

### 2. Set Up Environment Variables

Create a `.env` file with your credentials:
```
NEMO_TOKEN=your_nemo_api_token_here
GDRIVE_PARENT_ID=your_google_drive_folder_id_here
```

### 3. Set Up Google Drive API

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Drive API:
   - Go to "APIs & Services" > "Library"
   - Search for "Google Drive API"
   - Click on it and press "Enable"
4. Create credentials:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" → "OAuth 2.0 Client IDs"
   - Choose "Desktop application"
   - **IMPORTANT**: Under "Authorized redirect URIs", add: `http://localhost:8080/`
   - Click "Create"
   - Download the credentials file and rename it to `credentials.json`

### 4. Deploy to VM

```bash
# Transfer files to VM
scp setup_vm.sh nemo_to_drive.py credentials.json .env requirements.txt user@your-vm-ip:~/

# SSH into VM
ssh user@your-vm-ip

# Make setup script executable and run it
chmod +x setup_vm.sh
./setup_vm.sh
```

### 5. Verify Setup

```bash
# Check automation status
./check_status.sh

# Test the script manually
cd ~/nemo_automation
source .venv/bin/activate
python nemo_to_drive.py
```

## What the Setup Script Does

The `setup_vm.sh` script automatically:

1. **System Setup**: Updates packages and installs Python
2. **Virtual Environment**: Creates isolated Python environment
3. **Dependencies**: Installs packages from `requirements.txt`
4. **File Management**: Copies all necessary files to `~/nemo_automation/`
5. **Security**: Sets appropriate file permissions
6. **Scheduling**: Creates cron job to run twice daily (8 AM and 8 PM)
7. **Environment**: Configures wrapper script with proper environment variables

## File Structure After Setup

```
~/nemo_automation/
├── nemo_to_drive.py          # Main automation script
├── credentials.json          # Google Drive API credentials
├── .env                      # Environment variables
├── requirements.txt          # Python dependencies
├── .venv/                    # Virtual environment
├── run_nemo_script.sh        # Cron wrapper script
└── nemo_log.txt              # Execution logs (created after first run)
```

## Monitoring and Maintenance

### Check Status
```bash
./check_status.sh
```

### View Logs
```bash
tail -f ~/nemo_automation/nemo_log.txt
```

### Manual Execution
```bash
cd ~/nemo_automation
source .venv/bin/activate
python nemo_to_drive.py
```

### Update Cron Jobs
```bash
crontab -e
```

## Security Considerations

- Credentials are stored in `~/nemo_automation/` with restricted permissions
- Virtual environment isolates dependencies
- Cron job runs as your user account
- Files are readable only by the owner

## Troubleshooting

### Common Issues

**ModuleNotFoundError**: Activate virtual environment first
```bash
cd ~/nemo_automation
source .venv/bin/activate
```

**Permission Denied**: Check file permissions
```bash
chmod 600 ~/nemo_automation/.env
chmod 600 ~/nemo_automation/credentials.json
```

**Cron Job Not Running**: Check cron service
```bash
sudo systemctl status cron
```

### Duplicate Cron Jobs
If you ran setup multiple times:
```bash
crontab -e  # Remove duplicate entries
```

## Local Development

For local development without VM deployment:

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run script
python nemo_to_drive.py
```

## How It Works

1. **Scheduling**: Cron job runs twice daily at 8 AM and 8 PM
2. **Environment**: Virtual environment loads with all dependencies
3. **Authentication**: Uses cached Google Drive tokens or prompts for new ones
4. **Data Fetching**: Retrieves billing data from Nemo API for current month
5. **Processing**: Converts data to CSV format
6. **Upload**: Creates monthly folder and uploads CSV to Google Drive
7. **Cleanup**: Removes local CSV file after successful upload
8. **Logging**: Records execution details to log file

## Support

For issues or questions:
1. Check the log file: `~/nemo_automation/nemo_log.txt`
2. Run status check: `./check_status.sh`
3. Test manually: `python nemo_to_drive.py` 