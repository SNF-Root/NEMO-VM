# Nemo to Google Drive Billing Data Automation

This project automatically fetches billing data from the Nemo API and uploads it to Google Drive with organized folder structure. It includes automated VM setup scripts for easy deployment using Google Service Account authentication.

## Features

- Fetches billing data from Nemo API
- Saves data to CSV format with monthly organization (YYYY_MM format)
- Uploads to Google Drive with automatic folder creation
- Uses Google Service Account for reliable headless authentication
- Automated VM setup with virtual environment isolation
- Cron job scheduling for hands-off operation
- Comprehensive error handling and logging
- Automatic cleanup of local files after upload
- Organized folder structure: Year/Billing_Data/

## Quick Start (VM Deployment)

### Prerequisites
- Ubuntu/Debian VM with SSH access
- Your Nemo API token
- Google Service Account credentials
- Google Shared Drive ID

### 1. Prepare Your Files Locally

Ensure you have these files in your local directory:
```
Sanity-Check/
├── setup_vm.sh
├── nemo_billing_to_drive.py
├── credentials.json
├── .env
└── requirements.txt
```

### 2. Set Up Environment Variables

Create a `.env` file with your credentials:
```
NEMO_TOKEN=your_nemo_api_token_here
GDRIVE_PARENT_ID=your_shared_drive_id_here
```

**Important**: The `.env` file format must be exact - no spaces around the `=` sign. Use `KEY=value`, not `KEY = value`.

### 3. Set Up Google Service Account

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Drive API:
   - Go to "APIs & Services" > "Library"
   - Search for "Google Drive API"
   - Click on it and press "Enable"
4. Create a Service Account:
   - Go to "IAM & Admin" > "Service Accounts"
   - Click "Create Service Account"
   - Give it a name like "nemo-automation"
   - Grant it "Editor" role for Google Drive
   - Create and download a JSON key file
   - Rename the downloaded file to `credentials.json`

### 4. Set Up Shared Drive

1. Go to [Google Drive](https://drive.google.com)
2. Click "Shared drives" in the left sidebar
3. Click "New" → "Shared drive"
4. Name it something like "Nemo Automation Data"
5. Add your service account email as a member with "Editor" role
6. Get the Shared Drive ID from the URL (after `/folders/`)

### 5. Deploy to VM

```bash
# Transfer files to VM
scp setup_vm.sh nemo_billing_to_drive.py credentials.json .env requirements.txt user@your-vm-ip:~/

# SSH into VM
ssh user@your-vm-ip

# Make setup script executable and run it
chmod +x setup_vm.sh
./setup_vm.sh
```

### 6. Verify Setup

```bash
# Check automation status
./check_status.sh

# Test the script manually
cd ~/nemo_automation
source .venv/bin/activate
python nemo_billing_to_drive.py
```

## What the Setup Script Does

The `setup_vm.sh` script automatically:

1. **System Setup**: Updates packages and installs Python
2. **Virtual Environment**: Creates isolated Python environment
3. **Dependencies**: Installs packages from `requirements.txt`
4. **File Management**: Copies all necessary files to `~/nemo_automation/`
5. **Security**: Sets appropriate file permissions
6. **Scheduling**: Creates cron job to run hourly
7. **Environment**: Configures wrapper script with proper environment variables

## File Structure After Setup

```
~/nemo_automation/
├── nemo_billing_to_drive.py          # Main automation script
├── credentials.json          # Google Service Account credentials
├── .env                      # Environment variables
├── requirements.txt          # Python dependencies
├── .venv/                    # Virtual environment
├── run_nemo_script.sh        # Cron wrapper script
└── nemo_log.txt              # Execution logs (created after first run)
```

## Google Drive Folder Structure

The script creates an organized folder hierarchy:

```
Shared Drive/
├── 2024/
│   └── Billing_Data/
│       ├── billing_data_2024_01.csv
│       ├── billing_data_2024_02.csv
│       └── ...
├── 2025/
│   └── Billing_Data/
│       ├── billing_data_2025_01.csv
│       ├── billing_data_2025_02.csv
│       └── ...
└── ...
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
python nemo_billing_to_drive.py
```

### Update Cron Jobs
```bash
crontab -e
```

## Security Considerations

- Service account credentials are stored securely with restricted permissions
- Virtual environment isolates dependencies
- Cron job runs as your user account
- Files are readable only by the owner
- Uses shared drives for better security and no storage quotas

## Troubleshooting

### Common Issues

**Service Account Authentication Failed**: Check credentials and permissions
```bash
# Verify service account has access to shared drive
# Check credentials.json contains service account key
```

**Shared Drive Access Denied**: Add service account to shared drive
```bash
# Go to shared drive → Manage members → Add service account email
# Grant "Editor" role
```

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
python nemo_billing_to_drive.py
```

## How It Works

1. **Scheduling**: Cron job runs hourly
2. **Environment**: Virtual environment loads with all dependencies
3. **Authentication**: Uses Google Service Account for reliable authentication
4. **Data Fetching**: Retrieves billing data from Nemo API for current month
5. **Processing**: Converts data to CSV format
6. **Folder Creation**: Creates Year/Billing_Data folder structure as needed
7. **Upload**: Uploads CSV to appropriate folder in shared drive
8. **Cleanup**: Removes local CSV file after successful upload
9. **Logging**: Records execution details to log file

## Support

For issues or questions:
1. Check the log file: `~/nemo_automation/nemo_log.txt`
2. Run status check: `./check_status.sh`
3. Test manually: `python nemo_billing_to_drive.py` 