# Nemo to Google Drive Billing Data Script

This script automatically fetches billing data from the Nemo API and uploads it to Google Drive with monthly folder organization.

## Features

- Fetches billing data from Nemo API
- Saves data to CSV format
- Uploads to Google Drive with monthly folder organization (YYYY_MM format)
- Automatically cleans up local files after upload
- Handles date ranges for current month data

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set up Google Drive API

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
   - Place `credentials.json` in the same directory as the script

### 3. Set up Environment Variables

Create a `.env` file in the same directory with your Nemo API token:

```
NEMO_TOKEN=your_nemo_api_token_here
```

### 4. First Run

On the first run, the script will:
1. Open a browser window for Google Drive authentication
2. Ask you to log in to your Google account
3. Grant permissions to access Google Drive
4. Save authentication tokens for future runs

## Usage

Run the script:

```bash
python nemo_to_drive.py
```

## How it Works

1. **Date Range**: The script automatically determines the date range for the current month
2. **Data Fetching**: Fetches billing data from Nemo API for the specified date range
3. **CSV Creation**: Converts the data to CSV format with filename `billing_data_YYYY_MM.csv`
4. **Folder Organization**: Creates or uses existing Google Drive folder named `YYYY_MM`
5. **Upload**: Uploads the CSV file to the appropriate monthly folder
6. **Cleanup**: Deletes the local CSV file after successful upload

## File Structure

- `nemo_to_drive.py` - Main script
- `requirements.txt` - Python dependencies
- `credentials.json` - Google Drive API credentials (you need to add this)
- `.env` - Environment variables (you need to add this)
- `token.pickle` - Google authentication tokens (created automatically)

## Scheduling

To run this script multiple times per day, you can use:

- **Cron (Linux/Mac)**: Add to crontab
- **Task Scheduler (Windows)**: Create a scheduled task
- **Cloud services**: Use services like AWS Lambda, Google Cloud Functions, etc.

Example cron job to run twice daily:
```bash
0 9,17 * * * /path/to/python /path/to/nemo_to_drive.py
```

## Error Handling

The script includes comprehensive error handling for:
- Missing environment variables
- API authentication failures
- Network connectivity issues
- Google Drive upload failures
- File system operations

## Notes

- The script creates monthly folders automatically
- Authentication tokens are cached locally for convenience
- Local CSV files are automatically cleaned up after upload
- The script handles month transitions automatically 