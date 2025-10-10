import pandas as pd
from datetime import datetime, timedelta
import requests
import json
from dotenv import load_dotenv
import os
import time
import csv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
import pickle
from dateutil.relativedelta import relativedelta
import urllib.parse
import utils

"""Usage Events Data Processing and Upload Script

This script processes historical usage events data from the Nemo API and uploads it to Google Drive.

FEATURES:
- Fetches all usage events data from Nemo API
- Processes data by month and tool
- Filters and cleans data (removes unwanted columns, formats JSON)
- Cross-references tool and user IDs with lookup tables
- Creates Excel files with auto-formatted columns
- Uploads to Google Drive with organized folder structure: Year/Usage_Events/Month/
- Splits data by tool into separate Excel files
- No local file storage - direct upload only

BATCH UPLOAD:
- Processes all data from January 2024 to current month
- Creates organized folder structure in Google Drive
- Provides detailed progress tracking and timing
- Handles large datasets efficiently

FOLDER STRUCTURE:
Shared Drive/
├── 2024/
│   └── Usage_Events/
│       ├── 01/ (January files by tool)
│       ├── 02/ (February files by tool)
│       └── ...
└── 2025/
    └── Usage_Events/
        └── ...
"""

# Google Drive API setup
SCOPES = ['https://www.googleapis.com/auth/drive.file']
BASE_URL = "https://nemo.stanford.edu/api/usage_events/"

def authenticate_google_drive():
    """Authenticate with Google Drive API using service account"""
    try:
        # Use service account credentials
        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        SERVICE_ACCOUNT_FILE = 'credentials.json'
        
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        print("Successfully authenticated with service account")
        return build('drive', 'v3', credentials=credentials)
        
    except Exception as e:
        print(f"Service account authentication failed: {e}")
        print("\nTo fix this issue:")
        print("1. Make sure credentials.json contains your service account key")
        print("2. Ensure the service account has Editor role on the target Google Drive")
        print("3. Verify the service account email has access to the target folder")
        raise

def upload_to_drive(service, file_path, folder_id, filename):
    """Upload file to Google Drive in the specified folder, overwriting if exists"""
    
    # Check if file already exists
    query = f"name='{filename}' and '{folder_id}' in parents"
    results = service.files().list(q=query, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = results.get('files', [])
    
    if files:
        # File exists, update it
        file_id = files[0]['id']
        media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
        
        file = service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        
        print(f"File updated in Google Drive with ID: {file.get('id')}")
        return file.get('id')
    else:
        # File doesn't exist, create new one
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        print(f"File uploaded to Google Drive with ID: {file.get('id')}")
        return file.get('id')

def fetch_usage_events_data(start_date, end_date, token, target_year=None, target_month=None):
    """Fetch usage events data from Nemo API and filter by date if specified"""
    base_url = BASE_URL
    
    headers = {
        "Authorization": f"Token {token}"
    }
    
    try:
        # Make the GET request - usage events API doesn't support date parameters
        response = requests.get(base_url, headers=headers)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        print(f"Successfully fetched {len(data)} usage events records")
        
        # Load tool list for cross-referencing
        tool_mapping = utils.load_tool_list()
        
        # Load user list for cross-referencing
        user_mapping = utils.load_user_list()
        
        # Filter by date if target year and month are specified
        if target_year and target_month:
            data = utils.filter_usage_events_by_date(data, target_year, target_month)
        
        # Limit to most recent events (since API doesn't support pagination)
        data = utils.limit_to_recent_events(data, max_events=2000)
        
        # Filter out events without data in pre_run_data or run_data
        data = utils.filter_usage_events_with_data(data)
        
        # Remove unwanted columns
        data = utils.remove_unwanted_columns(data)
        
        # Add tool names
        data = utils.add_tool_names(data, tool_mapping)
        
        # Add user names and emails
        data = utils.add_user_info(data, user_mapping)
        
        # Remove numerical ID columns and operator columns
        data = utils.remove_id_and_operator_columns(data)
        
        # Format JSON fields for better readability
        data = utils.format_json_fields(data)
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return None

def get_date_range():
    """Get the date range for the current month"""
    today = datetime.now()
    start_of_month = today.replace(day=1)
    
    # If it's the first day of the month, get data from previous month
    if today.day == 1:
        start_of_month = (today - timedelta(days=1)).replace(day=1)
        end_of_month = today - timedelta(days=1)
    else:
        end_of_month = today
    
    return start_of_month, end_of_month

def process_month(service, token, year, month, parent_folder_id):
    start_date = datetime(year, month, 1)
    # Calculate end of month
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    start_date_str = start_date.strftime('%m/%d/%Y')
    end_date_str = end_date.strftime('%m/%d/%Y')
    print(f"\nProcessing {start_date.strftime('%B %Y')} ({start_date_str} to {end_date_str})")
    
    usage_events_data = fetch_usage_events_data(start_date_str, end_date_str, token, year, month)
    if not usage_events_data:
        print(f"No data for {start_date.strftime('%B %Y')}, skipping upload.")
        return
    
    # Split data by tool
    tool_groups = utils.split_data_by_tool(usage_events_data)
    
    # Process each tool group
    total_files_uploaded = 0
    for tool_name, tool_data in tool_groups.items():
        if not tool_data:
            continue
            
        # Create safe filename (replace special characters)
        safe_tool_name = "".join(c for c in tool_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_tool_name = safe_tool_name.replace(' ', '_')
        
        # Create filename for this tool
        filename = f"{safe_tool_name}_{year}_{month:02d}.xlsx"
        
        # Save to Excel
        excel_filename = utils.save_to_excel(tool_data, filename)
        if not excel_filename:
            print(f"Failed to save data for tool: {tool_name}")
            continue
        
        # Upload to Google Drive
        try:
            upload_to_drive(service, excel_filename, parent_folder_id, excel_filename)
            print(f"Uploaded {excel_filename} ({len(tool_data)} events)")
            total_files_uploaded += 1
        except Exception as e:
            print(f"Error uploading {excel_filename}: {e}")
    
    print(f"Total files uploaded for {start_date.strftime('%B %Y')}: {total_files_uploaded}")
    return total_files_uploaded

def batch_upload_all_months():
    # Start timer
    start_time = time.time()
    
    print("🚀 Starting batch upload of all historical usage events data...")
    print("=" * 60)
    
    load_dotenv()
    token = os.getenv('NEMO_TOKEN')
    shared_drive_id = os.getenv('GDRIVE_PARENT_ID')
    
    if not token:
        print("Error: NEMO_TOKEN not found in environment variables")
        return
    if not shared_drive_id:
        print("Error: GDRIVE_PARENT_ID not found in environment variables")
        return
    
    print(f"Using shared drive ID: {shared_drive_id}")
    
    try:
        service = authenticate_google_drive()
    except Exception as e:
        print(f"Failed to authenticate with Google Drive: {e}")
        return
    
    # Start from January 2024 to current month
    current = datetime.now()
    year = 2024
    month = 1
    total_months_processed = 0
    total_files_uploaded = 0
    
    print(f"Processing data from January 2024 to {current.strftime('%B %Y')}")
    print("=" * 60)
    
    while (year < current.year) or (year == current.year and month <= current.month):
        month_start_time = time.time()
        
        print(f"\n📅 Processing {datetime(year, month, 1).strftime('%B %Y')}...")
        
        # Get or create the target folder path for this month
        target_folder_id = utils.get_target_folder_path(service, shared_drive_id, year, month)
        
        # Process this month and get the number of files uploaded
        files_uploaded = process_month(service, token, year, month, target_folder_id)
        total_files_uploaded += files_uploaded
        total_months_processed += 1
        
        month_time = time.time() - month_start_time
        print(f"✅ {datetime(year, month, 1).strftime('%B %Y')} completed in {month_time:.2f} seconds ({files_uploaded} files)")
        
        # Move to next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    
    total_time = time.time() - start_time
    print("\n" + "=" * 60)
    print("🎉 BATCH UPLOAD COMPLETE!")
    print(f"📊 Summary:")
    print(f"   • Months processed: {total_months_processed}")
    print(f"   • Total files uploaded: {total_files_uploaded}")
    print(f"   • Total execution time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    print("=" * 60)


def main():
    # Start timer
    start_time = time.time()
    
    # Load environment variables
    load_dotenv()
    token = os.getenv('NEMO_TOKEN')
    shared_drive_id = os.getenv('GDRIVE_PARENT_ID')
    
    if not token:
        print("Error: NEMO_TOKEN not found in environment variables")
        return
    if not shared_drive_id:
        print("Error: GDRIVE_PARENT_ID not found in environment variables")
        return
    
    print(f"Using shared drive ID: {shared_drive_id}")
    
    # Get date range for current month
    start_date, end_date = get_date_range()
    
    # Format dates for API
    start_date_str = start_date.strftime('%m/%d/%Y')
    end_date_str = end_date.strftime('%m/%d/%Y')
    
    print(f"Fetching all usage events data")
    
    # Fetch usage events data (API returns all data, not filtered by date)
    descriptor = utils.get_base_url_descriptor(BASE_URL)
    usage_events_data = fetch_usage_events_data(start_date_str, end_date_str, token, start_date.year, start_date.month)
    
    if not usage_events_data:
        print("Failed to fetch usage events data")
        return
    
    # Split data by tool
    tool_groups = utils.split_data_by_tool(usage_events_data)
    
    # Authenticate with Google Drive
    try:
        service = authenticate_google_drive()
    except Exception as e:
        print(f"Failed to authenticate with Google Drive: {e}")
        print("Make sure you have credentials.json file in the same directory")
        return
    
    # Get or create the target folder path: Year/Usage_Events/Month
    target_folder_id = utils.get_target_folder_path(service, shared_drive_id, start_date.year, start_date.month)
    print(f"Target folder ID: {target_folder_id}")
    
    # Process each tool group
    total_files_uploaded = 0
    for tool_name, tool_data in tool_groups.items():
        if not tool_data:
            continue
            
        # Create safe filename (replace special characters)
        safe_tool_name = "".join(c for c in tool_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_tool_name = safe_tool_name.replace(' ', '_')
        
        # Create filename for this tool
        filename = f"{safe_tool_name}_{start_date.year}_{start_date.month:02d}.xlsx"
        
        # Save to Excel
        excel_filename = utils.save_to_excel(tool_data, filename)
        if not excel_filename:
            print(f"Failed to save data for tool: {tool_name}")
            continue
        
        # Upload to Google Drive
        try:
            upload_to_drive(service, excel_filename, target_folder_id, excel_filename)
            
            print(f"Successfully uploaded {excel_filename} ({len(tool_data)} events)")
            total_files_uploaded += 1
            
        except Exception as e:
            print(f"Error uploading {excel_filename}: {e}")
    
    print(f"\nProcess completed successfully!")
    print(f"Total files uploaded: {total_files_uploaded}")
    
    # Print execution time
    execution_time = time.time() - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    # Run the batch upload for all months
    batch_upload_all_months()
    # Uncomment the next line to run just the current month instead
    #main()