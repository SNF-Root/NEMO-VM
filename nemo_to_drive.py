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

"""What this script needs to do:
1. Fetch billing data from Nemo a couple time a day
2. Save the billing data to a csv file
3. Upload the csv file to Google Drive
4. Delete the csv file from the local directory after uploading to Google Drive
It should upload the data to a folder in the drive based on the month and year of the data
i.e. all the data for one month should be in one csv file, and then at the beggining of the next month we create a new csv file and upload it to the drive
"""

# Google Drive API setup
SCOPES = ['https://www.googleapis.com/auth/drive.file']
BASE_URL = "https://nemo.stanford.edu/api/billing/billing_data/"

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
        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)
        
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
        
        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        print(f"File uploaded to Google Drive with ID: {file.get('id')}")
        return file.get('id')

def fetch_billing_data(start_date, end_date, token):
    """Fetch billing data from Nemo API"""
    base_url = BASE_URL
    
    headers = {
        "Authorization": f"Token {token}"
    }
    
    try:
        # Make the GET request
        response = requests.get(f"{base_url}?start={start_date}&end={end_date}", headers=headers)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        print(f"Successfully fetched {len(data)} billing records")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return None

def save_to_csv(data, filename):
    """Save billing data to CSV file"""
    if not data:
        print("No data to save")
        return False
    
    try:
        # Convert to DataFrame and save to CSV
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        return True
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def cleanup_local_file(filename):
    """Delete local CSV file after upload"""
    try:
        os.remove(filename)
        print(f"Local file {filename} deleted successfully")
    except Exception as e:
        print(f"Error deleting local file: {e}")

def get_base_url_descriptor(base_url):
    """Return a descriptor string based on the base URL for use in filenames."""
    # Extract the last non-empty part of the path as a descriptor
    parsed = urllib.parse.urlparse(base_url)
    path_parts = [p for p in parsed.path.split('/') if p]
    if path_parts:
        return path_parts[-1].replace('-', '_')
    return 'data'

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
    # Use the same base_url as in fetch_billing_data
    descriptor = get_base_url_descriptor(BASE_URL)
    billing_data = fetch_billing_data(start_date_str, end_date_str, token)
    if not billing_data:
        print(f"No data for {start_date.strftime('%B %Y')}, skipping upload.")
        return
    filename = f"{descriptor}_{year}_{month:02d}.csv"
    if not save_to_csv(billing_data, filename):
        print(f"Failed to save data for {start_date.strftime('%B %Y')}")
        return
    try:
        upload_to_drive(service, filename, parent_folder_id, filename)
        cleanup_local_file(filename)
        print(f"Uploaded and cleaned up {filename}")
    except Exception as e:
        print(f"Error uploading {filename}: {e}")

def batch_upload_all_months():
    load_dotenv()
    token = os.getenv('NEMO_TOKEN')
    shared_drive_id = os.getenv('GDRIVE_PARENT_ID')
    if not token:
        print("Error: NEMO_TOKEN not found in environment variables")
        return
    if not shared_drive_id:
        print("Error: GDRIVE_PARENT_ID not found in environment variables")
        return
    try:
        service = authenticate_google_drive()
    except Exception as e:
        print(f"Failed to authenticate with Google Drive: {e}")
        return
    # Start from January 2024 to current month
    current = datetime.now()
    year = 2024
    month = 1
    while (year < current.year) or (year == current.year and month <= current.month):
        # Get or create the target folder path for this month
        target_folder_id = get_target_folder_path(service, shared_drive_id, year, month)
        process_month(service, token, year, month, target_folder_id)
        # Move to next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    print("\nBatch upload complete!")

def get_or_create_folder(service, parent_id, folder_name):
    """Get or create a folder with the given name in the parent folder"""
    print(f"Looking for folder '{folder_name}' in parent ID: {parent_id}")
    
    # Check if folder already exists
    query = f"name='{folder_name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = results.get('files', [])
    
    if files:
        # Folder exists, return its ID
        print(f"Found existing folder '{folder_name}' with ID: {files[0]['id']}")
        return files[0]['id']
    else:
        # Create new folder
        print(f"Creating new folder '{folder_name}' in parent ID: {parent_id}")
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        
        folder = service.files().create(
            body=folder_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        print(f"Created folder: {folder_name} with ID: {folder.get('id')}")
        return folder.get('id')

def get_target_folder_path(service, shared_drive_id, year, month):
    """Get or create the folder path: Year/Billing_Data"""
    # Create or get year folder
    year_folder_name = str(year)
    year_folder_id = get_or_create_folder(service, shared_drive_id, year_folder_name)
    
    # Create or get billing_data folder inside year folder
    billing_folder_name = "Billing_Data"
    billing_folder_id = get_or_create_folder(service, year_folder_id, billing_folder_name)
    
    return billing_folder_id


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
    
    print(f"Fetching billing data from {start_date_str} to {end_date_str}")
    
    # Fetch billing data
    descriptor = get_base_url_descriptor(BASE_URL)
    billing_data = fetch_billing_data(start_date_str, end_date_str, token)
    
    if not billing_data:
        print("Failed to fetch billing data")
        return
    
    # Create filename based on month and year
    filename = f"{descriptor}_{start_date.year}_{start_date.month:02d}.csv"
    
    # Save to CSV
    if not save_to_csv(billing_data, filename):
        print("Failed to save data to CSV")
        return
    
    # Authenticate with Google Drive
    try:
        service = authenticate_google_drive()
    except Exception as e:
        print(f"Failed to authenticate with Google Drive: {e}")
        print("Make sure you have credentials.json file in the same directory")
        return
    
    # Get or create the target folder path: Year/Billing_Data
    target_folder_id = get_target_folder_path(service, shared_drive_id, start_date.year, start_date.month)
    print(f"Target folder ID: {target_folder_id}")
    
    # Upload to Google Drive
    try:
        upload_to_drive(service, filename, target_folder_id, filename)
        
        # Clean up local file
        cleanup_local_file(filename)
        
        print("Process completed successfully!")
        
    except Exception as e:
        print(f"Error uploading to Google Drive: {e}")
    
    # Print execution time
    execution_time = time.time() - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    # Uncomment the next line to run the batch upload for all months
    #batch_upload_all_months()
    main()