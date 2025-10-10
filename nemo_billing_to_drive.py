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

def update_master_csv_for_year(service, token, year, shared_drive_id):
    """Update the master CSV file for a year with the latest data"""
    print(f"\nUpdating master CSV for {year}...")
    
    # Get current month's data (same logic as main function)
    current = datetime.now()
    if year == current.year:
        start_date = current.replace(day=1)
        end_date = current
    else:
        # For past years, get the full year
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
    
    start_date_str = start_date.strftime('%m/%d/%Y')
    end_date_str = end_date.strftime('%m/%d/%Y')
    
    print(f"Fetching latest data from {start_date_str} to {end_date_str}...")
    latest_data = fetch_billing_data(start_date_str, end_date_str, token)
    
    if not latest_data:
        print(f"No new data found for {year}")
        return
    
    # Create master CSV filename
    descriptor = get_base_url_descriptor(BASE_URL)
    master_filename = f"{descriptor}_{year}_master.csv"
    
    # Get or create master folder
    year_folder_id = get_or_create_folder(service, shared_drive_id, str(year))
    master_folder_id = get_or_create_folder(service, year_folder_id, "Master_CSV")
    
    # Check if master CSV already exists
    query = f"name='{master_filename}' and '{master_folder_id}' in parents"
    results = service.files().list(q=query, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    existing_files = results.get('files', [])
    
    if existing_files:
        # Download existing master CSV
        print("Downloading existing master CSV...")
        file_id = existing_files[0]['id']
        request = service.files().get_media(fileId=file_id)
        
        with open(master_filename, 'wb') as f:
            f.write(request.execute())
        
        # Read existing data
        try:
            existing_df = pd.read_csv(master_filename)
            print(f"Loaded existing master CSV with {len(existing_df)} records")
            
            # Convert new data to DataFrame
            new_df = pd.DataFrame(latest_data)
            
            # Combine dataframes, removing duplicates based on all columns
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates()
            
            print(f"Combined data: {len(combined_df)} total records ({len(combined_df) - len(existing_df)} new)")
            
        except Exception as e:
            print(f"Error reading existing CSV, creating new one: {e}")
            combined_df = pd.DataFrame(latest_data)
    else:
        # Create new master CSV
        print("Creating new master CSV...")
        combined_df = pd.DataFrame(latest_data)
    
    # Save updated master CSV
    if save_to_csv(combined_df.to_dict('records'), master_filename):
        print(f"Master CSV updated: {master_filename} with {len(combined_df)} total records")
        
        try:
            upload_to_drive(service, master_filename, master_folder_id, master_filename)
            cleanup_local_file(master_filename)
            print(f"Master CSV uploaded successfully!")
        except Exception as e:
            print(f"Error uploading master CSV: {e}")
    else:
        print("Failed to update master CSV")

def create_master_csv_for_year(service, token, year, shared_drive_id):
    """Create a master CSV file for an entire year by fetching all months at once"""
    print(f"\nCreating master CSV for {year}...")
    
    # Get all monthly data for the year
    all_data = []
    current = datetime.now()
    
    # Determine which months to process
    if year == current.year:
        max_month = current.month
    else:
        max_month = 12
    
    for month in range(1, max_month + 1):
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        print(f"Fetching data for {start_date.strftime('%B %Y')}...")
        monthly_data = fetch_billing_data(start_date_str, end_date_str, token)
        
        if monthly_data:
            all_data.extend(monthly_data)
            print(f"  Added {len(monthly_data)} records")
        else:
            print(f"  No data for {start_date.strftime('%B %Y')}")
    
    if not all_data:
        print(f"No data found for {year}")
        return
    
    # Create master CSV filename
    descriptor = get_base_url_descriptor(BASE_URL)
    master_filename = f"{descriptor}_{year}_master.csv"
    
    # Save master CSV
    if save_to_csv(all_data, master_filename):
        print(f"Master CSV created: {master_filename} with {len(all_data)} total records")
        
        # Upload to Google Drive in the year folder
        year_folder_id = get_or_create_folder(service, shared_drive_id, str(year))
        master_folder_id = get_or_create_folder(service, year_folder_id, "Master_CSV")
        
        try:
            upload_to_drive(service, master_filename, master_folder_id, master_filename)
            cleanup_local_file(master_filename)
            print(f"Master CSV uploaded successfully!")
        except Exception as e:
            print(f"Error uploading master CSV: {e}")
    else:
        print("Failed to create master CSV")

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
        
        print("Monthly CSV process completed successfully!")
        
        # Update master CSV for current year
        print("\nUpdating master CSV...")
        update_master_csv_for_year(service, token, start_date.year, shared_drive_id)
        
    except Exception as e:
        print(f"Error uploading to Google Drive: {e}")
    
    # Print execution time
    execution_time = time.time() - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")

def update_master_csvs_for_years():
    """Update master CSV files for multiple years with latest data"""
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
    
    # Update master CSVs for 2024 and 2025
    for year in [2024, 2025]:
        update_master_csv_for_year(service, token, year, shared_drive_id)
    
    print("\nMaster CSV update complete!")

def create_master_csvs_for_years():
    """Create master CSV files for multiple years"""
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
    
    # Create master CSVs for 2024 and 2025
    for year in [2024, 2025]:
        create_master_csv_for_year(service, token, year, shared_drive_id)
    
    print("\nMaster CSV creation complete!")

if __name__ == "__main__":
    # Uncomment the next line to run the batch upload for all months
    #batch_upload_all_months()
    
    # Uncomment the next line to create master CSV files for all years
    create_master_csvs_for_years()
    
    # Uncomment the next line to update master CSV files for all years
    #update_master_csvs_for_years()
    
    main()