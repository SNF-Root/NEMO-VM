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
    """Authenticate with Google Drive API"""
    creds = None
    
    # Load existing credentials if available
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials available, let user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                # Use a fixed port for consistency
                creds = flow.run_local_server(port=8080)
            except Exception as e:
                print(f"OAuth authentication failed: {e}")
                print("\nTo fix this issue:")
                print("1. Go to Google Cloud Console (https://console.cloud.google.com/)")
                print("2. Select your project")
                print("3. Go to 'APIs & Services' > 'Credentials'")
                print("4. Find your OAuth 2.0 Client ID and click on it")
                print("5. Under 'Authorized redirect URIs', add: http://localhost:8080/")
                print("6. Save the changes")
                print("7. Download the updated credentials.json file")
                print("8. Replace your current credentials.json with the new one")
                raise
        
        # Save credentials for next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(service, file_path, folder_id, filename):
    """Upload file to Google Drive in the specified folder, overwriting if exists"""
    
    # Check if file already exists
    query = f"name='{filename}' and '{folder_id}' in parents"
    results = service.files().list(q=query).execute()
    files = results.get('files', [])
    
    if files:
        # File exists, update it
        file_id = files[0]['id']
        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)
        
        file = service.files().update(
            fileId=file_id,
            media_body=media
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
            fields='id'
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
    parent_folder_id = os.getenv('GDRIVE_PARENT_ID')
    if not token:
        print("Error: NEMO_TOKEN not found in environment variables")
        return
    if not parent_folder_id:
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
        process_month(service, token, year, month, parent_folder_id)
        # Move to next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    print("\nBatch upload complete!")



def main():
    # Start timer
    start_time = time.time()
    
    # Load environment variables
    load_dotenv()
    token = os.getenv('NEMO_TOKEN')
    parent_folder_id = os.getenv('GDRIVE_PARENT_ID')
    
    if not token:
        print("Error: NEMO_TOKEN not found in environment variables")
        return
    if not parent_folder_id:
        print("Error: GDRIVE_PARENT_ID not found in environment variables")
        return
    
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
    


    # Upload to Google Drive
    try:
        upload_to_drive(service, filename, parent_folder_id, filename)
        
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