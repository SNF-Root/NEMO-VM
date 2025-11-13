import pandas as pd
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import os
import time

#start a timer
start_time = time.time()

load_dotenv()
token = os.getenv('NEMO_TOKEN')

start_date = '11/01/2025'
end_date = '11/02/2025'


headers = {
    "Authorization": f"Token {token}"
}

def fetch_billing_data():
    # Base URL
    base_url = "https://nemo.stanford.edu/api/billing/billing_data/"
    #base_url = "https://nemo.stanford.edu/api/reservations/"
    
    try:
        # Make the GET request with params (automatically URL-encodes dates)
        response = requests.get(base_url, params={'start': start_date, 'end': end_date}, headers=headers)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Pretty print the JSON data
        print(json.dumps(data, indent=2))
        
        # Optionally, save to a file
        with open('billing_data.json', 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"\nData has been saved to billing_data.json")
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def process_json_data():
    # Read the JSON file
    current_month_name = datetime.now().strftime('%B')
    current_year = datetime.now().year
    
    print("Reading JSON file...")
    try:
        with open('billing_data.json', 'r') as f:
            data = pd.read_json(f)
            # Convert the data list to a DataFrame 
            if isinstance(data, dict) and 'data' in data:
                df = pd.DataFrame(data['data'])
            else:
                df = pd.DataFrame(data)
    except FileNotFoundError:
        print("Error: billing_data.json file not found")
        return None
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None

    # Basic data cleaning
    print("\nCleaning data...")
    # Remove any completely empty rows
    df = df.dropna(how='all')
    # Remove any completely empty columns
    df = df.dropna(axis=1, how='all')
    df.drop(['account_id','project_id','department','department_id','application','reference_po','rate_category','validated','waived'], axis=1, inplace=True)
    df['hours']=round(df['amount']/60,2)
    # Format datetime columns
    date_columns = ['start', 'end']
    for col in date_columns:
        if col in df.columns:
            # Convert to datetime using ISO8601 format
            df[col] = pd.to_datetime(df[col], format='ISO8601')
            # Format as YYYY-MM-DD HH:MM:SS
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Separate data by item_type
    print("\nSeparating data by item type...")
    item_types = df['item_type'].unique()
    print(f"Found item types: {item_types}")
    
    # Create Excel writer object
    with pd.ExcelWriter(f'{current_month_name}_{current_year}_sanity_check.xlsx', datetime_format='YYYY-MM-DD HH:MM:SS') as writer:
        # Save each item type to a separate sheet
        for item_type in item_types:
            sheet_name = f"{item_type}_data"
            # Filter data for current item type
            filtered_df = df[df['item_type'] == item_type]
            # Sort by amount in descending order
            filtered_df = filtered_df.sort_values('amount', ascending=False)
            
            # Save to Excel sheet
            filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"All data has been saved to {current_month_name}_{current_year}_sanity_check.xlsx")
    
    return df

fetch_billing_data()
process_json_data()


#stop the timer
end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")