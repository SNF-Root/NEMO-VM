import requests
import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from usage_questions import limit_to_recent_events, filter_usage_events_with_data, remove_unwanted_columns, format_json_fields

# Load environment variables
load_dotenv()
token = os.getenv('NEMO_TOKEN')

if not token:
    print("ERROR: NEMO_TOKEN not found in environment variables")
    exit(1)

def limit_to_recent_events(data, max_events=1000):
    """Limit processing to the most recent events (highest IDs)"""
    if len(data) <= max_events:
        print(f"Processing all {len(data)} events (within limit)")
        return data
    
    # Sort by ID (highest first) and take the most recent
    sorted_data = sorted(data, key=lambda x: x.get('id', 0), reverse=True)
    limited_data = sorted_data[:max_events]
    
    print(f"Limited from {len(data)} to {len(limited_data)} most recent events")
    return limited_data

def filter_usage_events_with_data(data):
    """Filter out usage events that have empty pre_run_data and run_data fields"""
    filtered_data = []
    
    for event in data:
        pre_run_data = event.get('pre_run_data', '')
        run_data = event.get('run_data', '')
        
        # Keep events that have data in either pre_run_data or run_data
        if pre_run_data or run_data:
            filtered_data.append(event)
    
    print(f"Filtered from {len(data)} to {len(filtered_data)} events with data")
    return filtered_data

def remove_unwanted_columns(data):
    """Remove unwanted columns from usage events data"""
    columns_to_remove = ['validated', 'remote_work', 'training', 'validated_by', 'waived_by']
    
    for event in data:
        for column in columns_to_remove:
            if column in event:
                del event[column]
    
    print(f"Removed columns: {', '.join(columns_to_remove)}")
    return data

def format_json_fields(data):
    """Format JSON fields to make them more readable in CSV"""
    for event in data:
        # Format pre_run_data
        if event.get('pre_run_data'):
            try:
                # Try to parse and pretty-print JSON
                json_data = json.loads(event['pre_run_data'])
                event['pre_run_data'] = json.dumps(json_data, indent=2, separators=(',', ': '))
            except (json.JSONDecodeError, TypeError):
                # If it's not valid JSON, keep as is
                pass
        
        # Format run_data
        if event.get('run_data'):
            try:
                # Try to parse and pretty-print JSON
                json_data = json.loads(event['run_data'])
                event['run_data'] = json.dumps(json_data, indent=2, separators=(',', ': '))
            except (json.JSONDecodeError, TypeError):
                # If it's not valid JSON, keep as is
                pass
    
    print("Formatted JSON fields for better readability")
    return data

# Test API endpoint
base_url = "https://nemo.stanford.edu/api/usage_events/"
headers = {"Authorization": f"Token {token}"}

try:
    # Fetch all data
    print("Fetching all usage events data...")
    response = requests.get(base_url, headers=headers)
    response.raise_for_status()
    data = response.json()
    print(f"Retrieved {len(data)} records")
    
    # Apply the same processing pipeline as usage_questions.py
    print("\n=== Processing Pipeline ===")
    
    # Step 1: Limit to recent events
    data = limit_to_recent_events(data, max_events=1000)
    
    # Step 2: Filter events with data
    data = filter_usage_events_with_data(data)
    
    # Step 3: Remove unwanted columns
    data = remove_unwanted_columns(data)
    
    # Step 4: Format JSON fields
    data = format_json_fields(data)
    
    # Step 5: Save to CSV
    if data:
        df = pd.DataFrame(data)
        filename = f"usage_events_test_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"\nSaved {len(data)} processed events to {filename}")
        
        # Show sample of processed data
        print(f"\nSample processed record:")
        print(data[0])
        
        # Show column names
        print(f"\nColumns in final CSV:")
        print(list(data[0].keys()))
    else:
        print("No data to save after processing")

except requests.exceptions.RequestException as e:
    print(f"Error connecting to API: {e}")
except Exception as e:
    print(f"Error processing data: {e}")
