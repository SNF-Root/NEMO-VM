# 

import pandas as pd
import numpy as np
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()
token = os.getenv('NEMO_TOKEN')
print(f"Token loaded: {'Yes' if token else 'No'}")

# Set dates with time and timezone information
start_date = '2025-01-01T00:00:00-07:00'  # Start of day
end_date = '2025-05-26T23:59:59-07:00'    # End of day


headers = {
    "Authorization": f"Token {token}"
}

def fetch_reservations_data():
    # Base URL
    base_url = "https://nemo.stanford.edu/api/reservations/"
    
    # Parameters for the request
    params = {
        'start': start_date,
        'end': end_date
    }
    
    try:
        # Make the GET request with params
        response = requests.get(base_url, headers=headers, params=params)
        
        # Print response details for debugging
        print(f"\nResponse status code: {response.status_code}")
        print(f"Request URL: {response.url}")
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Print the structure of the response
        print("\nResponse structure:")
        print(f"Type of response: {type(data)}")
        print(f"Response content: {data}")
        
        if isinstance(data, list):
            print(f"\nNumber of reservations found: {len(data)}")
            if len(data) > 0:
                print("\nFirst reservation example:")
                print(json.dumps(data[0], indent=2))
        elif isinstance(data, dict):
            print(f"Keys in response: {data.keys()}")
            print(f"Count: {data.get('count', 'No count')}")
            print(f"Next page: {data.get('next', 'No next page')}")
            print(f"Number of results in this page: {len(data.get('results', []))}")
        
        # Save results to a file
        with open('reservations_data.json', 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"\nData has been saved to reservations_data.json")
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def test_api_access():
    # Test a different endpoint - this is just an example, we'll need to adjust based on available endpoints
    test_url = "https://nemo.stanford.edu/api/"
    try:
        response = requests.get(test_url, headers=headers)
        print(f"\nTesting API access:")
        print(f"Status code: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        if response.status_code == 200:
            print("Successfully connected to API")
        else:
            print(f"Error response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")

# Run both tests
print("\nTesting API access first:")
test_api_access()
print("\nNow testing reservations endpoint:")
fetch_reservations_data()