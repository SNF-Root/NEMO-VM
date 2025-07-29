import requests
from dotenv import load_dotenv
import os
import threading
import time

def check_privileges():
    # Load environment variables
    load_dotenv()
    token = os.getenv('KAVEH_NEMO_TOKEN')
    print(token)
    
    if not token:
        print("Error: NEMO_TOKEN not found in environment variables")
        return
        
    # API endpoint
    url = "https://nemo.stanford.edu/api/reservations/"
    
    # Set up headers with token
    headers = {
        "Authorization": f"Token {token}"
    }

    # Timer function
    def print_timer(stop_event):
        elapsed = 0
        while not stop_event.is_set():
            time.sleep(2)
            elapsed += 2
            print(f"...waiting {elapsed} seconds...")

    stop_event = threading.Event()
    timer_thread = threading.Thread(target=print_timer, args=(stop_event,))
    timer_thread.start()
    
    try:
        # Make GET request to test access
        response = requests.head(url, headers=headers)
        stop_event.set()
        timer_thread.join()
        # Check response
        if response.status_code == 200:
            print("Token has access to reservations API")
            print(f"Response status: {response.status_code}")
        else:
            print("Token does not have sufficient privileges")
            print(f"Response status: {response.status_code}")
            print(f"Error message: {response.text}")
            
    except requests.exceptions.Timeout:
        stop_event.set()
        timer_thread.join()
        print("Request timed out. The server may be down or slow.")
    except requests.exceptions.RequestException as e:
        stop_event.set()
        timer_thread.join()
        print(f"Error making request: {e}")

if __name__ == "__main__":
    check_privileges()
