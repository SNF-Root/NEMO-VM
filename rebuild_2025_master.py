#!/usr/bin/env python3
"""
Script to rebuild the 2025 master CSV file with full year data.
This will fetch all months of 2025 and create a complete master CSV.
"""

from nemo_billing_to_drive import (
    authenticate_google_drive,
    create_master_csv_for_year,
    load_dotenv
)
import os

def main():
    print("="*80)
    print("REBUILDING 2025 MASTER CSV WITH FULL YEAR DATA")
    print("="*80)
    
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
    
    # Authenticate with Google Drive
    try:
        service = authenticate_google_drive()
    except Exception as e:
        print(f"Failed to authenticate with Google Drive: {e}")
        return
    
    # Create master CSV for 2025
    print("\n🚀 Starting rebuild of 2025 master CSV...")
    print("⚠️  This will OVERWRITE the existing 2025 master CSV file in Google Drive.")
    print("    The existing file will be replaced with the complete full-year data.\n")
    
    create_master_csv_for_year(service, token, 2025, shared_drive_id)
    
    print("\n" + "="*80)
    print("✅ Rebuild complete!")
    print("="*80)
    print("\nThe 2025 master CSV has been rebuilt with full year data.")
    print("The existing file in Google Drive has been OVERWRITTEN.")
    print("A local backup has been saved to: local_backups/billing_data_2025_master.csv")

if __name__ == "__main__":
    main()

