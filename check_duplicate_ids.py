#!/usr/bin/env python3
"""
Script to download the last 90 days of billing data and check for duplicate item_ids.
This helps us understand if the NEMO API returns duplicates or if our processing creates them.
"""

import pandas as pd
from datetime import datetime, timedelta
import requests
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
token = os.getenv('NEMO_TOKEN')

if not token:
    print("Error: NEMO_TOKEN not found in environment variables")
    exit(1)

BASE_URL = "https://nemo.stanford.edu/api/billing/billing_data/"

def fetch_billing_data(start_date, end_date, token):
    """Fetch billing data from Nemo API"""
    headers = {
        "Authorization": f"Token {token}"
    }
    
    try:
        response = requests.get(BASE_URL, params={'start': start_date, 'end': end_date}, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched {len(data)} billing records")
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return None

def analyze_duplicates(data):
    """Analyze the data for duplicate item_ids"""
    if not data:
        print("No data to analyze")
        return
    
    df = pd.DataFrame(data)
    
    print("\n" + "="*80)
    print("DATA ANALYSIS")
    print("="*80)
    
    print(f"\n📊 Basic Statistics:")
    print(f"  - Total records: {len(df):,}")
    print(f"  - Total columns: {len(df.columns)}")
    
    # Check for item_id column
    if 'item_id' not in df.columns:
        print("\n⚠️  WARNING: 'item_id' column not found in data!")
        print(f"   Available columns: {', '.join(df.columns)}")
        return
    
    # Check for null item_ids
    null_item_ids = df['item_id'].isna().sum()
    print(f"  - Records with null item_id: {null_item_ids:,}")
    print(f"  - Records with non-null item_id: {len(df) - null_item_ids:,}")
    
    # Analyze duplicates
    print(f"\n🔍 Duplicate Analysis:")
    
    # Get non-null item_ids
    non_null_df = df[df['item_id'].notna()].copy()
    
    if len(non_null_df) == 0:
        print("  ⚠️  No records with non-null item_id to analyze")
        return
    
    # Find duplicates
    duplicate_mask = non_null_df.duplicated(subset=['item_id'], keep=False)
    duplicate_records = non_null_df[duplicate_mask]
    
    if len(duplicate_records) == 0:
        print(f"  ✅ No duplicate item_ids found!")
        print(f"     All {len(non_null_df):,} records with item_id are unique")
    else:
        duplicate_item_ids = duplicate_records['item_id'].unique()
        print(f"  ⚠️  Found {len(duplicate_item_ids):,} item_ids that appear multiple times")
        print(f"     Total duplicate records: {len(duplicate_records):,}")
        print(f"     Unique item_ids: {len(non_null_df['item_id'].unique()):,}")
        print(f"     Expected unique records: {len(non_null_df['item_id'].unique()):,}")
        print(f"     Actual records: {len(non_null_df):,}")
        print(f"     Difference: {len(non_null_df) - len(non_null_df['item_id'].unique()):,} extra records")
        
        # Show duplicate counts
        duplicate_counts = duplicate_records['item_id'].value_counts()
        print(f"\n  📋 Duplicate item_ids (showing first 20):")
        for item_id, count in duplicate_counts.head(20).items():
            print(f"     item_id {item_id}: appears {count} times")
        
        if len(duplicate_counts) > 20:
            print(f"     ... and {len(duplicate_counts) - 20} more duplicate item_ids")
        
        # Analyze the duplicate records
        print(f"\n  🔬 Analyzing duplicate records:")
        
        # Group by item_id and see what's different
        sample_item_id = duplicate_counts.index[0]
        sample_duplicates = duplicate_records[duplicate_records['item_id'] == sample_item_id]
        
        print(f"\n     Example: item_id {sample_item_id} appears {len(sample_duplicates)} times")
        print(f"     Differences between duplicates:")
        
        # Check which columns differ
        for col in sample_duplicates.columns:
            unique_values = sample_duplicates[col].nunique()
            if unique_values > 1:
                print(f"       - {col}: {unique_values} different values")
                if unique_values <= 5:
                    print(f"         Values: {sample_duplicates[col].unique().tolist()}")
        
        # Save duplicate records to file for inspection
        duplicate_output_file = "duplicate_records_sample.json"
        sample_duplicates_dict = sample_duplicates.to_dict('records')
        with open(duplicate_output_file, 'w') as f:
            json.dump(sample_duplicates_dict, f, indent=2, default=str)
        print(f"\n     💾 Saved sample duplicate records to: {duplicate_output_file}")
    
    # Check for records with null item_id that might be duplicates by other means
    if null_item_ids > 0:
        null_df = df[df['item_id'].isna()].copy()
        print(f"\n  🔍 Analyzing {null_item_ids:,} records with null item_id:")
        
        # Check if any of these might be duplicates by other fields
        if len(null_df) > 1:
            # Check for duplicates by (start, end, item_type) or similar
            potential_dupe_cols = ['start', 'end', 'item_type', 'user', 'tool']
            available_cols = [col for col in potential_dupe_cols if col in null_df.columns]
            
            if available_cols:
                null_duplicates = null_df.duplicated(subset=available_cols, keep=False)
                null_dupe_count = null_duplicates.sum()
                if null_dupe_count > 0:
                    print(f"     ⚠️  Found {null_dupe_count:,} records with null item_id that might be duplicates")
                    print(f"        (based on columns: {', '.join(available_cols)})")
                else:
                    print(f"     ✅ No obvious duplicates among null item_id records")
    
    return df

def main():
    print("="*80)
    print("CHECKING FOR DUPLICATE ITEM_IDS IN NEMO API DATA")
    print("="*80)
    
    # Calculate date range for last 90 days
    current = datetime.now()
    cutoff_date = current - timedelta(days=90)
    
    start_date_str = cutoff_date.strftime('%m/%d/%Y')
    end_date_str = current.strftime('%m/%d/%Y')
    
    print(f"\n📥 Fetching data from {start_date_str} to {end_date_str} (last 90 days)...")
    
    # Fetch data
    data = fetch_billing_data(start_date_str, end_date_str, token)
    
    if not data:
        print("Failed to fetch data")
        return
    
    # Save raw data
    output_file = "last_90_days_raw_data.json"
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"💾 Saved raw data to: {output_file}")
    
    # Analyze for duplicates
    df = analyze_duplicates(data)
    
    # Save as CSV for easy inspection
    if df is not None:
        csv_file = "last_90_days_data.csv"
        df.to_csv(csv_file, index=False)
        print(f"\n💾 Saved data as CSV to: {csv_file}")
        
        # Create a summary of item_id distribution
        if 'item_id' in df.columns:
            item_id_summary = df['item_id'].value_counts()
            duplicate_item_ids = item_id_summary[item_id_summary > 1]
            
            if len(duplicate_item_ids) > 0:
                summary_file = "duplicate_item_ids_summary.csv"
                duplicate_item_ids.to_csv(summary_file, header=['count'])
                print(f"💾 Saved duplicate item_ids summary to: {summary_file}")
    
    print("\n" + "="*80)
    print("✅ Analysis complete!")
    print("="*80)

if __name__ == "__main__":
    main()

