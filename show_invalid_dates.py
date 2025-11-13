#!/usr/bin/env python3
"""
Script to show records with invalid/null dates from the Year Master CSV
"""

import pandas as pd
from datetime import datetime

def show_invalid_dates():
    year_master_path = "/Users/adenton/Desktop/NEMO-VM/local_backups/billing_data_2025_master.csv"
    
    print("="*80)
    print("RECORDS WITH INVALID/NULL DATES IN YEAR MASTER CSV")
    print("="*80)
    
    # Load the CSV
    print(f"\n📂 Loading {year_master_path}...")
    df = pd.read_csv(year_master_path, low_memory=False)
    print(f"   Total records: {len(df):,}")
    
    # Parse start dates
    df['start'] = pd.to_datetime(df['start'], errors='coerce', utc=True)
    
    # Find records with invalid dates
    invalid_mask = df['start'].isna()
    invalid_records = df[invalid_mask].copy()
    
    print(f"\n📊 Summary:")
    print(f"   Records with invalid/null dates: {len(invalid_records):,}")
    print(f"   Records with valid dates: {len(df) - len(invalid_records):,}")
    
    if len(invalid_records) == 0:
        print("\n✅ No records with invalid dates found!")
        return
    
    print(f"\n🔍 Showing first 20 records with invalid dates:")
    print("="*80)
    
    # Show key columns for these records
    key_columns = ['item_id', 'item_type', 'start', 'end', 'user', 'tool', 'name', 'amount', 'quantity']
    available_columns = [col for col in key_columns if col in invalid_records.columns]
    
    # Show sample records
    sample = invalid_records[available_columns].head(20)
    
    # Print in a readable format
    for idx, row in sample.iterrows():
        print(f"\nRecord {idx + 1}:")
        for col in available_columns:
            value = row[col]
            if pd.isna(value):
                value = "NULL"
            elif isinstance(value, float) and pd.isna(value):
                value = "NULL"
            print(f"  {col:20s}: {value}")
        print("-" * 80)
    
    # Show statistics
    print(f"\n📈 Statistics for invalid date records:")
    print(f"   Item types: {invalid_records['item_type'].value_counts().to_dict()}")
    
    if 'item_id' in invalid_records.columns:
        print(f"\n   Item IDs (first 20): {invalid_records['item_id'].head(20).tolist()}")
    
    # Check if 'end' column also has invalid dates
    if 'end' in invalid_records.columns:
        invalid_records['end_parsed'] = pd.to_datetime(invalid_records['end'], errors='coerce', utc=True)
        invalid_end = invalid_records['end_parsed'].isna().sum()
        print(f"\n   Records with invalid 'end' dates: {invalid_end:,}")
    
    # Save to file for inspection
    output_file = "invalid_date_records.csv"
    invalid_records.to_csv(output_file, index=False)
    print(f"\n💾 Saved all {len(invalid_records):,} invalid date records to: {output_file}")

if __name__ == "__main__":
    show_invalid_dates()

