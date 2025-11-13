#!/usr/bin/env python3
"""Check date format differences between year master and master master CSVs"""

import pandas as pd

# Read both files
year_df = pd.read_csv('local_backups/billing_data_2025_master.csv', nrows=10, low_memory=False)
master_df = pd.read_csv('local_backups/billing_data_master_master.csv', low_memory=False)

print("="*80)
print("DATE FORMAT COMPARISON")
print("="*80)

print("\n📅 Year Master CSV - Sample start dates:")
if 'start' in year_df.columns:
    print(year_df['start'].head(5).tolist())
else:
    print("No 'start' column")

print("\n📅 Master Master CSV - Sample start dates (first 5):")
if 'start' in master_df.columns:
    print(master_df['start'].head(5).tolist())
else:
    print("No 'start' column")

# Check for 2025 dates in master master
print("\n🔍 Checking for 2025 dates in Master Master CSV...")
master_2025_string = master_df[master_df['start'].astype(str).str.contains('2025', na=False)]
print(f"Records with '2025' in start string: {len(master_2025_string):,}")

if len(master_2025_string) > 0:
    print("\nSample 2025 dates from Master Master:")
    print(master_2025_string['start'].head(10).tolist())
    
    # Try to parse them
    print("\n🔍 Attempting to parse 2025 dates...")
    master_2025_string['start_parsed'] = pd.to_datetime(master_2025_string['start'], errors='coerce', utc=True)
    
    parsed_count = master_2025_string['start_parsed'].notna().sum()
    failed_count = master_2025_string['start_parsed'].isna().sum()
    
    print(f"Successfully parsed: {parsed_count:,}")
    print(f"Failed to parse: {failed_count:,}")
    
    if parsed_count > 0:
        valid_2025 = master_2025_string[master_2025_string['start_parsed'].notna()]
        years = valid_2025['start_parsed'].dt.year.unique()
        print(f"Years found after parsing: {sorted(years.tolist())}")
    
    if failed_count > 0:
        print("\n⚠️  Sample dates that failed to parse:")
        failed_dates = master_2025_string[master_2025_string['start_parsed'].isna()]['start'].head(5)
        print(failed_dates.tolist())

