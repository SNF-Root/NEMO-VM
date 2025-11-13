import pandas as pd
from datetime import datetime
import os

def compare_2025_data():
    """Compare 2025 data between the year master CSV and the master master CSV"""
    
    year_master_path = "/Users/adenton/Desktop/NEMO-VM/local_backups/billing_data_2025_master.csv"
    master_master_path = "/Users/adenton/Desktop/NEMO-VM/local_backups/billing_data_master_master.csv"
    
    print("="*80)
    print("COMPARING 2025 DATA BETWEEN YEAR MASTER AND MASTER MASTER CSVs")
    print("="*80)
    
    # Load both CSVs
    print("\n📂 Loading files...")
    print(f"  - Year Master: {year_master_path}")
    year_df = pd.read_csv(year_master_path, low_memory=False)
    print(f"    ✓ Loaded {len(year_df):,} records")
    
    print(f"  - Master Master: {master_master_path}")
    master_df = pd.read_csv(master_master_path, low_memory=False)
    print(f"    ✓ Loaded {len(master_df):,} total records (all years)")
    
    # Filter master master for 2025 data
    print("\n🔍 Filtering Master Master CSV for 2025 data...")
    
    # Parse the 'start' column to datetime
    master_df['start'] = pd.to_datetime(master_df['start'], errors='coerce', utc=True)
    
    # Debug: Check date parsing
    valid_dates = master_df['start'].notna().sum()
    invalid_dates = master_df['start'].isna().sum()
    print(f"    - Records with valid dates: {valid_dates:,}")
    print(f"    - Records with invalid dates: {invalid_dates:,}")
    
    if valid_dates > 0:
        # Show date range of valid dates
        valid_start_dates = master_df[master_df['start'].notna()]['start']
        print(f"    - Date range: {valid_start_dates.min()} to {valid_start_dates.max()}")
        print(f"    - Years present: {sorted(valid_start_dates.dt.year.unique().tolist())}")
    
    # Filter for 2025 - only records where year is 2025
    # First, try filtering by valid dates
    master_2025_df = master_df[master_df['start'].dt.year == 2025].copy()
    
    # If we got very few records, there might be an issue with date parsing
    # Let's also check if there are records with invalid dates that might be 2025
    # (but we can't know for sure without other indicators)
    if len(master_2025_df) == 0 and valid_dates > 0:
        print(f"    ⚠️  WARNING: No 2025 records found! This might indicate a parsing issue.")
        print(f"       Checking if dates are in a different format...")
    
    print(f"    ✓ Found {len(master_2025_df):,} records from 2025 in Master Master CSV")
    
    # Parse year master start column too
    year_df['start'] = pd.to_datetime(year_df['start'], errors='coerce', utc=True)
    
    # Basic comparison
    print("\n" + "="*80)
    print("📊 COMPARISON RESULTS")
    print("="*80)
    
    print(f"\n📈 Record Counts:")
    print(f"  - Year Master (2025):        {len(year_df):,} records")
    print(f"  - Master Master (2025):      {len(master_2025_df):,} records")
    print(f"  - Difference:                 {len(year_df) - len(master_2025_df):,} records")
    
    # Check for unique identifiers - use item_id as primary key
    only_in_year = set()
    only_in_master = set()
    
    if 'item_id' in year_df.columns and 'item_id' in master_2025_df.columns:
        print(f"\n🔑 Comparing by item_id...")
        
        year_item_ids = set(year_df['item_id'].dropna().astype(int))
        master_item_ids = set(master_2025_df['item_id'].dropna().astype(int))
        
        only_in_year = year_item_ids - master_item_ids
        only_in_master = master_item_ids - year_item_ids
        in_both = year_item_ids & master_item_ids
        
        print(f"  - Item IDs in Year Master:        {len(year_item_ids):,}")
        print(f"  - Item IDs in Master Master:      {len(master_item_ids):,}")
        print(f"  - Item IDs in both:               {len(in_both):,}")
        print(f"  - Item IDs only in Year Master:   {len(only_in_year):,}")
        print(f"  - Item IDs only in Master Master: {len(only_in_master):,}")
        
        if only_in_year:
            print(f"\n⚠️  Found {len(only_in_year)} item_ids in Year Master but NOT in Master Master:")
            print(f"    Sample (first 10): {sorted(list(only_in_year))[:10]}")
            
            # Get full records for these
            missing_records = year_df[year_df['item_id'].isin(only_in_year)]
            print(f"\n    Details of missing records:")
            print(f"    - Date range: {missing_records['start'].min()} to {missing_records['start'].max()}")
            print(f"    - Item types: {missing_records['item_type'].value_counts().to_dict()}")
        
        if only_in_master:
            print(f"\n⚠️  Found {len(only_in_master)} item_ids in Master Master but NOT in Year Master:")
            print(f"    Sample (first 10): {sorted(list(only_in_master))[:10]}")
            
            # Get full records for these
            extra_records = master_2025_df[master_2025_df['item_id'].isin(only_in_master)]
            print(f"\n    Details of extra records:")
            print(f"    - Date range: {extra_records['start'].min()} to {extra_records['start'].max()}")
            print(f"    - Item types: {extra_records['item_type'].value_counts().to_dict()}")
    
    # Date range comparison
    print(f"\n📅 Date Range Comparison:")
    print(f"  - Year Master date range:    {year_df['start'].min()} to {year_df['start'].max()}")
    print(f"  - Master Master date range:  {master_2025_df['start'].min()} to {master_2025_df['start'].max()}")
    
    # Check for records with same item_id but different data
    if 'item_id' in year_df.columns and 'item_id' in master_2025_df.columns:
        print(f"\n🔍 Checking for records with same item_id but different data...")
        
        # Merge on item_id
        merged = year_df.merge(master_2025_df, on='item_id', how='inner', suffixes=('_year', '_master'))
        
        # Compare key fields
        differences = []
        key_fields = ['amount', 'quantity', 'start', 'end', 'item_type']
        
        for field in key_fields:
            year_col = f'{field}_year' if field in year_df.columns else None
            master_col = f'{field}_master' if field in master_2025_df.columns else None
            
            if year_col and master_col and year_col in merged.columns and master_col in merged.columns:
                # Compare values
                if field in ['start', 'end']:
                    # For dates, compare as strings after normalization
                    diff_mask = merged[year_col].astype(str) != merged[master_col].astype(str)
                else:
                    diff_mask = merged[year_col] != merged[master_col]
                
                diff_count = diff_mask.sum()
                if diff_count > 0:
                    differences.append((field, diff_count))
                    print(f"    - {field}: {diff_count:,} records differ")
        
        if not differences:
            print(f"    ✓ All matching item_ids have identical key fields")
    
    # Monthly breakdown
    print("\n" + "="*80)
    print("📅 MONTHLY BREAKDOWN")
    print("="*80)
    
    year_df['month'] = year_df['start'].dt.month
    master_2025_df['month'] = master_2025_df['start'].dt.month
    
    # Count records with invalid dates
    year_invalid_dates = year_df['start'].isna().sum()
    master_invalid_dates = master_2025_df['start'].isna().sum()
    
    print("\nRecords per month:")
    print(f"{'Month':<10} {'Year Master':<15} {'Master Master':<15} {'Difference':<15}")
    print("-" * 60)
    
    for month in range(1, 13):
        year_count = len(year_df[year_df['month'] == month])
        master_count = len(master_2025_df[master_2025_df['month'] == month])
        diff = year_count - master_count
        month_name = datetime(2025, month, 1).strftime('%B')
        print(f"{month_name:<10} {year_count:<15,} {master_count:<15,} {diff:<15,}")
    
    # Show records with invalid dates
    if year_invalid_dates > 0 or master_invalid_dates > 0:
        print(f"{'Invalid Dates':<10} {year_invalid_dates:<15,} {master_invalid_dates:<15,} {year_invalid_dates - master_invalid_dates:<15,}")
        print(f"\n  ⚠️  Note: Records with invalid/null dates are excluded from monthly counts")
        print(f"     but included in total record counts and item_id comparisons.")
    
    # Analyze the missing records from Master Master
    if only_in_master:
        print(f"\n🔍 Analyzing {len(only_in_master):,} records in Master Master but not in Year Master...")
        extra_records = master_2025_df[master_2025_df['item_id'].isin(only_in_master)]
        
        print(f"\n   Monthly distribution of missing records:")
        monthly_missing = extra_records['month'].value_counts().sort_index()
        for month, count in monthly_missing.items():
            month_name = datetime(2025, month, 1).strftime('%B')
            print(f"     {month_name}: {count:,} records")
    
    # Summary
    print("\n" + "="*80)
    print("📋 SUMMARY")
    print("="*80)
    
    if len(year_df) == len(master_2025_df) and len(only_in_year) == 0 and len(only_in_master) == 0:
        print("✅ Files match perfectly! All 2025 records are identical.")
    else:
        print("⚠️  Files have differences:")
        if len(year_df) != len(master_2025_df):
            print(f"   - Record count mismatch: {len(year_df):,} vs {len(master_2025_df):,}")
            print(f"   - Year Master is missing {len(master_2025_df) - len(year_df):,} records")
        if only_in_year:
            print(f"   - {len(only_in_year):,} records in Year Master but not in Master Master")
        if only_in_master:
            print(f"   - {len(only_in_master):,} records in Master Master but not in Year Master")
            print(f"\n   💡 RECOMMENDATION: Run create_master_csv_for_year(2025) to rebuild")
            print(f"      the full year master CSV with all 2025 data.")
    
    print("="*80)

if __name__ == "__main__":
    compare_2025_data()

