# Usage Events Script - Quick Reference

## Normal Usage (Current Month Only)

When the script runs automatically via cron or when you run it manually:

```bash
cd /Users/adenton/Desktop/NEMO-VM  # or ~/nemo_automation
source .venv/bin/activate
python usage_questions.py
```

**This will:**
1. Auto-update tool and user lists from Nemo API
2. Fetch usage events for the **current month only**
3. Process and split by tool
4. Upload to Google Drive
5. **Clean up local Excel files automatically**

---

## Batch Mode (All Historical Data)

To process all months from January 2024 to present:

```bash
python usage_questions.py --batch
```

**Use this when:**
- Setting up for the first time
- Need to reprocess historical data
- Backfilling missing months

---

## What Gets Cleaned Up

After successful upload to Google Drive, the script automatically deletes:
- All generated Excel files (e.g., `lesker-sputter_2024_01.xlsx`)
- This keeps your directory clean
- Files are only deleted after confirmed upload

---

## What Files Stay

These files are kept in the directory:
- `tool_list.csv` - Updated from API before each run
- `user_list.csv` - Updated from API before each run
- `.env` - Your credentials
- `credentials.json` - Service account key
- `.venv/` - Virtual environment
- Source code files (`.py`, `.sh`)

---

## Example Output (Normal Mode)

```
Running in NORMAL mode - processing current month only...
(Use '--batch' argument to process all historical months)

============================================================
📋 Updating reference data from Nemo API...
============================================================
Fetching latest tool list from Nemo API...
Retrieved 199 tools from API
✓ Successfully updated tool_list.csv with 199 tools

Fetching latest user list from Nemo API...
Retrieved 1097 users from API
✓ Successfully updated user_list.csv with 1097 users
============================================================

Using shared drive ID: 0AKT8yv0oamIFUk9PVA
Fetching all usage events data
Successfully fetched 101198 usage events records
Loaded 199 tools from tool_list.csv
Loaded 1097 users from user_list.csv
Filtered to 3945 events for 10/2025
Limited from 3945 to 2000 most recent events
Filtered from 2000 to 175 events with data

Split data into 15 tool groups:
  - lesker2-sputter: 25 events
  - aja-evap: 18 events
  - cvd-nanotube: 15 events
  ...

Data saved to lesker2-sputter_2025_10.xlsx
File uploaded to Google Drive with ID: 1abc...
✓ Cleaned up local file: lesker2-sputter_2025_10.xlsx

Process completed successfully!
Total files uploaded: 15
Total execution time: 12.34 seconds
```

---

## Cron Integration

When running via `run_nemo_script.sh`, the script:
1. Changes to the script's directory automatically
2. Loads `.env` file
3. Activates virtual environment
4. Runs billing script
5. Runs usage events script (current month)
6. Cleans up all Excel files after upload
7. Logs success/failure status

Both billing and usage events data are uploaded monthly with no local file accumulation.

