#!/usr/bin/env python3
"""Quick test to verify the auto-update feature works"""

import os
from dotenv import load_dotenv
import utils

print("=" * 70)
print("Testing Auto-Update Feature for Tool and User Lists")
print("=" * 70)
print(f"Current working directory: {os.getcwd()}")
print()

# Load environment variables
load_dotenv()
token = os.getenv('NEMO_TOKEN')

if not token:
    print("ERROR: NEMO_TOKEN not found in .env file")
    exit(1)

print(f"✓ NEMO_TOKEN found: {token[:15]}...")
print()

# Test updating tool list
print("-" * 70)
print("1. Testing tool list update from API...")
print("-" * 70)
success = utils.update_tool_list_from_api(token)

if success:
    print("\n✓ Tool list update successful!")
    # Verify we can load it
    tool_mapping = utils.load_tool_list()
    print(f"  Verified: Loaded {len(tool_mapping)} tools")
    # Check for lesker2-sputter specifically
    lesker_id = 171
    if lesker_id in tool_mapping:
        print(f"  ✓ Found lesker2-sputter (ID {lesker_id}): {tool_mapping[lesker_id]}")
else:
    print("\n✗ Tool list update failed")

print()

# Test updating user list
print("-" * 70)
print("2. Testing user list update from API...")
print("-" * 70)
success = utils.update_user_list_from_api(token)

if success:
    print("\n✓ User list update successful!")
    # Verify we can load it
    user_mapping = utils.load_user_list()
    print(f"  Verified: Loaded {len(user_mapping)} users")
else:
    print("\n✗ User list update failed")

print()
print("=" * 70)
print("Test Complete!")
print("=" * 70)
print(f"\nGenerated files in: {os.getcwd()}")
print("  - tool_list.csv")
print("  - user_list.csv")

