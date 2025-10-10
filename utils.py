import pandas as pd
import json
from datetime import datetime
import urllib.parse

def load_user_list():
    """Load the user list CSV to create a mapping of user IDs to user names and emails"""
    try:
        user_df = pd.read_csv('user_list.csv')
        user_mapping = {}
        for _, row in user_df.iterrows():
            # Create full name from first_name and last_name
            first_name = row.get('first_name', '')
            last_name = row.get('last_name', '')
            full_name = f"{first_name} {last_name}".strip() if first_name or last_name else row.get('username', 'Unknown User')
            
            user_mapping[row['id']] = {
                'username': row.get('username', 'Unknown User'),
                'full_name': full_name,
                'email': row.get('email', 'Unknown Email')
            }
        print(f"Loaded {len(user_mapping)} users from user_list.csv")
        return user_mapping
    except FileNotFoundError:
        print("Warning: user_list.csv not found, user names and emails will not be added")
        return {}
    except Exception as e:
        print(f"Error loading user list: {e}")
        return {}

def load_tool_list():
    """Load the tool list CSV to create a mapping of tool IDs to tool names"""
    try:
        tool_df = pd.read_csv('tool_list.csv')
        tool_mapping = dict(zip(tool_df['id'], tool_df['name']))
        print(f"Loaded {len(tool_mapping)} tools from tool_list.csv")
        return tool_mapping
    except FileNotFoundError:
        print("Warning: tool_list.csv not found, tool names will not be added")
        return {}
    except Exception as e:
        print(f"Error loading tool list: {e}")
        return {}

def limit_to_recent_events(data, max_events=2000):
    """Limit processing to the most recent events (highest IDs)"""
    if len(data) <= max_events:
        print(f"Processing all {len(data)} events (within limit)")
        return data
    
    # Sort by ID (highest first) and take the most recent
    sorted_data = sorted(data, key=lambda x: x.get('id', 0), reverse=True)
    limited_data = sorted_data[:max_events]
    
    print(f"Limited from {len(data)} to {len(limited_data)} most recent events")
    return limited_data

def filter_usage_events_by_date(data, target_year, target_month):
    """Filter usage events data by year and month using the 'start' timestamp"""
    filtered_data = []
    
    for event in data:
        try:
            # Parse the start timestamp
            start_time = datetime.fromisoformat(event['start'].replace('Z', '+00:00'))
            
            # Check if the event is in the target year and month
            if start_time.year == target_year and start_time.month == target_month:
                filtered_data.append(event)
        except (KeyError, ValueError) as e:
            # Skip events with invalid timestamps
            print(f"Warning: Skipping event with invalid timestamp: {e}")
            continue
    
    print(f"Filtered to {len(filtered_data)} events for {target_month:02d}/{target_year}")
    return filtered_data

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

def add_tool_names(data, tool_mapping):
    """Add tool names to usage events data based on tool IDs"""
    if not tool_mapping:
        return data
    
    for event in data:
        tool_id = event.get('tool')
        if tool_id and tool_id in tool_mapping:
            event['tool_name'] = tool_mapping[tool_id]
        else:
            event['tool_name'] = 'Unknown Tool'
    
    print("Added tool names to usage events data")
    return data

def add_user_info(data, user_mapping):
    """Add user names and emails to usage events data based on user IDs"""
    if not user_mapping:
        return data
    
    for event in data:
        user_id = event.get('user')
        
        # Add user info
        if user_id and user_id in user_mapping:
            event['user_username'] = user_mapping[user_id]['username']
            event['user_full_name'] = user_mapping[user_id]['full_name']
            event['user_email'] = user_mapping[user_id]['email']
        else:
            event['user_username'] = 'Unknown User'
            event['user_full_name'] = 'Unknown User'
            event['user_email'] = 'Unknown Email'
    
    print("Added user names and emails to usage events data")
    return data

def remove_id_and_operator_columns(data):
    """Remove numerical ID columns and other unwanted columns after adding human-readable names"""
    columns_to_remove = ['id', 'user', 'tool', 'has_ended', 'waived', 'waived_on', 'operator', 'project']
    
    for event in data:
        for column in columns_to_remove:
            if column in event:
                del event[column]
    
    print(f"Removed unwanted columns: {', '.join(columns_to_remove)}")
    return data

def format_json_fields(data):
    """Extract user_input from nested JSON fields, handling complex structures"""
    for event in data:
        # Process pre_run_data
        if event.get('pre_run_data'):
            try:
                # Try to parse JSON
                json_data = json.loads(event['pre_run_data'])
                # Extract user_input from complex structure
                user_input = extract_user_input(json_data)
                event['pre_run_data'] = user_input
            except (json.JSONDecodeError, TypeError):
                # If it's not valid JSON, keep as is
                event['pre_run_data'] = 'Invalid JSON'
        
        # Process run_data
        if event.get('run_data'):
            try:
                # Try to parse JSON
                json_data = json.loads(event['run_data'])
                # Extract user_input from complex structure
                user_input = extract_user_input(json_data)
                event['run_data'] = user_input
            except (json.JSONDecodeError, TypeError):
                # If it's not valid JSON, keep as is
                event['run_data'] = 'Invalid JSON'
    
    print("Extracted user_input from JSON fields for cleaner data")
    return data

def extract_user_input(json_data):
    """Recursively extract user_input from complex JSON structures"""
    if isinstance(json_data, dict):
        # Check if this level has user_input
        if 'user_input' in json_data:
            user_input = json_data['user_input']
            
            # If user_input is a dict/object, format it nicely
            if isinstance(user_input, dict):
                # Format as key-value pairs
                formatted_parts = []
                for key, value in user_input.items():
                    if value is not None:
                        if isinstance(value, dict):
                            # Handle nested objects (like group data)
                            nested_parts = []
                            for nested_key, nested_value in value.items():
                                if nested_value is not None:
                                    nested_parts.append(f"{nested_key}: {nested_value}")
                            if nested_parts:
                                formatted_parts.append(f"{key} ({'; '.join(nested_parts)})")
                        else:
                            formatted_parts.append(f"{key}: {value}")
                return "; ".join(formatted_parts) if formatted_parts else "No values"
            else:
                # Simple string/number value
                return str(user_input)
        
        # Recursively search all nested objects for user_input
        all_user_inputs = []
        for key, value in json_data.items():
            if isinstance(value, dict):
                nested_input = extract_user_input(value)
                if nested_input and nested_input != "No user_input found" and nested_input != "No values":
                    all_user_inputs.append(f"{key}: {nested_input}")
        
        if all_user_inputs:
            return "; ".join(all_user_inputs)
        else:
            return "No user_input found"
    
    return "No user_input found"

def get_base_url_descriptor(base_url):
    """Return a descriptor string based on the base URL for use in filenames."""
    # Extract the last non-empty part of the path as a descriptor
    parsed = urllib.parse.urlparse(base_url)
    path_parts = [p for p in parsed.path.split('/') if p]
    if path_parts:
        return path_parts[-1].replace('-', '_')
    return 'data'

def save_local_copy(data, filename):
    """Save a local copy of the processed data for inspection during testing"""
    try:
        # Convert to DataFrame and save to Excel
        df = pd.DataFrame(data)
        
        # Convert .csv filename to .xlsx for local copy
        local_filename = f"local_{filename.replace('.csv', '.xlsx')}"
        
        # Save to Excel with formatting
        with pd.ExcelWriter(local_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Usage Events', index=False)
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Usage Events']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Local copy saved to {local_filename}")
        return True
    except Exception as e:
        print(f"Error saving local copy: {e}")
        return False

def save_to_excel(data, filename):
    """Save usage events data to Excel file"""
    if not data:
        print("No data to save")
        return False
    
    try:
        # Convert to DataFrame and save to Excel
        df = pd.DataFrame(data)
        
        # Convert .csv filename to .xlsx
        excel_filename = filename.replace('.csv', '.xlsx')
        
        # Save to Excel with formatting
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Usage Events', index=False)
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Usage Events']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Data saved to {excel_filename}")
        return excel_filename  # Return the actual filename used
        
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        return False

def get_target_folder_path(service, shared_drive_id, year, month):
    """Get or create the folder path: Year/Usage_Events/Month"""
    # Create or get year folder
    year_folder_name = str(year)
    year_folder_id = get_or_create_folder(service, shared_drive_id, year_folder_name)
    
    # Create or get usage_events folder inside year folder
    usage_events_folder_name = "Usage_Events"
    usage_events_folder_id = get_or_create_folder(service, year_folder_id, usage_events_folder_name)
    
    # Create or get month folder inside usage_events folder
    month_folder_name = f"{month:02d}"
    month_folder_id = get_or_create_folder(service, usage_events_folder_id, month_folder_name)
    
    return month_folder_id

def get_or_create_folder(service, parent_id, folder_name):
    """Get or create a folder with the given name in the parent folder"""
    print(f"Looking for folder '{folder_name}' in parent ID: {parent_id}")
    
    # Check if folder already exists
    query = f"name='{folder_name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = results.get('files', [])
    
    if files:
        # Folder exists, return its ID
        print(f"Found existing folder '{folder_name}' with ID: {files[0]['id']}")
        return files[0]['id']
    else:
        # Create new folder
        print(f"Creating new folder '{folder_name}' in parent ID: {parent_id}")
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        
        folder = service.files().create(
            body=folder_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        print(f"Created folder: {folder_name} with ID: {folder.get('id')}")
        return folder.get('id') 

def split_data_by_tool(data):
    """Split usage events data by tool name"""
    tool_groups = {}
    
    for event in data:
        tool_name = event.get('tool_name', 'Unknown Tool')
        
        if tool_name not in tool_groups:
            tool_groups[tool_name] = []
        
        tool_groups[tool_name].append(event)
    
    print(f"Split data into {len(tool_groups)} tool groups:")
    for tool_name, events in tool_groups.items():
        print(f"  - {tool_name}: {len(events)} events")
    
    return tool_groups 