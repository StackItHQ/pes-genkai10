import os
import json
import time
import mysql.connector  # For MySQL (use psycopg2 for PostgreSQL)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'creds.json'  # Path to your credentials file

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)

# Define spreadsheet ID and range
SPREADSHEET_ID = '1zO2utop0RwkMhiU59TUWF5d7cU4v7_UyogBWE49axfg'  
RANGE_NAME = 'Sheet1!A1:C20'  

# MySQL database connection
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'harsha2004',
    'database': 'google_sheet_sync'
}

# Connect to MySQL
db_conn = mysql.connector.connect(**db_config)
cursor = db_conn.cursor()

# Read data from Google Sheets
def read_google_sheet():
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])
    return values

# Write data to MySQL
def write_to_db(data):
    for row in data:
        cursor.execute("INSERT INTO sheet_data (name, email) VALUES (%s, %s)", (row[1], row[2]))
    db_conn.commit()

'''def fetch_updated_rows(last_sync_time):
    query = "SELECT * FROM sheet_data WHERE last_updated > %s"
    cursor.execute(query, (last_sync_time,))
    updated_rows = cursor.fetchall()
    return updated_rows'''

def fetch_updated_rows(data_ids):
    if not data_ids:
        return []

    # Create a placeholder string with the correct number of placeholders
    placeholders = ','.join(['%s'] * len(data_ids))
    query = f"SELECT * FROM sheet_data WHERE id IN ({placeholders})"

    cursor.execute(query, tuple(data_ids))
    rows = cursor.fetchall()
    return rows

def fetch_all_db_rows():
    cursor.execute("SELECT id, name, email FROM sheet_data")
    return cursor.fetchall()

def fetch_changes_from_log(last_sync_time):
    query = "SELECT * FROM change_log WHERE change_time > %s"
    cursor.execute(query, (last_sync_time,))
    change_log = cursor.fetchall()
    return change_log

def update_google_sheet(data):
    body = {
        'values': data
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='RAW',
        body=body
    ).execute()
    print(f"{result.get('updatedCells')} cells updated in Google Sheets.")

def fetch_google_sheet_data(sheet_name='Sheet1', range='A:Z'):
    try:
        # Fetches data from the Google Sheet
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f'{sheet_name}!{range}').execute()
        values = result.get('values', [])

        if not values:
            print('No data found in Google Sheets.')
            return []
        else:
            return values
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []


def sync_db_to_google_sheet(last_sync_time):
    # Fetch changes from the log since the last sync time
    changes = fetch_changes_from_log(last_sync_time)

    if changes:
        data_ids = [change[2] for change in changes]  # Assuming `data_id` is the third column
        updated_rows = fetch_updated_rows(data_ids)

        if updated_rows:
            # Fetch column names dynamically
            cursor.execute("SHOW COLUMNS FROM sheet_data")
            columns = [column[0] for column in cursor.fetchall()]

            # Prepare data for Google Sheets
            data = [columns]  # Use column names as headers
            for row in updated_rows:
                data.append(list(row))  # Convert tuple to list and append it

            # Sync data with Google Sheets
            if data:
                update_google_sheet(data)
                print("Sync complete: Database changes have been reflected in Google Sheets.")
            else:
                print("No data to update.")
        else:
            print("No data to update.")

        # Handle deletions
        db_existing_ids = {row[0] for row in fetch_all_db_rows()}  # Get all existing IDs in the DB
        google_sheet_ids = {row[0] for row in fetch_google_sheet_data()[1:]}  # Fetch current Google Sheet data, skipping headers

        # Find IDs that are in Google Sheets but not in the database (deleted from DB)
        ids_to_delete = google_sheet_ids - db_existing_ids

        if ids_to_delete:
            # Mark rows for deletion in Google Sheets
            mark_deleted_in_google_sheet(ids_to_delete)
            print(f"Marked {len(ids_to_delete)} rows for deletion in Google Sheets.")
        else:
            print("No rows to delete in Google Sheets.")
    else:
        print("No changes detected in the change log.")

def mark_deleted_in_google_sheet(ids_to_delete, sheet_name='Sheet1'):
    if not ids_to_delete:
        print("No rows to delete.")
        return

    # Fetch existing data from Google Sheets
    sheet_data = fetch_google_sheet_data(sheet_name)
    
    # Check if the first row contains headers and get the column index for ID
    headers = sheet_data[0]
    id_col_index = headers.index('ID') if 'ID' in headers else None
    
    if id_col_index is None:
        print("ID column not found in Google Sheets.")
        return

    # Create a map of IDs to row indexes
    id_to_row_map = {row[id_col_index]: index + 1 for index, row in enumerate(sheet_data[1:])}  # +1 to account for header row

    # Find rows to delete
    rows_to_delete = [id_to_row_map[_id] for _id in ids_to_delete if _id in id_to_row_map]

    if rows_to_delete:
        # Sort rows in reverse order to avoid shifting issues when deleting
        rows_to_delete.sort(reverse=True)
        
        # Create the Google Sheets API client
        service = build('sheets', 'v4', credentials=credentials)
        sheet_id = SPREADSHEET_ID  # Update with your actual spreadsheet ID
        
        for row_index in rows_to_delete:
            range_name = f"{sheet_name}!A{row_index + 1}"  # Use the sheet name dynamically
            body = {
                'values': [[]]  # Empty values to clear the row
            }
            # Clear the row in the Google Sheet
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body=body
                ).execute()
            except Exception as e:
                print(f"Failed to clear row {row_index}: {e}")
        
        print(f"Marked {len(rows_to_delete)} rows for deletion in Google Sheets.")
    else:
        print("No rows matched for deletion.")


'''
def sync_google_sheet_to_db(sheet_data):
    # Assume the first row of the sheet_data contains the headers
    headers = sheet_data[0]

    # Map column names to database fields (assuming headers match database column names)
    column_map = {i: header for i, header in enumerate(headers)}

    # Remove the header row before processing the data
    sheet_data = sheet_data[1:]

    # Fetch existing data from the database
    db_data = fetch_all_db_rows()
    db_data_dict = {row[0]: row for row in db_data}  # Assuming 'id' is the primary key

    # Prepare data for insertion/updating
    for row in sheet_data:
        # Skip rows where 'id' is not a number
        if not row[0].isdigit():
            continue

        row_id = int(row[0])  # Convert the ID to an integer

        # Dynamically build the SQL query based on the sheet headers
        columns = ", ".join([f"{column_map[i]}" for i in range(1, len(row))])
        placeholders = ", ".join(["%s" for _ in range(1, len(row))])
        update_placeholders = ", ".join([f"{column_map[i]} = %s" for i in range(1, len(row))])

        # Check if row_id already exists in the database
        if row_id in db_data_dict:
            # Update existing row
            cursor.execute(f"UPDATE sheet_data SET {update_placeholders} WHERE id = %s", (*row[1:], row_id))
        else:
            # Insert new row
            cursor.execute(f"INSERT INTO sheet_data (id, {columns}) VALUES (%s, {placeholders})", (row_id, *row[1:]))
    
    # Commit changes to the database
    db_conn.commit()
'''

def sync_google_sheet_to_db(sheet_data):
    # Ensure sheet_data is not empty
    if not sheet_data:
        print("No data to process from Google Sheets.")
        return

    # Assume the first row of the sheet_data contains the headers
    headers = sheet_data[0]

    # Map column names to database fields (assuming headers match database column names)
    column_map = {i: header for i, header in enumerate(headers)}

    # Remove the header row before processing the data
    sheet_data = sheet_data[1:]

    # Fetch existing data from the database
    db_data = fetch_all_db_rows()
    db_data_dict = {row[0]: row for row in db_data}  # Assuming 'id' is the primary key

    # Track IDs from the Google Sheet
    sheet_ids = set()
    for row in sheet_data:
        if len(row) < len(headers):
            print(f"Skipping incomplete row: {row}")
            continue

        if not row[0].isdigit():
            print(f"Skipping row with invalid ID: {row[0]}")
            continue

        row_id = int(row[0])
        sheet_ids.add(row_id)

        # Dynamically build the SQL query based on the sheet headers
        columns = ", ".join([f"{column_map[i]}" for i in range(1, len(row))])
        placeholders = ", ".join(["%s" for _ in range(1, len(row))])
        update_placeholders = ", ".join([f"{column_map[i]} = %s" for i in range(1, len(row))])

        if row_id in db_data_dict:
            # Update existing row
            cursor.execute(f"UPDATE sheet_data SET {update_placeholders} WHERE id = %s", (*row[1:], row_id))
        else:
            # Insert new row
            cursor.execute(f"INSERT INTO sheet_data (id, {columns}) VALUES (%s, {placeholders})", (row_id, *row[1:]))

    # Delete rows that are not present in Google Sheets
    ids_to_delete = [row_id for row_id in db_data_dict if row_id not in sheet_ids]
    if ids_to_delete:
        cursor.executemany("DELETE FROM sheet_data WHERE id = %s", [(row_id,) for row_id in ids_to_delete])

    # Commit changes to the database
    db_conn.commit()
    print("Sync complete: Google Sheets data has been reflected in the database, including deletions.")

def delete_rows_from_google_sheet(row_indices, sheet_name='Sheet1'):
    if not row_indices:
        print("No rows to delete.")
        return

    # Ensure row_indices are unique and sorted in reverse order
    row_indices = sorted(set(row_indices), reverse=True)

    # Prepare batch update request to delete rows
    requests = []
    for row_index in row_indices:
        # Convert 0-based index to 1-based for Google Sheets API
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': get_sheet_id(sheet_name),
                    'dimension': 'ROWS',
                    'startIndex': row_index - 1,
                    'endIndex': row_index
                }
            }
        })
    
    # Send batch update request to Google Sheets
    body = {
        'requests': requests
    }
    
    try:
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()
        print(f"Successfully deleted {len(row_indices)} rows from Google Sheets.")
    except Exception as e:
        print(f"Failed to delete rows: {e}")

def get_sheet_id(sheet_name):
    # Retrieve sheet metadata and get the sheet ID for the given sheet name
    sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = sheet_metadata.get('sheets', [])
    
    for sheet in sheets:
        properties = sheet.get('properties', {})
        if properties.get('title') == sheet_name:
            return properties.get('sheetId')
    
    raise ValueError(f"Sheet with name '{sheet_name}' not found.")

def update_last_sync_time():
    # Insert or update the last sync time in the change_log table
    cursor.execute("""
        INSERT INTO change_log (action, data_id, change_time)
        VALUES ('SYNC', 0, NOW())
        ON DUPLICATE KEY UPDATE change_time = VALUES(change_time)
    """)
    db_conn.commit()

def get_last_sync_time():
    cursor.execute("SELECT change_time FROM change_log WHERE action = 'SYNC' ORDER BY change_time DESC LIMIT 1")
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

'''
def poll_sync():
    last_sync_time = time.time()
    while True:
        sync_db_to_google_sheet(last_sync_time)
        last_sync_time = time.time()
        time.sleep(300)  # 5 minutes interval
'''
POLL_INTERVAL=10

def poll_sync():
    last_sync_time = get_last_sync_time()

    try:
        while True:
            print("Polling for changes...")
            
            # Sync Google Sheet to database
            sheet_data = read_google_sheet()
            sync_google_sheet_to_db(sheet_data)
            
            # Sync database to Google Sheet
            sync_db_to_google_sheet(last_sync_time)
            
            # Update the last sync time
            update_last_sync_time()
            
            # Wait for the next poll cycle
            time.sleep(POLL_INTERVAL)  # POLL_INTERVAL is the delay between each poll, in seconds
            
    except KeyboardInterrupt:
        print("Polling stopped by user.")


# Example usage
if __name__ == '__main__':
    poll_sync()
