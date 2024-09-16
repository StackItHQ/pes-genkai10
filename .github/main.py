import os
import json
import time
import datetime
import mysql.connector  # For MySQL (use psycopg2 for PostgreSQL)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sql import *
from sheet import *

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


def sync_db_to_google_sheet(last_sync_time):
    if not last_sync_time:
        print("No valid last sync time provided.")
        return

    changes = fetch_changes_from_log(last_sync_time)  # Fetch changes from the log since the last sync time
    print(f"Fetched changes: {changes}")

    if changes:
        columns = get_change_log_columns()  # Fetch column names
        data_id_column = 'data_id'  # Replace with actual column name if different
        
        if data_id_column not in columns:
            raise ValueError(f"{data_id_column} column not found in change_log")

        data_ids = [change[columns.index(data_id_column)] for change in changes]  # Get data_ids dynamically
        updated_rows = fetch_updated_rows(data_ids)

        print(f"Fetched updated rows: {updated_rows}")

        if updated_rows:
            cursor.execute("SHOW COLUMNS FROM sheet_data")
            columns = [column[0] for column in cursor.fetchall()]
            print(f"Columns in DB: {columns}")

            # Prepare data for Google Sheets
            data = [columns]  # Use column names as headers
            for row in updated_rows:
                data.append(list(row))  # Convert tuple to list

            if data:
                update_google_sheet(data)
                print("Sync complete: Database changes have been reflected in Google Sheets.")
        else:
            print("No data to update.")

        # Fetch current Google Sheet data
        try:
            google_sheet_data = fetch_google_sheet_data()
            if not google_sheet_data:
                print("No data fetched from Google Sheets.")
                return
        except Exception as e:
            print(f"Error fetching Google Sheet data: {e}")
            return

        # Extract IDs from Google Sheet data
        try:
            google_sheet_ids = {int(row[0]) for row in google_sheet_data[1:] if row[0].isdigit()}  # Skipping headers
            print(f"IDs from Google Sheets: {google_sheet_ids}")
        except Exception as e:
            print(f"Error processing Google Sheet data: {e}")
            return

        db_existing_ids = {int(row[0]) for row in fetch_all_db_rows()}  # Get all existing IDs in the DB
        print(f"IDs from DB: {db_existing_ids}")

        # Handle deletions
        ids_to_delete_in_sheet = google_sheet_ids - db_existing_ids
        print(f"IDs to delete from Google Sheets: {ids_to_delete_in_sheet}")

        if ids_to_delete_in_sheet:
            delete_rows_from_google_sheet(list(ids_to_delete_in_sheet))
            print(f"Deleted {len(ids_to_delete_in_sheet)} rows from Google Sheets.")
        else:
            print("No rows to delete in Google Sheets.")
    else:
        print("No changes detected in the change log.")


def sync_google_sheet_to_db(sheet_data):
    last_update_time_tuple = read_last_update_time_from_sheet()  # returns a tuple
    last_sync_time = get_last_sync_time()  # returns a datetime object

    if last_update_time_tuple is None or len(last_update_time_tuple) == 0:
        print("Failed to read last update time from Google Sheets.")
        return

    # Unpack the tuple to get the datetime object
    last_update_time = last_update_time_tuple[0]

    # Ensure both are datetime objects
    if not isinstance(last_update_time, datetime.datetime) or not isinstance(last_sync_time, datetime.datetime):
        print("Invalid datetime values for comparison.")
        return

    if not sheet_data:
        print("No data to process from Google Sheets.")
        return

    headers = sheet_data[0]
    column_map = {i: header for i, header in enumerate(headers)}

    sheet_data = sheet_data[1:]

    # Fetch all data from the database and map by ID
    db_data = fetch_all_db_rows()
    db_data_dict = {row[0]: row for row in db_data}

    sheet_ids = set()
    update_count = 0  # Counter for the number of updates made

    # Compare last update time with last sync time
    if last_update_time > last_sync_time:    
        for row in sheet_data:
            if len(row) < len(headers):
                print(f"Skipping incomplete row: {row}")
                continue

            if not row[0].isdigit():
                print(f"Skipping row with invalid ID: {row[0]}")
                continue

            row_id = int(row[0])
            sheet_ids.add(row_id)

            # Map sheet data to database columns
            new_data = [row[i] for i in range(1, len(headers))]  # Use indices to get data from row
        
            if row_id in db_data_dict:
                db_row = db_data_dict[row_id]
                db_data_row = list(db_row[1:])  # Exclude the ID
            
                # Compare existing data with new data
                if new_data != db_data_row:
                    update_placeholders = ", ".join([f"{column_map[i]} = %s" for i in range(1, len(headers))])
                    cursor.execute(f"UPDATE sheet_data SET {update_placeholders} WHERE id = %s", (*new_data, row_id))
                    update_count += 1
                else:
                    continue
            else:
                # Prepare data for insertion
                columns = ", ".join([column_map[i] for i in range(1, len(headers))])
                placeholders = ", ".join(["%s" for _ in range(1, len(headers))])
                cursor.execute(f"INSERT INTO sheet_data (id, {columns}) VALUES (%s, {placeholders})", (row_id, *new_data))
                update_count += 1

        db_ids = set(db_data_dict.keys())
        ids_to_delete = db_ids - sheet_ids

        if ids_to_delete:
            print(f"Deleting {len(ids_to_delete)} rows from the database.")
            cursor.executemany("DELETE FROM sheet_data WHERE id = %s", [(row_id,) for row_id in ids_to_delete])

        db_conn.commit()
        print(f"Sync complete: {update_count} rows updated/inserted in the database.")
    else:
        print("No updates required: Google Sheet data is already synced.")


def update_last_sync_time():
    cursor.execute("""
        INSERT INTO change_log (action, data_id, change_time)
        VALUES ('SYNC', 0, NOW())
        ON DUPLICATE KEY UPDATE change_time = VALUES(change_time)
    """)
    db_conn.commit()

def get_last_sync_time():
    cursor.execute("SELECT change_time FROM change_log WHERE action = 'SYNC' ORDER BY change_time DESC LIMIT 1")
    result = cursor.fetchone()
    #print(result)
    if result:
        return result[0]
    return None

def read_last_update_time_from_sheet():
    """
    Fetch the last update timestamp from Google Sheets cell Z1 and parse it.
    This function assumes the timestamp may include both date and time, even if
    the visible cell only shows the date. It returns a tuple with a datetime object.
    """
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Sheet1!Z1').execute()
    values = result.get('values', [])
    
    if values and values[0]:
        timestamp_str = values[0][0]
        try:
            # Try parsing the timestamp with both date and time
            last_update_time = datetime.datetime.strptime(timestamp_str, '%m/%d/%Y %H:%M:%S')
            
            # Return the datetime object wrapped in a tuple
            return (last_update_time,)
        except ValueError:
            try:
                # If parsing the full date-time fails, try just the date
                last_update_time = datetime.datetime.strptime(timestamp_str, '%m/%d/%Y')
                last_update_time = last_update_time.replace(hour=0, minute=0, second=0)
                
                return (last_update_time,)
            except ValueError as e:
                print(f"Error parsing timestamp: {e}")
                return None
    else:
        print("No timestamp found in cell Z1.")
        return None




POLL_INTERVAL = 10

def poll_sync():
    last_sync_time = get_last_sync_time()

    try:
        while True:
            print("Polling for changes...")
            
            sheet_data = read_google_sheet()
            sync_google_sheet_to_db(sheet_data)
            
            sync_db_to_google_sheet(last_sync_time)
            
            update_last_sync_time()
            
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Polling stopped by user.")

# Example usage
if __name__ == '__main__':
    poll_sync()
