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

def sync_db_to_google_sheet(last_sync_time):
    changes = fetch_changes_from_log(last_sync_time)
    
    if changes:
        data_ids = [change[2] for change in changes]  # Assuming `data_id` is the third column
        updated_rows = fetch_updated_rows(data_ids)
        
        # Prepare data for Google Sheets
        data = [['ID', 'Name', 'Email']]  # Adjust headers as per your schema
        for row in updated_rows:
            data.append([row[0], row[1], row[2]])  # Adjust indices as per your schema
        
        if data:
            update_google_sheet(data)
            print("Sync complete: Database changes have been reflected in Google Sheets.")
        else:
            print("No data to update.")
    else:
        print("No changes detected in the change log.")

def sync_google_sheet_to_db(sheet_data):
    # Fetch existing data from the database
    db_data = fetch_all_db_rows()
    db_data_dict = {row[0]: row for row in db_data}  # Assuming 'id' is the primary key

    # Prepare data for insertion/updating
    for index, row in enumerate(sheet_data):
        # Skip header row if it's the first row or if row[0] is not an integer (e.g., 'ID')
        if index == 0 or not row[0].isdigit():
            continue

        row_id = int(row[0])  # Convert the ID to an integer
        name = row[1]
        email = row[2]

        if row_id in db_data_dict:
            # Update existing row
            cursor.execute("UPDATE sheet_data SET name = %s, email = %s WHERE id = %s", (name, email, row_id))
        else:
            # Insert new row
            cursor.execute("INSERT INTO sheet_data (id, name, email) VALUES (%s, %s, %s)", (row_id, name, email))
    
    # Commit changes to the database
    db_conn.commit()



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
    last_sync_time = get_last_sync_time()  # Retrieve the last sync time

    # Sync from database to Google Sheets
    sync_db_to_google_sheet(last_sync_time)

    # Read data from Google Sheets
    sheet_data = read_google_sheet()

    # Sync from Google Sheets to database
    sync_google_sheet_to_db(sheet_data)

    # Update the last sync time
    update_last_sync_time()  # Implement this function to update the timestamp

    # Wait before polling again
    time.sleep(POLL_INTERVAL)


# Example usage
if __name__ == '__main__':
    poll_sync()
