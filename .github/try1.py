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
RANGE_NAME = 'Sheet1!A1:C10'  

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

def fetch_updated_rows(last_sync_time):
    query = "SELECT * FROM sheet_data WHERE last_updated > %s"
    cursor.execute(query, (last_sync_time,))
    updated_rows = cursor.fetchall()
    return updated_rows

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
    data = [['ID', 'Name', 'Email']]  # Example header, adjust to your schema
    for change in changes:
        data_id = change[2]  # Assuming `data_id` is the third column
        cursor.execute("SELECT * FROM sheet_data WHERE id = %s", (data_id,))
        row = cursor.fetchone()
        if row:
            data.append([row[0], row[1], row[2]])
    if data:
        update_google_sheet(data)
        print("Sync complete: Database changes have been reflected in Google Sheets.")
    else:
        print("No changes detected in the database.")

def poll_sync():
    last_sync_time = time.time()
    while True:
        sync_db_to_google_sheet(last_sync_time)
        last_sync_time = time.time()
        time.sleep(300)  # 5 minutes interval
        


# Example usage
if __name__ == '__main__':
    sheet_data = read_google_sheet()
    write_to_db(sheet_data)
    print("Data has been synced!")
