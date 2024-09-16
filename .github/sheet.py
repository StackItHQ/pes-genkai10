import os
import json
import time
import datetime
import mysql.connector  # For MySQL (use psycopg2 for PostgreSQL)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
    
def update_google_sheet_row(row_idx, updated_row):
    """
    Update a specific row in Google Sheets.
    
    Args:
        row_idx (int): The row index in Google Sheets to update (1-based index).
        updated_row (list): The new data to update the row with.
    """
    # Define the range for the row, assuming headers start at row 1
    range_str = f'A{row_idx}:Z{row_idx}'  # Adjust 'Z' based on the number of columns in your sheet

    # Update the row in Google Sheets with the new data
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
        valueInputOption="RAW",
        body={"values": [updated_row]}
    ).execute()
    
    print(f"Row {row_idx} in Google Sheets updated.")

def append_google_sheet_rows(data_to_insert):
    """
    Append new rows to Google Sheets.
    
    Args:
        data_to_insert (list of lists): List of rows to append, each row is a list of cell values.
    """
    # Determine the range where new rows should be appended
    # The range 'A1' here is symbolic, Google Sheets will automatically append rows
    range_str = 'A1'  # Assuming the sheet auto-detects the append location based on the data range

    # Append the new rows to the Google Sheets
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": data_to_insert}
    ).execute()

    print(f"Inserted {len(data_to_insert)} new rows into Google Sheets.")

def delete_rows_from_google_sheet(row_indices, sheet_name='Sheet1'):
    if not row_indices:
        print("No rows to delete.")
        return

    row_indices = sorted(set(row_indices), reverse=True)

    requests = []
    for row_index in row_indices:
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

    body = {'requests': requests}
    
    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()
        print(f"Successfully deleted {len(row_indices)} rows from Google Sheets.")
    except Exception as e:
        print(f"Failed to delete rows: {e}")


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


def get_sheet_id(sheet_name):
    sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = sheet_metadata.get('sheets', [])
    
    for sheet in sheets:
        properties = sheet.get('properties', {})
        if properties.get('title') == sheet_name:
            return properties.get('sheetId')
    
    raise ValueError(f"Sheet with name '{sheet_name}' not found.")