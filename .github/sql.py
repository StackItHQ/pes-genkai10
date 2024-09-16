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


def fetch_updated_rows(data_ids):
    if not data_ids:
        return []

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

def get_change_log_columns():
    cursor.execute("SHOW COLUMNS FROM change_log")
    columns = cursor.fetchall()
    return [column[0] for column in columns]  # Return list of column names

# Write data to MySQL
def write_to_db(data):
    for row in data:
        cursor.execute("INSERT INTO sheet_data (name, email) VALUES (%s, %s)", (row[1], row[2]))
    db_conn.commit()


