"""
Module: ingest_to_db.py
Description: This script handles the ingestion of scraped Telegram messages from 
             local JSON files into a PostgreSQL database. It supports batch 
             processing for efficiency and stores raw data as JSONB.
Author: Addisu
"""

import os
import json
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# Load environment variables (DB credentials)
load_dotenv()

# Note: Keeping the debug print as requested, but usually removed in production
print(f"DEBUG: Connecting with user: {os.getenv('POSTGRES_USER')} and password: {os.getenv('POSTGRES_PASSWORD')}")

def get_db_connection():
    """
    Creates and returns a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: A connection object to the database.
    """
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,  # Standardized to 5432 based on previous context
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def ingest_data():
    """
    Main ingestion logic:
    1. Establishes DB connection and prepares the 'raw' schema.
    2. Navigates the partitioned local directory 'data/raw/telegram_messages'.
    3. Reads JSON files and performs batch inserts using psycopg2 extras.
    
    Returns:
        None
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. Create Schema and Table (The Landing Zone)
        # We use JSONB for the 'content' column to allow flexible querying in Postgres
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                channel TEXT,
                content JSONB,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Iterate through your partitioned data
        base_path = 'data/raw/telegram_messages'
        if not os.path.exists(base_path):
            print("Data directory not found. Did you run the scraper?")
            return

        for date_folder in os.listdir(base_path):
            date_path = os.path.join(base_path, date_folder)
            if os.path.isdir(date_path):
                for file_name in os.listdir(date_path):
                    if file_name.endswith('.json'):
                        file_full_path = os.path.join(date_path, file_name)
                        
                        with open(file_full_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            channel_name = file_name.replace('.json', '')
                            
                            # Prepare data for batch insert for efficiency
                            # This avoids running thousands of individual INSERT statements
                            insert_query = "INSERT INTO raw.telegram_messages (channel, content) VALUES %s"
                            values = [(channel_name, json.dumps(msg)) for msg in data]
                            
                            extras.execute_values(cur, insert_query, values)
                            print(f"Ingested {len(values)} messages from {channel_name}")

        conn.commit()
        print("Data ingestion completed successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"An error occurred during ingestion: {e}")
    
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    """
    Entry point for the script. Triggers the ingestion of local JSON files to DB.
    """
    ingest_data()