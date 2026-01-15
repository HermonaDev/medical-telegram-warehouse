import os
import json
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

load_dotenv()

print(f"DEBUG: Connecting with user: {os.getenv('POSTGRES_USER')} and password: {os.getenv('POSTGRES_PASSWORD')}")

def get_db_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def ingest_data():
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. Create Schema and Table (The Landing Zone)
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
                    with open(os.path.join(date_path, file_name), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        channel_name = file_name.replace('.json', '')
                        
                        # Prepare data for batch insert for efficiency
                        insert_query = "INSERT INTO raw.telegram_messages (channel, content) VALUES %s"
                        values = [(channel_name, json.dumps(msg)) for msg in data]
                        
                        extras.execute_values(cur, insert_query, values)
                        print(f"Ingested {len(values)} messages from {channel_name}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    ingest_data()