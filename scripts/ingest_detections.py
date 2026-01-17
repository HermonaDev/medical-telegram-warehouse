import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

df = pd.read_csv('data/yolo_results.csv')

# Use psycopg2 directly to ensure a hard commit
conn = psycopg2.connect(
    host="localhost",
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    port="5432"
)
cur = conn.cursor()

# Create table manually to be 100% sure
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
cur.execute("""
    CREATE TABLE IF NOT EXISTS raw.detection_results (
        channel TEXT,
        image_path TEXT,
        label TEXT,
        confidence FLOAT,
        x_min FLOAT,
        y_min FLOAT,
        x_max FLOAT,
        y_max FLOAT
    );
""")
conn.commit()

# Now use SQLAlchemy to push the data
engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}")
df.to_sql('detection_results', engine, schema='raw', if_exists='replace', index=False)

print("DATA PUSHED. VERIFYING...")
cur.execute("SELECT count(*) FROM raw.detection_results;")
print(f"Verified in DB: {cur.fetchone()[0]} rows")

cur.close()
conn.close()