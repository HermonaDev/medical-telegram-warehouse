import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from logger_config import get_logger  

# Load environment variables
load_dotenv()

# Initialize professional logger
logger = get_logger("DetectionIngest")

def setup_raw_schema(db_params: dict) -> None:
    """Creates the raw schema and detection table using psycopg2 for hard commit."""
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
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
            logger.info("Database schema and table verified.")
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error during schema setup: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def ingest_csv_to_postgres(file_path: str, db_url: str) -> None:
    """Reads CSV and uses SQLAlchemy to push data to the warehouse."""
    if not os.path.exists(file_path):
        logger.error(f"Source file not found: {file_path}")
        return

    try:
        df = pd.read_csv(file_path)
        engine = create_engine(db_url)
        
        # Using 'replace' as per your original logic, but wrapped in logging
        df.to_sql('detection_results', engine, schema='raw', if_exists='replace', index=False)
        
        logger.info(f"Successfully ingested {len(df)} rows into raw.detection_results")
        
        # Verification step
        with engine.connect() as connection:
            # Wrap the string in text()
            result = connection.execute(text("SELECT count(*) FROM raw.detection_results"))
            count = result.scalar()
            logger.info(f"Verification successful: {count} rows present in DB.")
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error during ingestion: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Configuration
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASS = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = os.getenv('POSTGRES_DB')
    DB_HOST = "localhost"
    DB_PORT = "5432"

    DB_PARAMS = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "port": DB_PORT
    }
    
    SQLALCHEMY_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    CSV_PATH = 'data/yolo_results.csv'

    logger.info("Starting ingestion process...")
    setup_raw_schema(DB_PARAMS)
    ingest_csv_to_postgres(CSV_PATH, SQLALCHEMY_URL)
    logger.info("Ingestion process completed.")