"""
Module: ingest_detections.py
Description: This script handles the ingestion of YOLO object detection results from a 
             CSV file into a PostgreSQL data warehouse. It ensures the target schema 
             exists and performs a verification count after ingestion.
Author: Addisu
"""

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from logger_config import get_logger  

# Load environment variables from .env file
load_dotenv()

# Initialize professional logger
logger = get_logger("DetectionIngest")

def setup_raw_schema(db_params: dict) -> None:
    """
    Creates the 'raw' schema and the 'detection_results' table if they do not exist.
    
    Uses the psycopg2 library for a direct, hard-committed connection to handle 
    Data Definition Language (DDL) operations.

    Args:
        db_params (dict): A dictionary containing database connection parameters:
                          host, database, user, password, and port.

    Raises:
        psycopg2.Error: If the database connection or execution fails.
    """
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            # Ensure the raw schema exists for the data warehouse
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            
            # Create the results table with specific types for detection data
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
    """
    Reads a CSV file containing detections and pushes it to PostgreSQL using SQLAlchemy.

    The function uses pandas for efficient CSV reading and SQLAlchemy for the 
    database transaction. It replaces the existing table data with new data.

    Args:
        file_path (str): The local path to the YOLO results CSV file.
        db_url (str): The SQLAlchemy-formatted connection string.

    Returns:
        None
    """
    if not os.path.exists(file_path):
        logger.error(f"Source file not found: {file_path}")
        return

    try:
        # Load the detection data
        df = pd.read_csv(file_path)
        engine = create_engine(db_url)
        
        # Load data into PostgreSQL (schema 'raw')
        # if_exists='replace' ensures the warehouse table is fresh
        df.to_sql('detection_results', engine, schema='raw', if_exists='replace', index=False)
        
        logger.info(f"Successfully ingested {len(df)} rows into raw.detection_results")
        
        # Verification step: Count rows in the database to ensure sync
        with engine.connect() as connection:
            result = connection.execute(text("SELECT count(*) FROM raw.detection_results"))
            count = result.scalar()
            logger.info(f"Verification successful: {count} rows present in DB.")
            
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error during ingestion: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    """
    Main entry point for the ingestion script.
    Loads configurations and executes the schema setup and ingestion functions.
    """
    # Configuration from Environment Variables
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASS = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = os.getenv('POSTGRES_DB')
    DB_HOST = "localhost"
    DB_PORT = "5432"

    # Connection dictionary for psycopg2
    DB_PARAMS = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "port": DB_PORT
    }
    
    # Connection string for SQLAlchemy
    SQLALCHEMY_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    CSV_PATH = 'data/yolo_results.csv'

    logger.info("Starting ingestion process...")
    
    # 1. Prepare Database Structure
    setup_raw_schema(DB_PARAMS)
    
    # 2. Ingest Data
    ingest_csv_to_postgres(CSV_PATH, SQLALCHEMY_URL)
    
    logger.info("Ingestion process completed.")