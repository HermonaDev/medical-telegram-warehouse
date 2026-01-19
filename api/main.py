from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import database, schemas

app = FastAPI(title="Medical Telegram Warehouse API")

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/analytics/top-products", response_model=List[schemas.DetectionSummary])
def get_top_medical_products(db: Session = Depends(get_db)):
    """
    Business Question: What products are mentioned most?
    Since we don't have a 'label' column yet, we will count mentions in message_text.
    """
    query = text("""
        SELECT message_text as label, count(*) as total 
        FROM fct_messages 
        WHERE message_text IS NOT NULL
        GROUP BY message_text 
        ORDER BY total DESC 
        LIMIT 10
    """)
    try:
        return db.execute(query).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Top Products Error: {str(e)}")

@app.get("/analytics/channel-activity", response_model=List[schemas.ChannelActivity])
def get_channel_volume(db: Session = Depends(get_db)):
    """
    Business Question: Which channels are most active?
    We JOIN fct_messages with dim_channels to get the human-readable channel name.
    """
    query = text("""
        SELECT c.channel_name, count(f.message_id) as message_count 
        FROM fct_messages f
        JOIN dim_channels c ON f.channel_key = c.channel_key
        GROUP BY c.channel_name 
        ORDER BY message_count DESC
    """)
    try:
        return db.execute(query).mappings().all()
    except Exception as e:
        # Fallback if the join fails: use the key instead
        query_fallback = text("SELECT CAST(channel_key AS TEXT) as channel_name, count(*) as message_count FROM fct_messages GROUP BY channel_key")
        return db.execute(query_fallback).mappings().all()