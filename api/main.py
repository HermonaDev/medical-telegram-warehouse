from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import List
import os

# Internal project imports
from . import database, schemas


app = FastAPI(
    title="Medical Telegram Analytics API",
    description="Relational analytics for scraped medical data and YOLO detections.",
    version="1.1.0"
)

# Dependency to get the database session
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    except SQLAlchemyError as e:
        # Avoid printing sensitive details in production
        print(f"Database Connection Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        db.close()

@app.get("/")
def root():
    return {"message": "Medical Data Warehouse API is Live", "docs": "/docs"}

# --- TASK 3 & 4: ANALYTICAL ENDPOINTS ---

@app.get("/analytics/channel-activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(db: Session = Depends(get_db)):
    """Requirement: Reports message volume per channel."""
    sql = text("""
        SELECT c.channel_name, count(f.message_id) as message_count 
        FROM marts.fct_messages f
        JOIN marts.dim_channels c ON f.channel_key = c.channel_key
        GROUP BY c.channel_name 
        ORDER BY message_count DESC
    """)
    return db.execute(sql).mappings().all()

@app.get("/analytics/search", response_model=List[schemas.MessageSearch])
def search_messages(
    query: str = Query(..., min_length=3, description="Text to search for in messages"), 
    db: Session = Depends(get_db)
):
    """Requirement: Message Search endpoint with ILIKE pattern matching."""
    sql = text("""
        SELECT message_id, channel_key, message_text, timestamp 
        FROM marts.fct_messages 
        WHERE message_text ILIKE :q
        LIMIT 50
    """)
    return db.execute(sql, {"q": f"%{query}%"}).mappings().all()

@app.get("/analytics/visual-report", response_model=List[schemas.VisualReport])
def get_visual_content_report(db: Session = Depends(get_db)):
    """Requirement: Summary of detections grouped by the new image_category field."""
    sql = text("""
        SELECT image_category, count(*) as detection_count, AVG(confidence_score) as avg_confidence
        FROM marts.fct_image_detections 
        GROUP BY image_category
        ORDER BY detection_count DESC
    """)
    return db.execute(sql).mappings().all()

@app.get("/analytics/detections", response_model=List[schemas.DetectionDetail])
def get_detailed_detections(db: Session = Depends(get_db)):
    """Requirement: Aligned detections with date_key, detected_class, and confidence_score."""
    sql = text("""
        SELECT message_id, date_key, detected_class, confidence_score, image_category 
        FROM marts.fct_image_detections
        LIMIT 100
    """)
    return db.execute(sql).mappings().all()

# --- HEALTH & DEBUG ---

@app.get("/health")
def health_check():
    return {"status": "healthy", "environment": os.getenv("ENV", "development")}