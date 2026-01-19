from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import database, schemas
from sqlalchemy.exc import SQLAlchemyError

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

@app.get("/analytics/detections", response_model=List[schemas.DetectionDetail])
def get_all_detections(db: Session = Depends(get_db)):
    """New endpoint to align with analytical contract requirements."""
    query = text("SELECT message_id, detected_item, image_category, confidence FROM marts.fct_image_detections")
    return db.execute(query).mappings().all()