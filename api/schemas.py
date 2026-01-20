from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

# --- Channel Analytics ---
class ChannelActivity(BaseModel):
    channel_name: str
    message_count: int

    class Config:
        from_attributes = True

# --- Message Search  ---
class MessageSearch(BaseModel):
    message_id: int
    channel_key: int
    message_text: Optional[str]
    timestamp: datetime

    class Config:
        from_attributes = True

# --- Detection Detail
class DetectionDetail(BaseModel):
    message_id: int
    date_key: int
    detected_class: str      
    confidence_score: float  
    image_category: str      

    class Config:
        from_attributes = True

# --- Visual Content Report ---
class VisualReport(BaseModel):
    image_category: str
    detection_count: int
    avg_confidence: float

    class Config:
        from_attributes = True