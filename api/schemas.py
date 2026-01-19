from pydantic import BaseModel

class ChannelActivity(BaseModel):
    # This must match the name in your SQL 'SELECT ... AS name'
    channel_name: str 
    message_count: int

    class Config:
        from_attributes = True

class DetectionSummary(BaseModel):
    label: str
    total: int

    class Config:
        from_attributes = True
        
class DetectionDetail(BaseModel):
    message_id: int
    detected_item: str
    image_category: str
    confidence: float

    class Config:
        from_attributes = True