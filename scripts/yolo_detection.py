import os
import cv2
import pandas as pd
from ultralytics import YOLO
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Load YOLOv8 model (using the nano version for speed)
model = YOLO('yolov8n.pt') 

def run_detection():
    image_dir = 'data/raw/images'
    results_list = []

    # Iterate through channel folders
    for channel in os.listdir(image_dir):
        channel_path = os.path.join(image_dir, channel)
        if not os.path.isdir(channel_path): continue
        
        for img_name in os.listdir(channel_path):
            img_path = os.path.join(channel_path, img_name)
            
            # Run inference
            results = model(img_path)
            
            for r in results:
                for box in r.boxes:
                    results_list.append({
                        'channel': channel,
                        'image_path': img_name,
                        'label': model.names[int(box.cls)],
                        'confidence': float(box.conf),
                        'x_min': float(box.xyxy[0][0]),
                        'y_min': float(box.xyxy[0][1]),
                        'x_max': float(box.xyxy[0][2]),
                        'y_max': float(box.xyxy[0][3])
                    })
    
    # Save to CSV for the interim report evidence
    df = pd.DataFrame(results_list)
    df.to_csv('data/yolo_results.csv', index=False)
    print(f"Detected {len(df)} objects in images.")

    # Optional: Push to Postgres
    engine = create_engine(f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}")
    df.to_sql('detection_results', engine, schema='raw', if_exists='replace', index=False)

if __name__ == "__main__":
    run_detection()