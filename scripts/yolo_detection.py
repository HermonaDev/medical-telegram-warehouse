"""
Module: yolo_detection.py
Description: This script processes images collected from Telegram channels using 
             the YOLOv8 computer vision model. It identifies objects, extracts 
             bounding box coordinates, and saves the detections to both a 
             local CSV and a PostgreSQL data warehouse.
Author: Addisu
"""

import os
import cv2
import pandas as pd
from ultralytics import YOLO
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables for database credentials
load_dotenv()

# Initialize the YOLOv8 model
# 'yolov8n.pt' is the nano version, optimized for CPU speed and low latency
model = YOLO('yolov8n.pt') 

def run_detection() -> None:
    """
    Iterates through image directories, performs object detection, and exports results.
    
    The function follows these steps:
    1. Scans the 'data/raw/images' directory for channel-specific folders.
    2. Runs YOLO inference on every image found.
    3. Normalizes detection data (labels, confidence, bounding boxes).
    4. Saves results to 'data/yolo_results.csv'.
    5. Attempts to push results to the 'raw.detection_results' table in PostgreSQL.

    Returns:
        None
    """
    image_dir = 'data/raw/images'
    results_list = []

    # Check if the directory exists to avoid errors
    if not os.path.exists(image_dir):
        print(f"Error: Image directory '{image_dir}' not found.")
        return

    # Iterate through each channel folder (partitioned storage)
    for channel in os.listdir(image_dir):
        channel_path = os.path.join(image_dir, channel)
        if not os.path.isdir(channel_path):
            continue
        
        print(f"Processing images for channel: {channel}...")
        
        for img_name in os.listdir(channel_path):
            img_path = os.path.join(channel_path, img_name)
            
            # Run YOLO inference
            # results is a list of ultralytics.engine.results.Results objects
            results = model(img_path)
            
            for r in results:
                for box in r.boxes:
                    # Append detection metadata to results list
                    results_list.append({
                        'channel': channel,
                        'image_path': img_name,
                        'label': model.names[int(box.cls)],  # Convert class index to string label
                        'confidence': float(box.conf),       # Accuracy score (0.0 to 1.0)
                        'x_min': float(box.xyxy[0][0]),      # Bounding box coordinates
                        'y_min': float(box.xyxy[0][1]),
                        'x_max': float(box.xyxy[0][2]),
                        'y_max': float(box.xyxy[0][3])
                    })
    
    # Create DataFrame for structured data handling
    df = pd.DataFrame(results_list)
    
    # Save to CSV - serves as backup and interim report evidence
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/yolo_results.csv', index=False)
    print(f"Success! Detected {len(df)} objects in images. Results saved to CSV.")

    # Database Export Logic
    try:
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        db_name = os.getenv('POSTGRES_DB')
        
        # Build the SQLAlchemy engine
        engine = create_engine(f"postgresql://{user}:{password}@localhost:5432/{db_name}")
        
        # Push to Postgres - Uses 'replace' to ensure the latest detections are in the warehouse
        df.to_sql('detection_results', engine, schema='raw', if_exists='replace', index=False)
        print("Data successfully exported to PostgreSQL table 'raw.detection_results'.")
        
    except Exception as e:
        print(f"Database Export Failed: {e}")
        print("Tip: Ensure your .env credentials are correct and the database is running.")

if __name__ == "__main__":
    """
    Script entry point. Executes the object detection pipeline.
    """
    run_detection()