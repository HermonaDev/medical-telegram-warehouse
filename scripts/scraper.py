"""
Module: scraper.py
Description: Scrapes messages and media from specified Telegram channels using the 
             Telethon library. Data is partitioned by date in JSON format, and 
             images are stored in channel-specific directories.
Author: Addisu
"""

import os
import json
import logging
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv
from scripts.logger_config import get_logger

# Load environment variables (TG_API_ID and TG_API_HASH)
load_dotenv()
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

# Initialize professional logger
logger = get_logger("TelegramScraper")

# List of medical-related channels to scrape
CHANNELS = [
    'lobelia4cosmetics', 
    'CheMed123', 
    'tikvahpharma',
    'HakimApps_Guideline', 
    'rayapharmaceuticals', 
    'cafimadEt', 
    'ellamedicals'
]

async def scrape_channel(client: TelegramClient, channel_username: str) -> None:
    """
    Scrapes the most recent messages and associated media from a Telegram channel.

    The function performs the following steps:
    1. Creates local directories for image storage and JSON metadata.
    2. Iterates through the last 100 messages in the channel.
    3. Downloads photos if present.
    4. Saves all metadata into a partitioned JSON file.

    Args:
        client (TelegramClient): An authenticated Telethon client instance.
        channel_username (str): The username/handle of the Telegram channel.

    Returns:
        None
    """
    # 1. Directory Setup
    # Path for images: data/raw/images/<channel_name>/
    image_path = f'data/raw/images/{channel_username}'
    os.makedirs(image_path, exist_ok=True)
    
    # Path for JSON: data/raw/telegram_messages/YYYY-MM-DD/
    date_str = datetime.now().strftime('%Y-%m-%d')
    json_path = f'data/raw/telegram_messages/{date_str}'
    os.makedirs(json_path, exist_ok=True)

    messages_data = []
    logger.info(f"Starting scrape for {channel_username}...")

    # 2. In-depth Scraping
    async for message in client.iter_messages(channel_username, limit=100):
        # Store metadata in a dictionary
        data = {
            'channel': channel_username,
            'id': message.id,
            'date': str(message.date),
            'text': message.text,
            'views': message.views,
            'forwards': message.forwards,
            'image_path': None  # Default if no photo exists
        }
        
        # Download Image if the message contains media
        if message.photo:
            filename = f"{channel_username}_{message.id}.jpg"
            save_path = os.path.join(image_path, filename)
            await client.download_media(message.photo, file=save_path)
            data['image_path'] = filename
        
        messages_data.append(data)

    # 3. Data Persistence
    output_file = f"{json_path}/{channel_username}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(messages_data, f, ensure_ascii=False, indent=4)
    
    logger.info(f"Successfully scraped {len(messages_data)} messages from {channel_username}")

async def main():
    """
    Asynchronous main entry point. 
    Initializes the TelegramClient session and iterates through the channel list.
    """
    # 'menorah_session' stores authentication locally to avoid logging in every time
    async with TelegramClient('menorah_session', api_id, api_hash) as client:
        for channel in CHANNELS:
            try:
                await scrape_channel(client, channel)
            except Exception as e:
                logger.error(f"Error scraping {channel}: {e}")

if __name__ == "__main__":
    import asyncio
    
    # Run the asynchronous event loop
    logger.info("Starting Telegram Scraper service...")
    asyncio.run(main())
    logger.info("Telegram Scraper service finished.")