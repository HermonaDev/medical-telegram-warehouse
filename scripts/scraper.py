import os
import json
import logging
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv
from scripts.logger_config import get_logger

# Load environment variables
load_dotenv()
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

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

# Configure Logging
logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def scrape_channel(client, channel_username):
    """
    Scrapes messages from a specific Telegram channel.
    """
    # Ensure directory for images exists
    image_path = f'data/raw/images/{channel_username}'
    os.makedirs(image_path, exist_ok=True)
    
    # Ensure directory for partitioned JSON exists (YYYY-MM-DD)
    date_str = datetime.now().strftime('%Y-%m-%d')
    json_path = f'data/raw/telegram_messages/{date_str}'
    os.makedirs(json_path, exist_ok=True)

    messages_data = []
    logging.info(f"Starting scrape for {channel_username}...")

    async for message in client.iter_messages(channel_username, limit=100):
        # 1. Store Metadata
        data = {
            'channel': channel_username,
            'id': message.id,
            'date': str(message.date),
            'text': message.text,
            'views': message.views,
            'forwards': message.forwards
        }
        
        # 2. Download Image if exists
        if message.photo:
            filename = f"{channel_username}_{message.id}.jpg"
            await client.download_media(message.photo, file=os.path.join(image_path, filename))
            data['image_path'] = filename
        
        messages_data.append(data)

    # Save to JSON
    with open(f"{json_path}/{channel_username}.json", 'w', encoding='utf-8') as f:
        json.dump(messages_data, f, ensure_ascii=False, indent=4)
    
    logging.info(f"Successfully scraped {len(messages_data)} messages from {channel_username}")

async def main():
    async with TelegramClient('menorah_session', api_id, api_hash) as client:
        for channel in CHANNELS:
            try:
                await scrape_channel(client, channel)
            except Exception as e:
                logging.error(f"Error scraping {channel}: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())