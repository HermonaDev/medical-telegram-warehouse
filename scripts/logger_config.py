"""
Module: logger_config.py
Description: Provides a centralized logging configuration for the medical-telegram-warehouse 
             pipeline. It ensures logs are consistently formatted and directed to both 
             the console and a persistent log file.
Author: Addisu
"""

import logging
import os

def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a unified logger instance for the project.

    This function sets up a logger that outputs to both the standard console (stdout) 
    and a log file located in the 'logs/' directory. It prevents duplicate handlers 
    if the logger is initialized multiple times across different modules.

    Args:
        name (str): The name of the logger, typically the module name 
                    (e.g., __name__ or 'Scraper').

    Returns:
        logging.Logger: A configured logger instance set to the INFO level.
    """
    # Create logs directory if it doesn't exist to prevent FileNotFoundError
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Get a logger with the specified name
    logger = logging.getLogger(name)
    
    # Check if handlers already exist to avoid duplicate logs in long-running processes
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Define a consistent format: [Timestamp] - [Module Name] - [Log Level] - [Message]
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console Handler: For real-time monitoring in the terminal
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        
        # File Handler: For persistent record keeping
        # We use 'a' (append) mode by default and UTF-8 encoding for safety
        fh = logging.FileHandler('logs/warehouse_pipeline.log', encoding='utf-8')
        fh.setFormatter(formatter)

        # Add both handlers to the logger instance
        logger.addHandler(ch)
        logger.addHandler(fh)
        
    return logger