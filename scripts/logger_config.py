import logging
import os

def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a unified logger instance.
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console Handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        
        # File Handler
        fh = logging.FileHandler(f'logs/warehouse_pipeline.log')
        fh.setFormatter(formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)
        
    return logger