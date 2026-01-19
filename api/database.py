import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

# Create the SQLAlchemy engine
# Use pool_pre_ping=True to help recover from lost connections
# engine = create_engine(DATABASE_URL, pool_pre_ping=True)

engine = create_engine(
    DATABASE_URL, 
    # This line is the magic fix for "UndefinedTable"
    connect_args={'options': '-csearch_path=staging,public'}, 
    pool_pre_ping=True
)
# Create a SessionLocal class for database requests
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for our database models
Base = declarative_base()