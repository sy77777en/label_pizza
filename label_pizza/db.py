# db.py  – lives next to models.py and app.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv(".env")  # loads DBURL

# Base class for all models
Base = declarative_base()

# Import models to ensure they are registered with Base
from label_pizza.models import *  # This ensures all models are registered with Base

# Production database configuration
engine = create_engine(os.environ["DBURL"], echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# Test database configuration
test_engine = create_engine(os.environ.get("TEST_DBURL", "sqlite:///:memory:"), echo=False, future=True)
TestSessionLocal = sessionmaker(bind=test_engine, expire_on_commit=False)

def init_test_db():
    """Initialize the test database by creating all tables."""
    Base.metadata.drop_all(test_engine)
    Base.metadata.create_all(test_engine)

def get_test_session():
    """Get a new test database session."""
    return TestSessionLocal()
