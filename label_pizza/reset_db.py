"""
Reset Database Script
====================
This script drops all tables and recreates them with initial data.
Use with caution as it will delete all existing data.

Run with:
    python reset_db.py
"""
from __future__ import annotations
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db import SessionLocal, engine
from models import Base
from services import AuthService

# Load environment variables
load_dotenv(".env")

def reset_database():
    """Drop all tables and recreate them with initial data."""
    try:
        print("Starting database reset...")
        
        # Drop all tables
        with engine.connect() as conn:
            # Drop all tables in the public schema
            conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE;"))
            conn.execute(text("CREATE SCHEMA public;"))
            conn.commit()
        
        print("Dropped all tables.")
        
        # Create all tables
        Base.metadata.create_all(engine)
        print("Recreated all tables.")
        
        # Seed initial data
        with SessionLocal() as s:
            try:
                # Create admin user
                AuthService.seed_admin(s)
                print("Created admin user.")
                
                # Add more initial data here if needed
                
                print("Database reset complete!")
            except SQLAlchemyError as e:
                print(f"Error seeding initial data: {str(e)}")
                sys.exit(1)
                
    except SQLAlchemyError as e:
        print(f"Error during database reset: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Confirm before proceeding
    response = input("This will delete ALL data in the database. Are you sure? (yes/no): ")
    if response.lower() == "yes":
        reset_database()
    else:
        print("Database reset cancelled.") 