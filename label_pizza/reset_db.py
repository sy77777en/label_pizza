#!/usr/bin/env python3
"""
Database Reset Script for Label Pizza
=====================================
This script will:
1. Drop all existing tables
2. Recreate fresh tables
3. Seed the admin user
4. Optionally load sample data

WARNING: This will DELETE ALL DATA in your database!
"""

import os
import sys
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.orm import Session

# Load environment variables
load_dotenv()

# Import your models and services
try:
    from models import Base, User
    from services import AuthService
    from db import engine, SessionLocal
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print("Make sure you're running this from the correct directory.")
    sys.exit(1)

def confirm_reset() -> bool:
    """Ask user to confirm database reset"""
    print("🚨 WARNING: This will DELETE ALL DATA in your database!")
    print("This action cannot be undone.")
    print()
    
    # Show current database URL (masked for security)
    db_url = os.getenv("DBURL", "Not found")
    if db_url != "Not found":
        # Mask password in URL for display
        masked_url = db_url
        if "@" in masked_url:
            parts = masked_url.split("@")
            user_pass = parts[0].split("//")[1]
            if ":" in user_pass:
                user, password = user_pass.split(":", 1)
                masked_password = password[:3] + "*" * (len(password) - 3)
                masked_url = masked_url.replace(f":{password}@", f":{masked_password}@")
        print(f"Database: {masked_url}")
    
    print()
    response = input("Type 'RESET' to confirm database reset: ")
    return response.strip() == "RESET"

def backup_before_reset() -> bool:
    """Ask if user wants to create a backup first"""
    print()
    print("💡 Tip: Consider creating a backup before resetting.")
    print("You can use the backup script or Supabase dashboard.")
    print()
    response = input("Continue with reset? (y/N): ")
    return response.lower() in ['y', 'yes']

def drop_all_tables(engine):
    """Drop all tables in the database"""
    print("🗑️  Dropping all tables...")
    
    # Reflect the current database schema
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    # Drop all tables
    metadata.drop_all(bind=engine)
    print(f"   ✅ Dropped {len(metadata.tables)} tables")

def create_all_tables(engine):
    """Create all tables from models"""
    print("🏗️  Creating fresh tables...")
    
    # Create all tables defined in models
    Base.metadata.create_all(bind=engine)
    
    # Count created tables
    metadata = MetaData()
    metadata.reflect(bind=engine)
    print(f"   ✅ Created {len(metadata.tables)} tables")

def seed_admin_user():
    """Create the default admin user"""
    print("👤 Seeding admin user...")
    
    with SessionLocal() as session:
        # Check if admin already exists
        existing_admin = session.execute(
            text("SELECT COUNT(*) FROM users WHERE email = 'zhiqiulin98@gmail.com'")
        ).scalar()
        
        if existing_admin == 0:
            AuthService.seed_admin(session)
            print("   ✅ Admin user created")
            print("   📧 Email: zhiqiulin98@gmail.com")
            print("   🔑 Password: zhiqiulin98")
        else:
            print("   ℹ️  Admin user already exists")

def seed_sample_data():
    """Optionally seed some sample data for testing"""
    print()
    response = input("Would you like to seed sample data for testing? (y/N): ")
    
    if response.lower() not in ['y', 'yes']:
        return
    
    print("🌱 Seeding sample data...")
    
    with SessionLocal() as session:
        try:
            # Import services
            from services import (
                VideoService, QuestionService, QuestionGroupService, 
                SchemaService, ProjectService
            )
            
            # Add sample videos
            sample_videos = [
                "https://example.com/video1.mp4",
                "https://example.com/video2.mp4", 
                "https://example.com/video3.mp4"
            ]
            
            for url in sample_videos:
                try:
                    VideoService.add_video(url, session, {"sample": True})
                except Exception as e:
                    print(f"   ⚠️  Skipped video {url}: {str(e)}")
            
            # Add sample questions
            sample_questions = [
                {
                    "text": "What is the main object in this video?",
                    "type": "single",
                    "options": ["Person", "Car", "Animal", "Building", "Other"]
                },
                {
                    "text": "Describe what happens in this video",
                    "type": "description",
                    "options": None
                },
                {
                    "text": "What is the video quality?",
                    "type": "single", 
                    "options": ["Poor", "Fair", "Good", "Excellent"]
                }
            ]
            
            question_ids = []
            for q in sample_questions:
                try:
                    question = QuestionService.add_question(
                        text=q["text"],
                        display_text=q["text"],
                        qtype=q["type"],
                        options=q["options"],
                        default=q["options"][0] if q["options"] else None,
                        session=session
                    )
                    question_ids.append(question.id)
                    print(f"   ✅ Created question: {q['text'][:50]}...")
                except Exception as e:
                    print(f"   ⚠️  Skipped question: {str(e)}")
            
            # Create sample question group
            if question_ids:
                try:
                    group = QuestionGroupService.create_group(
                        title="Sample Questions",
                        description="Sample questions for testing",
                        is_reusable=True,
                        question_ids=question_ids,
                        verification_function=None,
                        session=session
                    )
                    print(f"   ✅ Created question group: {group.title}")
                    
                    # Create sample schema
                    schema = SchemaService.create_schema(
                        name="Sample Schema",
                        question_group_ids=[group.id],
                        session=session
                    )
                    print(f"   ✅ Created schema: {schema.name}")
                    
                    # Create sample project
                    video_ids = VideoService.get_video_ids_by_uids(
                        ["video1.mp4", "video2.mp4", "video3.mp4"], 
                        session
                    )
                    if video_ids:
                        ProjectService.create_project(
                            name="Sample Project",
                            schema_id=schema.id,
                            video_ids=video_ids,
                            session=session
                        )
                        print(f"   ✅ Created sample project")
                    
                except Exception as e:
                    print(f"   ⚠️  Error creating sample data: {str(e)}")
                    
        except ImportError as e:
            print(f"   ⚠️  Could not seed sample data: {e}")

def verify_reset():
    """Verify the reset was successful"""
    print("🔍 Verifying reset...")
    
    with SessionLocal() as session:
        try:
            # Check that admin user exists
            admin_count = session.execute(
                text("SELECT COUNT(*) FROM users WHERE user_type = 'admin'")
            ).scalar()
            
            if admin_count > 0:
                print("   ✅ Admin user verified")
            else:
                print("   ❌ Admin user not found")
                
            # Check tables exist
            tables = session.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
            ).fetchall()
            
            table_names = [row[0] for row in tables]
            expected_tables = [
                'users', 'videos', 'projects', 'schemas', 'questions',
                'question_groups', 'annotator_answers', 'reviewer_ground_truth'
            ]
            
            for table in expected_tables:
                if table in table_names:
                    print(f"   ✅ Table '{table}' exists")
                else:
                    print(f"   ❌ Table '{table}' missing")
                    
        except Exception as e:
            print(f"   ❌ Verification failed: {e}")

def main():
    """Main reset function"""
    print("🍕 Label Pizza Database Reset Script")
    print("=" * 40)
    
    # Check database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Check your DBURL in .env file")
        sys.exit(1)
    
    # Confirm reset
    if not confirm_reset():
        print("❌ Reset cancelled")
        sys.exit(0)
    
    # Final confirmation
    if not backup_before_reset():
        print("❌ Reset cancelled")
        sys.exit(0)
    
    try:
        # Perform reset
        print("\n🚀 Starting database reset...")
        
        drop_all_tables(engine)
        create_all_tables(engine)
        seed_admin_user()
        seed_sample_data()
        verify_reset()
        
        print("\n🎉 Database reset completed successfully!")
        print()
        print("You can now run your Streamlit app:")
        print("  streamlit run app.py")
        print()
        print("Login credentials:")
        print("  Email: zhiqiulin98@gmail.com")
        print("  Password: zhiqiulin98")
        
    except Exception as e:
        print(f"\n❌ Reset failed: {e}")
        print("Your database may be in an inconsistent state.")
        print("Consider restoring from backup or contact support.")
        sys.exit(1)

if __name__ == "__main__":
    main()