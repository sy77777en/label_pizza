#!/usr/bin/env python3
"""
Database Initialization/Reset Script for Label Pizza
====================================================
This script can:
1. INIT mode: Create missing tables and seed admin user (safe for existing databases)
2. RESET mode: Drop all tables, recreate them, and seed admin user (DESTRUCTIVE!)

Usage:
    # Initialize database (safe, won't affect existing tables)
    python init_or_reset_db.py --mode init --email admin@example.com --password mypass --user-id "Admin"
    
    # Full reset (DESTRUCTIVE!)
    python init_or_reset_db.py --mode reset --email admin@example.com --password mypass --user-id "Admin"

    # Quick init with defaults
    python init_or_reset_db.py --mode init
"""

import argparse
import os
import sys
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import text, MetaData, Engine
from sqlalchemy.orm import Session

# Load environment variables
load_dotenv()

# Import your models and services
try:
    from label_pizza.models import Base, User
    from label_pizza.services import AuthService
    from label_pizza.db import init_database as init_db
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print("Make sure you're running this from the correct directory.")
    sys.exit(1)

def seed_admin_user(email: str, password: str, user_id: str, session_local: Session) -> bool:
    """Create an admin user with specified credentials"""
    try:
        with session_local() as session:
            AuthService.seed_admin(
                session=session,
                email=email,
                password=password,
                user_id=user_id
            )
            
            print(f"✅ Admin user created successfully!")
            print(f"   📧 Email: {email}")
            print(f"   👤 User ID: {user_id}")
            print(f"   🔑 Password: {'*' * len(password)}")
            
            return True
            
    except Exception as e:
        print(f"❌ Failed to create admin user: {e}")
        return False

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

def create_all_tables(engine, mode="reset"):
    """Create all tables from models"""
    if mode == "init":
        print("🏗️  Creating missing tables (safe mode)...")
    else:
        print("🏗️  Creating fresh tables...")
    
    # Create all tables defined in models
    # checkfirst=True by default, so safe for existing tables
    Base.metadata.create_all(bind=engine)
    
    # Count created tables
    metadata = MetaData()
    metadata.reflect(bind=engine)
    print(f"   ✅ Created/verified {len(metadata.tables)} tables")

def seed_sample_data(session_local: Session):
    """Seed some sample data for testing"""
    print("🌱 Seeding sample data...")
    
    with session_local() as session:
        try:
            # Import services
            from label_pizza.services import (
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
                        display_title="Sample Questions",
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

def verify_database(email: str, session_local: Session):
    """Verify the database setup was successful"""
    print("🔍 Verifying database...")
    
    with session_local() as session:
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

def init_database(email: str, password: str, user_id: str, force: bool = False, seed_sample: bool = False, engine: Engine = None, session_local: Session = None):
    """Initialize database safely (won't affect existing tables)"""
    print("🍕 Label Pizza Database Initialization")
    print("=" * 40)
    print("Mode: INIT (safe for existing databases)")
    print()
    
    if not force:
        print(f"📧 Email: {email}")
        print(f"👤 User ID: {user_id}")
        print(f"🔑 Password: {'*' * len(password)}")
        print()
        response = input("Initialize database with these settings? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("❌ Initialization cancelled")
            return False
    
    try:
        print("\n🚀 Starting database initialization...")
        
        # Create missing tables (safe operation)
        create_all_tables(engine, mode="init")
        
        # Seed admin user
        seed_admin_user(email, password, user_id, session_local)
        
        # Optional sample data
        if seed_sample:
            seed_sample_data(session_local)
        
        # Verify setup
        verify_database(email, session_local)
        
        print("\n🎉 Database initialization completed successfully!")
        print()
        print("You can now run your Streamlit app:")
        print("  streamlit run app.py")
        print()
        print("Login credentials:")
        print(f"  Email: {email}")
        print(f"  Password: {password}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Initialization failed: {e}")
        return False

def reset_database(email: str, password: str, user_id: str, force: bool = False, seed_sample: bool = False, engine: Engine = None, session_local: Session = None):
    """Reset database completely (DESTRUCTIVE!)"""
    print("🍕 Label Pizza Database Reset")
    print("=" * 40)
    print("Mode: RESET (DESTRUCTIVE - will delete all data!)")
    print()
    
    # Confirm reset
    if not force and not confirm_reset():
        print("❌ Reset cancelled")
        return False
    
    # Final confirmation
    if not force and not backup_before_reset():
        print("❌ Reset cancelled")
        return False
    
    try:
        print("\n🚀 Starting database reset...")
        
        # Drop and recreate all tables
        drop_all_tables(engine)
        create_all_tables(engine, mode="reset")
        
        # Seed admin user
        seed_admin_user(email, password, user_id, session_local)
        
        # Optional sample data
        if seed_sample:
            seed_sample_data(session_local)
        
        # Verify setup
        verify_database(email, session_local)
        
        print("\n🎉 Database reset completed successfully!")
        print()
        print("You can now run your Streamlit app:")
        print("  streamlit run app.py")
        print()
        print("Login credentials:")
        print(f"  Email: {email}")
        print(f"  Password: {password}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Reset failed: {e}")
        print("Your database may be in an inconsistent state.")
        print("Consider restoring from backup or contact support.")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Initialize or reset Label Pizza database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Safe initialization (recommended for first-time setup)
  python init_or_reset_db.py --mode init
  
  # Safe initialization with custom admin credentials
  python init_or_reset_db.py --mode init --email admin@mycompany.com --password SecurePass123 --user-id "My Admin"
  
  # Initialize with sample data for testing
  python init_or_reset_db.py --mode init --seed-sample-data
  
  # Full reset (DESTRUCTIVE!)
  python init_or_reset_db.py --mode reset --email admin@example.com --password mypass
  
  # Skip all confirmations
  python init_or_reset_db.py --mode init --force
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["init", "reset"],
        default="init",
        help="Operation mode: 'init' (safe, creates missing tables) or 'reset' (destructive, drops all tables first)"
    )
    
    parser.add_argument(
        "--email",
        default="admin@example.com",
        help="Email address for the admin user (default: admin@example.com)"
    )
    
    parser.add_argument(
        "--password",
        default="password123",
        help="Password for the admin user (default: password123)"
    )
    
    parser.add_argument(
        "--user-id",
        default="Admin User",
        help="User ID for the admin user (default: Admin User)"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip all confirmation prompts"
    )
    
    parser.add_argument(
        "--seed-sample-data",
        action="store_true",
        help="Seed sample data for testing (disabled by default)"
    )

    parser.add_argument(
        "--database-url-name",
        default="DBURL", 
        help="Environment variable name for database URL (default: DBURL)"
    )
    
    args = parser.parse_args()

    try:
        init_db(args.database_url_name)
        print(f"✅ Database initialized using {args.database_url_name}")
        from label_pizza.db import engine, SessionLocal
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        sys.exit(1)
    
    # Check database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Check your DBURL in .env file")
        sys.exit(1)
    
    # Run the appropriate mode
    if args.mode == "init":
        success = init_database(args.email, args.password, args.user_id, args.force, args.seed_sample_data, engine, SessionLocal)
    else:  # reset
        success = reset_database(args.email, args.password, args.user_id, args.force, args.seed_sample_data, engine, SessionLocal)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()