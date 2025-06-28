#!/usr/bin/env python3
"""
Migration script to add display_title column and populate it.
This script:
1. Adds the display_title column if it doesn't exist
2. Populates it with existing title values
"""

import os
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url-name", default="DBURL")
    args = parser.parse_args()
    
    # Load environment variables from .env file (same as your db.py)
    print("🔧 Loading environment variables...")
    
    # Try to find .env file in current directory or parent directory
    env_paths = [".env", "../.env", "label_pizza/.env"]
    env_loaded = False
    
    for env_path in env_paths:
        if os.path.exists(env_path):
            print(f"📁 Found .env file at: {env_path}")
            load_dotenv(env_path)
            env_loaded = True
            break
    
    if not env_loaded:
        print("⚠️  No .env file found, trying environment variables directly...")
        load_dotenv()  # Try to load from default locations
    
    # Get database URL from environment
    db_url = os.environ.get(args.database_url_name)
    if not db_url:
        print(f"❌ Environment variable {args.database_url_name} not found")
        print(f"\n💡 Available environment variables containing 'DB' or 'URL':")
        relevant_vars = []
        for key in sorted(os.environ.keys()):
            if 'DB' in key.upper() or 'URL' in key.upper():
                # Don't show the full value for security, just confirm it exists
                value_preview = f"{os.environ[key][:20]}..." if len(os.environ[key]) > 20 else os.environ[key]
                relevant_vars.append(f"   - {key}: {value_preview}")
        
        if relevant_vars:
            for var in relevant_vars:
                print(var)
        else:
            print("   (No environment variables found containing 'DB' or 'URL')")
        
        print(f"\n🔍 Current working directory: {os.getcwd()}")
        print(f"📁 Looking for .env file in: {', '.join(env_paths)}")
        
        return
    
    print(f"✅ Using database URL from {args.database_url_name}")
    print(f"🔧 Connecting to database...")
    
    engine = create_engine(db_url)
    
    try:
        with engine.begin() as conn:
            # Step 1: Check if display_title column exists
            print("🔍 Checking if display_title column exists...")
            
            check_column = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = 'question_groups' 
                AND column_name = 'display_title'
            """))
            
            column_exists = check_column.fetchone()[0] > 0
            
            if column_exists:
                print("✅ display_title column already exists")
            else:
                print("➕ Adding display_title column...")
                
                # Add the column
                conn.execute(text("""
                    ALTER TABLE question_groups 
                    ADD COLUMN display_title VARCHAR
                """))
                
                print("✅ display_title column added successfully")
            
            # Step 2: Check current state
            print("\n📊 Checking current question groups...")
            
            result = conn.execute(text("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN display_title IS NOT NULL AND display_title != '' THEN 1 ELSE 0 END) as with_display_title
                FROM question_groups
            """))
            
            row = result.fetchone()
            total_groups = row[0]
            groups_with_display_title = row[1]
            needs_migration = total_groups - groups_with_display_title
            
            print(f"📋 Found {total_groups} question groups")
            print(f"✅ Already have display_title: {groups_with_display_title}")
            print(f"🔄 Need migration: {needs_migration}")
            
            if needs_migration == 0:
                print("🎉 All question groups already have display_title!")
                return
            
            # Step 3: Show preview
            print("\n📝 Preview of groups to be migrated:")
            
            preview = conn.execute(text("""
                SELECT id, title 
                FROM question_groups 
                WHERE display_title IS NULL OR display_title = ''
                ORDER BY id
                LIMIT 10
            """))
            
            preview_rows = preview.fetchall()
            for row in preview_rows:
                print(f"  - ID {row[0]}: '{row[1]}' -> '{row[1]}' (display_title)")
            
            if needs_migration > 10:
                print(f"  ... and {needs_migration - 10} more")
            
            # Step 4: Ask for confirmation
            response = input(f"\n❓ Proceed with migrating {needs_migration} groups? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("❌ Migration cancelled.")
                return
            
            # Step 5: Perform migration
            print("\n🚀 Executing migration...")
            result = conn.execute(text("""
                UPDATE question_groups 
                SET display_title = title 
                WHERE display_title IS NULL OR display_title = ''
            """))
            
            updated_count = result.rowcount
            print(f"✅ Updated {updated_count} question groups")
            
            # Step 6: Verify results
            print("\n🔍 Verifying migration...")
            verify_result = conn.execute(text("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN display_title IS NOT NULL AND display_title != '' THEN 1 ELSE 0 END) as with_display_title
                FROM question_groups
            """))
            
            row = verify_result.fetchone()
            print(f"📊 Final result: {row[1]}/{row[0]} groups have display_title")
            
            if row[1] == row[0]:
                print("🎉 Migration completed successfully!")
                
                # Show a few examples
                print("\n📋 Sample migrated groups:")
                sample_result = conn.execute(text("""
                    SELECT id, title, display_title 
                    FROM question_groups 
                    WHERE display_title IS NOT NULL 
                    ORDER BY id 
                    LIMIT 5
                """))
                
                for row in sample_result:
                    print(f"  - ID {row[0]}: title='{row[1]}', display_title='{row[2]}'")
                    
            else:
                print("⚠️ Some groups may still be missing display_title")
                
                # Show any remaining issues
                missing_result = conn.execute(text("""
                    SELECT id, title 
                    FROM question_groups 
                    WHERE display_title IS NULL OR display_title = ''
                    LIMIT 5
                """))
                
                missing_rows = missing_result.fetchall()
                if missing_rows:
                    print("\n❌ Groups still missing display_title:")
                    for row in missing_rows:
                        print(f"  - ID {row[0]}: '{row[1]}'")
            
    except Exception as e:
        print(f"💥 Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🍕 Display Title Column Migration")
    print("=" * 35)
    print("This script will:")
    print("1. 📁 Load .env file (like your db.py)")
    print("2. ➕ Add display_title column (if needed)")
    print("3. 📋 Copy title -> display_title for all groups")
    print("4. ✅ Verify the migration")
    print()
    main()