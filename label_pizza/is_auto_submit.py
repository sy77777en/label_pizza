import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
db_url = os.environ["DBURL"]

engine = create_engine(db_url)

def column_exists(engine, table_name, column_name):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table AND column_name = :column
            """),
            {"table": table_name, "column": column_name}
        )
        return result.first() is not None

def add_is_auto_submit_column(engine):
    if not column_exists(engine, "question_groups", "is_auto_submit"):
        print("Adding is_auto_submit column to question_groups...")
        with engine.connect() as conn:
            # Start a transaction
            trans = conn.begin()
            try:
                conn.execute(
                    text("ALTER TABLE question_groups ADD COLUMN is_auto_submit BOOLEAN DEFAULT FALSE")
                )
                # Commit the transaction
                trans.commit()
                print("✓ Column added successfully.")
            except Exception as e:
                # Rollback on error
                trans.rollback()
                print(f"✗ Error adding column: {e}")
                raise
    else:
        print("✓ Column is_auto_submit already exists in question_groups.")

if __name__ == "__main__":
    print("🔧 Checking and adding is_auto_submit column...")
    add_is_auto_submit_column(engine)
    print("🎉 Migration complete!")