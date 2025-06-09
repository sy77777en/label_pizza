#!/usr/bin/env python3
"""
Database cleanup script for Label Pizza
Run this when you have connection issues with Supabase
"""

import os
import psycopg2
from dotenv import load_dotenv
import sys

def cleanup_database_connections():
    """Force cleanup of database connections"""
    
    load_dotenv(".env")
    
    try:
        # Parse the database URL to get connection details
        db_url = os.environ["DBURL"]
        
        # Extract connection details from URL
        # Expected format: postgresql://user:pass@host:port/db
        if "://" in db_url:
            protocol_part, rest = db_url.split("://", 1)
            
            if "@" in rest:
                auth_part, host_part = rest.split("@", 1)
                user, password = auth_part.split(":", 1)
                
                if "/" in host_part:
                    host_port, database = host_part.split("/", 1)
                    if ":" in host_port:
                        host, port = host_port.split(":", 1)
                    else:
                        host, port = host_port, "5432"
                else:
                    host, port, database = host_part, "5432", "postgres"
            else:
                print("Invalid database URL format")
                return False
        else:
            print("Invalid database URL format")
            return False
        
        print(f"Connecting to {host}:{port} as {user}...")
        
        # Connect directly using psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Get current connections
        cursor.execute("""
            SELECT pid, usename, application_name, client_addr, state, query_start 
            FROM pg_stat_activity 
            WHERE usename = %s AND state = 'idle'
        """, (user,))
        
        idle_connections = cursor.fetchall()
        print(f"Found {len(idle_connections)} idle connections")
        
        # Terminate idle connections
        for conn_info in idle_connections:
            pid = conn_info[0]
            try:
                cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
                print(f"Terminated connection {pid}")
            except Exception as e:
                print(f"Could not terminate {pid}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Database cleanup completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        return False

if __name__ == "__main__":
    print("🧹 Label Pizza Database Cleanup")
    print("=" * 40)
    
    success = cleanup_database_connections()
    
    if success:
        print("\n✅ Cleanup completed! You can now restart the app.")
        sys.exit(0)
    else:
        print("\n❌ Cleanup failed. Check your database connection.")
        sys.exit(1)