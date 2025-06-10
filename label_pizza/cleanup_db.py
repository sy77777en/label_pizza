#!/usr/bin/env python3
"""
Improved Database cleanup script for Label Pizza
Run this when you have connection issues with Supabase
"""
import os
import psycopg2
from dotenv import load_dotenv
import sys
import time
from urllib.parse import urlparse

def test_connectivity(host, port):
    """Test basic network connectivity"""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"❌ Socket test failed: {e}")
        return False

def parse_db_url(db_url):
    """Parse database URL into components"""
    try:
        parsed = urlparse(db_url)
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'user': parsed.username,
            'password': parsed.password,
            'database': parsed.path.lstrip('/') or 'postgres'
        }
    except Exception as e:
        print(f"❌ Error parsing database URL: {e}")
        return None

def cleanup_database_connections():
    """Force cleanup of database connections with improved error handling"""
    
    load_dotenv(".env")
    
    # Get database URL
    db_url = os.environ.get("DBURL")
    if not db_url:
        print("❌ DBURL environment variable not found")
        return False
    
    print(f"📡 Database URL found: {db_url[:50]}...")
    
    # Parse connection details
    conn_params = parse_db_url(db_url)
    if not conn_params:
        return False
    
    host = conn_params['host']
    port = conn_params['port']
    user = conn_params['user']
    
    print(f"🔍 Testing connectivity to {host}:{port}...")
    
    # Test basic connectivity first
    if not test_connectivity(host, port):
        print(f"❌ Cannot reach {host}:{port}")
        print("💡 Troubleshooting suggestions:")
        print("   • Check your internet connection")
        print("   • Try a different network (mobile hotspot)")
        print("   • Check if port 5432 is blocked by firewall")
        print("   • Visit https://status.supabase.com/ for service status")
        return False
    
    print(f"✅ Host is reachable!")
    print(f"🔌 Attempting database connection as {user}...")
    
    try:
        # Try multiple connection strategies
        connection_configs = [
            # Strategy 1: Default with longer timeout
            {
                **conn_params,
                'connect_timeout': 30,
                'application_name': 'label_pizza_cleanup'
            },
            # Strategy 2: With SSL disabled (if allowed)
            {
                **conn_params,
                'connect_timeout': 30,
                'sslmode': 'prefer',
                'application_name': 'label_pizza_cleanup'
            }
        ]
        
        conn = None
        for i, config in enumerate(connection_configs, 1):
            print(f"🔄 Trying connection strategy {i}...")
            try:
                conn = psycopg2.connect(**config)
                print(f"✅ Connected using strategy {i}!")
                break
            except psycopg2.OperationalError as e:
                print(f"❌ Strategy {i} failed: {e}")
                if i < len(connection_configs):
                    print("   Trying next strategy...")
                    time.sleep(2)
        
        if not conn:
            print("❌ All connection strategies failed")
            return False
        
        cursor = conn.cursor()
        
        # Get database info
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"📊 Connected to: {version}")
        
        # Get current connections
        cursor.execute("""
            SELECT pid, usename, application_name, client_addr, state, 
                   extract(epoch from (now() - query_start)) as idle_seconds
            FROM pg_stat_activity 
            WHERE usename = %s AND state = 'idle'
            ORDER BY idle_seconds DESC
        """, (user,))
        
        idle_connections = cursor.fetchall()
        print(f"🔍 Found {len(idle_connections)} idle connections")
        
        if idle_connections:
            print("📋 Idle connections:")
            for i, conn_info in enumerate(idle_connections[:5], 1):  # Show first 5
                pid, username, app_name, client_addr, state, idle_seconds = conn_info
                print(f"   {i}. PID {pid}: {app_name or 'Unknown'} (idle {idle_seconds:.0f}s)")
            
            if len(idle_connections) > 5:
                print(f"   ... and {len(idle_connections) - 5} more")
        
        # Terminate idle connections older than 5 minutes
        terminated_count = 0
        for conn_info in idle_connections:
            pid, username, app_name, client_addr, state, idle_seconds = conn_info
            
            # Only terminate connections idle for more than 5 minutes
            if idle_seconds > 300:  # 5 minutes
                try:
                    cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
                    print(f"🔥 Terminated connection {pid} (idle {idle_seconds:.0f}s)")
                    terminated_count += 1
                except Exception as e:
                    print(f"❌ Could not terminate {pid}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Database cleanup completed! Terminated {terminated_count} connections.")
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection error: {e}")
        print("💡 Common solutions:")
        print("   • Check your database credentials")
        print("   • Verify the database URL is correct")
        print("   • Try using the direct connection string instead of pooler")
        print("   • Check Supabase dashboard for connection limits")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during cleanup: {e}")
        return False

if __name__ == "__main__":
    print("🧹 Label Pizza Database Cleanup (Enhanced)")
    print("=" * 50)
    
    success = cleanup_database_connections()
    
    if success:
        print("\n🎉 Cleanup completed successfully!")
        print("💡 You can now restart your application.")
        sys.exit(0)
    else:
        print("\n❌ Cleanup failed. Please check the suggestions above.")
        print("🔗 For more help, check:")
        print("   • Supabase docs: https://supabase.com/docs")
        print("   • Status page: https://status.supabase.com/")
        sys.exit(1)