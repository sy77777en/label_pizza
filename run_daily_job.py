#!/usr/bin/env python3
"""
Repeating Job Runner - Execute commands with configurable intervals

Usage:
    python run_daily_job.py --command "python script.py --arg value" --interval 3600
    python run_daily_job.py --command "python label_pizza/google_sheets_export.py --database-url-name MOTION_DBURL --master-sheet-id 1R8jvlETlM_WPHbaP0YYGbahyeNz3vF7xn3rbK2Hde3c --sheet-prefix CameraPizza" --interval 86400
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime

def run_command(command: str):
    """Execute a command and print results to console"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Starting command: {command}")
    
    try:
        # Execute command
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Print results
        if result.returncode == 0:
            print(f"[{timestamp}] Command completed successfully")
            if result.stdout:
                print("STDOUT:")
                print(result.stdout.strip())
        else:
            print(f"[{timestamp}] Command failed with exit code {result.returncode}")
            if result.stderr:
                print("STDERR:")
                print(result.stderr.strip())
            if result.stdout:
                print("STDOUT:")
                print(result.stdout.strip())
        
        return result.returncode
        
    except subprocess.TimeoutExpired:
        print(f"[{timestamp}] Command timed out after 1 hour")
        return 124
    except Exception as e:
        print(f"[{timestamp}] Error executing command: {str(e)}")
        return 1

def main():
    parser = argparse.ArgumentParser(description="Run commands repeatedly with configurable intervals")
    parser.add_argument("--command", required=True, help="Command to execute")
    parser.add_argument("--interval", type=int, default=86400, 
                       help="Seconds between reruns (default: 86400 = 24 hours)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    
    args = parser.parse_args()
    
    if args.once:
        # Run once and exit
        exit_code = run_command(args.command)
        sys.exit(exit_code)
    else:
        # Run repeatedly
        print(f"Running command every {args.interval} seconds ({args.interval/3600:.1f} hours)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                run_command(args.command)
                
                print(f"Waiting {args.interval} seconds until next run...")
                time.sleep(args.interval)
                
        except KeyboardInterrupt:
            print("\nStopped by user")
            sys.exit(0)

if __name__ == "__main__":
    main()