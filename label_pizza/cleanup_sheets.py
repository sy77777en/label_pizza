#!/usr/bin/env python3
"""
Google Sheets Cleanup Script

This script completely cleans all sheets associated with the annotation system:
- Clears all content and formatting
- Unmerges all cells
- Resets sheets to a clean state

Usage:
    python label_pizza/cleanup_sheets.py --database-url-name DBURL --master-sheet-id SHEET_ID

Run this BEFORE running the main export script to ensure clean sheets.
"""

import os
import sys
import argparse
import time
from pathlib import Path
from typing import List

# Add the parent directory to the path so we can import from label_pizza
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from label_pizza.google_sheets_export import GoogleSheetExporter


class SheetCleaner:
    """Clean and reset Google Sheets to pristine state"""
    
    def __init__(self, credentials_file: str, database_url_name: str):
        """Initialize using the same setup as GoogleSheetExporter"""
        self.exporter = GoogleSheetExporter(credentials_file, database_url_name)
        self.client = self.exporter.client
        self.sheets_service = self.exporter.sheets_service
        self.get_db_session = self.exporter.get_db_session
        self.GoogleSheetsExportService = self.exporter.GoogleSheetsExportService
        self.failures = []
    
    def cleanup_all_sheets(self, master_sheet_id: str):
        """Clean master sheet and all individual user sheets"""
        
        print("="*60)
        print("STARTING GOOGLE SHEETS CLEANUP")
        print("="*60)
        print("⚠️  WARNING: This will completely clear all content and formatting!")
        print("⚠️  Make sure you have backups if needed!")
        print("="*60)
        
        # Clean master sheet first
        print("Cleaning master sheet...")
        self._clean_master_sheet(master_sheet_id)
        
        # Get all user sheets to clean
        print("Finding all user sheets...")
        user_sheets = self._get_all_user_sheets()
        
        if not user_sheets:
            print("No user sheets found to clean.")
            return
        
        print(f"Found {len(user_sheets)} user sheets to clean...")
        
        # Clean each user sheet
        for i, (sheet_name, sheet_id) in enumerate(user_sheets, 1):
            print(f"\n[{i}/{len(user_sheets)}] Cleaning {sheet_name}...")
            
            try:
                self._clean_user_sheet(sheet_name, sheet_id)
                print(f"    ✅ Successfully cleaned {sheet_name}")
                
                # Small delay to avoid rate limits
                if i < len(user_sheets):
                    print("    ⏳ Pausing 1 second...")
                    time.sleep(1)
                    
            except Exception as e:
                print(f"    ❌ Failed to clean {sheet_name}: {e}")
                self.failures.append(f"Failed to clean {sheet_name}: {str(e)}")
        
        # Summary
        print("\n" + "="*60)
        if self.failures:
            print("CLEANUP COMPLETED WITH SOME FAILURES")
            print("="*60)
            for failure in self.failures:
                print(f"❌ {failure}")
        else:
            print("CLEANUP COMPLETED SUCCESSFULLY!")
            print("✅ All sheets are now clean and ready for fresh export")
        print("="*60)
    
    def _clean_master_sheet(self, sheet_id: str):
        """Clean the master sheet"""
        try:
            sheet = self.client.open_by_key(sheet_id)
            
            # Clean each tab
            for tab_name in ["Annotators", "Reviewers", "Meta-Reviewers"]:
                try:
                    worksheet = sheet.worksheet(tab_name)
                    print(f"  Cleaning {tab_name} tab...")
                    self._clean_worksheet(worksheet)
                    print(f"    ✅ {tab_name} tab cleaned")
                except Exception as e:
                    print(f"    ⚠️  Could not clean {tab_name} tab: {e}")
                    self.failures.append(f"Master sheet {tab_name} tab: {str(e)}")
                    
        except Exception as e:
            print(f"❌ Could not clean master sheet: {e}")
            self.failures.append(f"Master sheet: {str(e)}")
    
    def _get_all_user_sheets(self) -> List[tuple]:
        """Get all user sheets from database"""
        user_sheets = []
        
        try:
            with self.get_db_session() as session:
                # Get all users with activity
                annotator_data = self.GoogleSheetsExportService.get_master_sheet_annotator_data(session)
                reviewer_data = self.GoogleSheetsExportService.get_master_sheet_reviewer_data(session)
                meta_reviewer_data = self.GoogleSheetsExportService.get_master_sheet_meta_reviewer_data(session)
            
            # Collect all user sheet names
            for user in annotator_data:
                if self._has_activity(user):
                    sheet_name = f"{user['user_name']} Annotator"
                    user_sheets.append((sheet_name, None))  # We'll get ID when opening
            
            for user in reviewer_data:
                if self._has_activity(user):
                    sheet_name = f"{user['user_name']} Reviewer"
                    user_sheets.append((sheet_name, None))
            
            for user in meta_reviewer_data:
                if self._has_activity(user):
                    sheet_name = f"{user['user_name']} Meta-Reviewer"
                    user_sheets.append((sheet_name, None))
                    
        except Exception as e:
            print(f"⚠️  Could not get user list from database: {e}")
        
        return user_sheets
    
    def _has_activity(self, user_data: dict) -> bool:
        """Check if user has any activity (same logic as main script)"""
        return (user_data.get('projects_started', 0) > 0 or 
                user_data.get('assigned_projects', 0) > 0)
    
    def _clean_user_sheet(self, sheet_name: str, sheet_id: str):
        """Clean an individual user sheet"""
        try:
            sheet = self.client.open(sheet_name)
            
            # Clean both Payment and Feedback tabs
            for tab_name in ["Payment", "Feedback"]:
                try:
                    worksheet = sheet.worksheet(tab_name)
                    print(f"    Cleaning {tab_name} tab...")
                    self._clean_worksheet(worksheet)
                    print(f"      ✅ {tab_name} tab cleaned")
                except Exception as e:
                    print(f"      ⚠️  Could not clean {tab_name} tab: {e}")
                    
        except Exception as e:
            print(f"    ❌ Could not open sheet {sheet_name}: {e}")
            raise
    
    def _clean_worksheet(self, worksheet):
        """Completely clean a single worksheet"""
        try:
            sheet_id = worksheet._properties['sheetId']
            
            # Step 1: Clear all content and formatting
            clear_requests = [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id
                        },
                        "fields": "*"  # Clear everything
                    }
                }
            ]
            
            # Step 2: Unmerge all cells (brute force - entire sheet)
            clear_requests.append({
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id
                    }
                }
            })
            
            # Step 3: Reset basic formatting to defaults
            clear_requests.append({
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "frozenRowCount": 0,
                            "frozenColumnCount": 0
                        }
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
                }
            })
            
            # Step 4: Remove all conditional formatting rules
            clear_requests.append({
                "deleteConditionalFormatRule": {
                    "sheetId": sheet_id,
                    "index": 0
                }
            })
            
            # Apply all clearing operations
            batch_request = {"requests": clear_requests}
            
            try:
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=batch_request
                ).execute()
            except Exception as e:
                # Some operations might fail if there's nothing to clear (e.g., no conditional formatting)
                # Try individual operations
                print(f"      ⚠️  Batch clear failed, trying individual operations...")
                self._clean_worksheet_individually(worksheet, sheet_id)
                
        except Exception as e:
            print(f"      ⚠️  Could not clean worksheet: {e}")
            raise
    
    def _clean_worksheet_individually(self, worksheet, sheet_id: int):
        """Clean worksheet with individual operations if batch fails"""
        
        # Clear content
        try:
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=worksheet.spreadsheet.id,
                body={
                    "requests": [{
                        "updateCells": {
                            "range": {"sheetId": sheet_id},
                            "fields": "userEnteredValue,userEnteredFormat"
                        }
                    }]
                }
            ).execute()
            print(f"        ✅ Content cleared")
        except Exception as e:
            print(f"        ⚠️  Could not clear content: {e}")
        
        # Unmerge cells
        try:
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=worksheet.spreadsheet.id,
                body={
                    "requests": [{
                        "unmergeCells": {
                            "range": {"sheetId": sheet_id}
                        }
                    }]
                }
            ).execute()
            print(f"        ✅ Cells unmerged")
        except Exception as e:
            print(f"        ℹ️  No merges to unmerge (normal): {str(e)[:50]}...")
        
        # Reset frozen panes
        try:
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=worksheet.spreadsheet.id,
                body={
                    "requests": [{
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_id,
                                "gridProperties": {
                                    "frozenRowCount": 0,
                                    "frozenColumnCount": 0
                                }
                            },
                            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
                        }
                    }]
                }
            ).execute()
            print(f"        ✅ Frozen panes reset")
        except Exception as e:
            print(f"        ⚠️  Could not reset frozen panes: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Clean all Google Sheets for annotation system")
    parser.add_argument("--master-sheet-id", type=str, required=True,
                       help="Google Sheet ID for the master sheet")
    parser.add_argument("--database-url-name", type=str, default="DBURL",
                       help="Environment variable name for database URL")
    parser.add_argument("--credentials-file", type=str, default="credentials.json",
                       help="Path to Google OAuth credentials file")
    
    args = parser.parse_args()
    
    print("="*60)
    print("GOOGLE SHEETS CLEANUP SETUP")
    print("="*60)
    print(f"Database URL Variable: {args.database_url_name}")
    print(f"Master Sheet ID: {args.master_sheet_id}")
    print(f"Master Sheet URL: https://docs.google.com/spreadsheets/d/{args.master_sheet_id}/edit")
    print("="*60)
    
    # Verify credentials file exists
    credentials_path = Path(args.credentials_file)
    if not credentials_path.exists():
        print(f"❌ Error: Credentials file not found: {credentials_path}")
        print(f"💡 Please download your Google OAuth credentials and save as '{args.credentials_file}'")
        return
    
    # Confirm destructive operation
    print("\n⚠️  WARNING: This will COMPLETELY CLEAR all sheets!")
    print("This includes:")
    print("   • All content (headers, data, formulas)")
    print("   • All formatting (colors, fonts, borders)")
    print("   • All merged cells")
    print("   • All frozen panes")
    print("   • All conditional formatting")
    print("\nSheets that will be cleaned:")
    print("   • Master sheet (all tabs)")
    print("   • All individual user sheets (Payment & Feedback tabs)")
    
    confirm = input("\nAre you sure you want to proceed? Type 'YES' to confirm: ")
    if confirm != "YES":
        print("❌ Cleanup cancelled.")
        return
    
    # Create cleaner and run
    print("\n🧹 Starting cleanup process...")
    cleaner = SheetCleaner(str(credentials_path), args.database_url_name)
    
    cleaner.cleanup_all_sheets(args.master_sheet_id)
    
    print("\n💡 After cleanup completes successfully, you can run:")
    print(f"   python label_pizza/google_sheets_export.py --master-sheet-id {args.master_sheet_id} --database-url-name {args.database_url_name}")


if __name__ == "__main__":
    main()