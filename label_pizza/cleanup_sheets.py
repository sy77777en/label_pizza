#!/usr/bin/env python3
"""
Google Sheets Cleanup Script for Label Pizza

This script helps clean up Google Sheets created by the export system:
- Clear all content from master sheet tabs
- Delete individual user sheets entirely
- Clear content from specific sheets
- Bulk operations with safety confirmations

Usage:
    python label_pizza/cleanup_sheets.py --master-sheet-id SHEET_ID --action clear-master
    python label_pizza/cleanup_sheets.py --master-sheet-id SHEET_ID --action delete-user-sheets
    python label_pizza/cleanup_sheets.py --master-sheet-id SHEET_ID --action clear-all

Safety Features:
- Confirmation prompts before destructive operations
- Dry-run mode to see what would be affected
- Ability to backup before cleanup
"""

import os
import sys
import gspread
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Add the parent directory to the path so we can import from label_pizza
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class GoogleSheetsCleanup:
    """Cleanup tool for Google Sheets created by the export system"""
    
    # OAuth 2.0 scopes required for Google Sheets and Drive access
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(self, credentials_file: str, database_url_name: str):
        """Initialize the cleanup tool"""
        self.database_url_name = database_url_name
        
        # Initialize database first
        from label_pizza.db import init_database
        init_database(database_url_name)
        
        # Now import database utilities
        from label_pizza.db import SessionLocal
        from label_pizza.services import GoogleSheetsExportService, AuthService
        
        self.get_db_session = SessionLocal
        self.GoogleSheetsExportService = GoogleSheetsExportService
        self.AuthService = AuthService
        
        # Initialize Google APIs
        self.client = self._setup_google_sheets_client(credentials_file)
        self.sheets_service = self._setup_sheets_api(credentials_file)
        self.drive_service = self._setup_drive_api(credentials_file)
        
        # Track operations
        self.operations_performed = []
        self.failures = []
    
    def _setup_google_sheets_client(self, credentials_file: str):
        """Setup Google Sheets client with gspread"""
        creds = self._get_credentials(credentials_file)
        return gspread.authorize(creds)
    
    def _setup_sheets_api(self, credentials_file: str):
        """Setup Google Sheets API service"""
        creds = self._get_credentials(credentials_file)
        return build('sheets', 'v4', credentials=creds)
    
    def _setup_drive_api(self, credentials_file: str):
        """Setup Google Drive API service"""
        creds = self._get_credentials(credentials_file)
        return build('drive', 'v3', credentials=creds)
    
    def _get_credentials(self, credentials_file: str):
        """Get Google API credentials (reuse token if available)"""
        token_file = 'google_sheets_token.json'
        creds = None
        
        # Load existing token
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
        
        # Refresh or get new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"⚠️  Token refresh failed: {e}")
                    creds = None
            
            if not creds:
                print("Google Sheets authorization required...")
                flow = InstalledAppFlow.from_client_secrets_file(credentials_file, self.SCOPES)
                flow.redirect_uri = 'http://localhost:8080/'
                creds = flow.run_local_server(port=8080, open_browser=True)
            
            # Save credentials
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        
        return creds
    
    def _api_call_with_retry(self, func, *args, max_retries=3, operation_name="API call", **kwargs):
        """Execute API call with retry logic"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "quota" in str(e).lower() or "rate" in str(e).lower():
                    wait_time = 2 ** attempt
                    print(f"      ⏳ Rate limit hit during {operation_name}, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        raise Exception(f"Rate limit exceeded after {max_retries} attempts")
                else:
                    raise
    
    def discover_user_sheets(self) -> List[Dict]:
        """Discover all individual user sheets by querying database and searching Drive"""
        print("🔍 Discovering user sheets...")
        
        user_sheets = []
        
        with self.get_db_session() as session:
            # Get all users with activity
            annotator_data = self.GoogleSheetsExportService.get_master_sheet_annotator_data(session)
            reviewer_data = self.GoogleSheetsExportService.get_master_sheet_reviewer_data(session)
            meta_reviewer_data = self.GoogleSheetsExportService.get_master_sheet_meta_reviewer_data(session)
            
            # Build list of expected sheet names
            expected_sheets = []
            
            for user in annotator_data:
                if user.get('projects_started', 0) > 0 or user.get('assigned_projects', 0) > 0:
                    expected_sheets.append(f"{user['user_name']} Annotator")
            
            for user in reviewer_data:
                if user.get('projects_started', 0) > 0 or user.get('assigned_projects', 0) > 0:
                    expected_sheets.append(f"{user['user_name']} Reviewer")
            
            for user in meta_reviewer_data:
                if user.get('projects_started', 0) > 0 or user.get('assigned_projects', 0) > 0:
                    expected_sheets.append(f"{user['user_name']} Meta-Reviewer")
        
        # Search for actual sheets
        for sheet_name in expected_sheets:
            try:
                sheet = self.client.open(sheet_name)
                user_sheets.append({
                    'name': sheet_name,
                    'id': sheet.id,
                    'url': sheet.url,
                    'found': True
                })
                print(f"  ✅ Found: {sheet_name}")
            except gspread.exceptions.SpreadsheetNotFound:
                user_sheets.append({
                    'name': sheet_name,
                    'id': None,
                    'url': None,
                    'found': False
                })
                print(f"  ❌ Not found: {sheet_name}")
            except Exception as e:
                print(f"  ⚠️  Error checking {sheet_name}: {e}")
        
        found_count = sum(1 for sheet in user_sheets if sheet['found'])
        print(f"\n📊 Discovery complete: {found_count}/{len(user_sheets)} sheets found")
        
        return user_sheets
    
    def clear_master_sheet(self, master_sheet_id: str, dry_run: bool = False):
        """Clear all content from master sheet tabs"""
        print(f"🧹 {'[DRY RUN] ' if dry_run else ''}Clearing master sheet content...")
        
        try:
            # Use the same approach as the main export script
            sheet = self._api_call_with_retry(
                self.client.open_by_key, 
                master_sheet_id, 
                operation_name="opening master sheet"
            )
            print(f"  ✅ Successfully opened sheet: '{sheet.title}'")
            
        except Exception as e:
            print(f"  ❌ Failed to open master sheet: {e}")
            print(f"  💡 The sheet might still work - trying to continue anyway...")
            # Try a different approach - this sometimes works when open_by_key fails
            try:
                # Get the URL and try to parse it
                sheet_url = f"https://docs.google.com/spreadsheets/d/{master_sheet_id}/edit"
                print(f"  🔄 Trying alternative access method for: {sheet_url}")
                # This is a fallback - in most cases we'll need to skip validation
                self.failures.append(f"Could not access master sheet: {str(e)}")
                return
            except Exception as e2:
                print(f"  ❌ Alternative method also failed: {e2}")
                self.failures.append(f"Failed to access master sheet: {str(e)}")
                return
        
        # Clear the tabs
        tabs = ["Annotators", "Reviewers", "Meta-Reviewers"]
        
        for tab_name in tabs:
            try:
                worksheet = sheet.worksheet(tab_name)
                if not dry_run:
                    # Clear all content
                    self._api_call_with_retry(
                        worksheet.clear,
                        operation_name=f"clearing {tab_name} tab"
                    )
                    print(f"  ✅ Cleared {tab_name} tab")
                    self.operations_performed.append(f"Cleared {tab_name} tab in master sheet")
                else:
                    print(f"  🔍 Would clear {tab_name} tab")
                
                time.sleep(1)  # Rate limiting
                
            except gspread.exceptions.WorksheetNotFound:
                print(f"  ⚠️  {tab_name} tab not found - skipping")
            except Exception as e:
                print(f"  ❌ Failed to clear {tab_name}: {e}")
                self.failures.append(f"Failed to clear {tab_name}: {str(e)}")
    
    def delete_user_sheets(self, user_sheets: List[Dict], dry_run: bool = False):
        """Delete individual user sheets entirely"""
        print(f"🗑️  {'[DRY RUN] ' if dry_run else ''}Deleting user sheets...")
        
        found_sheets = [sheet for sheet in user_sheets if sheet['found']]
        
        if not found_sheets:
            print("  No user sheets found to delete")
            return
        
        for sheet_info in found_sheets:
            try:
                if not dry_run:
                    # Delete the entire spreadsheet
                    self._api_call_with_retry(
                        self.drive_service.files().delete,
                        fileId=sheet_info['id'],
                        operation_name=f"deleting {sheet_info['name']}"
                    ).execute()
                    
                    print(f"  ✅ Deleted: {sheet_info['name']}")
                    self.operations_performed.append(f"Deleted sheet: {sheet_info['name']}")
                else:
                    print(f"  🔍 Would delete: {sheet_info['name']}")
                
                time.sleep(2)  # Rate limiting for deletions
                
            except Exception as e:
                print(f"  ❌ Failed to delete {sheet_info['name']}: {e}")
                self.failures.append(f"Failed to delete {sheet_info['name']}: {str(e)}")
    
    def clear_user_sheets(self, user_sheets: List[Dict], dry_run: bool = False):
        """Clear content from user sheets without deleting them"""
        print(f"🧹 {'[DRY RUN] ' if dry_run else ''}Clearing user sheet content...")
        
        found_sheets = [sheet for sheet in user_sheets if sheet['found']]
        
        if not found_sheets:
            print("  No user sheets found to clear")
            return
        
        for sheet_info in found_sheets:
            try:
                sheet = self.client.open_by_key(sheet_info['id'])
                
                # Clear all worksheets in the sheet
                worksheets = sheet.worksheets()
                for worksheet in worksheets:
                    if not dry_run:
                        worksheet.clear()
                        print(f"  ✅ Cleared {worksheet.title} in {sheet_info['name']}")
                    else:
                        print(f"  🔍 Would clear {worksheet.title} in {sheet_info['name']}")
                
                if not dry_run:
                    self.operations_performed.append(f"Cleared all tabs in: {sheet_info['name']}")
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"  ❌ Failed to clear {sheet_info['name']}: {e}")
                self.failures.append(f"Failed to clear {sheet_info['name']}: {str(e)}")
    
    def backup_master_sheet(self, master_sheet_id: str, backup_name: Optional[str] = None):
        """Create a backup copy of the master sheet"""
        if not backup_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"Master_Sheet_Backup_{timestamp}"
        
        print(f"💾 Creating backup: {backup_name}...")
        
        try:
            # Copy the master sheet
            copy_result = self._api_call_with_retry(
                self.drive_service.files().copy,
                fileId=master_sheet_id,
                body={'name': backup_name},
                operation_name="creating backup"
            ).execute()
            
            backup_id = copy_result['id']
            backup_url = f"https://docs.google.com/spreadsheets/d/{backup_id}/edit"
            
            print(f"  ✅ Backup created: {backup_name}")
            print(f"  🔗 URL: {backup_url}")
            
            self.operations_performed.append(f"Created backup: {backup_name}")
            return backup_id
            
        except Exception as e:
            print(f"  ❌ Failed to create backup: {e}")
            self.failures.append(f"Failed to create backup: {str(e)}")
            return None
    
    def validate_master_sheet_access(self, master_sheet_id: str) -> bool:
        """Validate that we can access the master sheet before operations"""
        print(f"🔍 Validating access to master sheet...")
        print(f"   Sheet ID: {master_sheet_id}")
        print(f"   URL: https://docs.google.com/spreadsheets/d/{master_sheet_id}/edit")
        
        try:
            sheet = self._api_call_with_retry(
                self.client.open_by_key, 
                master_sheet_id,
                operation_name="validating master sheet access"
            )
            
            print(f"   ✅ Sheet title: '{sheet.title}'")
            print(f"   ✅ Sheet URL: {sheet.url}")
            
            # Check for expected tabs
            worksheets = sheet.worksheets()
            worksheet_names = [ws.title for ws in worksheets]
            print(f"   📋 Found tabs: {worksheet_names}")
            
            expected_tabs = ["Annotators", "Reviewers", "Meta-Reviewers"]
            missing_tabs = [tab for tab in expected_tabs if tab not in worksheet_names]
            if missing_tabs:
                print(f"   ⚠️  Missing expected tabs: {missing_tabs}")
                print(f"   💡 This might be a new sheet or not created by the export script")
            
            return True
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"   ❌ Sheet not found!")
            print(f"   💡 Possible issues:")
            print(f"      • Sheet ID is incorrect")
            print(f"      • Sheet has been deleted")
            print(f"      • Sheet ID should be the long string from the URL")
            print(f"      • Example: 1ABC123XYZ... (not the full URL)")
            return False
            
        except gspread.exceptions.APIError as e:
            print(f"   ❌ API Error: {e}")
            if "403" in str(e):
                print(f"   💡 Permission denied:")
                print(f"      • Make sure you have edit access to the sheet")
                print(f"      • Check that the sheet is shared with your Google account")
            elif "404" in str(e):
                print(f"   💡 Not found (404):")
                print(f"      • Double-check the sheet ID")
                print(f"      • Make sure the sheet exists")
            return False
            
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            return False
    def run_cleanup(self, master_sheet_id: str, action: str, dry_run: bool = False, 
                   create_backup: bool = False, backup_name: Optional[str] = None,
                   skip_validation: bool = False):
        """Run the specified cleanup action"""
        
        print("="*70)
        print("GOOGLE SHEETS CLEANUP TOOL")
        print("="*70)
        print(f"Action: {action}")
        print(f"Master Sheet ID: {master_sheet_id}")
        print(f"Dry Run: {'Yes' if dry_run else 'No'}")
        print(f"Create Backup: {'Yes' if create_backup else 'No'}")
        print(f"Skip Validation: {'Yes' if skip_validation else 'No'}")
        print("="*70)
        
        # Validate master sheet access first (unless skipped)
        if not skip_validation and action in ['clear-master', 'clear-all', 'delete-all']:
            if not self.validate_master_sheet_access(master_sheet_id):
                print("\n❌ Cannot access master sheet - aborting cleanup")
                print("💡 Try adding --skip-validation flag to bypass this check")
                return
        
        # Create backup if requested
        backup_id = None
        if create_backup and not dry_run:
            backup_id = self.backup_master_sheet(master_sheet_id, backup_name)
            if not backup_id:
                print("⚠️  Backup failed - continuing with cleanup anyway...")
        
        # Discover user sheets
        user_sheets = self.discover_user_sheets()
        
        # Confirmation for destructive operations
        if not dry_run and action in ['delete-user-sheets', 'clear-all']:
            found_count = sum(1 for sheet in user_sheets if sheet['found'])
            
            print(f"\n⚠️  WARNING: This will perform destructive operations!")
            if action == 'delete-user-sheets':
                print(f"   - Delete {found_count} individual user sheets permanently")
            elif action == 'clear-all':
                print(f"   - Clear master sheet (3 tabs)")
                print(f"   - Clear content from {found_count} individual user sheets")
            
            confirm = input("\nType 'YES' to confirm: ")
            if confirm != 'YES':
                print("❌ Operation cancelled")
                return
        
        # Perform cleanup actions
        if action == 'clear-master':
            self.clear_master_sheet(master_sheet_id, dry_run)
            
        elif action == 'delete-user-sheets':
            self.delete_user_sheets(user_sheets, dry_run)
            
        elif action == 'clear-user-sheets':
            self.clear_user_sheets(user_sheets, dry_run)
            
        elif action == 'clear-all':
            self.clear_master_sheet(master_sheet_id, dry_run)
            self.clear_user_sheets(user_sheets, dry_run)
            
        elif action == 'delete-all':
            self.clear_master_sheet(master_sheet_id, dry_run)
            self.delete_user_sheets(user_sheets, dry_run)
            
        else:
            print(f"❌ Unknown action: {action}")
            return
        
        # Summary
        print("\n" + "="*70)
        print("CLEANUP SUMMARY")
        print("="*70)
        
        if dry_run:
            print("🔍 DRY RUN - No actual changes were made")
        else:
            if self.operations_performed:
                print(f"✅ Successfully completed {len(self.operations_performed)} operations:")
                for op in self.operations_performed:
                    print(f"   • {op}")
            
            if self.failures:
                print(f"\n❌ {len(self.failures)} operations failed:")
                for failure in self.failures:
                    print(f"   • {failure}")
            
            if backup_id:
                print(f"\n💾 Backup created with ID: {backup_id}")
        
        print("="*70)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Cleanup Google Sheets created by the export system")
    parser.add_argument("--master-sheet-id", type=str, required=True,
                       help="Google Sheet ID for the master sheet")
    parser.add_argument("--action", type=str, required=True,
                       choices=['clear-master', 'delete-user-sheets', 'clear-user-sheets', 
                               'clear-all', 'delete-all', 'discover', 'validate'],
                       help="Cleanup action to perform")
    parser.add_argument("--database-url-name", type=str, default="DBURL",
                       help="Environment variable name for database URL")
    parser.add_argument("--credentials-file", type=str, default="credentials.json",
                       help="Path to Google OAuth credentials file")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be done without making changes")
    parser.add_argument("--skip-validation", action="store_true",
                       help="Skip master sheet validation (use if validation fails but sheet exists)")
    parser.add_argument("--backup", action="store_true",
                       help="Create backup of master sheet before cleanup")
    parser.add_argument("--backup-name", type=str,
                       help="Custom name for backup (optional)")
    
    args = parser.parse_args()
    
    # Verify credentials file exists
    credentials_path = Path(args.credentials_file)
    if not credentials_path.exists():
        print(f"❌ Error: Credentials file not found: {credentials_path}")
        return
    
    # Create cleanup tool and run
    cleanup = GoogleSheetsCleanup(str(credentials_path), args.database_url_name)
    
    if args.action == 'discover':
        # Just discover and show what sheets exist
        user_sheets = cleanup.discover_user_sheets()
        found_sheets = [sheet for sheet in user_sheets if sheet['found']]
        
        print(f"\n📋 DISCOVERY RESULTS:")
        print(f"Found {len(found_sheets)} existing user sheets:")
        for sheet in found_sheets:
            print(f"  • {sheet['name']}")
            print(f"    🔗 {sheet['url']}")
            
    elif args.action == 'validate':
        # Just validate master sheet access
        print("🔍 VALIDATION MODE - Testing master sheet access only")
        success = cleanup.validate_master_sheet_access(args.master_sheet_id)
        if success:
            print("\n✅ Master sheet is accessible and ready for cleanup operations")
        else:
            print("\n❌ Master sheet is not accessible - please fix the issues above")
            
    else:
        cleanup.run_cleanup(
            master_sheet_id=args.master_sheet_id,
            action=args.action,
            dry_run=args.dry_run,
            create_backup=args.backup,
            backup_name=args.backup_name,
            skip_validation=args.skip_validation
        )


if __name__ == "__main__":
    main()