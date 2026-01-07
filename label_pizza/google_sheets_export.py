#!/usr/bin/env python3
"""
Google Sheets Export System for Video Annotation Project Statistics

This script exports comprehensive statistics for video annotation and review work
to Google Sheets, creating a master tracking sheet with links to individual user 
performance sheets.

Usage:
    python label_pizza/google_sheets_export.py --master-sheet-id SHEET_ID --database-url-name DBURL

Features:
- Three-tab master sheet (Annotators, Reviewers, Meta-Reviewers)
- Individual user sheets with Payment and Feedback tabs
- Automatic permission management based on database admin status
- Data preservation for manual columns during updates
- Professional formatting and smart chip links
"""

import os
import sys
import json
import gspread
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Add the parent directory to the path so we can import from label_pizza
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class GoogleSheetExporter:
    """Export annotation statistics to Google Sheets with comprehensive tracking"""
    
    # OAuth 2.0 scopes required for Google Sheets and Drive access
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(self, credentials_file: str, database_url_name: str):
        """Initialize the Google Sheets exporter"""
        self.database_url_name = database_url_name
        
        # Initialize database first
        from label_pizza.db import init_database
        init_database(database_url_name)
        
        # Now import database utilities
        from label_pizza.db import SessionLocal
        from label_pizza.services import GoogleSheetsExportService, AuthService
        from label_pizza.models import User
        
        self.get_db_session = SessionLocal
        self.GoogleSheetsExportService = GoogleSheetsExportService
        self.AuthService = AuthService
        self.User = User
        
        # Initialize Google Sheets API
        self.client = self._setup_google_sheets_client(credentials_file)
        self.sheets_service = self._setup_sheets_api(credentials_file)
        self.drive_service = self._setup_drive_api(credentials_file)
        
        # Track export failures
        self.export_failures = []
    
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
    
    # def _get_credentials(self, credentials_file: str):
    #     """Get Google API credentials"""
    #     token_file = 'google_sheets_token.json'
    #     creds = None
        
    #     # Load existing token
    #     if os.path.exists(token_file):
    #         creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
        
    #     # Refresh or get new credentials
    #     if not creds or not creds.valid:
    #         if creds and creds.expired and creds.refresh_token:
    #             try:
    #                 creds.refresh(Request())
    #             except Exception as e:
    #                 print(f"⚠️  Token refresh failed: {e}")
    #                 print("🔄 Will require re-authorization...")
    #                 creds = None
            
    #         if not creds:
    #             print("="*60)
    #             print("GOOGLE SHEETS AUTHORIZATION REQUIRED")
    #             print("="*60)
    #             print("Required permissions:")
    #             print("  ✅ Google Sheets: Read, write, and manage spreadsheets")
    #             print("  ✅ Google Drive: Create and manage files")
    #             print("="*60)
    #             print()
                
    #             try:
    #                 flow = InstalledAppFlow.from_client_secrets_file(credentials_file, self.SCOPES)
    #                 flow.redirect_uri = 'http://localhost:8080/'
    #                 creds = flow.run_local_server(port=8080, open_browser=True)
    #             except Exception as e:
    #                 print(f"❌ Error during authorization: {e}")
    #                 print("\n💡 If the browser didn't open automatically:")
    #                 print("   1. Copy the authorization URL from above")
    #                 print("   2. Open it in your browser")
    #                 print("   3. Complete the authorization")
    #                 raise
            
    #         # Save credentials for next run
    #         with open(token_file, 'w') as token:
    #             token.write(creds.to_json())
            
    #         print("✅ Authorization successful! Credentials saved.")
    #         print()
        
    #     return creds
    
    def _get_credentials(self, credentials_file: str):
        """Get Google API credentials"""
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
                    print("🔄 Will require re-authorization...")
                    creds = None
            
            if not creds:
                print("="*60)
                print("GOOGLE SHEETS AUTHORIZATION REQUIRED")
                print("="*60)
                print("Required permissions:")
                print("  ✅ Google Sheets: Read, write, and manage spreadsheets")
                print("  ✅ Google Drive: Create and manage files")
                print("="*60)
                print()
                
                # Manual OAuth flow for environments without browser
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, self.SCOPES, redirect_uri='http://localhost:8080')
                
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                print('1. Go to this URL in your browser:')
                print(auth_url)
                print('\n2. Click "Advanced" -> "Go to [App Name] (unsafe)"')
                print('3. Authorize the application')
                print('4. ⚠️  IMPORTANT: Grant BOTH Sheets AND Drive permissions')
                print('5. The browser will show "This site can\'t be reached" - this is expected!')
                print('6. Copy the authorization code from the failed URL')
                print('\n   Example URL: http://localhost:8080/?code=AUTHORIZATION_CODE&scope=...')
                print('   Copy only the part after "code=" and before "&"')
                print('='*60)
                
                auth_code = input('\nEnter the authorization code: ').strip()
                if not auth_code:
                    raise Exception('No authorization code provided')
                
                flow.fetch_token(code=auth_code)
                creds = flow.credentials
            
            # Save credentials for next run
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
            
            print("✅ Authorization successful! Credentials saved.")
            print()
        
        return creds

    # def _api_call_with_retry(self, func, *args, max_retries=3, operation_name="API call", **kwargs):
    #     """Execute API call with retry logic for rate limiting"""
    #     for attempt in range(max_retries):
    #         try:
    #             return func(*args, **kwargs)
    #         except Exception as e:
    #             if "429" in str(e) or "quota" in str(e).lower() or "rate" in str(e).lower():
    #                 wait_time = 2 ** attempt
    #                 print(f"      ⏳ Rate limit hit during {operation_name}, waiting {wait_time}s...")
    #                 time.sleep(wait_time)
    #                 if attempt == max_retries - 1:
    #                     raise Exception(f"Rate limit exceeded after {max_retries} attempts during {operation_name}")
    #             else:
    #                 raise
    def _api_call_with_retry(self, func, *args, max_retries=5, operation_name="API call", **kwargs):
        """Execute API call with retry logic for rate limiting"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                if "429" in str(e) or "quota" in error_str or "rate" in error_str:
                    # Exponential backoff with longer waits: 5, 10, 20, 40, 80 seconds
                    wait_time = 5 * (2 ** attempt)
                    print(f"      ⏳ Rate limit hit during {operation_name}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        raise Exception(f"Rate limit exceeded after {max_retries} attempts during {operation_name}")
                else:
                    raise
    
    def _col_num_to_letter(self, col_num):
        """Convert column number to letter (1 -> A, 26 -> Z, 27 -> AA)"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result
    
    def _format_timestamp(self, timestamp):
        """Format timestamp for display"""
        if not timestamp:
            return ""
        if isinstance(timestamp, str):
            return timestamp
        return timestamp.strftime("%Y-%m-%d %H:%M")
    
    def export_all_sheets(self, master_sheet_id: str, skip_individual: bool = False, resume_from: str = None, sheet_prefix: str = "Pizza"):
        """Export all data to Google Sheets"""
        
        print("="*60)
        print("STARTING GOOGLE SHEETS EXPORT")
        print("="*60)
        
        # Export master sheet
        self._export_master_sheet(master_sheet_id)
        
        # Manage master sheet permissions
        print("Managing master sheet permissions...")
        self._manage_sheet_permissions(master_sheet_id, "Master Sheet")
        
        if skip_individual:
            print("\n✅ Master sheet export completed (individual sheets skipped)")
            return
        
        # Export individual user sheets
        with self.get_db_session() as session:
            annotator_data = self.GoogleSheetsExportService.get_master_sheet_annotator_data(session)
            reviewer_data = self.GoogleSheetsExportService.get_master_sheet_reviewer_data(session)
            meta_reviewer_data = self.GoogleSheetsExportService.get_master_sheet_meta_reviewer_data(session)
        
        all_users = []
        for user in annotator_data:
            if self._has_activity(user):
                all_users.append((user, "Annotator"))
        for user in reviewer_data:
            if self._has_activity(user):
                all_users.append((user, "Reviewer"))
        for user in meta_reviewer_data:
            if self._has_activity(user):
                all_users.append((user, "Meta-Reviewer"))
        
        total_users = len(all_users)
        current_user = 0
        
        print(f"\nExporting individual user sheets ({total_users} total)...")
        print("⚠️  Note: Processing slowly to avoid Google Sheets rate limits...")
        
        # Store sheet IDs for hyperlinks
        user_sheet_ids = {}
        
        # Determine where to start based on resume_from parameter
        skip_until_found = resume_from is not None

        for user_data, role in all_users:
            current_user += 1
            sheet_key = f"{sheet_prefix}-{user_data['user_name']} {role}"  # Add prefix to key
            
            # Skip users until we reach the resume point
            if skip_until_found:
                if sheet_key == resume_from:
                    skip_until_found = False
                    print(f"🔄 Resuming from: {sheet_key}")
                else:
                    print(f"⏭️  Skipping {sheet_key} (resuming from {resume_from})")
                    continue
            
            print(f"\n[{current_user}/{total_users}] Processing {user_data['user_name']} {role}...")
            
            # Add delay between users to prevent rate limits
            if current_user > 1:
                print("    ⏳ Pausing 3 seconds between users...")
                time.sleep(3)
            
            try:
                sheet_id = self._export_user_sheet(user_data, role, master_sheet_id, sheet_prefix)
                user_sheet_ids[sheet_key] = sheet_id
                
                # Manage permissions for this sheet
                self._manage_sheet_permissions(sheet_id, f"{sheet_prefix}-{user_data['user_name']} {role}")
                
                print(f"    ✅ Successfully exported {sheet_prefix}-{user_data['user_name']} {role}")
            except Exception as e:
                print(f"    ❌ Failed to export {sheet_prefix}-{user_data['user_name']} {role}: {e}")
                self.export_failures.append(f"Failed to export {sheet_prefix}-{user_data['user_name']} {role}: {str(e)}")

        # Update master sheet with correct hyperlinks
        if user_sheet_ids:
            print(f"\n⏳ Pausing 5 seconds before updating master sheet links...")
            time.sleep(5)
            print("Updating master sheet hyperlinks...")
            try:
                self._update_master_sheet_links(master_sheet_id, user_sheet_ids, sheet_prefix)
            except Exception as e:
                print(f"❌ Failed to update master sheet links: {e}")
                self.export_failures.append(f"Failed to update master sheet links: {str(e)}")
        
        print("="*60)
        if self.export_failures:
            print("EXPORT COMPLETED WITH SOME FAILURES!")
            print("="*60)
            print("❌ The following operations failed:")
            for failure in self.export_failures:
                print(f"   - {failure}")
            print(f"\n⚠️  Some data may not be synced. Consider running the script again.")
        else:
            print("EXPORT COMPLETED SUCCESSFULLY!")
        
        print("="*60)
        print(f"📊 Master Sheet: https://docs.google.com/spreadsheets/d/{master_sheet_id}/edit")
        print("="*60)
    
    def _has_activity(self, user_data: Dict) -> bool:
        """Check if user has any activity"""
        return (user_data.get('projects_started', 0) > 0 or 
                user_data.get('assigned_projects', 0) > 0)
    
    def _export_master_sheet(self, sheet_id: str):
        """Export the master sheet with annotator, reviewer, and meta-reviewer tabs"""
        try:
            sheet = self._api_call_with_retry(self.client.open_by_key, sheet_id, 
                                            operation_name="opening master sheet")
        except Exception as e:
            print(f"❌ Error: Could not open master sheet with ID {sheet_id}")
            print(f"   Details: {str(e)}")
            print(f"\n💡 Possible solutions:")
            print(f"   1. Verify the sheet ID is correct")
            print(f"   2. Make sure you have edit access to the sheet")
            print(f"   3. Create a new Google Sheet and use its ID")
            return
        
        print("Exporting master sheet...")
        
        with self.get_db_session() as session:
            # Export Annotators tab
            annotator_data = self.GoogleSheetsExportService.get_master_sheet_annotator_data(session)
            self._export_master_tab(sheet, "Annotators", annotator_data, "Annotator")
            
            # Export Reviewers tab
            reviewer_data = self.GoogleSheetsExportService.get_master_sheet_reviewer_data(session)
            self._export_master_tab(sheet, "Reviewers", reviewer_data, "Reviewer")
            
            # Export Meta-Reviewers tab
            meta_reviewer_data = self.GoogleSheetsExportService.get_master_sheet_meta_reviewer_data(session)
            self._export_master_tab(sheet, "Meta-Reviewers", meta_reviewer_data, "Meta-Reviewer")
    
    def _export_master_tab(self, sheet, tab_name: str, user_data: List[Dict], role: str):
        """Export a single tab in the master sheet"""
        print(f"  Exporting {tab_name} tab...")
        
        try:
            worksheet = sheet.worksheet(tab_name)
            print(f"    Found existing {tab_name} tab")
        except gspread.exceptions.WorksheetNotFound:
            print(f"    Creating new {tab_name} tab...")
            worksheet = self._api_call_with_retry(sheet.add_worksheet, title=tab_name, rows=100, cols=20, 
                                                operation_name=f"creating {tab_name} tab")
        
        # FIXED: Shortened header texts for master sheet
        if role == "Annotator":
            headers = [
                "User Name", "Email", "Role", "Annotation Sheet", "Last Annotated Time",
                "Assigned Projects", "Started Projects", "Completed Projects"  # SHORTENED
            ]
        elif role == "Reviewer":
            headers = [
                "User Name", "Email", "Role", "Review Sheet", "Last Review Time",
                "Assigned Projects", "Started Projects"  # SHORTENED
            ]
        else:  # Meta-Reviewer
            headers = [
                "User Name", "Email", "Role", "Meta-Review Sheet", "Last Modified Time",
                "Assigned Projects", "Started Projects"  # FIXED: Added missing Started Projects column
            ]
        
        # Prepare data rows
        rows = [headers]
        for user in user_data:
            if not self._has_activity(user):
                continue
            
            row = [
                user['user_name'],
                user['email'],
                user['role'],
                f"Link to {user['user_name']} Sheet",  # Placeholder for hyperlink
                self._format_timestamp(user.get('last_annotation_time') or user.get('last_review_time') or user.get('last_modified_time')),
                user.get('assigned_projects', 0),
                user.get('projects_started', 0)
            ]
            
            if role == "Annotator":
                row.append(user.get('projects_completed', 0))
            
            rows.append(row)
        
        # Update sheet
        if rows:
            end_col = self._col_num_to_letter(len(headers))
            self._api_call_with_retry(
                worksheet.update, 
                values=rows, 
                range_name=f'A1:{end_col}{len(rows)}', 
                operation_name=f"updating {len(rows)-1} user records"
            )
            
            # Apply master sheet formatting
            self._apply_master_sheet_formatting(worksheet, len(rows)-1, len(headers))
            
            print(f"    ✅ Successfully updated {len(rows)-1} users in {tab_name} tab")
    
    # def _export_user_sheet(self, user_data: Dict, role: str, master_sheet_id: str, sheet_prefix: str) -> str:
    #     """Export individual user sheet and return sheet ID"""
    #     sheet_name = f"{sheet_prefix}-{user_data['user_name']} {role}"  # Add prefix
    #     print(f"  Exporting {sheet_name} sheet...")
        
    #     try:
    #         # Try to open existing sheet
    #         sheet = self.client.open(sheet_name)
    #         print(f"    Found existing sheet: {sheet_name}")
    #     # except (gspread.exceptions.SpreadsheetNotFound, Exception):
    #     #     # Create new sheet
    #     #     print(f"    Creating new sheet: {sheet_name}")
    #     #     sheet = self._api_call_with_retry(self.client.create, sheet_name, 
    #     #                                     operation_name=f"creating sheet '{sheet_name}'")
    #     except (gspread.exceptions.SpreadsheetNotFound, Exception) as e:
    #         # Create new sheet
    #         print(f"    DEBUG: Sheet not found. Exception type: {type(e).__name__}, message: {str(e)[:100]}")
    #         print(f"    Creating new sheet: {sheet_name}")
    #         sheet = self._api_call_with_retry(self.client.create, sheet_name, 
    #                                         operation_name=f"creating sheet '{sheet_name}'")

    #     # Get user project data
    #     with self.get_db_session() as session:
    #         if role == "Annotator":
    #             project_data = self.GoogleSheetsExportService.get_user_project_data_annotator(user_data['user_id'], session)
    #         elif role == "Reviewer":
    #             project_data = self.GoogleSheetsExportService.get_user_project_data_reviewer(user_data['user_id'], session)
    #         else:  # Meta-Reviewer
    #             project_data = self.GoogleSheetsExportService.get_user_project_data_meta_reviewer(user_data['user_id'], session)
        
    #     # Calculate required rows (headers + data + padding) - handle massive scale
    #     header_rows = 2  # All roles now have 2-row headers
    #     data_rows = len(project_data)
        
    #     # For very large datasets, use more conservative padding
    #     if data_rows > 1000:
    #         total_rows = header_rows + data_rows + 10  # Minimal padding for large datasets
    #     else:
    #         total_rows = max(100, header_rows + data_rows + 50)  # Standard padding
        
    #     total_cols = 20  # Standard column count
        
    #     # Ensure sheet has enough rows and columns
    #     try:
    #         # Use the Sheets API service to get sheet properties
    #         sheet_metadata = self._api_call_with_retry(
    #             self.sheets_service.spreadsheets().get,
    #             spreadsheetId=sheet.id,
    #             operation_name="getting sheet properties"
    #         ).execute()
            
    #         first_sheet_props = sheet_metadata.get('sheets', [{}])[0].get('properties', {}).get('gridProperties', {})
    #         current_rows = first_sheet_props.get('rowCount', 1000)
    #         current_cols = first_sheet_props.get('columnCount', 26)
            
    #         if current_rows < total_rows or current_cols < total_cols:
    #             print(f"    📏 Resizing sheet to {total_rows} rows × {total_cols} columns...")
    #             first_sheet = sheet.get_worksheet(0)
    #             self._api_call_with_retry(
    #                 first_sheet.resize, 
    #                 rows=total_rows, 
    #                 cols=total_cols,
    #                 operation_name="resizing sheet"
    #             )
    #     except Exception as e:
    #         print(f"    ⚠️  Could not resize sheet: {e}")
        
    #     # Export Payment tab
    #     try:
    #         payment_worksheet = self._get_or_create_worksheet(sheet, "Payment")
    #         self._export_user_tab(payment_worksheet, project_data, role, include_payment=True)
    #         print(f"    ✅ Payment tab updated")
    #     except Exception as e:
    #         print(f"    ❌ Failed to update Payment tab: {e}")
    #         self.export_failures.append(f"Payment tab for {sheet_name}: {str(e)}")
        
    #     # Export Feedback tab
    #     try:
    #         feedback_worksheet = self._get_or_create_worksheet(sheet, "Feedback")
    #         self._export_user_tab(feedback_worksheet, project_data, role, include_payment=False)
    #         print(f"    ✅ Feedback tab updated")
    #     except Exception as e:
    #         print(f"    ❌ Failed to update Feedback tab: {e}")
    #         self.export_failures.append(f"Feedback tab for {sheet_name}: {str(e)}")
        
    #     return sheet.id
    
    # def _export_user_sheet(self, user_data: Dict, role: str, master_sheet_id: str, sheet_prefix: str) -> str:
    #     """Export individual user sheet and return sheet ID"""
    #     sheet_name = f"{sheet_prefix}-{user_data['user_name']} {role}"
    #     print(f"  Exporting {sheet_name} sheet...")
        
    #     sheet = None
        
    #     # Try to find existing sheet by listing all spreadsheets
    #     try:
    #         # First try direct open (fastest)
    #         sheet = self.client.open(sheet_name)
    #         print(f"    Found existing sheet: {sheet_name}")
    #     except gspread.exceptions.SpreadsheetNotFound:
    #         # Sheet genuinely doesn't exist
    #         print(f"    Creating new sheet: {sheet_name}")
    #         sheet = self._api_call_with_retry(self.client.create, sheet_name, 
    #                                         operation_name=f"creating sheet '{sheet_name}'")
    #     except gspread.exceptions.APIError as e:
    #         # Rate limit or other API error - don't create new sheet!
    #         if "429" in str(e) or "quota" in str(e).lower():
    #             print(f"    ⏳ Rate limit hit while searching for sheet, waiting...")
    #             time.sleep(10)
    #             # Retry once
    #             try:
    #                 sheet = self.client.open(sheet_name)
    #                 print(f"    Found existing sheet after retry: {sheet_name}")
    #             except gspread.exceptions.SpreadsheetNotFound:
    #                 print(f"    Creating new sheet: {sheet_name}")
    #                 sheet = self._api_call_with_retry(self.client.create, sheet_name,
    #                                                 operation_name=f"creating sheet '{sheet_name}'")
    #         else:
    #             raise
    #     except Exception as e:
    #         # Log unexpected errors but don't blindly create new sheet
    #         print(f"    ⚠️ Unexpected error looking for sheet: {type(e).__name__}: {str(e)[:100]}")
    #         raise
        
    def _export_user_sheet(self, user_data: Dict, role: str, master_sheet_id: str, sheet_prefix: str) -> str:
        """Export individual user sheet and return sheet ID"""
        sheet_name = f"{sheet_prefix}-{user_data['user_name']} {role}"
        print(f"  Exporting {sheet_name} sheet...")
        
        sheet = None
        
        # Try to find existing sheet by listing all spreadsheets
        try:
            # First try direct open (fastest)
            sheet = self.client.open(sheet_name)
            print(f"    Found existing sheet: {sheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            # Sheet genuinely doesn't exist
            print(f"    Creating new sheet: {sheet_name}")
            sheet = self._api_call_with_retry(self.client.create, sheet_name, 
                                            operation_name=f"creating sheet '{sheet_name}'")
        except gspread.exceptions.APIError as e:
            # Rate limit or other API error - don't create new sheet!
            if "429" in str(e) or "quota" in str(e).lower():
                print(f"    ⏳ Rate limit hit while searching for sheet, waiting...")
                time.sleep(10)
                # Retry once
                try:
                    sheet = self.client.open(sheet_name)
                    print(f"    Found existing sheet after retry: {sheet_name}")
                except gspread.exceptions.SpreadsheetNotFound:
                    print(f"    Creating new sheet: {sheet_name}")
                    sheet = self._api_call_with_retry(self.client.create, sheet_name,
                                                    operation_name=f"creating sheet '{sheet_name}'")
            else:
                raise
        except Exception as e:
            # Log unexpected errors but don't blindly create new sheet
            print(f"    ⚠️ Unexpected error looking for sheet: {type(e).__name__}: {str(e)[:100]}")
            raise

        # ========== THIS ENTIRE SECTION IS MISSING ==========
        # Get user project data
        with self.get_db_session() as session:
            if role == "Annotator":
                project_data = self.GoogleSheetsExportService.get_user_project_data_annotator(user_data['user_id'], session)
            elif role == "Reviewer":
                project_data = self.GoogleSheetsExportService.get_user_project_data_reviewer(user_data['user_id'], session)
            else:  # Meta-Reviewer
                project_data = self.GoogleSheetsExportService.get_user_project_data_meta_reviewer(user_data['user_id'], session)
        
        # Calculate required rows (headers + data + padding) - handle massive scale
        header_rows = 2  # All roles now have 2-row headers
        data_rows = len(project_data)
        
        # For very large datasets, use more conservative padding
        if data_rows > 1000:
            total_rows = header_rows + data_rows + 10  # Minimal padding for large datasets
        else:
            total_rows = max(100, header_rows + data_rows + 50)  # Standard padding
        
        total_cols = 20  # Standard column count
        
        # Ensure sheet has enough rows and columns
        try:
            # Use the Sheets API service to get sheet properties
            sheet_metadata = self._api_call_with_retry(
                self.sheets_service.spreadsheets().get,
                spreadsheetId=sheet.id,
                operation_name="getting sheet properties"
            ).execute()
            
            first_sheet_props = sheet_metadata.get('sheets', [{}])[0].get('properties', {}).get('gridProperties', {})
            current_rows = first_sheet_props.get('rowCount', 1000)
            current_cols = first_sheet_props.get('columnCount', 26)
            
            if current_rows < total_rows or current_cols < total_cols:
                print(f"    📏 Resizing sheet to {total_rows} rows × {total_cols} columns...")
                first_sheet = sheet.get_worksheet(0)
                self._api_call_with_retry(
                    first_sheet.resize, 
                    rows=total_rows, 
                    cols=total_cols,
                    operation_name="resizing sheet"
                )
        except Exception as e:
            print(f"    ⚠️  Could not resize sheet: {e}")
        
        # Export Payment tab
        try:
            payment_worksheet = self._get_or_create_worksheet(sheet, "Payment")
            self._export_user_tab(payment_worksheet, project_data, role, include_payment=True)
            print(f"    ✅ Payment tab updated")
        except Exception as e:
            print(f"    ❌ Failed to update Payment tab: {e}")
            self.export_failures.append(f"Payment tab for {sheet_name}: {str(e)}")
        
        # Export Feedback tab
        try:
            feedback_worksheet = self._get_or_create_worksheet(sheet, "Feedback")
            self._export_user_tab(feedback_worksheet, project_data, role, include_payment=False)
            print(f"    ✅ Feedback tab updated")
        except Exception as e:
            print(f"    ❌ Failed to update Feedback tab: {e}")
            self.export_failures.append(f"Feedback tab for {sheet_name}: {str(e)}")
        
        return sheet.id

    def _get_or_create_worksheet(self, sheet, tab_name: str):
        """Get existing worksheet or create new one"""
        try:
            return sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            return self._api_call_with_retry(
                sheet.add_worksheet, 
                title=tab_name, 
                rows=100, 
                cols=20,
                operation_name=f"creating {tab_name} worksheet"
            )
    
    # def _export_user_tab(self, worksheet, project_data: List[Dict], role: str, include_payment: bool):
    #     """Export individual user sheet tab with improved formatting"""
    #     if not project_data:
    #         return
        
    #     # FIXED: Create headers with proper structure and span
    #     self._create_user_sheet_headers(worksheet, role, include_payment)
        
    #     # Prepare data rows with preserved manual data
    #     existing_data = self._get_existing_manual_data(worksheet, len(project_data), role, include_payment)
    #     data_rows = []
        
    #     for i, project in enumerate(project_data):
    #         row = self._create_project_row(project, role, include_payment)
            
    #         # Preserve manual data
    #         if i < len(existing_data):
    #             preserved_data = existing_data[i]
    #             # Determine the manual column positions based on role and payment tab
    #             manual_col_positions = self._get_manual_column_positions(role, include_payment)
                
    #             # Preserve data in manual columns
    #             for pos in manual_col_positions:
    #                 if pos < len(preserved_data) and pos < len(row) and preserved_data[pos].strip():
    #                     row[pos] = preserved_data[pos]
            
    #         data_rows.append(row)
        
    #     # Update data starting from row 3 (after headers) - all roles now have 2-row headers
    #     if data_rows:
    #         start_row = 3  # All roles now have 2-row headers
    #         self._update_data_with_preservation(worksheet, data_rows, start_row)
        
    #     # Apply formatting
    #     self._apply_user_sheet_formatting(worksheet, len(data_rows), role, include_payment)
    
    def _get_existing_manual_data_mapped_to_projects(self, worksheet, role: str, include_payment: bool) -> Dict[str, Dict]:
        """Get existing manual data mapped to project names for preservation during sorting"""
        try:
            all_values = worksheet.get_all_values()
            if len(all_values) <= 2:  # No data rows (only headers)
                return {}
            
            project_manual_data = {}
            start_row = 2  # Skip 2-row headers (0-indexed, so row 3 in 1-indexed)
            manual_col_positions = self._get_manual_column_positions(role, include_payment)
            
            # Map each project name to its manual data
            for row_idx in range(start_row, len(all_values)):
                row = all_values[row_idx]
                if len(row) > 0 and row[0]:  # Has project name
                    project_name = row[0].strip()
                    if project_name:
                        # Extract manual data from this row
                        manual_data = {}
                        for pos in manual_col_positions:
                            if pos < len(row):
                                manual_data[pos] = row[pos]
                        project_manual_data[project_name] = manual_data
            
            return project_manual_data
        except Exception as e:
            print(f"      ⚠️ Could not read existing manual data: {e}")
            return {}

    def _export_user_tab(self, worksheet, project_data: List[Dict], role: str, include_payment: bool):
        """Export individual user sheet tab with sorting and project-aware manual data preservation"""
        if not project_data:
            return
        
        # STEP 1: Get existing manual data mapped to project names (BEFORE sorting)
        existing_manual_data = self._get_existing_manual_data_mapped_to_projects(worksheet, role, include_payment)
        
        # STEP 2: Sort projects by most recent activity (NOW SAFE TO SORT)
        if role == "Meta-Reviewer":
            # Sort by last_modified for meta-reviewers
            project_data.sort(key=lambda x: x.get('last_modified') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        else:
            # Sort by last_submitted for annotators and reviewers
            project_data.sort(key=lambda x: x.get('last_submitted') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        
        # STEP 3: Create headers
        self._create_user_sheet_headers(worksheet, role, include_payment)
        
        # STEP 4: Build data rows with project-aware manual data preservation
        data_rows = []
        manual_col_positions = self._get_manual_column_positions(role, include_payment)
        
        for project in project_data:
            # Create base row with automatic data
            row = self._create_project_row(project, role, include_payment)
            
            # Apply preserved manual data for this specific project
            project_name = project['project_name']
            if project_name in existing_manual_data:
                preserved_data = existing_manual_data[project_name]
                
                # Restore manual data to correct columns for this project
                for pos in manual_col_positions:
                    if pos in preserved_data and pos < len(row) and preserved_data[pos].strip():
                        row[pos] = preserved_data[pos]
            
            data_rows.append(row)
        
        # STEP 5: Update sheet with sorted data and preserved manual columns
        if data_rows:
            start_row = 3  # After 2-row headers
            self._update_data_with_preservation(worksheet, data_rows, start_row)
        
        # STEP 6: Apply formatting
        self._apply_user_sheet_formatting(worksheet, len(data_rows), role, include_payment)


    def _get_manual_column_positions(self, role: str, include_payment: bool) -> List[int]:
        """Get the column positions for manual data that should be preserved"""
        positions = []
        
        if role == "Annotator":
            # Columns: Project Name, Schema Name, Video Count, Last Submitted, [Payment/Feedback], [Overall Stats...]
            if include_payment:
                positions = [4, 5, 6]  # Payment Time, Base Salary, Bonus Salary
            else:
                positions = [4]  # Feedback
        elif role == "Reviewer":
            # Columns: Project Name, Schema Name, Video Count, GT%, All GT%, Review%, All Rev%, Last Submitted, [Payment/Feedback], [Overall Stats...]
            if include_payment:
                positions = [8, 9, 10]  # Payment Time, Base Salary, Bonus Salary
            else:
                positions = [8]  # Feedback
        else:  # Meta-Reviewer - UPDATED: Modified Ratio By columns moved to end
            # Columns: Project Name, Schema Name, Video Count, Last Modified, [Payment/Feedback], Modified Ratio By User%, Modified Ratio By All%
            if include_payment:
                positions = [4, 5, 6]  # Payment Time, Base Salary, Bonus Salary
            else:
                positions = [4]  # Feedback
        
        return positions
    
    def _create_user_sheet_headers(self, worksheet, role: str, include_payment: bool):
        """FIXED: Create properly structured headers for user sheets"""
        # Create the proper header structure for each role
        if role == "Annotator":
            row1 = ["Project Name", "Schema Name", "Video Count", "Last Submitted"]
            if include_payment:
                row1.extend(["Payment Time", "Base Salary", "Bonus Salary"])
            else:
                row1.append("Feedback")
            # Add Overall Stats spanning headers
            row1.extend(["Overall Stats", "", "", "", "", ""])
            
            # Row 2 has sub-headers only for Overall Stats
            row2 = [""] * (len(row1) - 6)  # Empty for first columns
            row2.extend(["Accuracy%", "Completion%", "Reviewed%", "Completed", "Reviewed", "Wrong"])  # FIXED: "Reviewed%" 
            
        elif role == "Reviewer":
            row1 = ["Project Name", "Schema Name", "Video Count", "GT%", "All GT%", "Review%", "All Rev%", "Last Submitted"]
            if include_payment:
                row1.extend(["Payment Time", "Base Salary", "Bonus Salary"])
            else:
                row1.append("Feedback")
            # Add Overall Stats spanning headers
            row1.extend(["Overall Stats", "", "", "", "", ""])
            
            # Row 2 has sub-headers only for Overall Stats
            row2 = [""] * (len(row1) - 6)  # Empty for first columns
            row2.extend(["GT%", "GT Acc%", "Review%", "GT Done", "GT Wrong", "Rev Done"])
            
        else:  # Meta-Reviewer - UPDATED: Modified Ratio By columns moved to end
            row1 = ["Project Name", "Schema Name", "Video Count", "Last Modified"]
            if include_payment:
                row1.extend(["Payment Time", "Base Salary", "Bonus Salary"])
            else:
                row1.append("Feedback")
            # Add Modified Ratio By columns at the end
            row1.extend(["Modified Ratio By", ""])
            
            # Row 2 has sub-headers for Modified Ratio By at the end
            row2 = ["", "", "", ""]  # First 4 columns
            if include_payment:
                row2.extend(["", "", ""])  # Payment columns
            else:
                row2.append("")  # Feedback column
            row2.extend(["User %", "All %"])  # Modified Ratio By sub-headers
        
        # Update headers
        end_col = self._col_num_to_letter(len(row1))
        self._api_call_with_retry(
            worksheet.update, 
            values=[row1, row2], 
            range_name=f'A1:{end_col}2', 
            operation_name="updating headers"
        )
        
        # Apply header merging and formatting
        self._apply_header_merging(worksheet, row1, row2)
        self._apply_header_formatting(worksheet, len(row1))
    
    def _apply_header_formatting(self, worksheet, num_columns: int):
        """Apply color formatting and styling to headers"""
        try:
            sheet_id = worksheet._properties['sheetId']
            
            # Apply header background color and text formatting
            header_format_requests = []
            
            # Header background color (light blue)
            header_format_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 2,  # Both header rows
                        "startColumnIndex": 0,
                        "endColumnIndex": num_columns
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.85,
                                "green": 0.92,
                                "blue": 1.0
                            },
                            "textFormat": {
                                "bold": True,
                                "fontSize": 10
                            },
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE"
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
                }
            })
            
            # Apply formatting
            if header_format_requests:
                batch_request = {"requests": header_format_requests}
                self._api_call_with_retry(
                    self.sheets_service.spreadsheets().batchUpdate,
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=batch_request,
                    operation_name="applying header formatting"
                ).execute()
                
        except Exception as e:
            print(f"      ⚠️ Could not apply header formatting: {e}")
    
    def _apply_header_merging(self, worksheet, row1: List[str], row2: List[str]):
        """FIXED: Apply proper header merging for spans and single cells"""
        try:
            # Get the sheet ID from the worksheet
            sheet_id = worksheet._properties['sheetId']
            
            # First, try to unmerge ALL existing merged cells in the header area using a more robust approach
            try:
                # Clear all merges in the first 2 rows by unmerging the entire header area
                unmerge_requests = [{
                    'unmergeCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 2,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(row1)
                        }
                    }
                }]
                
                unmerge_batch_request = {'requests': unmerge_requests}
                self._api_call_with_retry(
                    self.sheets_service.spreadsheets().batchUpdate,
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=unmerge_batch_request,
                    operation_name="unmerging entire header area"
                ).execute()
                    
            except Exception as e:
                # If unmerging fails, that's usually because there are no merges to unmerge, which is fine
                print(f"      ℹ️  No existing merges to unmerge (this is normal): {str(e)[:100]}...")
            
            # Small delay to ensure unmerge is processed
            import time
            time.sleep(0.5)
            
            # Now apply new merges
            merge_requests = []
            
            # Handle spanning headers first ("Overall Stats" and "Modified Ratio By")
            i = 0
            while i < len(row1):
                if row1[i] in ["Overall Stats", "Modified Ratio By"]:
                    start_col = i
                    end_col = i
                    
                    # For "Overall Stats", span 6 columns
                    # For "Modified Ratio By", span 2 columns  
                    if row1[i] == "Overall Stats":
                        end_col = i + 5  # Span 6 columns (i to i+5)
                    elif row1[i] == "Modified Ratio By":
                        end_col = i + 1  # Span 2 columns (i to i+1)
                    
                    # Ensure we don't go beyond the row length
                    end_col = min(end_col, len(row1) - 1)
                    
                    if end_col > start_col:
                        merge_requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': 0,
                                    'endRowIndex': 1,  # Only merge the first row for spanning headers
                                    'startColumnIndex': start_col,
                                    'endColumnIndex': end_col + 1
                                },
                                'mergeType': 'MERGE_ALL'
                            }
                        })
                    
                    i = end_col + 1  # Skip to after the merged range
                else:
                    # Handle single-cell headers that span both rows
                    if row1[i] and (i >= len(row2) or not row2[i]):
                        # This cell has content in row1 but empty in row2, so merge vertically
                        merge_requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': 0,
                                    'endRowIndex': 2,  # Merge both rows for this cell
                                    'startColumnIndex': i,
                                    'endColumnIndex': i + 1
                                },
                                'mergeType': 'MERGE_ALL'
                            }
                        })
                    i += 1
            
            # Apply merge requests if any
            if merge_requests:
                batch_update_request = {
                    'requests': merge_requests
                }
                
                self._api_call_with_retry(
                    self.sheets_service.spreadsheets().batchUpdate,
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=batch_update_request,
                    operation_name="merging header cells"
                ).execute()
                
                print(f"      ✅ Applied {len(merge_requests)} header merges successfully")
                
        except Exception as e:
            print(f"      ⚠️ Could not apply header merging: {e}")
            # Continue without merging - the sheet will still function, just without fancy header merging
    

    
    def _create_project_row(self, project: Dict, role: str, include_payment: bool) -> List:
        """Create a project data row with correct column alignment"""
        if role == "Annotator":
            row = [
                project['project_name'],
                project['schema_name'],
                project['video_count'],  # NEW: Video Count
                self._format_timestamp(project['last_submitted'])
            ]
            # Add payment/feedback placeholder
            if include_payment:
                row.extend(['', '', ''])  # Payment Time, Base Salary, Bonus Salary
            else:
                row.append('')  # Feedback column
            
            # Add overall statistics
            row.extend([
                f"{project['accuracy']:.0f}%",
                f"{project['completion']:.0f}%",
                f"{project['reviewed_ratio']:.0f}%",  # FIXED: Use reviewed_ratio as percentage
                project['completed'],
                project['reviewed'],
                project['wrong']
            ])
            
        elif role == "Reviewer":
            row = [
                project['project_name'],
                project['schema_name'],
                project['video_count'],  # NEW: Video Count
                f"{project['gt_ratio']:.0f}%",
                f"{project['all_gt_ratio']:.0f}%",
                f"{project['review_ratio']:.0f}%",
                f"{project['all_review_ratio']:.0f}%",
                self._format_timestamp(project['last_submitted'])
            ]
            # Add payment/feedback placeholder
            if include_payment:
                row.extend(['', '', ''])  # Payment Time, Base Salary, Bonus Salary
            else:
                row.append('')  # Feedback column
            
            # Add overall statistics
            row.extend([
                f"{project['gt_completion']:.0f}%",
                f"{project['gt_accuracy']:.0f}%",
                f"{project['review_completion']:.0f}%",
                project['gt_completed'],
                project['gt_wrong'],
                project['review_completed']
            ])
            
        else:  # Meta-Reviewer - UPDATED: Modified Ratio By columns moved to end
            row = [
                project['project_name'],
                project['schema_name'],
                project['video_count'],  # NEW: Video Count
                self._format_timestamp(project['last_modified'])
            ]
            # Add payment/feedback placeholder
            if include_payment:
                row.extend(['', '', ''])  # Payment Time, Base Salary, Bonus Salary
            else:
                row.append('')  # Feedback column
            
            # Add Modified Ratio By columns at the end
            row.extend([
                f"{project['ratio_modified_by_user']:.0f}%",
                f"{project['ratio_modified_by_all']:.0f}%"
            ])
        
        return row
    
    def _update_data_with_preservation(self, worksheet, data_rows: List[List], start_row: int):
        """Update data rows while preserving manual columns"""
        if not data_rows:
            return
        
        end_row = start_row + len(data_rows) - 1
        end_col = self._col_num_to_letter(len(data_rows[0]))
        
        self._api_call_with_retry(
            worksheet.update, 
            values=data_rows, 
            range_name=f'A{start_row}:{end_col}{end_row}',
            operation_name="updating project data"
        )
    
    def _apply_alternating_row_colors(self, worksheet, data_rows_count: int, header_rows: int):
        """Apply alternating row colors as the FINAL formatting step to ensure they stick"""
        if data_rows_count <= 0:
            return
        
        try:
            sheet_id = worksheet._properties['sheetId']
            
            # Define colors
            light_blue_color = {
                "red": 0.95,
                "green": 0.98, 
                "blue": 1.0
            }
            
            white_color = {
                "red": 1.0,
                "green": 1.0,
                "blue": 1.0
            }
            
            # Build comprehensive alternating color requests
            alternating_requests = []
            
            # Calculate the range of data rows (after headers)
            first_data_row = header_rows  # 0-indexed (row 3 in 1-indexed becomes 2 in 0-indexed)
            last_data_row = header_rows + data_rows_count - 1
            
            # Apply colors to ALL data rows (both blue and white explicitly)
            for row_index in range(first_data_row, header_rows + data_rows_count):
                # Determine if this should be colored (every other row)
                is_colored_row = (row_index - first_data_row) % 2 == 1  # 0, 2, 4... = white; 1, 3, 5... = blue
                
                background_color = light_blue_color if is_colored_row else white_color
                
                alternating_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_index,
                            "endRowIndex": row_index + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 50  # Cover all relevant columns
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": background_color
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
            
            # Apply all alternating colors in one batch (HIGHER PRIORITY)
            if alternating_requests:
                alternating_batch_request = {"requests": alternating_requests}
                self._api_call_with_retry(
                    self.sheets_service.spreadsheets().batchUpdate,
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=alternating_batch_request,
                    operation_name="applying robust alternating row colors"
                ).execute()
                
                print(f"      ✅ Applied alternating colors to {data_rows_count} rows (pattern agnostic)")
                
        except Exception as e:
            print(f"      ⚠️ Could not apply alternating row colors: {e}")

    def _apply_user_sheet_formatting(self, worksheet, data_rows_count: int, role: str, include_payment: bool):
        """Apply professional formatting to user sheets with robust alternating colors"""
        try:
            print(f"      🎨 Applying formatting...")
            
            header_rows = 2  # All roles have 2-row headers
            last_row = header_rows + data_rows_count
            
            # STEP 1: Apply all other formatting FIRST (column widths, etc.)
            column_widths = [
                {"startIndex": 0, "endIndex": 1, "pixelSize": 150},   # A: Project Name
                {"startIndex": 1, "endIndex": 2, "pixelSize": 120},   # B: Schema Name
                {"startIndex": 2, "endIndex": 3, "pixelSize": 90},    # C: Video Count
            ]
            
            # Role-specific columns
            current_col = 3
            if role == "Annotator":
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 132}, # D: Last Submitted
                ])
                current_col += 1
            elif role == "Reviewer":
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 60},    # D: GT%
                    {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 60}, # E: All GT%
                    {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 60}, # F: Review%
                    {"startIndex": current_col + 3, "endIndex": current_col + 4, "pixelSize": 60}, # G: All Rev%
                    {"startIndex": current_col + 4, "endIndex": current_col + 5, "pixelSize": 132}, # H: Last Submitted
                ])
                current_col += 5
            else:  # Meta-Reviewer
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 132}, # D: Last Modified
                ])
                current_col += 1
            
            # Payment/Feedback columns
            if include_payment:
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 120},      # Payment Time
                    {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 99},   # Base Salary
                    {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 99},   # Bonus Salary
                ])
                current_col += 3
            else:
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 300},      # Feedback
                ])
                current_col += 1
            
            # Overall stats columns 
            if role in ["Annotator", "Reviewer"]:
                remaining_cols = 6
                for i in range(remaining_cols):
                    column_widths.append({
                        "startIndex": current_col + i, 
                        "endIndex": current_col + i + 1, 
                        "pixelSize": 80
                    })
            else:  # Meta-Reviewer: Modified Ratio By columns
                for i in range(2):  
                    column_widths.append({
                        "startIndex": current_col + i, 
                        "endIndex": current_col + i + 1, 
                        "pixelSize": 80
                    })
            
            # Apply column widths and other formatting
            requests = []
            for width_spec in column_widths:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet._properties['sheetId'],
                            "dimension": "COLUMNS",
                            "startIndex": width_spec["startIndex"],
                            "endIndex": width_spec["endIndex"]
                        },
                        "properties": {
                            "pixelSize": width_spec["pixelSize"]
                        },
                        "fields": "pixelSize"
                    }
                })
            
            # Apply row heights and freeze panes
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": header_rows
                    },
                    "properties": {
                        "pixelSize": 35
                    },
                    "fields": "pixelSize"
                }
            })
            
            # Set feedback row heights
            if not include_payment and data_rows_count > 0:  # Feedback tab
                for row_index in range(header_rows, header_rows + data_rows_count):
                    requests.append({
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": worksheet._properties['sheetId'],
                                "dimension": "ROWS",
                                "startIndex": row_index,
                                "endIndex": row_index + 1
                            },
                            "properties": {
                                "pixelSize": 60  # 60px tall for feedback
                            },
                            "fields": "pixelSize"
                        }
                    })
            
            # Freeze header rows
            requests.append({
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": worksheet._properties['sheetId'],
                        "gridProperties": {
                            "frozenRowCount": header_rows
                        }
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            })
            
            # STEP 2: Apply all structural formatting first
            if requests:
                batch_request = {"requests": requests}
                self._api_call_with_retry(
                    self.sheets_service.spreadsheets().batchUpdate,
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=batch_request,
                    operation_name="applying column widths and structural formatting"
                ).execute()
            
            # STEP 3: Format specific columns (Video Count)
            video_count_col = 2  # Column C (0-indexed)
            video_count_format_requests = [{
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "startRowIndex": header_rows,
                        "endRowIndex": header_rows + data_rows_count,
                        "startColumnIndex": video_count_col,
                        "endColumnIndex": video_count_col + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "#,##0"
                            },
                            "horizontalAlignment": "CENTER"
                            # NOTE: No backgroundColor here - will be overridden by alternating colors
                        }
                    },
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"  # Don't override backgroundColor
                }
            }]
            
            video_count_batch_request = {"requests": video_count_format_requests}
            self._api_call_with_retry(
                self.sheets_service.spreadsheets().batchUpdate,
                spreadsheetId=worksheet.spreadsheet.id,
                body=video_count_batch_request,
                operation_name="formatting video count column"
            ).execute()
            
            # STEP 4: Format feedback column (if feedback tab)
            if not include_payment:  # Feedback tab
                feedback_col_positions = self._get_manual_column_positions(role, include_payment)
                if feedback_col_positions:
                    feedback_col_num = feedback_col_positions[0] + 1  # Convert to 1-indexed
                    feedback_col = self._col_num_to_letter(feedback_col_num)
                    
                    feedback_format = {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "textFormat": {"fontSize": 9}
                        # NOTE: No backgroundColor here - will be overridden by alternating colors
                    }
                    self._api_call_with_retry(
                        worksheet.format, 
                        f"{feedback_col}{header_rows + 1}:{feedback_col}{last_row}", 
                        feedback_format,
                        operation_name="formatting feedback column with text wrapping"
                    )
            
            # STEP 5: Apply alternating row colors LAST (this is the key!)
            self._apply_alternating_row_colors(worksheet, data_rows_count, header_rows)
            
            print(f"      ✅ Applied comprehensive formatting with robust alternating rows")
            
        except Exception as e:
            print(f"      ⚠️ Worksheet formatting failed: {e}")

    def _apply_master_sheet_formatting(self, worksheet, data_rows_count: int, headers_count: int):
        """Apply formatting to master sheet with robust alternating colors"""
        try:
            last_row = 1 + data_rows_count
            header_rows = 1  # Master sheet has 1 header row
            
            # STEP 1: Apply structural formatting first
            column_widths = [
                {"startIndex": 0, "endIndex": 1, "pixelSize": 150},   # User Name
                {"startIndex": 1, "endIndex": 2, "pixelSize": 200},   # Email
                {"startIndex": 2, "endIndex": 3, "pixelSize": 80},    # Role
                {"startIndex": 3, "endIndex": 4, "pixelSize": 200},   # Sheet Link
                {"startIndex": 4, "endIndex": 5, "pixelSize": 132},   # Last Time
                {"startIndex": 5, "endIndex": 6, "pixelSize": 130},   # Assigned Projects
                {"startIndex": 6, "endIndex": 7, "pixelSize": 130},   # Started Projects
            ]
            
            # Add remaining columns
            for i in range(7, headers_count):
                if i == 7:  # Completed Projects column
                    column_widths.append({
                        "startIndex": i, 
                        "endIndex": i + 1, 
                        "pixelSize": 140
                    })
                else:
                    column_widths.append({
                        "startIndex": i, 
                        "endIndex": i + 1, 
                        "pixelSize": 100
                    })
            
            requests = []
            for width_spec in column_widths:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet._properties['sheetId'],
                            "dimension": "COLUMNS",
                            "startIndex": width_spec["startIndex"],
                            "endIndex": width_spec["endIndex"]
                        },
                        "properties": {
                            "pixelSize": width_spec["pixelSize"]
                        },
                        "fields": "pixelSize"
                    }
                })
            
            # Apply header formatting
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": headers_count
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.2,
                                "green": 0.4,
                                "blue": 0.8
                            },
                            "textFormat": {
                                "bold": True,
                                "fontSize": 10,
                                "foregroundColor": {
                                    "red": 1,
                                    "green": 1,
                                    "blue": 1
                                }
                            },
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE"
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
                }
            })
            
            # STEP 2: Apply structural formatting
            if requests:
                batch_request = {"requests": requests}
                self._api_call_with_retry(
                    self.sheets_service.spreadsheets().batchUpdate,
                    spreadsheetId=worksheet.spreadsheet.id,
                    body=batch_request,
                    operation_name="applying master sheet structural formatting"
                ).execute()
            
            # STEP 3: Apply alternating row colors LAST
            self._apply_alternating_row_colors(worksheet, data_rows_count, header_rows)
            
            print(f"      ✅ Applied master sheet formatting with robust alternating rows")
            
        except Exception as e:
            print(f"      ⚠️ Master sheet formatting failed: {e}")

    
    # def _apply_user_sheet_formatting(self, worksheet, data_rows_count: int, role: str, include_payment: bool):
    #     """Apply professional formatting to user sheets"""
    #     try:
    #         print(f"      🎨 Applying formatting...")
            
    #         # Header formatting - all roles now have 2-row headers
    #         header_rows = 2
    #         last_row = header_rows + data_rows_count
            
    #         # FIXED: Apply column widths including Video Count column
    #         column_widths = [
    #             {"startIndex": 0, "endIndex": 1, "pixelSize": 150},   # A: Project Name
    #             {"startIndex": 1, "endIndex": 2, "pixelSize": 120},   # B: Schema Name
    #             {"startIndex": 2, "endIndex": 3, "pixelSize": 90},    # C: Video Count
    #         ]
            
    #         # Role-specific columns
    #         current_col = 3
    #         if role == "Annotator":
    #             column_widths.extend([
    #                 {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 132}, # D: Last Submitted
    #             ])
    #             current_col += 1
    #         elif role == "Reviewer":
    #             column_widths.extend([
    #                 {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 60},    # D: GT%
    #                 {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 60}, # E: All GT%
    #                 {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 60}, # F: Review%
    #                 {"startIndex": current_col + 3, "endIndex": current_col + 4, "pixelSize": 60}, # G: All Rev%
    #                 {"startIndex": current_col + 4, "endIndex": current_col + 5, "pixelSize": 132}, # H: Last Submitted
    #             ])
    #             current_col += 5
    #         else:  # Meta-Reviewer - UPDATED: Last Modified comes after Video Count
    #             column_widths.extend([
    #                 {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 132}, # D: Last Modified
    #             ])
    #             current_col += 1
            
    #         # Payment/Feedback columns with FIXED salary column widths (10% increase)
    #         if include_payment:
    #             column_widths.extend([
    #                 {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 120},      # Payment Time
    #                 {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 99},   # Base Salary (90 + 10%)
    #                 {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 99},   # Bonus Salary (90 + 10%)
    #             ])
    #             current_col += 3
    #         else:
    #             column_widths.extend([
    #                 {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 300},      # Feedback - very wide
    #             ])
    #             current_col += 1
            
    #         # Overall stats columns (only for Annotator and Reviewer) OR Modified Ratio By (for Meta-Reviewer)
    #         if role in ["Annotator", "Reviewer"]:
    #             remaining_cols = 6
    #             for i in range(remaining_cols):
    #                 column_widths.append({
    #                     "startIndex": current_col + i, 
    #                     "endIndex": current_col + i + 1, 
    #                     "pixelSize": 80
    #                 })
    #         else:  # Meta-Reviewer: Modified Ratio By columns
    #             for i in range(2):  # User % and All %
    #                 column_widths.append({
    #                     "startIndex": current_col + i, 
    #                     "endIndex": current_col + i + 1, 
    #                     "pixelSize": 80
    #                 })
            
    #         # Apply column widths
    #         requests = []
    #         for width_spec in column_widths:
    #             requests.append({
    #                 "updateDimensionProperties": {
    #                     "range": {
    #                         "sheetId": worksheet._properties['sheetId'],
    #                         "dimension": "COLUMNS",
    #                         "startIndex": width_spec["startIndex"],
    #                         "endIndex": width_spec["endIndex"]
    #                     },
    #                     "properties": {
    #                         "pixelSize": width_spec["pixelSize"]
    #                     },
    #                     "fields": "pixelSize"
    #                 }
    #             })
            
    #         # Apply row heights and freeze panes
    #         requests.append({
    #             "updateDimensionProperties": {
    #                 "range": {
    #                     "sheetId": worksheet._properties['sheetId'],
    #                     "dimension": "ROWS",
    #                     "startIndex": 0,
    #                     "endIndex": header_rows
    #                 },
    #                 "properties": {
    #                     "pixelSize": 35
    #                 },
    #                 "fields": "pixelSize"
    #             }
    #         })
            
    #         # NEW: Set feedback row heights to 60px if this is feedback tab
    #         if not include_payment and data_rows_count > 0:  # Feedback tab
    #             for row_index in range(header_rows, header_rows + data_rows_count):
    #                 requests.append({
    #                     "updateDimensionProperties": {
    #                         "range": {
    #                             "sheetId": worksheet._properties['sheetId'],
    #                             "dimension": "ROWS",
    #                             "startIndex": row_index,
    #                             "endIndex": row_index + 1
    #                         },
    #                         "properties": {
    #                             "pixelSize": 60  # 60px tall for feedback
    #                         },
    #                         "fields": "pixelSize"
    #                     }
    #                 })
            
    #         # Freeze header rows
    #         requests.append({
    #             "updateSheetProperties": {
    #                 "properties": {
    #                     "sheetId": worksheet._properties['sheetId'],
    #                     "gridProperties": {
    #                         "frozenRowCount": header_rows
    #                     }
    #                 },
    #                 "fields": "gridProperties.frozenRowCount"
    #             }
    #         })
            
    #         if requests:
    #             batch_request = {"requests": requests}
    #             self._api_call_with_retry(
    #                 self.sheets_service.spreadsheets().batchUpdate,
    #                 spreadsheetId=worksheet.spreadsheet.id,
    #                 body=batch_request,
    #                 operation_name="applying column widths and formatting"
    #             ).execute()
            
    #         # NEW: Manual alternating row colors (like Script 2)
    #         # if data_rows_count > 0:
    #         #     even_row_format = {
    #         #         "backgroundColor": {"red": 0.95, "green": 0.98, "blue": 1.0}  # Light blue
    #         #     }
                
    #         #     # Apply to even rows starting from row 4 (data starts at row 3, so even rows are 4, 6, 8...)
    #         #     for row in range(4, header_rows + data_rows_count + 1, 2):  # Every other row starting from 4
    #         #         self._api_call_with_retry(
    #         #             worksheet.format, f"A{row}:ZZ{row}", even_row_format,
    #         #             operation_name=f"formatting alternating row {row}"
    #         #         )
    #         # NEW: Batch alternating row colors (much more efficient!)
    #         if data_rows_count > 0:
    #             alternating_requests = []
                
    #             # Build batch requests for alternating rows
    #             for row in range(4, header_rows + data_rows_count + 1, 2):  # Even rows: 4, 6, 8...
    #                 alternating_requests.append({
    #                     "repeatCell": {
    #                         "range": {
    #                             "sheetId": worksheet._properties['sheetId'],
    #                             "startRowIndex": row - 1,  # Convert to 0-indexed
    #                             "endRowIndex": row,
    #                             "startColumnIndex": 0,
    #                             "endColumnIndex": min(50, len(column_widths))  # Cap at reasonable column count
    #                         },
    #                         "cell": {
    #                             "userEnteredFormat": {
    #                                 "backgroundColor": {
    #                                     "red": 0.95,
    #                                     "green": 0.98,
    #                                     "blue": 1.0
    #                                 }
    #                             }
    #                         },
    #                         "fields": "userEnteredFormat.backgroundColor"
    #                     }
    #                 })
                
    #             # Apply all alternating row colors in a single batch request
    #             if alternating_requests:
    #                 alternating_batch_request = {"requests": alternating_requests}
    #                 self._api_call_with_retry(
    #                     self.sheets_service.spreadsheets().batchUpdate,
    #                     spreadsheetId=worksheet.spreadsheet.id,
    #                     body=alternating_batch_request,
    #                     operation_name="applying alternating row colors (batch)"
    #                 ).execute()
            
    #         # Format Video Count column as number
    #         video_count_col = 2  # Column C (0-indexed)
    #         video_count_format_requests = [{
    #             "repeatCell": {
    #                 "range": {
    #                     "sheetId": worksheet._properties['sheetId'],
    #                     "startRowIndex": header_rows,
    #                     "endRowIndex": header_rows + data_rows_count,
    #                     "startColumnIndex": video_count_col,
    #                     "endColumnIndex": video_count_col + 1
    #                 },
    #                 "cell": {
    #                     "userEnteredFormat": {
    #                         "numberFormat": {
    #                             "type": "NUMBER",
    #                             "pattern": "#,##0"
    #                         },
    #                         "horizontalAlignment": "CENTER"
    #                     }
    #                 },
    #                 "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"
    #             }
    #         }]
            
    #         # Apply video count formatting
    #         video_count_batch_request = {"requests": video_count_format_requests}
    #         self._api_call_with_retry(
    #             self.sheets_service.spreadsheets().batchUpdate,
    #             spreadsheetId=worksheet.spreadsheet.id,
    #             body=video_count_batch_request,
    #             operation_name="formatting video count column"
    #         ).execute()
            
    #         # NEW: Format feedback column with text wrapping (like Script 2)
    #         if not include_payment:  # Feedback tab
    #             # Calculate feedback column based on role and structure
    #             feedback_col_positions = self._get_manual_column_positions(role, include_payment)
    #             if feedback_col_positions:
    #                 feedback_col_num = feedback_col_positions[0] + 1  # Convert to 1-indexed
    #                 feedback_col = self._col_num_to_letter(feedback_col_num)
                    
    #                 feedback_format = {
    #                     "wrapStrategy": "WRAP",  # Enable text wrapping
    #                     "verticalAlignment": "TOP",
    #                     "textFormat": {"fontSize": 9}
    #                 }
    #                 self._api_call_with_retry(
    #                     worksheet.format, 
    #                     f"{feedback_col}{header_rows + 1}:{feedback_col}{last_row}", 
    #                     feedback_format,
    #                     operation_name="formatting feedback column with text wrapping"
    #                 )
            
    #         print(f"      ✅ Applied professional formatting with alternating rows and feedback text wrapping")
            
    #     except Exception as e:
    #         print(f"      ⚠️ Worksheet formatting failed: {e}")

    # def _apply_master_sheet_formatting(self, worksheet, data_rows_count: int, headers_count: int):
    #     """Apply formatting to master sheet with improved color schema"""
    #     try:
    #         # Basic formatting for master sheet
    #         last_row = 1 + data_rows_count
            
    #         # Set column widths with increased width for project count columns
    #         column_widths = [
    #             {"startIndex": 0, "endIndex": 1, "pixelSize": 150},   # User Name
    #             {"startIndex": 1, "endIndex": 2, "pixelSize": 200},   # Email
    #             {"startIndex": 2, "endIndex": 3, "pixelSize": 80},    # Role
    #             {"startIndex": 3, "endIndex": 4, "pixelSize": 200},   # Sheet Link
    #             {"startIndex": 4, "endIndex": 5, "pixelSize": 132},   # Last Time
    #             {"startIndex": 5, "endIndex": 6, "pixelSize": 130},   # Assigned Projects (increased from 100)
    #             {"startIndex": 6, "endIndex": 7, "pixelSize": 130},   # Started Projects (increased from 100)
    #         ]
            
    #         # Add remaining columns (like Completed Projects for Annotators)
    #         for i in range(7, headers_count):
    #             if i == 7:  # Completed Projects column
    #                 column_widths.append({
    #                     "startIndex": i, 
    #                     "endIndex": i + 1, 
    #                     "pixelSize": 140  # Even wider for "Completed Projects"
    #                 })
    #             else:
    #                 column_widths.append({
    #                     "startIndex": i, 
    #                     "endIndex": i + 1, 
    #                     "pixelSize": 100
    #                 })
            
    #         requests = []
    #         for width_spec in column_widths:
    #             requests.append({
    #                 "updateDimensionProperties": {
    #                     "range": {
    #                         "sheetId": worksheet._properties['sheetId'],
    #                         "dimension": "COLUMNS",
    #                         "startIndex": width_spec["startIndex"],
    #                         "endIndex": width_spec["endIndex"]
    #                     },
    #                     "properties": {
    #                         "pixelSize": width_spec["pixelSize"]
    #                     },
    #                     "fields": "pixelSize"
    #                 }
    #             })
            
    #         # Apply header formatting with dark blue background and white text
    #         requests.append({
    #             "repeatCell": {
    #                 "range": {
    #                     "sheetId": worksheet._properties['sheetId'],
    #                     "startRowIndex": 0,
    #                     "endRowIndex": 1,
    #                     "startColumnIndex": 0,
    #                     "endColumnIndex": headers_count
    #                 },
    #                 "cell": {
    #                     "userEnteredFormat": {
    #                         "backgroundColor": {
    #                             "red": 0.2,
    #                             "green": 0.4,
    #                             "blue": 0.8
    #                         },
    #                         "textFormat": {
    #                             "bold": True,
    #                             "fontSize": 10,
    #                             "foregroundColor": {
    #                                 "red": 1,
    #                                 "green": 1,
    #                                 "blue": 1
    #                             }
    #                         },
    #                         "horizontalAlignment": "CENTER",
    #                         "verticalAlignment": "MIDDLE"
    #                     }
    #                 },
    #                 "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
    #             }
    #         })
            
    #         if requests:
    #             batch_request = {"requests": requests}
    #             self._api_call_with_retry(
    #                 self.sheets_service.spreadsheets().batchUpdate,
    #                 spreadsheetId=worksheet.spreadsheet.id,
    #                 body=batch_request,
    #                 operation_name="applying master sheet formatting"
    #             ).execute()
            
    #         # NEW: Manual alternating row colors (like Script 2) 
    #         # if data_rows_count > 0:
    #         #     even_row_format = {
    #         #         "backgroundColor": {"red": 0.95, "green": 0.98, "blue": 1.0}  # Light blue
    #         #     }
                
    #         #     # Apply to even rows starting from row 3 (data starts at row 2, so even rows are 3, 5, 7...)
    #         #     for row in range(3, 1 + data_rows_count + 1, 2):  # Every other row starting from 3
    #         #         self._api_call_with_retry(
    #         #             worksheet.format, f"A{row}:ZZ{row}", even_row_format,
    #         #             operation_name=f"formatting master alternating row {row}"
    #         #         )
    #         # NEW: Batch alternating row colors for master sheet (much more efficient!)
    #         if data_rows_count > 0:
    #             master_alternating_requests = []
                
    #             # Build batch requests for alternating master sheet rows
    #             for row in range(3, 1 + data_rows_count + 1, 2):  # Odd rows: 3, 5, 7...
    #                 master_alternating_requests.append({
    #                     "repeatCell": {
    #                         "range": {
    #                             "sheetId": worksheet._properties['sheetId'],
    #                             "startRowIndex": row - 1,  # Convert to 0-indexed
    #                             "endRowIndex": row,
    #                             "startColumnIndex": 0,
    #                             "endColumnIndex": headers_count
    #                         },
    #                         "cell": {
    #                             "userEnteredFormat": {
    #                                 "backgroundColor": {
    #                                     "red": 0.95,
    #                                     "green": 0.98,
    #                                     "blue": 1.0
    #                                 }
    #                             }
    #                         },
    #                         "fields": "userEnteredFormat.backgroundColor"
    #                     }
    #                 })
                
    #             # Apply all master alternating row colors in a single batch request
    #             if master_alternating_requests:
    #                 master_alternating_batch_request = {"requests": master_alternating_requests}
    #                 self._api_call_with_retry(
    #                     self.sheets_service.spreadsheets().batchUpdate,
    #                     spreadsheetId=worksheet.spreadsheet.id,
    #                     body=master_alternating_batch_request,
    #                     operation_name="applying master alternating row colors (batch)"
    #                 ).execute()
            
    #         print(f"      ✅ Applied master sheet formatting with alternating rows")
            
    #     except Exception as e:
    #         print(f"      ⚠️ Master sheet formatting failed: {e}")

   
    # def _update_master_sheet_links(self, master_sheet_id: str, user_sheet_ids: Dict, sheet_prefix: str):
    #     """Update master sheet with smart chip links to user sheets"""
    #     try:
    #         sheet = self._api_call_with_retry(self.client.open_by_key, master_sheet_id,
    #                                         operation_name="opening master sheet for links")
    #     except:
    #         return

    #     # Update each tab with smart chip links
    #     for tab_name in ["Annotators", "Reviewers", "Meta-Reviewers"]:
    #         try:
    #             worksheet = sheet.worksheet(tab_name)
    #             all_data = worksheet.get_all_values()
                
    #             if len(all_data) < 2:
    #                 continue
                
    #             # Find the sheet link column (usually column 4: D)
    #             link_col_idx = 3  # Column D (0-indexed)
                
    #             # Process each user row
    #             for row_idx, row in enumerate(all_data[1:], start=2):  # Skip header
    #                 if len(row) > 0 and row[0]:  # Has user name
    #                     user_name = row[0]
    #                     role_name = tab_name[:-1]  # Remove 's' from tab name
    #                     sheet_key = f"{sheet_prefix}-{user_name} {role_name}"
                        
    #                     sheet_id = user_sheet_ids.get(sheet_key)
                        
    #                     if sheet_id:
    #                         sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
                            
    #                         # Create smart chip using chipRuns approach
    #                         try:
    #                             smart_chip_requests = [{
    #                                 "updateCells": {
    #                                     "rows": [{
    #                                         "values": [{
    #                                             "userEnteredValue": {
    #                                                 "stringValue": "@"  # Single @ placeholder
    #                                             },
    #                                             "chipRuns": [{
    #                                                 "startIndex": 0,  # @ is at position 0
    #                                                 "chip": {
    #                                                     "richLinkProperties": {
    #                                                         "uri": sheet_url
    #                                                     }
    #                                                 }
    #                                             }]
    #                                         }]
    #                                     }],
    #                                     "fields": "userEnteredValue,chipRuns",
    #                                     "range": {
    #                                         "sheetId": worksheet._properties['sheetId'],
    #                                         "startRowIndex": row_idx - 1,  # Convert to 0-indexed
    #                                         "startColumnIndex": link_col_idx,  # Column D (0-indexed)
    #                                         "endRowIndex": row_idx,
    #                                         "endColumnIndex": link_col_idx + 1
    #                                     }
    #                                 }
    #                             }]
                                
    #                             self._api_call_with_retry(
    #                                 self.sheets_service.spreadsheets().batchUpdate,
    #                                 spreadsheetId=sheet.id,
    #                                 body={"requests": smart_chip_requests},
    #                                 operation_name=f"updating smart chip for {user_name}"
    #                             ).execute()
                                
    #                         except Exception as e:
    #                             # Fallback to hyperlink formula if smart chip fails
    #                             print(f"        ⚠️ Smart chip failed for {user_name}, using hyperlink: {e}")
    #                             hyperlink_formula = f'=HYPERLINK("https://docs.google.com/spreadsheets/d/{sheet_id}/edit", "📊 {user_name} {role_name}")'
                                
    #                             # Update the cell
    #                             cell_address = f"{self._col_num_to_letter(link_col_idx + 1)}{row_idx}"
    #                             try:
    #                                 self._api_call_with_retry(
    #                                     worksheet.update,
    #                                     cell_address,
    #                                     [[hyperlink_formula]],
    #                                     value_input_option='USER_ENTERED',
    #                                     operation_name=f"updating hyperlink for {user_name}"
    #                                 )
    #                             except Exception as e2:
    #                                 print(f"        ⚠️ Could not update link for {user_name}: {e2}")
                
    #             print(f"      ✅ Updated smart chip links in {tab_name}")
                
    #         except Exception as e:
    #             print(f"      ⚠️ Could not update links for {tab_name}: {e}")

    def _update_master_sheet_links(self, master_sheet_id: str, user_sheet_ids: Dict, sheet_prefix: str):
        """Update master sheet with smart chip links to user sheets - BATCHED VERSION"""
        try:
            sheet = self._api_call_with_retry(self.client.open_by_key, master_sheet_id,
                                            operation_name="opening master sheet for links")
        except:
            return

        # Update each tab with smart chip links
        for tab_name in ["Annotators", "Reviewers", "Meta-Reviewers"]:
            try:
                worksheet = sheet.worksheet(tab_name)
                all_data = worksheet.get_all_values()
                
                if len(all_data) < 2:
                    continue
                
                # Find the sheet link column (usually column 4: D)
                link_col_idx = 3  # Column D (0-indexed)
                
                # BATCH: Collect all smart chip requests for this tab
                smart_chip_requests = []
                fallback_updates = []  # For hyperlink fallbacks
                
                # Process each user row
                for row_idx, row in enumerate(all_data[1:], start=2):  # Skip header
                    if len(row) > 0 and row[0]:  # Has user name
                        user_name = row[0]
                        role_name = tab_name[:-1]  # Remove 's' from tab name
                        sheet_key = f"{sheet_prefix}-{user_name} {role_name}"
                        
                        sheet_id = user_sheet_ids.get(sheet_key)
                        
                        if sheet_id:
                            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
                            
                            # Add to batch request
                            smart_chip_requests.append({
                                "updateCells": {
                                    "rows": [{
                                        "values": [{
                                            "userEnteredValue": {
                                                "stringValue": "@"
                                            },
                                            "chipRuns": [{
                                                "startIndex": 0,
                                                "chip": {
                                                    "richLinkProperties": {
                                                        "uri": sheet_url
                                                    }
                                                }
                                            }]
                                        }]
                                    }],
                                    "fields": "userEnteredValue,chipRuns",
                                    "range": {
                                        "sheetId": worksheet._properties['sheetId'],
                                        "startRowIndex": row_idx - 1,
                                        "startColumnIndex": link_col_idx,
                                        "endRowIndex": row_idx,
                                        "endColumnIndex": link_col_idx + 1
                                    }
                                }
                            })
                
                # Execute batch in chunks to avoid rate limits (max ~50 per batch)
                BATCH_SIZE = 50
                for i in range(0, len(smart_chip_requests), BATCH_SIZE):
                    batch_chunk = smart_chip_requests[i:i + BATCH_SIZE]
                    try:
                        self._api_call_with_retry(
                            self.sheets_service.spreadsheets().batchUpdate,
                            spreadsheetId=sheet.id,
                            body={"requests": batch_chunk},
                            operation_name=f"batch updating {len(batch_chunk)} smart chips for {tab_name}"
                        ).execute()
                        print(f"        ✅ Updated {len(batch_chunk)} links in batch")
                    except Exception as e:
                        print(f"        ⚠️ Batch smart chip failed, falling back to hyperlinks: {str(e)[:100]}")
                        # Fallback: use hyperlink formulas in a single batch update
                        self._fallback_hyperlink_batch(worksheet, all_data, user_sheet_ids, 
                                                    sheet_prefix, tab_name, link_col_idx,
                                                    i, min(i + BATCH_SIZE, len(smart_chip_requests)))
                    
                    # Small delay between batches
                    if i + BATCH_SIZE < len(smart_chip_requests):
                        time.sleep(2)
                
                print(f"      ✅ Updated smart chip links in {tab_name}")
                
            except Exception as e:
                print(f"      ⚠️ Could not update links for {tab_name}: {e}")

    def _fallback_hyperlink_batch(self, worksheet, all_data, user_sheet_ids, sheet_prefix, tab_name, link_col_idx, start_idx, end_idx):
        """Batch update hyperlinks as fallback"""
        role_name = tab_name[:-1]
        updates = []
        
        row_offset = 2  # Data starts at row 2 (1-indexed)
        for idx in range(start_idx, end_idx):
            row_idx = row_offset + idx
            if row_idx - 1 < len(all_data):
                row = all_data[row_idx - 1]
                if len(row) > 0 and row[0]:
                    user_name = row[0]
                    sheet_key = f"{sheet_prefix}-{user_name} {role_name}"
                    sheet_id = user_sheet_ids.get(sheet_key)
                    if sheet_id:
                        cell_address = f"{self._col_num_to_letter(link_col_idx + 1)}{row_idx}"
                        hyperlink_formula = f'=HYPERLINK("https://docs.google.com/spreadsheets/d/{sheet_id}/edit", "📊 {user_name} {role_name}")'
                        updates.append({
                            'range': cell_address,
                            'values': [[hyperlink_formula]]
                        })
        
        if updates:
            try:
                self._api_call_with_retry(
                    worksheet.batch_update,
                    updates,
                    value_input_option='USER_ENTERED',
                    operation_name=f"batch hyperlink fallback for {len(updates)} cells"
                )
            except Exception as e:
                print(f"        ⚠️ Fallback hyperlink batch also failed: {e}")
    # def _manage_sheet_permissions(self, sheet_id: str, sheet_name: str):
    #     """Manage sheet permissions based on database admin status"""
    #     try:
    #         print(f"      🔐 Managing permissions for {sheet_name}...")
            
    #         # Get admin users from database using service layer
    #         with self.get_db_session() as session:
    #             all_users_df = self.AuthService.get_all_users(session=session)
    #             admin_users_df = all_users_df[
    #                 (all_users_df["Role"] == "admin") & 
    #                 (all_users_df["Archived"] == False) &
    #                 (all_users_df["Email"].notna()) &
    #                 (all_users_df["Email"] != "")
    #             ]
            
    #         admin_emails = admin_users_df["Email"].tolist()
            
    #         # Set permissions: editors for admins, viewers for others
    #         for email in admin_emails:
    #             try:
    #                 self._api_call_with_retry(
    #                     self.drive_service.permissions().create,
    #                     fileId=sheet_id,
    #                     body={
    #                         'type': 'user',
    #                         'role': 'writer',
    #                         'emailAddress': email
    #                     },
    #                     operation_name=f"granting editor access to {email}"
    #                 ).execute()
    #             except Exception as e:
    #                 print(f"        ⚠️  Could not update permission for {email}: {e}")
            
    #         print(f"      ✅ Permissions managed for {sheet_name}")
            
    #     except Exception as e:
    #         print(f"      ⚠️  Could not manage permissions for {sheet_name}: {e}")

    def _manage_sheet_permissions(self, sheet_id: str, sheet_name: str):
        """Manage sheet permissions - only update when necessary to avoid unnecessary emails"""
        try:
            print(f"      🔐 Managing permissions for {sheet_name}...")
            
            # Get admin users from database using service layer
            with self.get_db_session() as session:
                all_users_df = self.AuthService.get_all_users(session=session)
                admin_users_df = all_users_df[
                    (all_users_df["Role"] == "admin") & 
                    (all_users_df["Archived"] == False) &
                    (all_users_df["Email"].notna()) &
                    (all_users_df["Email"] != "")
                ]
            
            admin_emails = set(admin_users_df["Email"].tolist())
            
            # STEP 1: Get current permissions first
            try:
                current_permissions = self._api_call_with_retry(
                    lambda: self.drive_service.permissions().list(
                        fileId=sheet_id,
                        fields="permissions(id,role,type,emailAddress,displayName)"
                    ).execute(),
                    operation_name=f"listing permissions for {sheet_name}"
                )
            except Exception as e:
                print(f"        ⚠️ Could not list permissions: {e}")
                return
            
            # STEP 2: Analyze current permissions
            current_editors = set()
            permissions_to_update = []
            permissions_found = current_permissions.get('permissions', [])
            
            print(f"        📋 Found {len(permissions_found)} existing permissions")
            
            for permission in permissions_found:
                email = permission.get('emailAddress')
                role = permission.get('role')
                perm_id = permission.get('id')
                perm_type = permission.get('type')
                
                # Skip non-user permissions or those without email
                if perm_type != 'user' or not email:
                    continue
                
                # Check if this user should be an admin
                if email in admin_emails:
                    if role == 'owner':
                        # Owner has higher permissions than writer - that's fine
                        print(f"        👑 {email} is owner (has full access)")
                        current_editors.add(email)
                    elif role == 'writer':
                        # Already has correct permission
                        print(f"        ✅ {email} already has writer access")
                        current_editors.add(email)
                    else:
                        # Needs to be updated to writer
                        permissions_to_update.append((perm_id, 'writer', email))
                        print(f"        📝 Will update {email} from {role} to writer")
                # Note: We're not managing non-admin permissions in this version
                # to keep it simple and focused on avoiding duplicate emails
            
            # STEP 3: Add missing admin permissions (only if they don't exist)
            emails_to_add = []
            for email in admin_emails:
                if email not in current_editors:
                    emails_to_add.append(email)
                    print(f"        ➕ Will add writer access for {email}")
                else:
                    print(f"        ⏭️ Skipping {email} - already has appropriate access")
            
            # STEP 4: Execute permission changes (only when necessary)
            changes_made = 0
            
            # Add new permissions
            for email in emails_to_add:
                try:
                    self._api_call_with_retry(
                        lambda e=email: self.drive_service.permissions().create(
                            fileId=sheet_id,
                            body={
                                'type': 'user',
                                'role': 'writer',
                                'emailAddress': e
                            }
                        ).execute(),
                        operation_name=f"adding writer permission for {email}"
                    )
                    print(f"        ✅ Added writer access for {email}")
                    changes_made += 1
                except Exception as e:
                    error_msg = str(e).lower()
                    # Check if error is because permission already exists
                    if any(phrase in error_msg for phrase in [
                        'already exists', 'duplicate', 'already has access', 
                        'already a collaborator', 'permission already granted'
                    ]):
                        print(f"        ⏭️ {email} already has access (confirmed by API)")
                    else:
                        print(f"        ⚠️ Could not add writer access for {email}: {e}")
            
            # Update existing permissions
            for perm_id, new_role, email in permissions_to_update:
                try:
                    self._api_call_with_retry(
                        lambda pid=perm_id, role=new_role: self.drive_service.permissions().update(
                            fileId=sheet_id,
                            permissionId=pid,
                            body={'role': role}
                        ).execute(),
                        operation_name=f"updating permission for {email} to {new_role}"
                    )
                    print(f"        ✅ Updated {email} to writer access")
                    changes_made += 1
                except Exception as e:
                    print(f"        ⚠️ Could not update permission for {email}: {e}")
            
            # STEP 5: Summary
            if changes_made == 0:
                print(f"        ✅ All permissions already correct - no emails sent")
            else:
                print(f"        ✅ Made {changes_made} permission changes - emails sent only for actual changes")
            
            print(f"      ✅ Permissions managed for {sheet_name}")
            
        except Exception as e:
            print(f"      ⚠️ Could not manage permissions for {sheet_name}: {e}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Export annotation statistics to Google Sheets")
    parser.add_argument("--master-sheet-id", type=str, required=True,
                       help="Google Sheet ID for the master sheet (from the URL)")
    parser.add_argument("--database-url-name", type=str, default="DBURL",
                       help="Environment variable name for database URL")
    parser.add_argument("--credentials-file", type=str, default="credentials.json",
                       help="Path to Google OAuth credentials file")
    parser.add_argument("--skip-individual", action="store_true",
                       help="Skip individual user sheets and only update master sheet")
    parser.add_argument("--resume-from", type=str,
                       help="Resume from specific user (format: 'User Name Role')")
    parser.add_argument("--sheet-prefix", type=str, default="Pizza",
                       help="Prefix for all individual sheet names (default: 'Pizza')")
    
    args = parser.parse_args()
    
    print("="*60)
    print("GOOGLE SHEETS EXPORT SETUP")
    print("="*60)
    print(f"Database URL Variable: {args.database_url_name}")
    print(f"Master Sheet ID: {args.master_sheet_id}")
    print(f"Master Sheet URL: https://docs.google.com/spreadsheets/d/{args.master_sheet_id}/edit")
    if args.skip_individual:
        print("Mode: Master sheet only (skipping individual user sheets)")
    elif args.resume_from:
        print(f"Mode: Resume from user '{args.resume_from}'")
    else:
        print("Mode: Full export (master sheet + individual user sheets)")
    print("="*60)
    
    # Verify credentials file exists
    credentials_path = Path(args.credentials_file)
    if not credentials_path.exists():
        print(f"❌ Error: Credentials file not found: {credentials_path}")
        print(f"💡 Please download your Google OAuth credentials and save as '{args.credentials_file}'")
        return
    
    # Create exporter and run
    exporter = GoogleSheetExporter(str(credentials_path), args.database_url_name)
    
    exporter.export_all_sheets(
        master_sheet_id=args.master_sheet_id,
        skip_individual=args.skip_individual,
        resume_from=args.resume_from,
        sheet_prefix=args.sheet_prefix
    )
    
    print("\n" + "="*60)
    print("📋 DATA PRESERVATION SUMMARY")
    print("="*60)
    print("✅ PRESERVED (Never Overwritten):")
    print("   • Payment Time")
    print("   • Base Salary")
    print("   • Bonus Salary")
    print("   • Feedback to User")
    print("\n🔄 AUTOMATICALLY UPDATED:")
    print("   • Project Names and Schema Names")
    print("   • Completion and Review Ratios")
    print("   • Last Activity Timestamps")
    print("   • All statistics (accuracy, counts, etc.)")
    print("\n🔐 PERMISSION MANAGEMENT:")
    print("   • Editor access: Database admin users only")
    print("   • Others: View-only access")
    print("   • Applied to master sheet and all individual user sheets")
    print("="*60)


if __name__ == "__main__":
    main()