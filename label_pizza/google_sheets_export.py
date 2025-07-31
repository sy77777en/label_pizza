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
- Fixed meta-reviewer column ordering
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
        
        # Initialize services
        from label_pizza.services import GoogleSheetsExportService
        self.GoogleSheetsExportService = GoogleSheetsExportService()
        
        # Setup Google authentication
        self._setup_google_auth(credentials_file)
        
        # Track any failures during export
        self.export_failures = []
    
    def _setup_google_auth(self, credentials_file: str):
        """Setup Google authentication using OAuth 2.0"""
        creds = None
        token_file = 'google_sheets_token.json'
        
        # Load existing token if available
        if os.path.exists(token_file):
            try:
                creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
                if not creds.has_scopes(self.SCOPES):
                    print("🔄 Token has insufficient scopes, deleting...")
                    os.remove(token_file)
                    creds = None
            except:
                print("🔄 Invalid token file, deleting...")
                os.remove(token_file)
                creds = None
        
        # If there are no (valid) credentials available, get authorization
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("🔄 Refreshing expired credentials...")
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"❌ Failed to refresh credentials: {e}")
                    print("🔄 Will require re-authorization...")
                    creds = None
            
            if not creds:
                # Manual OAuth flow
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, self.SCOPES, redirect_uri='http://localhost:8080')
                
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                print('='*60)
                print('GOOGLE SHEETS AUTHORIZATION REQUIRED')
                print('='*60)
                print('Required permissions:')
                print('  ✅ Google Sheets: Read, write, and manage spreadsheets')
                print('  ✅ Google Drive: Create and manage files')
                print('='*60)
                print('1. Go to this URL in your browser:')
                print(auth_url)
                print('\n2. Authorize the application')
                print('3. Copy the authorization code from the URL after authorization')
                print('   (The browser will show "This site can\'t be reached" - this is expected!)')
                print('='*60)
                
                auth_code = input('\nEnter the authorization code: ').strip()
                if not auth_code:
                    raise Exception('No authorization code provided')
                
                flow.fetch_token(code=auth_code)
                creds = flow.credentials
            
            # Save the credentials for the next run
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
                print(f"✅ Credentials saved to {token_file}")
        
        # Verify we have the correct scopes
        if not creds.has_scopes(self.SCOPES):
            available_scopes = getattr(creds, 'scopes', ['unknown'])
            print(f"❌ Authentication missing required scopes!")
            print(f"   Required: {self.SCOPES}")
            print(f"   Available: {available_scopes}")
            raise Exception(f"Authentication missing required scopes: {self.SCOPES}")
        
        # Authorize gspread client
        self.client = gspread.authorize(creds)
        
        # Create Google Sheets and Drive services for advanced operations
        self.sheets_service = build('sheets', 'v4', credentials=creds)
        self.drive_service = build('drive', 'v3', credentials=creds)
        
        print("✅ Google Sheets client authorized with full permissions")
    
    def get_db_session(self):
        """Get database session"""
        from label_pizza.database_utils import get_db_session
        return get_db_session()
    
    def _api_call_with_retry(self, func, *args, max_retries=5, **kwargs):
        """Execute API call with rate limiting and retry logic"""
        operation_name = kwargs.pop('operation_name', func.__name__)
        
        for attempt in range(max_retries):
            try:
                # Add delay between API calls to avoid rate limits
                if attempt > 0:
                    delay = min(2 ** attempt * 5, 60)  # Start at 10s, max 60s
                    print(f"      ⏳ Waiting {delay}s before retry {attempt + 1}/{max_retries} for {operation_name}...")
                    time.sleep(delay)
                else:
                    time.sleep(1.5)  # Small delay between all API calls
                
                result = func(*args, **kwargs)
                if attempt > 0:
                    print(f"      ✅ {operation_name} succeeded after {attempt + 1} attempts")
                return result
                
            except Exception as e:
                error_str = str(e).lower()
                if 'quota' in error_str or 'rate' in error_str or 'limit' in error_str or '429' in error_str:
                    if attempt == max_retries - 1:
                        failure_msg = f"RATE LIMIT FAILURE: {operation_name}"
                        print(f"      ❌ {failure_msg}")
                        self.export_failures.append(failure_msg)
                        raise Exception(f"Rate limit failure for {operation_name} after {max_retries} attempts")
                    print(f"      ⚠️  Rate limit detected in {operation_name}: {e}")
                    continue
                else:
                    # Not a rate limit error, re-raise immediately
                    failure_msg = f"UNEXPECTED ERROR: {operation_name} - {str(e)}"
                    print(f"      ❌ {failure_msg}")
                    self.export_failures.append(failure_msg)
                    raise
        
        raise Exception(f"{operation_name} failed after {max_retries} attempts")
    
    def _manage_sheet_permissions(self, sheet_id: str, sheet_name: str):
        """Manage permissions for a sheet based on database admin status"""
        print(f"      🔐 Managing permissions for {sheet_name}...")
        
        try:
            with self.get_db_session() as session:
                # Get admin users from database
                admin_users = self.GoogleSheetsExportService.get_admin_users(session)
                admin_emails = [user['email'] for user in admin_users if user['email']]
            
            # Get current permissions
            current_permissions = self._api_call_with_retry(
                lambda: self.drive_service.permissions().list(fileId=sheet_id).execute(),
                operation_name=f"listing permissions for {sheet_name}"
            )
            
            # Track current access
            current_editors = set()
            current_viewers = set()
            permissions_to_update = []
            
            # Analyze current permissions
            for permission in current_permissions.get('permissions', []):
                email = permission.get('emailAddress')
                role = permission.get('role')
                perm_id = permission.get('id')
                perm_type = permission.get('type')
                
                # Skip owner permissions and non-user permissions
                if role == 'owner' or perm_type != 'user' or not email:
                    continue
                
                if email in admin_emails:
                    if role != 'writer':
                        permissions_to_update.append((perm_id, 'writer', email))
                    current_editors.add(email)
                else:
                    if role == 'writer':
                        permissions_to_update.append((perm_id, 'reader', email))
                    current_viewers.add(email)
            
            # Add missing admin permissions
            for email in admin_emails:
                if email not in current_editors and email not in current_viewers:
                    try:
                        self._api_call_with_retry(
                            lambda: self.drive_service.permissions().create(
                                fileId=sheet_id,
                                body={
                                    'type': 'user',
                                    'role': 'writer', 
                                    'emailAddress': email
                                }
                            ).execute(),
                            operation_name=f"adding admin access for {email}"
                        )
                        print(f"        ✅ Added admin access for {email}")
                    except Exception as e:
                        print(f"        ⚠️  Could not add admin access for {email}: {e}")
            
            # Update existing permissions
            for perm_id, new_role, email in permissions_to_update:
                try:
                    self._api_call_with_retry(
                        lambda: self.drive_service.permissions().update(
                            fileId=sheet_id,
                            permissionId=perm_id,
                            body={'role': new_role}
                        ).execute(),
                        operation_name=f"updating permission for {email} to {new_role}"
                    )
                    action = "admin" if new_role == 'writer' else "viewer"
                    print(f"        ✅ Updated {email} to {action} access")
                except Exception as e:
                    print(f"        ⚠️  Could not update permission for {email}: {e}")
            
            print(f"      ✅ Permissions managed for {sheet_name}")
            
        except Exception as e:
            print(f"      ⚠️  Could not manage permissions for {sheet_name}: {e}")
    
    def export_all_sheets(self, master_sheet_id: str, skip_individual: bool = False, resume_from: str = None):
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
            # Get all users with activity
            active_users = []
            
            # Get annotators with activity
            annotators = self.GoogleSheetsExportService.get_all_user_data(session, "Annotator")
            for user in annotators:
                if self._has_activity(user):
                    active_users.append((user, "Annotator"))
            
            # Get reviewers with activity
            reviewers = self.GoogleSheetsExportService.get_all_user_data(session, "Reviewer")
            for user in reviewers:
                if self._has_activity(user):
                    active_users.append((user, "Reviewer"))
            
            # Get meta-reviewers with activity
            meta_reviewers = self.GoogleSheetsExportService.get_all_user_data(session, "Meta-Reviewer")
            for user in meta_reviewers:
                if self._has_activity(user):
                    active_users.append((user, "Meta-Reviewer"))
        
        total_users = len(active_users)
        current_user = 0
        user_sheet_ids = {}
        
        print(f"\nExporting individual user sheets ({total_users} total)...")
        print("⚠️  Note: Processing slowly to avoid Google Sheets rate limits...")
        
        # Determine where to start based on resume_from parameter
        skip_until_found = resume_from is not None
        
        for user_data, role in active_users:
            current_user += 1
            sheet_key = f"{user_data['user_name']} {role}"
            
            # Skip users until we reach the resume point
            if skip_until_found:
                if sheet_key == resume_from:
                    skip_until_found = False
                    print(f"🔄 Resuming from: {sheet_key}")
                else:
                    print(f"⏭️  Skipping {sheet_key} (resuming from {resume_from})")
                    continue
            
            print(f"\n[{current_user}/{total_users}] Processing {user_data['user_name']} {role}...")
            
            # Add delay between users
            if current_user > 1:
                print("    ⏳ Pausing 3 seconds between users...")
                time.sleep(3)
            
            try:
                sheet_id = self._export_user_sheet(user_data, role, master_sheet_id)
                user_sheet_ids[sheet_key] = sheet_id
                
                # Manage permissions for this sheet
                self._manage_sheet_permissions(sheet_id, f"{user_data['user_name']} {role}")
                
                print(f"    ✅ Successfully exported {user_data['user_name']} {role}")
            except Exception as e:
                print(f"    ❌ Failed to export {user_data['user_name']} {role}: {e}")
                self.export_failures.append(f"Failed to export {user_data['user_name']} {role}: {str(e)}")
        
        # Update master sheet with smart chip links
        if user_sheet_ids:
            print(f"\n⏳ Pausing 5 seconds before updating master sheet links...")
            time.sleep(5)
            print("Updating master sheet smart chip links...")
            try:
                self._update_master_sheet_smart_chip_links(master_sheet_id, user_sheet_ids)
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
            print(f"\n📊 Master Sheet: https://docs.google.com/spreadsheets/d/{master_sheet_id}/edit")
            print("="*60)
        else:
            print("EXPORT COMPLETED SUCCESSFULLY!")
            print("="*60)
            print(f"📊 Master Sheet: https://docs.google.com/spreadsheets/d/{master_sheet_id}/edit")
            print("="*60)
    
    def _has_activity(self, user_data: Dict) -> bool:
        """Check if user has any activity"""
        return (user_data.get('assigned_projects', 0) > 0 or 
                user_data.get('started_projects', 0) > 0 or
                user_data.get('completed_projects', 0) > 0)
    
    def _export_master_sheet(self, sheet_id: str):
        """Export the master sheet with all three tabs"""
        try:
            sheet = self._api_call_with_retry(self.client.open_by_key, sheet_id, 
                                            operation_name="opening master sheet")
        except Exception as e:
            print(f"❌ Error: Could not open master sheet with ID {sheet_id}")
            print(f"   Details: {str(e)}")
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
        
        # Create headers based on role
        if role == "Annotator":
            headers = [
                "User Name", "Email", "Role", "Annotation Sheet", "Last Annotated Time",
                "Assigned Projects", "Started Projects", "Completed Projects"
            ]
        elif role == "Reviewer":
            headers = [
                "User Name", "Email", "Role", "Review Sheet", "Last Review Time",
                "Assigned Projects", "Started Projects"
            ]
        else:  # Meta-Reviewer
            headers = [
                "User Name", "Email", "Role", "Meta-Review Sheet", "Last Modified Time",
                "Assigned Projects", "Started Projects"
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
                f"Link to {user['user_name']} {role}",  # Placeholder for smart chip
                self._format_timestamp(user.get('last_activity')),
                user.get('assigned_projects', 0),
                user.get('started_projects', 0)
            ]
            
            # Add completed projects column for annotators
            if role == "Annotator":
                row.append(user.get('completed_projects', 0))
            
            rows.append(row)
        
        # Update the worksheet
        self._api_call_with_retry(worksheet.clear, operation_name="clearing master sheet")
        if rows:
            end_col = self._col_num_to_letter(len(headers))
            self._api_call_with_retry(worksheet.update, 
                                    values=rows, 
                                    range_name=f'A1:{end_col}{len(rows)}',
                                    operation_name=f"updating {len(rows)-1} user records")
            
            # Apply master sheet formatting
            self._apply_master_sheet_formatting(worksheet, len(rows)-1, len(headers))
            
            print(f"    ✅ Successfully updated {len(rows)-1} users in {tab_name} tab")
    
    def _apply_master_sheet_formatting(self, worksheet, num_users: int, num_cols: int):
        """Apply consistent formatting to master sheet"""
        try:
            # Header formatting with consistent colors
            header_format = {
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.98},  # Light blue matching individual sheets
                "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }
            
            # Apply header formatting
            end_col = self._col_num_to_letter(num_cols)
            self._api_call_with_retry(
                worksheet.format, f"A1:{end_col}1", header_format,
                operation_name="formatting master sheet headers"
            )
            
            # Data row formatting (alternating colors)
            if num_users > 0:
                even_row_format = {
                    "backgroundColor": {"red": 0.95, "green": 0.98, "blue": 1.0}  # Very light blue
                }
                
                for row in range(3, 2 + num_users + 1, 2):  # Every other row starting from 3
                    self._api_call_with_retry(
                        worksheet.format, f"A{row}:{end_col}{row}", even_row_format,
                        operation_name=f"formatting master row {row}"
                    )
            
            # Apply column widths
            column_widths = [
                {"sheetId": worksheet.id, "startIndex": 0, "endIndex": 1, "pixelSize": 120},   # User Name
                {"sheetId": worksheet.id, "startIndex": 1, "endIndex": 2, "pixelSize": 180},   # Email
                {"sheetId": worksheet.id, "startIndex": 2, "endIndex": 3, "pixelSize": 80},    # Role
                {"sheetId": worksheet.id, "startIndex": 3, "endIndex": 4, "pixelSize": 120},   # Sheet Link
                {"sheetId": worksheet.id, "startIndex": 4, "endIndex": 5, "pixelSize": 150},   # Last Activity
                {"sheetId": worksheet.id, "startIndex": 5, "endIndex": 6, "pixelSize": 100},   # Assigned
                {"sheetId": worksheet.id, "startIndex": 6, "endIndex": 7, "pixelSize": 100},   # Started
            ]
            
            # Add completed projects column for annotators
            if num_cols > 7:
                column_widths.append({
                    "sheetId": worksheet.id, "startIndex": 7, "endIndex": 8, "pixelSize": 100  # Completed
                })
            
            # Apply column widths using batch update
            requests = []
            for width_spec in column_widths:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": width_spec["sheetId"],
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
            
            if requests:
                self._api_call_with_retry(
                    worksheet.spreadsheet.batch_update,
                    {"requests": requests},
                    operation_name="updating master sheet column widths"
                )
            
            print(f"    ✅ Applied master sheet formatting")
            
        except Exception as e:
            print(f"    ⚠️  Master sheet formatting failed: {e}")
    
    def _update_master_sheet_smart_chip_links(self, master_sheet_id: str, user_sheet_ids: Dict):
        """Update master sheet with smart chip links to user sheets"""
        try:
            sheet = self._api_call_with_retry(self.client.open_by_key, master_sheet_id,
                                            operation_name="opening master sheet for smart chip links")
        except:
            return
        
        # Update each tab with smart chip links
        for tab_name in ["Annotators", "Reviewers", "Meta-Reviewers"]:
            try:
                worksheet = sheet.worksheet(tab_name)
                all_data = worksheet.get_all_values()
                
                if len(all_data) < 2:
                    continue
                
                # Find the sheet link column (column D, index 3)
                link_col_idx = 3
                
                # Update each user's sheet link with smart chip
                for row_idx, row in enumerate(all_data[1:], start=2):  # Skip header
                    if len(row) > 0 and row[0]:  # Has user name
                        user_name = row[0]
                        role_name = tab_name[:-1]  # Remove 's' from tab name
                        sheet_key = f"{user_name} {role_name}"
                        
                        if sheet_key in user_sheet_ids:
                            sheet_id = user_sheet_ids[sheet_key]
                            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
                            
                            # Create smart chip using the advanced Sheets API
                            requests = [{
                                "updateCells": {
                                    "rows": [{
                                        "values": [{
                                            "userEnteredValue": {
                                                "stringValue": "@"  # Smart chip placeholder
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
                                        "sheetId": worksheet.id,
                                        "startRowIndex": row_idx - 1,  # Convert to 0-indexed
                                        "startColumnIndex": link_col_idx,
                                        "endRowIndex": row_idx,
                                        "endColumnIndex": link_col_idx + 1
                                    }
                                }
                            }]
                            
                            self._api_call_with_retry(
                                lambda: self.sheets_service.spreadsheets().batchUpdate(
                                    spreadsheetId=worksheet.spreadsheet.id,
                                    body={"requests": requests}
                                ).execute(),
                                operation_name=f"updating smart chip for {user_name}"
                            )
                        else:
                            # Fallback to text if no sheet ID
                            self._api_call_with_retry(
                                worksheet.update, 
                                values=[["Sheet not found"]], 
                                range_name=f'{self._col_num_to_letter(link_col_idx + 1)}{row_idx}',
                                operation_name=f"updating fallback text for {user_name}"
                            )
                
                print(f"    ✅ Updated {tab_name} tab smart chip links")
            except Exception as e:
                print(f"    ❌ Failed to update {tab_name} tab links: {e}")
    
    def _export_user_sheet(self, user_data: Dict, role: str, master_sheet_id: str) -> str:
        """Export individual user sheet and return sheet ID"""
        sheet_name = f"{user_data['user_name']} {role}"
        print(f"  Exporting {sheet_name} sheet...")
        
        try:
            # Try to open existing sheet
            sheet = self.client.open(sheet_name)
            print(f"    Found existing sheet: {sheet_name}")
        except (gspread.exceptions.SpreadsheetNotFound, Exception):
            # Create new sheet
            print(f"    Creating new sheet: {sheet_name}")
            sheet = self._api_call_with_retry(self.client.create, sheet_name, 
                                            operation_name=f"creating sheet '{sheet_name}'")
        
        # Export Payment tab
        self._export_user_tab(sheet, "Payment", user_data, role, include_payment=True)
        
        # Export Feedback tab
        self._export_user_tab(sheet, "Feedback", user_data, role, include_payment=False)
        
        return sheet.id
    
    def _export_user_tab(self, sheet, tab_name: str, user_data: Dict, role: str, include_payment: bool):
        """Export a single tab in a user sheet"""
        print(f"    Exporting {tab_name} tab...")
        
        try:
            worksheet = sheet.worksheet(tab_name)
            print(f"      Found existing {tab_name} tab")
        except gspread.exceptions.WorksheetNotFound:
            print(f"      Creating new {tab_name} tab...")
            worksheet = self._api_call_with_retry(sheet.add_worksheet, title=tab_name, rows=100, cols=50, 
                                                operation_name=f"creating {tab_name} worksheet")
        
        # Get existing manual data to preserve it
        existing_data = self._read_existing_manual_data(worksheet, role, include_payment)
        
        # Clear and create headers
        self._clear_header_area(worksheet)
        self._create_user_sheet_headers(worksheet, role, include_payment)
        
        # Get project data for this user
        with self.get_db_session() as session:
            project_data = self.GoogleSheetsExportService.get_user_project_data(session, user_data['user_id'], role)
        
        # Prepare data rows with manual data preservation
        data_rows = []
        manual_col_indices = self._get_manual_column_indices(role, include_payment)
        
        for project in project_data:
            # Create automatic data row
            auto_row = self._create_project_data_row(project, role, include_payment)
            
            # Merge with preserved manual data
            preserved_row = self._merge_with_manual_data(auto_row, project['project_name'], existing_data, manual_col_indices)
            data_rows.append(preserved_row)
        
        # Update data rows
        if data_rows:
            self._update_data_with_preservation(worksheet, data_rows, 3)  # Start from row 3
            print(f"      ✅ Successfully updated {len(data_rows)} projects with preserved manual data")
        else:
            print(f"      ℹ️  No data to update in {tab_name} tab")
        
        # Apply formatting
        self._apply_user_sheet_formatting(worksheet, len(data_rows), role, include_payment)
    
    def _clear_header_area(self, worksheet):
        """Clear and unmerge the header area"""
        print(f"      🧹 Clearing header area...")
        try:
            # Unmerge all cells in header area (rows 1-2)
            requests = [{
                "unmergeCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 2,
                        "startColumnIndex": 0,
                        "endColumnIndex": 50
                    }
                }
            }]
            
            try:
                self._api_call_with_retry(
                    lambda: self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body={"requests": requests}
                    ).execute(),
                    operation_name="unmerging header area"
                )
            except Exception as e:
                # Ignore errors about no merged cells to unmerge
                if "no merged cells" not in str(e).lower():
                    print(f"      ⚠️  Could not unmerge header area: {e}")
            
            # Clear the header area
            self._api_call_with_retry(worksheet.batch_clear, ['A1:ZZ2'], 
                                    operation_name="clearing header area")
            print(f"      ✅ Header area cleared successfully")
            
        except Exception as e:
            print(f"      ⚠️  Could not clear header area: {e}")
    
    def _create_user_sheet_headers(self, worksheet, role: str, include_payment: bool):
        """Create properly structured headers for user sheets with FIXED meta-reviewer structure"""
        print(f"      Creating {role} headers...")
        
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
            row2.extend(["Accuracy%", "Completion%", "Reviewed%", "Completed", "Reviewed", "Wrong"])
            
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
            
        else:  # Meta-Reviewer - FIXED STRUCTURE
            # FIXED: Move "Modified Ratio By" to the end, like other roles
            row1 = ["Project Name", "Schema Name", "Video Count", "Last Modified"]
            if include_payment:
                row1.extend(["Payment Time", "Base Salary", "Bonus Salary"])
            else:
                row1.append("Feedback")
            # FIXED: Add "Modified Ratio By" at the end, spanning 2 columns
            row1.extend(["Modified Ratio By", ""])
            
            # Row 2 has sub-headers for Modified Ratio By at the end
            row2 = ["", "", "", ""]  # Empty for first 4 columns (Project, Schema, Video Count, Last Modified)
            if include_payment:
                row2.extend(["", "", ""])  # Empty for payment columns
            else:
                row2.append("")  # Empty for feedback column
            # FIXED: Add sub-headers at the end
            row2.extend(["User %", "All %"])
        
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
    
    def _apply_header_merging(self, worksheet, row1: List[str], row2: List[str]):
        """Apply proper header merging for spans and single cells"""
        try:
            sheet_id = worksheet.id
            
            # First, ensure no existing merges in header area
            try:
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
                
                self._api_call_with_retry(
                    lambda: self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body={'requests': unmerge_requests}
                    ).execute(),
                    operation_name="unmerging entire header area"
                )
            except Exception:
                pass  # Ignore if no merges to unmerge
            
            time.sleep(0.5)  # Small delay
            
            # Apply new merges
            merge_requests = []
            
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
                self._api_call_with_retry(
                    lambda: self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body={'requests': merge_requests}
                    ).execute(),
                    operation_name="merging header cells"
                )
                
                print(f"      ✅ Applied {len(merge_requests)} header merges successfully")
                
        except Exception as e:
            print(f"      ⚠️ Could not apply header merging: {e}")
    
    def _apply_header_formatting(self, worksheet, num_columns: int):
        """Apply consistent color formatting and styling to headers"""
        try:
            sheet_id = worksheet.id
            
            # Apply header background color and text formatting - CONSISTENT COLORS
            header_format_requests = [{
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
                                "blue": 0.98  # Same light blue as master sheet
                            },
                            "textFormat": {
                                "bold": True,
                                "fontSize": 10,
                                "foregroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}  # Dark gray text
                            },
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE"
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
                }
            }]
            
            # Apply formatting
            if header_format_requests:
                self._api_call_with_retry(
                    lambda: self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body={"requests": header_format_requests}
                    ).execute(),
                    operation_name="applying header formatting"
                )
                
        except Exception as e:
            print(f"      ⚠️ Could not apply header formatting: {e}")
    
    def _read_existing_manual_data(self, worksheet, role: str, include_payment: bool) -> Dict:
        """Read existing manual data to preserve it during updates"""
        try:
            all_data = worksheet.get_all_values()
            if len(all_data) < 3:  # No data rows
                return {}
            
            manual_col_indices = self._get_manual_column_indices(role, include_payment)
            existing_manual = {}
            
            # Skip header rows (0, 1) and start from data rows (2+)
            for row_idx, row in enumerate(all_data[2:], start=3):
                if len(row) > 0 and row[0]:  # Has Project Name
                    project_name = row[0]
                    manual_data = {}
                    
                    # Extract manual column values
                    for col_name, col_idx in manual_col_indices.items():
                        if col_idx < len(row):
                            manual_data[col_name] = row[col_idx]
                        else:
                            manual_data[col_name] = ''
                    
                    existing_manual[project_name] = manual_data
            
            print(f"      📖 Preserved manual data for {len(existing_manual)} projects")
            return existing_manual
            
        except Exception as e:
            print(f"      ⚠️  Could not read existing data: {e}")
            return {}
    
    def _get_manual_column_indices(self, role: str, include_payment: bool) -> Dict[str, int]:
        """Get the column indices for manual (preserved) columns with FIXED meta-reviewer positions"""
        manual_cols = {}
        
        # Calculate payment/feedback start column based on role
        if role == "Annotator":
            payment_start_col = 4  # Column E for annotators (after Project, Schema, Video Count, Last Submitted)
        elif role == "Reviewer":
            payment_start_col = 8  # Column I for reviewers (after Project, Schema, Video Count, GT%, All GT%, Review%, All Rev%, Last Submitted)
        else:  # Meta-Reviewer - FIXED
            payment_start_col = 4  # Column E for meta-reviewers (after Project, Schema, Video Count, Last Modified)
        
        if include_payment:
            # Payment columns are manual
            manual_cols["Payment Time"] = payment_start_col      
            manual_cols["Base Salary"] = payment_start_col + 1        
            manual_cols["Bonus Salary"] = payment_start_col + 2       
        else:
            # Feedback column is manual
            manual_cols["Feedback"] = payment_start_col
        
        return manual_cols
    
    def _merge_with_manual_data(self, auto_row: List, project_name: str, existing_data: Dict, 
                               manual_col_indices: Dict[str, int]) -> List:
        """Merge automatic data with preserved manual data"""
        merged_row = auto_row.copy()
        
        # Overlay preserved manual data if it exists for this project
        if project_name in existing_data:
            preserved = existing_data[project_name]
            for col_name, col_idx in manual_col_indices.items():
                if col_idx < len(merged_row) and col_name in preserved:
                    merged_row[col_idx] = preserved[col_name]
        
        return merged_row
    
    def _create_project_data_row(self, project: Dict, role: str, include_payment: bool) -> List:
        """Create a data row for a project with FIXED meta-reviewer structure"""
        if role == "Annotator":
            row = [
                project['project_name'],
                project['schema_name'],
                project['video_count'],
                self._format_timestamp(project['last_submitted'])
            ]
            # Add payment/feedback placeholder
            if include_payment:
                row.extend(['', '', ''])  # Payment Time, Base Salary, Bonus Salary
            else:
                row.append('')  # Feedback column
            
            # Add overall stats
            row.extend([
                f"{project['accuracy']:.0f}%",
                f"{project['completion']:.0f}%",
                f"{project['reviewed']:.0f}%",
                project['completed'],
                project['reviewed_count'],
                project['wrong']
            ])
            
        elif role == "Reviewer":
            row = [
                project['project_name'],
                project['schema_name'],
                project['video_count'],
                f"{project['gt_completion']:.0f}%",
                f"{project['all_gt_completion']:.0f}%",
                f"{project['review_completion']:.0f}%",
                f"{project['all_review_completion']:.0f}%",
                self._format_timestamp(project['last_submitted'])
            ]
            # Add payment/feedback placeholder
            if include_payment:
                row.extend(['', '', ''])  # Payment Time, Base Salary, Bonus Salary
            else:
                row.append('')  # Feedback column
            
            # Add overall stats
            row.extend([
                f"{project['gt_completion']:.0f}%",
                f"{project['gt_accuracy']:.0f}%",
                f"{project['review_completion']:.0f}%",
                project['gt_completed'],
                project['gt_wrong'],
                project['review_completed']
            ])
            
        else:  # Meta-Reviewer - FIXED STRUCTURE
            row = [
                project['project_name'],
                project['schema_name'],
                project['video_count'],
                self._format_timestamp(project['last_modified'])  # FIXED: Move Last Modified here
            ]
            # Add payment/feedback placeholder
            if include_payment:
                row.extend(['', '', ''])  # Payment Time, Base Salary, Bonus Salary
            else:
                row.append('')  # Feedback column
            
            # FIXED: Add "Modified Ratio By" columns at the end
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
    
    def _apply_user_sheet_formatting(self, worksheet, data_rows_count: int, role: str, include_payment: bool):
        """Apply professional formatting to user sheets with consistent colors"""
        try:
            print(f"      🎨 Applying formatting...")
            
            # Apply alternating row colors - CONSISTENT WITH MASTER SHEET
            if data_rows_count > 0:
                even_row_format = {
                    "backgroundColor": {"red": 0.95, "green": 0.98, "blue": 1.0}  # Same light blue as master sheet
                }
                
                for row in range(4, 3 + data_rows_count + 1, 2):  # Every other row starting from 4
                    self._api_call_with_retry(
                        worksheet.format, f"A{row}:ZZ{row}", even_row_format,
                        operation_name=f"formatting row {row}"
                    )
            
            # Apply column widths - FIXED for meta-reviewer
            column_widths = [
                {"startIndex": 0, "endIndex": 1, "pixelSize": 150},   # A: Project Name
                {"startIndex": 1, "endIndex": 2, "pixelSize": 120},   # B: Schema Name
                {"startIndex": 2, "endIndex": 3, "pixelSize": 90},    # C: Video Count
            ]
            
            # Role-specific columns
            current_col = 3
            if role == "Annotator":
                column_widths.append(
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 132}  # D: Last Submitted
                )
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
            else:  # Meta-Reviewer - FIXED
                column_widths.append(
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 132}  # D: Last Modified
                )
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
                column_widths.append(
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 200}      # Feedback
                )
                current_col += 1
            
            # Overall stats columns (for Annotator and Reviewer) or Modified Ratio By (for Meta-Reviewer)
            if role in ["Annotator", "Reviewer"]:
                remaining_cols = 6
            else:  # Meta-Reviewer
                remaining_cols = 2  # Modified Ratio By columns
            
            for i in range(remaining_cols):
                column_widths.append({
                    "startIndex": current_col + i, 
                    "endIndex": current_col + i + 1, 
                    "pixelSize": 80
                })
            
            # Apply column widths
            requests = []
            for width_spec in column_widths:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
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
            
            if requests:
                self._api_call_with_retry(
                    lambda: self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body={"requests": requests}
                    ).execute(),
                    operation_name="applying user sheet formatting"
                )
            
            print(f"      ✅ Applied professional formatting with consistent colors")
            
        except Exception as e:
            print(f"      ⚠️  Some formatting may not have applied: {e}")
    
    def _col_num_to_letter(self, col_num):
        """Convert column number to Excel-style letter"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result
    
    def _format_timestamp(self, timestamp) -> str:
        """Format timestamp to readable format"""
        if not timestamp:
            return ''
        try:
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                dt = timestamp
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return str(timestamp) if timestamp else ''


def main():
    """Main function"""
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
    
    args = parser.parse_args()
    
    print("="*60)
    print("GOOGLE SHEETS EXPORT SETUP")
    print("="*60)
    print(f"Master Sheet ID: {args.master_sheet_id}")
    print(f"Master Sheet URL: https://docs.google.com/spreadsheets/d/{args.master_sheet_id}/edit")
    print(f"Database URL Environment Variable: {args.database_url_name}")
    print(f"Credentials File: {args.credentials_file}")
    if args.skip_individual:
        print("Mode: Master sheet only (skipping individual user sheets)")
    elif args.resume_from:
        print(f"Mode: Resume from user '{args.resume_from}'")
    else:
        print("Mode: Full export (master sheet + individual user sheets)")
    print("="*60)
    
    # Verify credentials file exists
    credentials_file = Path(args.credentials_file)
    if not credentials_file.exists():
        print(f"❌ Error: Credentials file not found: {credentials_file}")
        print(f"💡 Please download your Google OAuth credentials and save as '{args.credentials_file}'")
        return
    
    # Create exporter and run
    exporter = GoogleSheetExporter(str(credentials_file), args.database_url_name)
    
    exporter.export_all_sheets(
        master_sheet_id=args.master_sheet_id,
        skip_individual=args.skip_individual,
        resume_from=args.resume_from
    )
    
    print("\n" + "="*60)
    print("📋 EXPORT FEATURES SUMMARY")
    print("="*60)
    print("✅ FIXED ISSUES:")
    print("   • Meta-reviewer 'Modified Ratio By' columns moved to end")
    print("   • Smart chip links implemented for master sheet")
    print("   • Consistent color scheme across all sheets")
    print("   • Light blue headers (RGB: 0.85, 0.92, 0.98)")
    print("   • Light blue alternating rows (RGB: 0.95, 0.98, 1.0)")
    print("\n🔄 DATA PRESERVATION:")
    print("   • Payment Timestamp, Base Salary, Bonus Salary")
    print("   • Feedback to Annotator")
    print("\n🔐 PERMISSION MANAGEMENT:")
    print("   • Database admins get editor access")
    print("   • Other users get view-only access")
    print("   • Applied to master sheet and all individual sheets")
    print("="*60)


if __name__ == "__main__":
    main()