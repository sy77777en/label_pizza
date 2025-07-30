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
from datetime import datetime
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
        
        # Now import database utilities after initialization
        from label_pizza.database_utils import get_db_session
        from label_pizza.services import GoogleSheetsExportService
        
        # Store references
        self.get_db_session = get_db_session
        self.GoogleSheetsExportService = GoogleSheetsExportService
        
        self._setup_google_auth(credentials_file)
        
        # Track any failures during export
        self.export_failures = []
        
        # Get admin users for permission management
        with self.get_db_session() as session:
            self.admin_users = self.GoogleSheetsExportService.get_admin_users(session)
    
    def _setup_google_auth(self, credentials_file: str):
        """Setup Google authentication using OAuth 2.0"""
        creds = None
        
        # Look for credentials file relative to project root (one level up from script)
        if not os.path.isabs(credentials_file):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            credentials_file = os.path.join(project_root, credentials_file)
        
        # Token file location (in project root)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        token_file = os.path.join(project_root, 'google_sheets_token.json')
        
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
                print('🔍 The script needs Drive permissions to create individual user sheets.')
                print('='*60)
                print('1. Go to this URL in your browser:')
                print(auth_url)
                print('')
                print('2. Click "Advanced" -> "Go to [App Name] (unsafe)"')
                print('3. Authorize the application')
                print('4. ⚠️  IMPORTANT: Grant BOTH Sheets AND Drive permissions')
                print('5. The browser will show "This site can\'t be reached" - this is expected!')
                print('6. Copy the authorization code from the failed URL')
                print('')
                print('   Example URL: http://localhost:8080/?code=AUTHORIZATION_CODE&scope=...')
                print('   Copy only the part after "code=" and before "&"')
                print('='*60)
                
                auth_code = input('\\nEnter the authorization code: ').strip()
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
            raise Exception(f"Authentication missing required scopes: {self.SCOPES}")
        
        # Authorize gspread client
        self.client = gspread.authorize(creds)
        
        # Create Google Drive service for permission management
        self.drive_service = build('drive', 'v3', credentials=creds)
        
        print("✅ Google Sheets client authorized with full permissions")
    
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
                    time.sleep(1.5)  # Small delay between all calls
                
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
                        raise Exception(f"Rate limit failure for {operation_name}")
                    print(f"      ⚠️  Rate limit detected in {operation_name}: {e}")
                    continue
                else:
                    failure_msg = f"UNEXPECTED ERROR: {operation_name} - {str(e)}"
                    print(f"      ❌ {failure_msg}")
                    self.export_failures.append(failure_msg)
                    raise
        
        raise Exception(f"{operation_name} failed after {max_retries} attempts")
    
    def _manage_sheet_permissions(self, sheet_id: str, sheet_name: str):
        """Manage permissions for a sheet - give edit access to database admins only"""
        print(f"      🔐 Managing permissions for {sheet_name}...")
        
        try:
            # Get current permissions
            current_permissions = self._api_call_with_retry(
                lambda: self.drive_service.permissions().list(fileId=sheet_id).execute(),
                operation_name=f"listing permissions for {sheet_name}"
            )
            
            # Track emails that currently have access
            current_editors = set()
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
                
                # Check if this email should be an admin
                is_admin = any(admin['email'] == email for admin in self.admin_users if admin['email'])
                
                if is_admin:
                    if role != 'writer':
                        permissions_to_update.append((perm_id, 'writer', email))
                    current_editors.add(email)
                else:
                    if role == 'writer':
                        permissions_to_update.append((perm_id, 'reader', email))
            
            # Add missing admin permissions
            admin_emails = [admin['email'] for admin in self.admin_users if admin['email']]
            for email in admin_emails:
                if email not in current_editors:
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
                            operation_name=f"adding admin permission for {email}"
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
            print("\\n✅ Master sheet export completed (individual sheets skipped)")
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
        
        print(f"\\nExporting individual user sheets ({total_users} total)...")
        print("⚠️  Note: Processing slowly to avoid Google Sheets rate limits...")
        
        # Store sheet IDs for hyperlinks
        user_sheet_ids = {}
        
        # Determine where to start based on resume_from parameter
        skip_until_found = resume_from is not None
        
        for user_data, role in all_users:
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
            
            print(f"\\n[{current_user}/{total_users}] Processing {user_data['user_name']} {role}...")
            
            # Add delay between users to prevent rate limits
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
        
        # Update master sheet with correct hyperlinks
        if user_sheet_ids:
            print(f"\\n⏳ Pausing 5 seconds before updating master sheet links...")
            time.sleep(5)
            print("Updating master sheet hyperlinks...")
            try:
                self._update_master_sheet_links(master_sheet_id, user_sheet_ids)
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
            print(f"\\n⚠️  Some data may not be synced. Consider running the script again.")
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
            print(f"\\n💡 Possible solutions:")
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
        
        # Prepare headers based on role
        if role == "Annotator":
            headers = [
                "User Name", "Email", "Role", "Annotation Sheet", "Last Annotated Time",
                "Number of Assigned Projects", "Number of Project Started", "Number of Project Completed"
            ]
        elif role == "Reviewer":
            headers = [
                "User Name", "Email", "Role", "Review Sheet", "Last Review Time",
                "Number of Assigned Projects", "Number of Project Started"
            ]
        else:  # Meta-Reviewer
            headers = [
                "User Name", "Email", "Role", "Meta-Review Sheet", "Last Modified Time",
                "Number of Assigned Projects", "Number of Project Started",
                "Ratio Modified by This Admin", "Ratio Modified by All Admins"
            ]
        
        # Prepare data rows
        rows = [headers]
        for user in user_data:
            if self._has_activity(user):
                # Create placeholder links (will be updated later with actual URLs)
                sheet_link = "Link pending..."
                
                # Format timestamps
                if role == "Annotator":
                    timestamp = self._format_timestamp(user.get('last_annotation_time'))
                    row = [
                        user['user_name'], 
                        user['email'], 
                        user['role'],
                        sheet_link, 
                        timestamp,
                        user['assigned_projects'],
                        user['projects_started'],
                        user['projects_completed']
                    ]
                elif role == "Reviewer":
                    timestamp = self._format_timestamp(user.get('last_review_time'))
                    row = [
                        user['user_name'], 
                        user['email'], 
                        user['role'],
                        sheet_link, 
                        timestamp,
                        user['assigned_projects'],
                        user['projects_started']
                    ]
                else:  # Meta-Reviewer
                    timestamp = self._format_timestamp(user.get('last_modified_time'))
                    row = [
                        user['user_name'], 
                        user['email'], 
                        user['role'],
                        sheet_link, 
                        timestamp,
                        user['assigned_projects'],
                        user['projects_started'],
                        f"{user['ratio_modified_by_user']:.1f}%",
                        f"{user['ratio_modified_by_all']:.1f}%"
                    ]
                
                rows.append(row)
        
        # Update the worksheet
        self._api_call_with_retry(worksheet.clear, operation_name="clearing master sheet")
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
        
        # Get user project data
        with self.get_db_session() as session:
            if role == "Annotator":
                project_data = self.GoogleSheetsExportService.get_user_project_data_annotator(user_data['user_id'], session)
            elif role == "Reviewer":
                project_data = self.GoogleSheetsExportService.get_user_project_data_reviewer(user_data['user_id'], session)
            else:  # Meta-Reviewer
                project_data = self.GoogleSheetsExportService.get_user_project_data_meta_reviewer(user_data['user_id'], session)
        
        # Calculate required rows (headers + data + padding) - handle massive scale
        header_rows = 2 if role in ["Annotator", "Reviewer"] else 1
        data_rows = len(project_data)
        
        # For very large datasets, use more conservative padding
        if data_rows > 1000:
            padding_rows = 50  # Larger padding for big sheets
        elif data_rows > 100:
            padding_rows = 20
        else:
            padding_rows = 10
            
        required_rows = header_rows + data_rows + padding_rows
        required_cols = 30  # Increase from 20 to handle more columns
        
        print(f"      Data size: {data_rows} projects, requiring {required_rows} rows total")
        
        # Export Payment tab
        self._export_user_tab(sheet, "Payment", user_data, role, project_data, include_payment=True, 
                             required_rows=required_rows, required_cols=required_cols)
        
        # Export Feedback tab
        self._export_user_tab(sheet, "Feedback", user_data, role, project_data, include_payment=False, 
                             required_rows=required_rows, required_cols=required_cols)
        
        return sheet.id
    
    def _export_user_tab(self, sheet, tab_name: str, user_data: Dict, role: str, 
                        project_data: List[Dict], include_payment: bool, required_rows: int, required_cols: int):
        """Export a single tab in a user sheet"""
        print(f"    Exporting {tab_name} tab...")
        
        try:
            worksheet = sheet.worksheet(tab_name)
            print(f"      Found existing {tab_name} tab")
            
            # Check if existing worksheet has enough rows and columns
            current_rows = worksheet.row_count
            current_cols = worksheet.col_count
            
            needs_expansion = False
            if current_rows < required_rows:
                print(f"      Need to expand rows from {current_rows} to {required_rows}")
                needs_expansion = True
            if current_cols < required_cols:
                print(f"      Need to expand columns from {current_cols} to {required_cols}")
                needs_expansion = True
                
            if needs_expansion:
                try:
                    # Use resize instead of add_rows for better control
                    worksheet.resize(rows=max(required_rows, current_rows), cols=max(required_cols, current_cols))
                    print(f"      ✅ Expanded worksheet to {worksheet.row_count} rows × {worksheet.col_count} columns")
                except Exception as e:
                    print(f"      ⚠️  Could not expand worksheet: {e}")
            
        except gspread.exceptions.WorksheetNotFound:
            print(f"      Creating new {tab_name} tab with {required_rows} rows × {required_cols} columns...")
            worksheet = self._api_call_with_retry(
                sheet.add_worksheet, 
                title=tab_name, 
                rows=required_rows, 
                cols=required_cols, 
                operation_name=f"creating {tab_name} worksheet"
            )
        
        # Read existing data to preserve manual columns
        existing_data = self._read_existing_manual_data(worksheet, role, include_payment)
        
        # Clear and create headers
        self._clear_header_area(worksheet, role, include_payment)
        self._create_user_headers(worksheet, role, include_payment)
        
        # Prepare data rows preserving manual data
        data_rows = []
        manual_col_indices = self._get_manual_column_indices(role, include_payment)
        
        for project in project_data:
            # Create automatic data row
            auto_row = self._create_data_row(project, role, include_payment)
            
            # Merge with preserved manual data
            project_key = f"{project['project_name']}-{project['schema_name']}"
            preserved_row = self._merge_with_manual_data(auto_row, project_key, existing_data, manual_col_indices)
            data_rows.append(preserved_row)
        
        # Update data rows
        if data_rows:
            self._update_data_with_preservation(worksheet, data_rows, 3)
            print(f"      ✅ Successfully updated {len(data_rows)} rows with preserved manual data")
        else:
            print(f"      ℹ️  No data to update in {tab_name} tab")
        
        # Apply formatting
        self._apply_worksheet_formatting(worksheet, role, include_payment, len(data_rows))
    
    def _create_data_row(self, project: Dict, role: str, include_payment: bool) -> List:
        """Create a data row for a project"""
        if role == "Annotator":
            row = [
                project['project_name'],
                project['schema_name'],
                f"{project['completion_ratio']:.0f}%",
                f"{project['reviewed_ratio']:.0f}%",
                self._format_timestamp(project['last_submitted'])
            ]
        elif role == "Reviewer":
            row = [
                project['project_name'],
                project['schema_name'],
                f"{project['gt_ratio']:.0f}%",
                f"{project['all_gt_ratio']:.0f}%",
                f"{project['review_ratio']:.0f}%",
                f"{project['all_review_ratio']:.0f}%",
                self._format_timestamp(project['last_submitted'])
            ]
        else:  # Meta-Reviewer
            row = [
                project['project_name'],
                project['schema_name'],
                f"{project['ratio_modified_by_user']:.0f}%",
                f"{project['ratio_modified_by_all']:.0f}%",
                self._format_timestamp(project['last_modified'])
            ]
        
        # Add placeholders for manual columns
        if include_payment:
            row.extend(['', '', ''])  # Payment Timestamp, Base Salary, Bonus Salary
        else:
            row.append('')  # Feedback column
        
        # Add overall statistics
        if role == "Annotator":
            row.extend([
                f"{project['accuracy']:.0f}%",
                f"{project['completion']:.0f}%",
                project['reviewed'],
                project['completed'],
                project['reviewed'],
                project['wrong']
            ])
        elif role == "Reviewer":
            row.extend([
                f"{project['gt_completion']:.0f}%",
                f"{project['gt_accuracy']:.0f}%",
                f"{project['review_completion']:.0f}%",
                project['gt_completed'],
                project['gt_wrong'],
                project['review_completed']
            ])
        else:  # Meta-Reviewer
            row.extend([
                f"{project['ratio_modified_by_user']:.0f}%",
                f"{project['ratio_modified_by_all']:.0f}%"
            ])
        
        return row
    
    # Helper methods (formatting, manual data preservation, etc.)
    def _format_timestamp(self, timestamp) -> str:
        """Format timestamp to readable format"""
        if not timestamp:
            return ''
        try:
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp)
            else:
                dt = timestamp
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return str(timestamp) if timestamp else ''
    
    def _col_num_to_letter(self, col_num):
        """Convert column number to Excel-style letter"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result
    
    def _read_existing_manual_data(self, worksheet, role: str, include_payment: bool) -> Dict:
        """Read existing manual data to preserve it during updates"""
        try:
            all_data = worksheet.get_all_values()
            if len(all_data) < 3:
                return {}
            
            manual_col_indices = self._get_manual_column_indices(role, include_payment)
            existing_manual = {}
            
            for row_idx, row in enumerate(all_data[2:], start=3):
                if len(row) > 1 and row[0] and row[1]:  # Has Project Name and Schema Name
                    project_key = f"{row[0]}-{row[1]}"
                    manual_data = {}
                    
                    for col_name, col_idx in manual_col_indices.items():
                        if col_idx < len(row):
                            manual_data[col_name] = row[col_idx]
                        else:
                            manual_data[col_name] = ''
                    
                    existing_manual[project_key] = manual_data
            
            print(f"      📖 Preserved manual data for {len(existing_manual)} projects")
            return existing_manual
            
        except Exception as e:
            print(f"      ⚠️  Could not read existing data (will create fresh): {e}")
            return {}
    
    def _get_manual_column_indices(self, role: str, include_payment: bool) -> Dict[str, int]:
        """Get the column indices for manual (preserved) columns"""
        manual_cols = {}
        
        # Calculate payment/feedback start column based on role
        if role == "Annotator":
            payment_start_col = 5  # After Project, Schema, Completion, Reviewed, Last Submitted
        elif role == "Reviewer":
            payment_start_col = 7  # After Project, Schema, GT Ratio, All GT, Review Ratio, All Review, Last Submitted
        else:  # Meta-Reviewer
            payment_start_col = 5  # After Project, Schema, Ratio Modified User, Ratio Modified All, Last Modified
        
        if include_payment:
            manual_cols["Payment Timestamp"] = payment_start_col
            manual_cols["Base Salary"] = payment_start_col + 1
            manual_cols["Bonus Salary"] = payment_start_col + 2
        else:
            manual_cols["Feedback"] = payment_start_col
        
        return manual_cols
    
    def _merge_with_manual_data(self, auto_row: List, project_key: str, existing_data: Dict, 
                               manual_col_indices: Dict[str, int]) -> List:
        """Merge automatic data with preserved manual data"""
        merged_row = auto_row.copy()
        
        if project_key in existing_data:
            preserved = existing_data[project_key]
            for col_name, col_idx in manual_col_indices.items():
                if col_idx < len(merged_row) and col_name in preserved:
                    merged_row[col_idx] = preserved[col_name]
        
        return merged_row
    
    def _clear_header_area(self, worksheet, role: str, include_payment: bool):
        """Clear and unmerge the header area"""
        try:
            end_col = self._col_num_to_letter(20)  # Clear wide area
            
            # Unmerge cells
            requests = [{
                "unmergeCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": 2,
                        "startColumnIndex": 0,
                        "endColumnIndex": 20
                    }
                }
            }]
            
            try:
                self._api_call_with_retry(
                    worksheet.spreadsheet.batch_update,
                    {"requests": requests},
                    operation_name="unmerging header area"
                )
            except:
                pass  # Ignore if no merged cells
            
            # Clear the area
            self._api_call_with_retry(worksheet.batch_clear, [f'A1:{end_col}2'], 
                                    operation_name="clearing header area")
            
        except Exception as e:
            print(f"      ⚠️  Could not clear header area: {e}")
    
    def _create_user_headers(self, worksheet, role: str, include_payment: bool):
        """Create headers for user sheets"""
        if role == "Annotator":
            row1 = ["Project Name", "Schema Name", "Completion Ratio", "Reviewed Ratio", "Last Submitted"]
        elif role == "Reviewer":
            row1 = ["Project Name", "Schema Name", "GT Ratio", "All GT Ratio", "Review Ratio", "All Review Ratio", "Last Submitted"]
        else:  # Meta-Reviewer
            row1 = ["Project Name", "Schema Name", "Ratio Modified by This Admin", "Ratio Modified by All Admins", "Last Modified"]
        
        if include_payment:
            row1.extend(["Payment Timestamp", "Base Salary", "Bonus Salary"])
        else:
            row1.append("Feedback")
        
        # Add overall stats headers
        if role == "Annotator":
            row1.extend(["Overall Stats", "", "", "", "", ""])
            row2 = [""] * len(row1)
            row2[-6:] = ["Accuracy", "Completion", "Reviewed", "Completed", "Reviewed", "Wrong"]
        elif role == "Reviewer":
            row1.extend(["Overall Stats", "", "", "", "", ""])
            row2 = [""] * len(row1)
            row2[-6:] = ["GT Completion", "GT Accuracy", "Review Completion", "GT Completed", "GT Wrong", "Review Completed"]
        else:  # Meta-Reviewer
            row1.extend(["Overall Stats", ""])
            row2 = [""] * len(row1)
            row2[-2:] = ["Ratio Modified by This Admin", "Ratio Modified by All Admins"]
        
        if role in ["Annotator", "Reviewer"]:
            # Update headers
            end_col = self._col_num_to_letter(len(row1))
            self._api_call_with_retry(
                worksheet.update, 
                values=[row1, row2], 
                range_name=f'A1:{end_col}2', 
                operation_name="updating headers"
            )
        else:
            # Meta-reviewer has single row headers
            end_col = self._col_num_to_letter(len(row1))
            self._api_call_with_retry(
                worksheet.update, 
                values=[row1], 
                range_name=f'A1:{end_col}1', 
                operation_name="updating headers"
            )
    
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
            operation_name=f"updating {len(data_rows)} data rows"
        )
    
    def _apply_master_sheet_formatting(self, worksheet, num_users: int, num_cols: int):
        """Apply formatting to master sheet with improved column widths"""
        try:
            # Header formatting
            header_format = {
                "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
                "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }
            
            end_col = self._col_num_to_letter(num_cols)
            self._api_call_with_retry(worksheet.format, f"A1:{end_col}1", header_format,
                                    operation_name="formatting master sheet headers")
            
            # Apply master sheet column widths (using previous script sizes)
            column_widths = [
                {"startIndex": 0, "endIndex": 1, "pixelSize": 100},   # A: User Name
                {"startIndex": 1, "endIndex": 2, "pixelSize": 161},   # B: Email (15% wider)
                {"startIndex": 2, "endIndex": 3, "pixelSize": 70},    # C: Role (shorter for human/admin/model)
                {"startIndex": 3, "endIndex": 4, "pixelSize": 100},   # D: Sheet Link
                {"startIndex": 4, "endIndex": 5, "pixelSize": 144},   # E: Timestamp
                {"startIndex": 5, "endIndex": 6, "pixelSize": 96},    # F: Assigned Projects
                {"startIndex": 6, "endIndex": 7, "pixelSize": 96},    # G: Started Projects
            ]
            
            # Add role-specific columns
            if num_cols > 7:  # Annotator has completed projects column
                column_widths.append({"startIndex": 7, "endIndex": 8, "pixelSize": 96})
            if num_cols > 8:  # Meta-reviewer has ratio columns
                column_widths.extend([
                    {"startIndex": 8, "endIndex": 9, "pixelSize": 110},   # Ratio Modified by This Admin
                    {"startIndex": 9, "endIndex": 10, "pixelSize": 110},  # Ratio Modified by All Admins
                ])
            
            # Apply column widths using batch update
            requests = []
            for width_spec in column_widths:
                if width_spec["endIndex"] <= num_cols:  # Don't exceed actual columns
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
                    worksheet.spreadsheet.batch_update,
                    {"requests": requests},
                    operation_name="updating master sheet column widths"
                )
            
            # Alternating row colors
            if num_users > 0:
                even_row_format = {"backgroundColor": {"red": 0.95, "green": 0.98, "blue": 1.0}}
                for row in range(3, 2 + num_users + 1, 2):
                    self._api_call_with_retry(worksheet.format, f"A{row}:{end_col}{row}", even_row_format,
                                            operation_name=f"formatting master row {row}")
            
            print(f"    ✅ Applied master sheet formatting with improved column widths")
            
        except Exception as e:
            print(f"    ⚠️  Master sheet formatting failed: {e}")
    
    def _apply_worksheet_formatting(self, worksheet, role: str, include_payment: bool, num_data_rows: int):
        """Apply formatting to user worksheets with proper column widths and row heights"""
        try:
            # Header formatting
            header_format = {
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 1.0},
                "textFormat": {"bold": True, "fontSize": 11},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }
            
            if role in ["Annotator", "Reviewer"]:
                self._api_call_with_retry(worksheet.format, "A1:ZZ2", header_format,
                                        operation_name="formatting headers")
            else:  # Meta-reviewer (single row)
                self._api_call_with_retry(worksheet.format, "A1:ZZ1", header_format,
                                        operation_name="formatting headers")
            
            # Apply user sheet column widths (using previous script sizes)
            column_widths = [
                {"startIndex": 0, "endIndex": 1, "pixelSize": 150},   # A: Project Name (was Json Sheet Name)
                {"startIndex": 1, "endIndex": 2, "pixelSize": 120},   # B: Schema Name (new)
            ]
            
            # Role-specific columns
            current_col = 2
            if role == "Annotator":
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 60},    # C: Completion Ratio
                    {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 60}, # D: Reviewed Ratio
                    {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 132}, # E: Last Submitted
                ])
                current_col += 3
            elif role == "Reviewer":
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 60},    # C: GT Ratio
                    {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 60}, # D: All GT Ratio
                    {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 60}, # E: Review Ratio
                    {"startIndex": current_col + 3, "endIndex": current_col + 4, "pixelSize": 60}, # F: All Review Ratio
                    {"startIndex": current_col + 4, "endIndex": current_col + 5, "pixelSize": 132}, # G: Last Submitted
                ])
                current_col += 5
            else:  # Meta-Reviewer
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 110},    # C: Ratio Modified by User
                    {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 110}, # D: Ratio Modified by All
                    {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 132}, # E: Last Modified
                ])
                current_col += 3
            
            # Payment/Feedback columns
            if include_payment:
                column_widths.extend([
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 120},      # Payment Timestamp
                    {"startIndex": current_col + 1, "endIndex": current_col + 2, "pixelSize": 90},   # Base Salary
                    {"startIndex": current_col + 2, "endIndex": current_col + 3, "pixelSize": 90},   # Bonus Salary
                ])
                current_col += 3
            else:
                column_widths.append(
                    {"startIndex": current_col, "endIndex": current_col + 1, "pixelSize": 300}      # Feedback (very wide)
                )
                current_col += 1
            
            # Overall stats columns - all 84px (from previous script)
            if role == "Annotator":
                stats_cols = 6  # Accuracy, Completion, Reviewed, Completed, Reviewed, Wrong
            elif role == "Reviewer":
                stats_cols = 6  # GT Completion, GT Accuracy, Review Completion, GT Completed, GT Wrong, Review Completed
            else:  # Meta-Reviewer
                stats_cols = 2  # Ratio Modified by This Admin, Ratio Modified by All Admins
            
            for i in range(stats_cols):
                column_widths.append({
                    "startIndex": current_col + i, 
                    "endIndex": current_col + i + 1, 
                    "pixelSize": 84
                })
            
            # Apply column widths using batch update
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
                    worksheet.spreadsheet.batch_update,
                    {"requests": requests},
                    operation_name="updating user sheet column widths"
                )
            
            # Set row heights - CRITICAL for feedback tabs
            if num_data_rows > 0:
                start_row = 3 if role in ["Annotator", "Reviewer"] else 2
                
                # Different heights based on tab type
                if not include_payment:  # Feedback tab - make rows much taller
                    row_height = 120  # Very tall for 100-word feedback
                else:  # Payment tab - normal height
                    row_height = 35
                
                # Set row heights
                row_height_requests = []
                for row_index in range(start_row - 1, start_row - 1 + num_data_rows):  # Convert to 0-indexed
                    row_height_requests.append({
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": worksheet.id,
                                "dimension": "ROWS",
                                "startIndex": row_index,
                                "endIndex": row_index + 1
                            },
                            "properties": {
                                "pixelSize": row_height
                            },
                            "fields": "pixelSize"
                        }
                    })
                
                if row_height_requests:
                    self._api_call_with_retry(
                        worksheet.spreadsheet.batch_update,
                        {"requests": row_height_requests},
                        operation_name=f"setting row heights to {row_height}px"
                    )
            
            # Data formatting (alternating colors, text wrapping for feedback)
            if num_data_rows > 0:
                start_row = 3 if role in ["Annotator", "Reviewer"] else 2
                even_row_format = {"backgroundColor": {"red": 0.95, "green": 0.98, "blue": 1.0}}
                
                # Calculate the actual last row to avoid exceeding grid limits
                last_row = start_row + num_data_rows - 1
                
                # Apply alternating colors to even rows, but don't exceed the actual data range
                for row in range(start_row + 1, last_row + 1, 2):  # Every other row starting from start_row + 1
                    if row <= last_row:  # Extra safety check
                        try:
                            self._api_call_with_retry(worksheet.format, f"A{row}:ZZ{row}", even_row_format,
                                                    operation_name=f"formatting row {row}")
                        except Exception as e:
                            if 'exceeds grid limits' in str(e).lower():
                                print(f"      ℹ️  Skipping row {row} formatting due to grid limits")
                                break  # Stop trying to format more rows
                            else:
                                raise
                
                # Special formatting for feedback column
                if not include_payment:  # Feedback tab
                    # Calculate feedback column based on role and structure
                    if role == "Annotator":
                        feedback_col_num = 6  # Column F (Project, Schema, Completion, Reviewed, Last Submitted, Feedback)
                    elif role == "Reviewer":
                        feedback_col_num = 8  # Column H (Project, Schema, GT, All GT, Review, All Review, Last Submitted, Feedback)
                    else:  # Meta-Reviewer
                        feedback_col_num = 6  # Column F (Project, Schema, Ratio User, Ratio All, Last Modified, Feedback)
                    
                    feedback_col = self._col_num_to_letter(feedback_col_num)
                    
                    feedback_format = {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "textFormat": {"fontSize": 9}
                    }
                    self._api_call_with_retry(
                        worksheet.format, 
                        f"{feedback_col}{start_row}:{feedback_col}{last_row}", 
                        feedback_format,
                        operation_name="formatting feedback column"
                    )
            
            print(f"      ✅ Applied professional formatting with proper column widths and row heights")
            
        except Exception as e:
            print(f"      ⚠️  Worksheet formatting failed: {e}")
    
    def _update_master_sheet_links(self, master_sheet_id: str, user_sheet_ids: Dict):
        """Update master sheet with hyperlinks to user sheets"""
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
                
                for row_idx, row in enumerate(all_data[1:], start=2):  # Skip header
                    if len(row) > 0 and row[0]:  # Has user name
                        user_name = row[0]
                        role_suffix = tab_name[:-1]  # Remove 's' from tab name
                        if role_suffix == "Meta-Reviewer":
                            role_suffix = "Meta-Reviewer"
                        
                        sheet_key = f"{user_name} {role_suffix}"
                        sheet_id = user_sheet_ids.get(sheet_key)
                        
                        if sheet_id:
                            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
                            
                            # Create smart chip
                            requests = [{
                                "updateCells": {
                                    "rows": [{
                                        "values": [{
                                            "userEnteredValue": {"stringValue": "@"},
                                            "chipRuns": [{
                                                "startIndex": 0,
                                                "chip": {
                                                    "richLinkProperties": {"uri": sheet_url}
                                                }
                                            }]
                                        }]
                                    }],
                                    "fields": "userEnteredValue,chipRuns",
                                    "range": {
                                        "sheetId": worksheet.id,
                                        "startRowIndex": row_idx - 1,
                                        "startColumnIndex": link_col_idx,
                                        "endRowIndex": row_idx,
                                        "endColumnIndex": link_col_idx + 1
                                    }
                                }
                            }]
                            
                            self._api_call_with_retry(
                                worksheet.spreadsheet.batch_update,
                                {"requests": requests},
                                operation_name=f"updating smart chip for {user_name}"
                            )
                
                print(f"    ✅ Updated {tab_name} tab links")
            except Exception as e:
                print(f"    ❌ Failed to update {tab_name} tab links: {e}")


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
        resume_from=args.resume_from
    )
    
    print("\\n" + "="*60)
    print("📋 DATA PRESERVATION SUMMARY")
    print("="*60)
    print("✅ PRESERVED (Never Overwritten):")
    print("   • Payment Timestamp")
    print("   • Base Salary")
    print("   • Bonus Salary")
    print("   • Feedback to User")
    print("\\n🔄 AUTOMATICALLY UPDATED:")
    print("   • Project Names and Schema Names")
    print("   • Completion and Review Ratios")
    print("   • Last Activity Timestamps")
    print("   • All statistics (accuracy, counts, etc.)")
    print("\\n🔐 PERMISSION MANAGEMENT:")
    print("   • Editor access: Database admin users only")
    print("   • Others: View-only access")
    print("   • Applied to master sheet and all individual user sheets")
    print("="*60)


if __name__ == "__main__":
    main()