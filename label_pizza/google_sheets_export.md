# Google Sheets Export System Documentation

## Overview

The Google Sheets export system automatically generates comprehensive statistics for video annotation and review work across projects. It creates a master tracking sheet with links to individual user performance sheets, providing detailed metrics for project management, payment calculation, and quality monitoring.

## System Architecture

```
Master Sheet
├── Annotators Tab (users who create annotations)
├── Reviewers Tab (users who set ground truth and review annotations) 
└── Meta-Reviewers Tab (admins who override ground truth)
    │
    ├── Links to Individual User Sheets
    │
    └── Individual User Sheets (e.g. "John Doe Annotator")
        ├── Payment Tab (salary/payment info)
        └── Feedback Tab (performance feedback)
```

---

## Master Sheet Structure

### Annotators Tab

| Column | Description | Calculation |
|--------|-------------|-------------|
| **User Name** | Full name of annotator | From User.user_id_str |
| **Email** | Email address | From User.email |
| **Role** | User type in system | From User.user_type (human/admin/model) |
| **Annotation Sheet** | 🔗 Smart chip link to user's annotation sheet | Hyperlink to individual sheet |
| **Last Annotated Time** | Most recent annotation submission | Latest AnnotatorAnswer.modified_at for this user |
| **Number of Assigned Projects** | Projects assigned as annotator | Count of non-archived ProjectUserRole with role="annotator" on non-archived Projects |
| **Number of Project Started** | Projects with at least one annotation | Count of projects where user has AnnotatorAnswer |
| **Number of Project Completed** | Projects fully completed | Count of ProjectUserRole with completed_at timestamp |

### Reviewers Tab

| Column | Description | Calculation |
|--------|-------------|-------------|
| **User Name** | Full name of reviewer | From User.user_id_str |
| **Email** | Email address | From User.email |
| **Role** | User type in system | From User.user_type (human/admin/model) |
| **Review Sheet** | 🔗 Smart chip link to user's review sheet | Hyperlink to individual sheet |
| **Last Review Time** | Most recent review submission | Latest of (ReviewerGroundTruth.created_at, AnswerReview.reviewed_at) |
| **Number of Assigned Projects** | Projects assigned as reviewer | Count of non-archived ProjectUserRole with role="reviewer" on non-archived Projects |
| **Number of Project Started** | Projects with at least one review activity | Count of projects where user has ReviewerGroundTruth or AnswerReview |

### Meta-Reviewers Tab

| Column | Description | Calculation |
|--------|-------------|-------------|
| **User Name** | Full name of meta-reviewer | From User.user_id_str |
| **Email** | Email address | From User.email |
| **Role** | User type in system | From User.user_type (always "admin") |
| **Meta-Review Sheet** | 🔗 Smart chip link to user's meta-review sheet | Hyperlink to individual sheet |
| **Last Modified Time** | Most recent ground truth override | Latest ReviewerGroundTruth.modified_by_admin_at for this user |
| **Number of Assigned Projects** | Projects assigned as admin | Count of non-archived ProjectUserRole with role="admin" on non-archived Projects |
| **Number of Project Started** | Projects with at least one override | Count of projects where user modified ReviewerGroundTruth |
| **Ratio Modified by This Admin** | Percentage of ground truth modified by this admin | Modified records by user / Total ground truth records |
| **Ratio Modified by All Admins** | Percentage of ground truth modified by any admin | All admin-modified records / Total ground truth records |

---

## Permission Management

The export system automatically manages Google Sheets permissions for all created sheets:

### **Admin Access (Full Edit Permissions)**
Only users with User.user_type="admin" in the database receive automatic editor access to all sheets.

### **View-Only Access**
All other users receive view-only permissions.

### **Automatic Permission Updates**
- Permissions are updated every time the export runs
- Applied to both the master sheet and all individual user sheets
- Admin status is determined by database User.user_type field
- No manual permission management required

### **Security Features**
- Only database admins can modify data
- Other users can view but cannot edit
- Sheet owners retain full control
- Individual user sheets inherit the same permission structure

---

## Individual User Sheets

Each user has their own Google Sheet with two tabs: **Payment** and **Feedback**. The structure differs between Annotators, Reviewers, and Meta-Reviewers.

### Multi-Row Header Structure

The sheets use a **2-row header system** with merged cells for better organization:

**Row 1**: Main category headers (spans multiple columns)
**Row 2**: Specific metric headers (individual columns)

### Annotator Sheet Structure

#### Common Columns (Both Tabs)

```
Row 1: | Project Name | Schema Name | Video Count | Last Submitted | Payment/Feedback | Overall Stats |
Row 2: |              |             |             |                |                  | Accuracy % | Completion % | Reviewed % | Completed | Reviewed | Wrong |
```

| Column Group | Description | Formula |
|--------------|-------------|---------|
| **Project Name** | Name of the project | Project.name from AnnotatorAnswer.project_id |
| **Schema Name** | Name of the schema used | Schema.name from Project.schema_id |
| **Video Count** | Total number of videos in this project | Count from ProjectVideo table |
| **Last Submitted Timestamp** | Most recent annotation by this user | Latest AnnotatorAnswer.modified_at in this project |
| **Payment Tab Only** | Payment Timestamp | When payment was made | **Manual Entry** |
| | Base Salary | Base payment amount | **Manual Entry** |
| | Bonus Salary | Performance bonus | **Manual Entry** |
| **Feedback Tab Only** | Feedback to Annotator | Performance feedback | **Manual Entry** |

#### Overall Statistics Columns

| Column | Description | Formula |
|--------|-------------|---------|
| **Accuracy %** | % of reviewed work that was approved | (Approved reviews + Correct single-choice) / Total reviewed |
| **Completion %** | % of project completed by user | User's answers / Total possible answers |
| **Reviewed %** | Number of questions reviewed or auto-graded | Count of non-"pending" AnswerReview + Single-choice with ground truth |
| **Completed** | Number of questions completed | Count of AnnotatorAnswer by user in project |
| **Reviewed** | Number of questions that received review | Count of AnswerReview + Single-choice comparisons |
| **Wrong** | Number of incorrect answers | Count of rejected reviews + Incorrect single-choice answers |

### Reviewer Sheet Structure

#### Common Columns (Both Tabs)

```
Row 1: | Project Name | Schema Name | Video Count | GT % | All GT % | Review % | All Review % | Last Submitted | Payment/Feedback | Overall Stats |
Row 2: |              |             |             |      |          |          |              |                |                  | GT Completion % | GT Accuracy % | Review Completion % | GT Completed | GT Wrong | Review Completed |
```

| Column Group | Description | Formula |
|--------------|-------------|---------|
| **Project Name** | Name of the project | Project.name from ReviewerGroundTruth.project_id |
| **Schema Name** | Name of the schema used | Schema.name from Project.schema_id |
| **Video Count** | Total number of videos in this project | Count from ProjectVideo table |
| **GT Ratio %** | % of ground truth set by this reviewer | User's ReviewerGroundTruth / Total possible ground truth |
| **All GT Ratio %** | % of all ground truth completed | All ReviewerGroundTruth / Total possible ground truth |
| **Review Ratio %** | % of reviews done by this reviewer | User's AnswerReview / Total possible reviews |
| **All Review Ratio %** | % of all reviews completed | All AnswerReview / Total possible reviews |
| **Last Submitted Timestamp** | Most recent activity by this user | Latest of (ReviewerGroundTruth.created_at, AnswerReview.reviewed_at) |

#### Overall Statistics Columns

| Column | Description | Formula |
|--------|-------------|---------|
| **GT Completion %** | % of ground truth completed by user | User's ReviewerGroundTruth / Total possible ground truth |
| **GT Accuracy %** | % of user's ground truth that wasn't overridden | Original ground truth / User's total ground truth |
| **Review Completion %** | % of reviews completed by user | User's AnswerReview / Total possible reviews |
| **GT Completed** | Number of ground truth records created | Count of ReviewerGroundTruth by user |
| **GT Wrong** | Number of ground truth records overridden by admin | Count where modified_by_admin_id IS NOT NULL |
| **Review Completed** | Number of answer reviews completed | Count of AnswerReview by user |

### Meta-Reviewer Sheet Structure

#### Common Columns (Both Tabs)

```
Row 1: | Project Name | Schema Name | Video Count | Modified Ratio By | Last Modified | Payment/Feedback |
Row 2: |              |             |             | User % | All %     |               |                  |
```

| Column Group | Description | Formula |
|--------------|-------------|---------|
| **Project Name** | Name of the project | Project.name from modified ReviewerGroundTruth |
| **Schema Name** | Name of the schema used | Schema.name from Project.schema_id |
| **Video Count** | Total number of videos in this project | Count from ProjectVideo table |
| **Last Modified Timestamp** | Most recent override by this admin | Latest ReviewerGroundTruth.modified_by_admin_at |

#### Overall Statistics Columns

| Column | Description | Formula |
|--------|-------------|---------|
| **User %** | % of ground truth modified by this admin in this project | Admin's modifications in project / Total ground truth in project |
| **All %** | % of ground truth modified by any admin in this project | All admin modifications in project / Total ground truth in project |

---

## Calculation Details

### Key Metrics Explained

#### For Annotators

1. **Completion Ratio**: Shows annotator's progress on the project
   ```
   (AnnotatorAnswers by this user in project) / (Total questions × Total videos in project)
   ```

2. **Reviewed Ratio**: Shows what percentage of annotator's work has been quality-checked
   ```
   For single choice questions: (Answers where ReviewerGroundTruth exists) / (User's completed answers)
   For description questions: (Answers with AnswerReview status approved/rejected) / (User's completed answers)
   Combined: (Single choice with GT + Description approved/rejected) / (User's completed answers)
   ```

3. **Accuracy**: Quality metric across all question types
   ```
   (AnswerReview with status="approved" + Correct single-choice vs ground truth) / (Total reviewed answers)
   ```

#### For Reviewers

1. **GT Ratio**: Shows reviewer's contribution to ground truth creation
   ```
   (ReviewerGroundTruth by this user in project) / (Total questions × Total videos in project)
   ```

2. **Review Ratio**: Shows reviewer's contribution to answer review
   ```
   (AnswerReview by this user in project) / (Total completed description-type AnnotatorAnswers)
   ```

3. **GT Accuracy**: Shows quality of reviewer's ground truth (how often admin overrides)
   ```
   (ReviewerGroundTruth where original_answer_value = answer_value) / (User's total ReviewerGroundTruth)
   ```

#### For Meta-Reviewers

1. **Ratio Modified**: Shows admin's correction activity
   ```
   (ReviewerGroundTruth modified by this admin) / (Total ReviewerGroundTruth in project)
   ```

2. **Wrong Counts**: Track oversight and correction patterns
   ```
   Count of ReviewerGroundTruth where modified_by_admin_id = admin_user_id
   ```

### Status Definitions

- **Completed**: Has AnnotatorAnswer record
- **Reviewed**: For single choice: has ReviewerGroundTruth record for that question. For description: has AnswerReview record with status approved or rejected (not pending)
- **Ground Truth Set**: Has ReviewerGroundTruth record
- **Admin Modified**: ReviewerGroundTruth has modified_by_admin_id populated
- **Approved**: AnswerReview.status = "approved"
- **Rejected**: AnswerReview.status = "rejected" 
- **Pending**: AnswerReview.status = "pending"

---

## Service API Usage

All calculations use only the service APIs from services.py:

### Core Services Used
- **AuthService**: User management, project assignments, user lookup
- **AnnotatorService**: Get annotator answers and project statistics  
- **GroundTruthService**: Ground truth operations and accuracy calculations
- **ProjectService**: Project operations, get project annotators and metadata
- **Calculation Services**: For completion rates, accuracy rates, and progress metrics

### Key Service Methods
- `AuthService.get_all_users()` - Get user information and roles
- `AuthService.get_project_assignments()` - Get project role assignments
- `AnnotatorService.get_all_project_answers()` - Get all annotator answers for calculations
- `GroundTruthService.get_reviewer_accuracy()` - Calculate reviewer accuracy metrics
- `ProjectService.get_project_annotators()` - Get users who have submitted answers
- Database queries through services only - no direct model access

### Data Filtering
- All queries exclude `is_archived=True` records for both projects and project roles
- Only active assignments (ProjectUserRole.is_archived=False) are counted
- Timestamps use database timezone-aware fields

---

## File Structure Integration

### Database Connection
Uses DBURL environment variable for database connection:
```python
from label_pizza.db import init_database
init_database("DBURL")  # or custom environment variable name
```

### Project Structure
```
label_pizza/
├── google_sheets_export.py      # Export script (new)
├── services.py                  # All business logic APIs (includes new GoogleSheetsExportService)
├── models.py                    # Database models  
├── db.py                        # Database connection
└── ...                          # Other existing files
```

### Security Files (Add to .gitignore)
```gitignore
# Google Sheets API credentials and tokens
credentials.json
google_sheets_token.json

# Backup files (if any)
*.backup
```

---

## Manual vs Automatic Columns

### Automatic Columns (Updated on every export)
- All completion and review ratios
- Project and schema names
- Statistics counts (completed, reviewed, wrong)
- Timestamps
- Accuracy calculations

### Manual Columns (Preserved during updates)
- **Payment Tab**: Payment Timestamp, Base Salary, Bonus Salary
- **Feedback Tab**: Feedback to Annotator

**Important**: The export system will never overwrite data in manual columns. You can safely enter payment information and feedback without losing it on subsequent exports.

---

## Setup Guide

### **Prerequisites**

Before using the Google Sheets export system, you need:

1. **Google Account** (any Gmail account works)
2. **Google Cloud Project** (free to create)
3. **OAuth 2.0 credentials file** (`credentials.json`)
4. **Python dependencies** (already in your project)

### **Step 1: Create Google Cloud Project**

1. **Go to [Google Cloud Console](https://console.cloud.google.com/)**
2. **Sign in** with your Google account
3. **Create a new project**:
   - Click the project dropdown in the top navigation
   - Click **"New Project"**
   - Enter a **Project name** (e.g., "Sheets Export Tool")
   - Click **"Create"**
4. **Make sure your new project is selected** in the top dropdown

### **Step 2: Enable Required APIs**

1. **Go to [APIs & Services → Library](https://console.cloud.google.com/apis/library)**
2. **Enable Google Sheets API**:
   - Search for "Google Sheets API"
   - Click on it and click **"Enable"**
3. **Enable Google Drive API**:
   - Search for "Google Drive API" 
   - Click on it and click **"Enable"**

**⚠️ Important**: You need BOTH APIs enabled for the script to work properly.

### **Step 3: Configure OAuth Consent Screen**

1. **Go to [APIs & Services → OAuth consent screen](https://console.cloud.google.com/apis/credentials/consent)**

2. **Choose User Type**:
   - Select **"External"** (allows any Google account to use it)
   - Click **"Create"**

3. **Fill out App Information**:
   - **App name**: "Sheets Export Tool" (or any name you want)
   - **User support email**: Your email address
   - **Developer contact information**: Your email address
   - Leave other fields empty for now
   - Click **"Save and Continue"**

4. **Scopes section**:
   - Click **"Save and Continue"** (no changes needed)

5. **Test users section**:
   - Click **"Save and Continue"** (no changes needed)

6. **Summary**:
   - Click **"Back to Dashboard"**

### **Step 4: Create OAuth 2.0 Credentials**

1. **Go to [APIs & Services → Credentials](https://console.cloud.google.com/apis/credentials)**

2. **Create credentials**:
   - Click **"+ Create Credentials"**
   - Select **"OAuth client ID"**

3. **Configure the client**:
   - **Application type**: **"Desktop application"**
   - **Name**: "Sheets Export Client" (or any name)
   - Click **"Create"**

4. **Download credentials**:
   - A dialog will appear with your client ID and secret
   - Click **"Download JSON"**
   - Save the file as **`credentials.json`** in your project directory

**📁 File location**: Place `credentials.json` in your project root directory.

### **Step 5: Understanding the OAuth Flow**

#### **What happens when you run the script:**

1. **First time only**: Script will open a browser URL for authorization
2. **You'll see "This app isn't verified" warning** ⚠️
3. **Click "Advanced"** → **"Go to [Your App Name] (unsafe)"**
4. **Authorize the app** and grant permissions
5. **Copy the authorization code** from the failed localhost URL
6. **Paste it into the script** when prompted
7. **Script saves a token file** for future use

#### **The "Advanced" Workaround Process:**

```
1. Script prints authorization URL
2. You open it in browser
3. ⚠️ Warning: "This app isn't verified"
4. Click "Advanced" 
5. Click "Go to [App Name] (unsafe)"
6. Grant permissions (Sheets + Drive)
7. Browser redirects to http://localhost:8080/?code=ABC123...
8. ❌ Page shows "This site can't be reached" (EXPECTED!)
9. Copy the "code=ABC123..." part from the URL
10. Paste into script when prompted
```

#### **Why the "This app isn't verified" warning appears:**
- Google shows this warning for unverified apps that request sensitive scopes
- Sheets and Drive APIs are considered "sensitive"
- For personal/internal use, you can safely proceed through "Advanced"
- ✅ **Safe to click "Advanced" → "Go to [App Name] (unsafe)"** since you created the app yourself

### **Step 6: File Structure and Dependencies**

#### **Required Files Structure**
```
your-project/
├── credentials.json                 # OAuth credentials (you download this)
├── google_sheets_token.json         # Auto-generated after first auth
├── label_pizza/
│   ├── google_sheets_export.py      # The export script
│   ├── services.py                  # Your existing services (with new GoogleSheetsExportService class)
│   ├── models.py                    # Your existing models
│   ├── db.py                        # Database connection
│   └── ...
└── .env                            # Contains DBURL
```

#### **Add to .gitignore**
```gitignore
# Google Sheets API credentials and tokens
credentials.json
google_sheets_token.json

# Backup files (if any)
*.backup
```

**⚠️ Important**: Never commit `credentials.json` or `google_sheets_token.json` to version control as they contain sensitive authentication data.

### **Step 7: First Run (OAuth Authorization)**

```bash
# Navigate to your project directory
cd your-project

# First run (will trigger OAuth flow)
python label_pizza/google_sheets_export.py --master-sheet-id YOUR_SHEET_ID --database-url-name DBURL
```

#### **Expected OAuth Flow Output:**
```
🔄 Will require re-authorization...
='*60
GOOGLE SHEETS AUTHORIZATION REQUIRED
='*60
Required permissions:
  ✅ Google Sheets: Read, write, and manage spreadsheets
  ✅ Google Drive: Create and manage files
='*60
1. Go to this URL in your browser:
https://accounts.google.com/o/oauth2/auth?client_id=...

2. Authorize the application
3. Copy the authorization code from the URL
='*60

Enter the authorization code: [PASTE CODE HERE]
```

### **Step 8: Future Runs (Automatic)**

After the first successful authorization:

```bash
# Future runs use saved token automatically
python label_pizza/google_sheets_export.py --master-sheet-id YOUR_SHEET_ID --database-url-name DBURL
```

The script will automatically:
- Use the saved `google_sheets_token.json`
- Refresh expired tokens
- Only re-prompt if tokens are completely invalid

### **Common Setup Issues & Solutions**

#### **"This app isn't verified" Warning:**
```
SOLUTION: Click "Advanced" → "Go to [App Name] (unsafe)"
This is expected and safe for personal use.
```

#### **"Site can't be reached" after authorization:**
```
SOLUTION: This is EXPECTED! Copy the authorization code from the URL:
http://localhost:8080/?code=4/0AfJohXmY...
Copy everything after "code=" and before "&"
```

#### **"Invalid redirect_uri" error:**
```
SOLUTION: Make sure you selected "Desktop application" 
not "Web application" when creating OAuth credentials.
```

#### **"Token has expired" on future runs:**
```
SOLUTION: Delete google_sheets_token.json and re-authorize.
The script will automatically handle this.
```

#### **Permission denied errors:**
```
SOLUTION: Make sure both Google Sheets API and Google Drive API 
are enabled in your Google Cloud project.
```

#### **Database connection errors:**
```
SOLUTION: Verify DBURL in .env file is correct and database is accessible.
```

### **Setup Checklist**

Before running the script, verify:

- [ ] **Google Cloud project created**
- [ ] **Google Sheets API enabled**  
- [ ] **Google Drive API enabled**
- [ ] **OAuth consent screen configured**
- [ ] **Desktop OAuth credentials created**
- [ ] **credentials.json downloaded and placed in project root**
- [ ] **Database connection working** (DBURL in .env)
- [ ] **Master Google Sheet created** (get the ID from URL)
- [ ] **credentials.json and google_sheets_token.json added to .gitignore**

---

## Usage Examples

### Export for Database
```bash
python label_pizza/google_sheets_export.py --master-sheet-id 1ABC123XYZ --database-url-name DBURL
```

### Export with Custom Database URL
```bash
python label_pizza/google_sheets_export.py --master-sheet-id 1ABC123XYZ --database-url-name CUSTOM_DB_URL
```

### Export with Custom Credentials File
```bash
python label_pizza/google_sheets_export.py --master-sheet-id 1ABC123XYZ --credentials-file /path/to/credentials.json
```

### Master Sheet Only (Skip Individual User Sheets)
```bash
python label_pizza/google_sheets_export.py --master-sheet-id 1ABC123XYZ --skip-individual
```

### Resume from Specific User (After Rate Limit)
```bash
python label_pizza/google_sheets_export.py --master-sheet-id 1ABC123XYZ --resume-from "John Doe Annotator"
```

### Reading the Data

#### Payment Calculation Example
For annotator "John Doe" working on "Video Classification Project":
- Schema Name: "Content Safety Analysis"
- Completion Ratio: 85% → John completed 85% of possible annotations
- Accuracy: 92% → 92% of John's reviewed work was approved
- Base Salary: $500 (manual entry)
- Bonus: $50 (manual entry based on 92% accuracy)

#### Quality Monitoring Example
For reviewer "Jane Smith":
- GT Accuracy: 95% → Only 5% of Jane's ground truth was overridden by admins
- Review Completion: 30% → Jane reviewed 30% of available annotator answers
- Last Review Time: Shows when Jane last submitted a review or ground truth

#### Admin Oversight Example
For meta-reviewer "Admin User":
- Ratio Modified by This Admin: 8% → Admin corrected 8% of all ground truth
- Wrong by This Admin: 45 → Admin overrode 45 ground truth records
- Shows admin's correction patterns and oversight activity

#### Permission Management Example
When the export runs:
- Only users with User.user_type="admin" in database get editor access
- All other users get view-only access
- Permissions are automatically managed based on database roles
- No manual sharing required

---

## Best Practices

### For Administrators
1. **Run exports regularly** to keep data current
2. **Fill payment columns manually** after reviewing performance  
3. **Use accuracy metrics** to identify training needs
4. **Monitor completion ratios** to track project progress
5. **Verify admin permissions** are automatically managed based on database roles

### For Quality Control
1. **Check accuracy trends** across different annotators
2. **Review GT accuracy** to identify reviewer training needs
3. **Use feedback columns** to provide targeted guidance
4. **Monitor admin modification rates** to ensure appropriate oversight
5. **Track review ratios** to ensure adequate quality control

### For Payment Processing
1. **Verify completion ratios** before processing payments
2. **Apply bonus criteria** based on accuracy metrics
3. **Document payment dates** in manual columns
4. **Cross-reference with individual counts** for validation
5. **Trust automatic permission management** for secure access control

---

## Troubleshooting

### Common Issues

**Database Connection Errors**: 
- Ensure DBURL environment variable is set correctly in .env file
- Verify database is accessible and credentials are valid
- Check that database has required tables and schema

**Authentication Errors**: 
- Ensure OAuth 2.0 credentials are saved as `credentials.json`
- Verify both Sheets and Drive permissions were granted during OAuth flow
- Delete token file and re-authenticate if needed

**Missing Data**: 
- Ensure all users, projects, and answers exist in database
- Check that ProjectUserRole assignments are not archived
- Verify service APIs are returning expected data

**Incorrect Ratios**: 
- Check that is_archived flags are set correctly on projects and roles
- Verify question and video counts match expected project scope
- Ensure service calculations match database state

**Permission Errors**: 
- The script automatically manages permissions for admin users only
- Ensure you have access to create new Google Sheets in your Google Drive
- Master sheet will be shared automatically with database admins on export

**Export Failures**: 
- Rate limiting: Wait 1 hour or use `--resume-from 'User Name Role'` to continue
- Use `--skip-individual` to update only the master sheet
- Check Google Drive storage quota if sheet creation fails

### Data Validation

Always verify a few sample calculations manually:
1. Pick a user and project
2. Count actual answers/reviews in the database
3. Compare with sheet values  
4. Check that manual columns are preserved after re-export
5. Verify permission settings match database admin status