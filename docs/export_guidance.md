# Label Pizza Export Functions Documentation

## Overview

The export functions allow you to extract data from a Label Pizza database and save it as JSON files. These functions are useful for creating backups, migrating data between systems, or analyzing workspace contents. All export functions require an active SQLAlchemy session connected to your Label Pizza database.

### Prerequisite: Create your workspace folder

```
> mkdir ./export_workspace
```

## **1. `export_users`**

Function for exporting all users from the database to a JSON file

### 1.1 - Export users

Export all users including their authentication and status information.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_users

# Export all users to users.json
with label_pizza.db.SessionLocal() as session:
    export_users(session=session, output_path="./export_workspace/users.json")

# Output: users.json containing:
# [
#   {
#     "user_id": "user1@example.com",
#     "email": "user1@example.com", 
#     "password": "hashed_password",
#     "user_type": "human",        # "admin", "human", or "model"
#     "is_active": true
#   },
#   {
#     "user_id": "Model_1",
#     "email": null,               # model users don't have email
#     "password": "",              # model users don't have password
#     "user_type": "model",
#     "is_active": true
#   }
# ]
```

## **2. `export_videos`**

Function for exporting all videos from the database to a JSON file

### 2.1 - Export videos

Export all videos with their URLs, metadata, and status.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_videos

# Export all videos to videos.json
with label_pizza.db.SessionLocal() as session:
    export_videos(session=session, output_path="./export_workspace/videos.json")

# Output: videos.json containing:
# [
#   {
#     "video_uid": "sample_video.mp4",
#     "url": "https://example.com/videos/sample_video.mp4",
#     "video_metadata": {
#       "duration": 120,
#       "resolution": "1920x1080",
#       "fps": 30
#     },
#     "is_active": true
#   }
# ]
```

## **3. `export_question_groups`**

Function for exporting all question groups to separate JSON files in a folder

### 3.1 - Export question groups

Export all question groups with their questions to individual JSON files.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_question_groups

# Export all question groups to question_groups folder
with label_pizza.db.SessionLocal() as session:
    export_question_groups(
      session=session, 
      output_folder="./export_workspace/question_groups"
    )

# Output: Creates folder with files like:
# question_groups/
# ├── Safety_Assessment.json
# ├── Quality_Review.json
# └── Content_Analysis.json
```

### 3.2 - Question group file structure

Each question group file contains:

```json
{
  "title": "Safety Assessment",
  "display_title": "Safety Assessment Questions",
  "description": "Questions for assessing video safety",
  "is_reusable": true,
  "is_auto_submit": false,
  "verification_function": null,
  "questions": [
    {
      "qtype": "single",
      "text": "Is this content safe?",
      "display_text": "Content Safety",
      "options": ["safe", "unsafe", "uncertain"],
      "display_values": ["Safe", "Unsafe", "Uncertain"],
      "option_weights": [1.0, 0.0, 0.5],
      "default_option": "uncertain"
    },
    {
      "qtype": "description",
      "text": "Additional safety notes",
      "display_text": "Safety Notes",
      "default_option": ""
    }
  ]
}
```

## **4. `export_schemas`**

Function for exporting all schemas to a JSON file

### 4.1 - Export schemas

Export all schemas with their associated question groups.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_schemas

# Export all schemas to schemas.json
with label_pizza.db.SessionLocal() as session:
    export_schemas(session=session, output_path="./export_workspace/schemas.json")

# Output: schemas.json containing:
# [
#   {
#     "schema_name": "Video Analysis Schema",
#     "instructions_url": "https://example.com/instructions",
#     "has_custom_display": false,
#     "is_active": true,
#     "question_group_names": [
#       "Safety Assessment",
#       "Quality Review", 
#       "Content Analysis"
#     ]
#   }
# ]
```

## **5. `export_projects`**

Function for exporting all projects to a JSON file

### 5.1 - Export projects

Export all projects with their associated schemas and videos.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_projects

# Export all projects to projects.json
with label_pizza.db.SessionLocal() as session:
    export_projects(session=session, output_path="./export_workspace/projects.json")

# Output: projects.json containing:
# [
#   {
#     "project_name": "Safety Analysis Project",
#     "description": "Analyzing video content for safety",
#     "schema_name": "Video Analysis Schema",
#     "videos": [
#       "video1.mp4",
#       "video2.mp4", 
#       "video3.mp4"
#     ],
#     "is_active": true
#   }
# ]
```

## **6. `export_project_groups`**

Function for exporting all project groups to a JSON file

### 6.1 - Export project groups

Export all project groups with their associated projects.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_project_groups

# Export all project groups to project_groups.json
with label_pizza.db.SessionLocal() as session:
    export_project_groups(
      session=session, 
      output_path="./export_workspace/project_groups.json"
    )

# Output: project_groups.json containing:
# [
#   {
#     "project_group_name": "Content Moderation",
#     "description": "Projects related to content moderation",
#     "projects": [
#       "Safety Analysis Project",
#       "Violence Detection Project",
#       "Hate Speech Project"
#     ]
#   }
# ]
```

## **7. `export_assignments`**

Function for exporting all user project role assignments to a JSON file

### 7.1 - Export assignments

Export all user assignments including roles and weights.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_assignments

# Export all assignments to assignments.json
with label_pizza.db.SessionLocal() as session:
    export_assignments(session=session, output_path="./export_workspace/assignments.json")

# Output: assignments.json containing:
# [
#   {
#     "user_name": "annotator1@example.com",
#     "project_name": "Safety Analysis Project",
#     "role": "annotator",           # "annotator", "reviewer", or "admin"
#     "user_weight": 1.0,           # Weight for consensus calculations
#     "is_active": true
#   },
#   {
#     "user_name": "reviewer1@example.com", 
#     "project_name": "Safety Analysis Project",
#     "role": "reviewer",
#     "user_weight": 2.0,
#     "is_active": true
#   }
# ]
```

## **8. `export_annotations`**

Function for exporting all annotations grouped by question group to separate JSON files

### 8.1 - Export annotations

Export all annotations organized by question group.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_annotations

# Export all annotations to annotations folder
with label_pizza.db.SessionLocal() as session:
    export_annotations(session=session, output_folder="./export_workspace/annotations")

# Output: Creates folder with files like:
# annotations/
# ├── Safety_Assessment_annotations.json
# ├── Quality_Review_annotations.json
# └── Content_Analysis_annotations.json
```

### 8.2 - Annotation file structure

Each annotation file contains responses for that question group:

```json
[
  {
    "question_group_title": "Safety Assessment",
    "project_name": "Safety Analysis Project",
    "user_name": "annotator1@example.com",
    "video_uid": "video1.mp4",
    "answers": {
      "Is this content safe?": "safe",
      "Additional safety notes": "No safety concerns detected"
    },
    "is_ground_truth": false
  }
]
```

## **9. `export_ground_truths`**

Function for exporting all ground truth annotations grouped by question group to separate JSON files

### 9.1 - Export ground truths

Export all ground truth annotations organized by question group.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_ground_truths

# Export all ground truths to ground_truths folder
with label_pizza.db.SessionLocal() as session:
    export_ground_truths(session=session, output_folder="./export_workspace/ground_truths")

# Output: Creates folder with files like:
# ground_truths/
# ├── Safety_Assessment_ground_truths.json
# ├── Quality_Review_ground_truths.json
# └── Content_Analysis_ground_truths.json
```

### 9.2 - Ground truth file structure

Each ground truth file contains reviewer-verified answers:

```json
[
  {
    "question_group_title": "Safety Assessment",
    "project_name": "Safety Analysis Project", 
    "user_name": "reviewer1@example.com",
    "video_uid": "video1.mp4",
    "answers": {
      "Is this content safe?": "safe",
      "Additional safety notes": "Verified as safe content"
    },
    "is_ground_truth": true
  }
]
```

## **10. `export_workspace`**

Function for exporting entire workspace to a structured folder

### 10.1 - Export workspace

Export all workspace data in a single operation.

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_workspace

# Export entire workspace to workspace_backup folder
with label_pizza.db.SessionLocal() as session:
    export_workspace(session=session, output_folder="./export_workspace/workspace_backup")

# Output: Creates complete workspace structure:
# workspace_backup/
# ├── users.json
# ├── videos.json
# ├── schemas.json
# ├── projects.json
# ├── project_groups.json
# ├── assignments.json
# ├── question_groups/
# │   ├── Safety_Assessment.json
# │   ├── Quality_Review.json
# │   └── Content_Analysis.json
# ├── annotations/
# │   ├── Safety_Assessment_annotations.json
# │   ├── Quality_Review_annotations.json
# │   └── Content_Analysis_annotations.json
# └── ground_truths/
#     ├── Safety_Assessment_ground_truths.json
#     ├── Quality_Review_ground_truths.json
#     └── Content_Analysis_ground_truths.json
```

### 10.2 - Workspace structure

The exported workspace contains all necessary data to recreate or migrate your Label Pizza instance:

- **Core entities**: users.json, videos.json, schemas.json, projects.json
- **Organization**: project_groups.json, assignments.json
- **Content structure**: question_groups/ folder with individual question group definitions
- **Annotation data**: annotations/ and ground_truths/ folders with responses organized by question group

## Common Use Cases

### Regular Backup

```python
from datetime import datetime
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_workspace

# Create timestamped backup
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_folder = f"backup_{timestamp}"

with label_pizza.db.SessionLocal() as session:
    export_workspace(session=session, output_folder=backup_folder)
    print(f"Backup created in {backup_folder}")
```

### Migration Preparation

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_workspace

# Export for migration to new system
with label_pizza.db.SessionLocal() as session:
    export_workspace(
        session=session,
        output_folder="migration_export"
    )

# The exported data can then be imported into a new Label Pizza instance
# using the corresponding sync functions
```

### Data Analysis Setup

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_annotations, export_ground_truths

# Export specific components for analysis
with label_pizza.db.SessionLocal() as session:
    export_annotations(session=session, output_folder="analysis_annotations")
    export_ground_truths(session=session, output_folder="analysis_ground_truths")

# Use the exported JSON files for external analysis tools
```

### Selective Export

```python
from label_pizza.db import init_database
init_database("DBURL")

import label_pizza.db
from label_pizza.export_to_workspace import export_projects, export_assignments

# Export only specific components
with label_pizza.db.SessionLocal() as session:
    export_projects(session=session, output_path="projects_only.json")
    export_assignments(session=session, output_path="assignments_only.json")

# Useful for reviewing project structure without annotation data
```

## Important Notes

### File Naming Conventions

- Question group files use the title with spaces and special characters replaced with underscores
- Annotation and ground truth files append `_annotations` and `_ground_truths` respectively
- All files use UTF-8 encoding and pretty-printed JSON formatting