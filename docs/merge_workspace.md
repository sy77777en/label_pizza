# Label Pizza Merge Functions Documentation

## Overview

The merge functions allow you to combine data from two Label Pizza workspace folders with intelligent conflict resolution. When the same item exists in both folders, you can specify which folder should be used as the ground truth (gt_folder) to resolve conflicts. Each data type uses its appropriate unique key for identifying duplicates.

## **1. `merge_videos`**

Function for merging videos.json files from two folders

### 1.1 - Merge videos

Merge videos from two workspace folders, resolving conflicts using gt_folder parameter.

```python
from label_pizza.merge_utils import merge_videos

# Merge videos, using folder1 as ground truth for conflicts
merge_report = merge_videos(
    folder1_path="workspace1",
    folder2_path="workspace2", 
    output_path="merged_videos.json",
    gt_folder=1  # Use folder1 data when conflicts occur
)

print(f"Merged {merge_report['merged_count']} videos")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Output: merged_videos.json containing all videos from both folders
# Conflicts resolved using video_uid as unique key
```

### 1.2 - Understanding video merge conflicts

Videos are identified by their `video_uid`. When the same video_uid exists in both folders with different content, gt_folder determines which version to keep.

```python
# Use folder2 as ground truth for conflicts
merge_report = merge_videos(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_path="merged_videos.json", 
    gt_folder=2  # Use folder2 data when conflicts occur
)

# Merge report structure:
# {
#   "merged_count": 15,           # Total videos in merged file
#   "folder1_count": 10,          # Videos in folder1
#   "folder2_count": 8,           # Videos in folder2  
#   "conflicts_resolved": 3,      # Videos that existed in both with differences
#   "gt_folder_used": 2          # Which folder was used for conflict resolution
# }
```

## **2. `merge_users`**

Function for merging users.json files from two folders

### 2.1 - Merge users

Merge users from two workspace folders using user_id as the unique identifier.

```python
from label_pizza.merge_utils import merge_users

# Merge users, using folder2 as ground truth
merge_report = merge_users(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_path="merged_users.json",
    gt_folder=2
)

print(f"Merged {merge_report['merged_count']} users")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Users are identified by user_id
# Conflicts occur when same user_id has different email, password, etc.
```

## **3. `merge_schemas`**

Function for merging schemas.json files from two folders

### 3.1 - Merge schemas

Merge schemas using schema_name as the unique identifier.

```python
from label_pizza.merge_utils import merge_schemas

# Merge schemas
merge_report = merge_schemas(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_path="merged_schemas.json",
    gt_folder=1
)

# Schemas are identified by schema_name
# Conflicts occur when same schema has different question_group_names, etc.
```

## **4. `merge_projects`**

Function for merging projects.json files from two folders

### 4.1 - Merge projects

Merge projects using project_name as the unique identifier.

```python
from label_pizza.merge_utils import merge_projects

# Merge projects
merge_report = merge_projects(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_path="merged_projects.json",
    gt_folder=1
)

# Projects are identified by project_name
# Conflicts occur when same project has different schema_name, videos, etc.
```

## **5. `merge_project_groups`**

Function for merging project_groups.json files from two folders

### 5.1 - Merge project groups

Merge project groups using project_group_name as the unique identifier.

```python
from label_pizza.merge_utils import merge_project_groups

# Merge project groups
merge_report = merge_project_groups(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_path="merged_project_groups.json",
    gt_folder=2
)

# Project groups are identified by project_group_name
# Conflicts occur when same group has different projects list, description, etc.
```

## **6. `merge_assignments`**

Function for merging assignments.json files from two folders

### 6.1 - Merge assignments

Merge assignments using composite key of (user_name, project_name, role).

```python
from label_pizza.merge_utils import merge_assignments

# Merge assignments
merge_report = merge_assignments(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_path="merged_assignments.json",
    gt_folder=1
)

# Assignments are identified by combination of:
# user_name + project_name + role
# This allows same user to have different roles in different projects
# Conflicts occur when same assignment has different user_weight, is_active, etc.
```

## **7. `merge_question_groups`**

Function for merging question_groups folders from two folders

### 7.1 - Merge question groups

Merge question groups using title as the unique identifier.

```python
from label_pizza.merge_utils import merge_question_groups

# Merge question groups to output folder
merge_report = merge_question_groups(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_folder="merged_question_groups",
    gt_folder=2
)

# Output: Creates merged_question_groups/ folder with individual JSON files
# merged_question_groups/
# ├── Safety_Assessment.json
# ├── Quality_Review.json  
# └── Content_Analysis.json

# Question groups are identified by title
# Conflicts occur when same title has different questions, descriptions, etc.
```

## **8. `merge_annotations`**

Function for merging annotations folders from two folders

### 8.1 - Merge annotations

Merge annotations using composite key of (video_uid, project_name, question_group_title, user_name).

```python
from label_pizza.merge_utils import merge_annotations

# Merge annotations to output folder
merge_report = merge_annotations(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_folder="merged_annotations",
    gt_folder=1
)

# Output: Creates merged_annotations/ folder grouped by question group
# merged_annotations/
# ├── Safety_Assessment_annotations.json
# ├── Quality_Review_annotations.json
# └── Content_Analysis_annotations.json

# Annotations are identified by combination of:
# video_uid + project_name + question_group_title + user_name
# This ensures each user's annotation per video per project per question group is unique
```

## **9. `merge_ground_truths`**

Function for merging ground_truths folders from two folders

### 9.1 - Merge ground truths

Merge ground truths using composite key of (video_uid, project_name, question_group_title, user_name).

```python
from label_pizza.merge_utils import merge_ground_truths

# Merge ground truths to output folder
merge_report = merge_ground_truths(
    folder1_path="workspace1",
    folder2_path="workspace2",
    output_folder="merged_ground_truths",
    gt_folder=2
)

# Output: Creates merged_ground_truths/ folder grouped by question group  
# merged_ground_truths/
# ├── Safety_Assessment_ground_truths.json
# ├── Quality_Review_ground_truths.json
# └── Content_Analysis_ground_truths.json

# Ground truths use same key structure as annotations
```

## **10. `merge_workspace`**

Function for merging entire workspaces between two folders

### 10.1 - Complete workspace merge

Merge all components of two workspace folders in a single operation.

```python
from label_pizza.merge_utils import merge_workspace

# Merge entire workspaces using folder2 as ground truth
merge_report = merge_workspace(
    folder1_path="workspace1",
    folder2_path="workspace2", 
    output_folder="merged_workspace",
    gt_folder=2
)

# Output: Creates complete merged workspace structure:
# merged_workspace/
# ├── users.json
# ├── videos.json
# ├── schemas.json
# ├── projects.json
# ├── project_groups.json
# ├── assignments.json
# ├── question_groups/
# │   ├── Safety_Assessment.json
# │   └── Quality_Review.json
# ├── annotations/
# │   ├── Safety_Assessment_annotations.json
# │   └── Quality_Review_annotations.json
# └── ground_truths/
#     ├── Safety_Assessment_ground_truths.json
#     └── Quality_Review_ground_truths.json

print(f"Total conflicts resolved: {merge_report['summary']['total_conflicts_resolved']}")
```

### 10.2 - Understanding workspace merge report

The workspace merge returns detailed statistics for all components:

```python
merge_report = merge_workspace("workspace1", "workspace2", "merged", gt_folder=1)

# Complete merge report structure:
# {
#   "videos": {
#     "merged_count": 15,
#     "folder1_count": 10,  
#     "folder2_count": 8,
#     "conflicts_resolved": 3,
#     "gt_folder_used": 1
#   },
#   "users": {
#     "merged_count": 12,
#     "folder1_count": 8,
#     "folder2_count": 6,
#     "conflicts_resolved": 2,
#     "gt_folder_used": 1
#   },
#   ... // Similar structure for all components
#   "summary": {
#     "total_conflicts_resolved": 15,
#     "gt_folder_used": 1,
#     "merge_completed": true
#   }
# }
```

## Common Use Cases

### Merging Development and Production Data

```python
from label_pizza.merge_utils import merge_workspace

# Merge development workspace into production, keeping production as ground truth
merge_report = merge_workspace(
    folder1_path="development_workspace",
    folder2_path="production_workspace", 
    output_folder="combined_workspace",
    gt_folder=2  # Use production data for conflicts
)

print(f"Merged workspaces with {merge_report['summary']['total_conflicts_resolved']} conflicts")
```

### Combining Annotation Data from Multiple Teams

```python
from label_pizza.merge_utils import merge_annotations, merge_ground_truths

# Merge annotations from two annotation teams
annotations_report = merge_annotations(
    folder1_path="team_a_workspace",
    folder2_path="team_b_workspace",
    output_folder="combined_annotations", 
    gt_folder=1  # Prefer team A's data for conflicts
)

# Merge ground truths from reviewers
gt_report = merge_ground_truths(
    folder1_path="team_a_workspace", 
    folder2_path="team_b_workspace",
    output_folder="combined_ground_truths",
    gt_folder=2  # Prefer team B's ground truth data
)
```

## Important Notes

### Conflict Resolution Strategy

- **gt_folder=1**: Use folder1's data when conflicts occur
- **gt_folder=2**: Use folder2's data when conflicts occur
- **Conflicts**: Occur when the same unique key exists in both folders with different content
- **No conflicts**: Items that exist in only one folder are always included

### Unique Key Definitions

- **videos.json**: `video_uid`
- **users.json**: `user_id`
- **schemas.json**: `schema_name`
- **projects.json**: `project_name`
- **project_groups.json**: `project_group_name`
- **assignments.json**: `user_name + project_name + role` (composite key)
- **question_groups**: `title`
- **annotations**: `video_uid + project_name + question_group_title + user_name` (composite key)
- **ground_truths**: `video_uid + project_name + question_group_title + user_name` (composite key)

### File Structure Preservation

- **JSON files**: Merged into single output files maintaining original structure
- **Folder-based data**: Question groups, annotations, and ground truths maintain their folder structure with files grouped appropriately
- **UTF-8 encoding**: All output files use UTF-8 encoding with proper JSON formatting

### Error Handling

- Missing folders or files are treated as empty datasets
- Invalid JSON files are skipped with error messages
- Output directories are created automatically if they don't exist