# Label Pizza Compare Functions Documentation

## Overview
The compare functions allow you to compare JSON files and folders between two Label Pizza workspace exports and generate detailed diff reports. These functions help identify differences in data between different workspace states or backups.

## **1. `compare_videos`**
Function for comparing videos.json files between two folders

### 1.1 - Basic comparison
Compare videos.json files and get a boolean result indicating if they are identical.

```python
from label_pizza.compare_utils import compare_videos

# Compare videos between two workspace folders
folder1_path = "path/to/workspace1"
folder2_path = "path/to/workspace2"

is_identical = compare_videos(folder1_path, folder2_path)
print(f"Videos are identical: {is_identical}")
```

### 1.2 - Generate detailed diff report
Compare videos and generate a detailed JSON diff report file.

```python
# Compare and write diff report to diff_videos.json
is_identical = compare_videos(
    folder1_path="path/to/workspace1", 
    folder2_path="path/to/workspace2",
    write_to_file=True
)

# The diff_videos.json file will contain:
# {
#   "identical": false,
#   "folder1_only": [...],     # Videos only in folder1
#   "folder2_only": [...],     # Videos only in folder2
#   "different": [...],        # Videos with same video_uid but different content
#   "summary": {
#     "folder1_count": 10,
#     "folder2_count": 12,
#     "differences_found": 3
#   }
# }
```

## **2. `compare_users`**
Function for comparing users.json files between two folders

### 2.1 - Basic comparison
Compare users.json files using user_id as the unique identifier.

```python
from label_pizza.compare_utils import compare_users

folder1_path = "path/to/workspace1"
folder2_path = "path/to/workspace2"

is_identical = compare_users(folder1_path, folder2_path)
print(f"Users are identical: {is_identical}")
```

### 2.2 - Generate detailed diff report
Compare users and generate a detailed JSON diff report file.

```python
# Compare and write diff report to diff_users.json
is_identical = compare_users(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2", 
    write_to_file=True
)

# The diff_users.json file will contain:
# {
#   "identical": false,
#   "folder1_only": [...],     # Users only in folder1
#   "folder2_only": [...],     # Users only in folder2
#   "different": [...],        # Users with same user_id but different content
#   "summary": {
#     "folder1_count": 5,
#     "folder2_count": 7,
#     "differences_found": 2
#   }
# }
```

## **3. `compare_schemas`**
Function for comparing schemas.json files between two folders

### 3.1 - Basic comparison
Compare schemas.json files using schema_name as the unique identifier.

```python
from label_pizza.compare_utils import compare_schemas

is_identical = compare_schemas(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 3.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_schemas.json
is_identical = compare_schemas(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)
```

## **4. `compare_projects`**
Function for comparing projects.json files between two folders

### 4.1 - Basic comparison
Compare projects.json files using project_name as the unique identifier.

```python
from label_pizza.compare_utils import compare_projects

is_identical = compare_projects(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 4.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_projects.json
is_identical = compare_projects(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)
```

## **5. `compare_project_groups`**
Function for comparing project_groups.json files between two folders

### 5.1 - Basic comparison
Compare project_groups.json files using project_group_name as the unique identifier.

```python
from label_pizza.compare_utils import compare_project_groups

is_identical = compare_project_groups(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 5.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_project_groups.json
is_identical = compare_project_groups(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)
```

## **6. `compare_assignments`**
Function for comparing assignments.json files between two folders

### 6.1 - Basic comparison
Compare assignments.json files using a composite key of user_name + project_name + role.

```python
from label_pizza.compare_utils import compare_assignments

is_identical = compare_assignments(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 6.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_assignments.json
is_identical = compare_assignments(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)

# Note: Assignments use a composite key because a user can have 
# multiple assignments to different projects with different roles
```

## **7. `compare_question_groups`**
Function for comparing question_groups folders between two folders

### 7.1 - Basic comparison
Compare all JSON files in the question_groups folders using title as the unique identifier.

```python
from label_pizza.compare_utils import compare_question_groups

is_identical = compare_question_groups(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 7.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_question_groups.json
is_identical = compare_question_groups(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)

# This function loads all JSON files from the question_groups folder
# and compares them using the "title" field as the unique identifier
```

## **8. `compare_annotations`**
Function for comparing annotations folders between two folders

### 8.1 - Basic comparison
Compare all JSON files in the annotations folders using a composite key.

```python
from label_pizza.compare_utils import compare_annotations

is_identical = compare_annotations(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 8.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_annotations.json
is_identical = compare_annotations(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)

# Note: Annotations use a composite key of:
# video_uid + project_name + question_group_title + user_name
# This allows for multiple annotations per video by different users
```

## **9. `compare_ground_truths`**
Function for comparing ground_truths folders between two folders

### 9.1 - Basic comparison
Compare all JSON files in the ground_truths folders using a composite key.

```python
from label_pizza.compare_utils import compare_ground_truths

is_identical = compare_ground_truths(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)
```

### 9.2 - Generate detailed diff report
```python
# Compare and write diff report to diff_ground_truths.json
is_identical = compare_ground_truths(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)

# Note: Ground truths use the same composite key as annotations:
# video_uid + project_name + question_group_title + user_name
```

## **10. `compare_workspace`**
Function for comparing entire workspaces between two folders

### 10.1 - Complete workspace comparison
Compare all components of two workspace folders in a single operation.

```python
from label_pizza.compare_utils import compare_workspace

# Compare entire workspaces (all files and folders)
is_identical = compare_workspace(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2"
)

print(f"Workspaces are identical: {is_identical}")

# This function runs all individual compare functions:
# - compare_videos
# - compare_users  
# - compare_schemas
# - compare_projects
# - compare_project_groups
# - compare_assignments
# - compare_question_groups
# - compare_annotations
# - compare_ground_truths

# Each comparison generates its own diff file (diff_*.json)
```

### 10.2 - Complete workspace comparison with diff reports

Compare workspaces and generate detailed diff reports for all components.

```python
# Compare entire workspaces and generate all diff files
is_identical = compare_workspace(
    folder1_path="path/to/workspace1",
    folder2_path="path/to/workspace2",
    write_to_file=True
)

print(f"Workspaces are identical: {is_identical}")

# This function runs all individual compare functions with write_to_file=True:
# - compare_videos(folder1_path, folder2_path, write_to_file=True)
# - compare_users(folder1_path, folder2_path, write_to_file=True)
# - compare_schemas(folder1_path, folder2_path, write_to_file=True)
# - compare_projects(folder1_path, folder2_path, write_to_file=True)
# - compare_project_groups(folder1_path, folder2_path, write_to_file=True)
# - compare_assignments(folder1_path, folder2_path, write_to_file=True)
# - compare_question_groups(folder1_path, folder2_path, write_to_file=True)
# - compare_annotations(folder1_path, folder2_path, write_to_file=True)
# - compare_ground_truths(folder1_path, folder2_path, write_to_file=True)

# All diff files will be generated in the current directory
```

### 10.3 - Understanding the results

When `write_to_file=True` is used, each comparison generates a specific diff file:

```
workspace_comparison/
├── diff_videos.json
├── diff_users.json
├── diff_schemas.json
├── diff_projects.json
├── diff_project_groups.json
├── diff_assignments.json
├── diff_question_groups.json
├── diff_annotations.json
└── diff_ground_truths.json
```

Each diff file contains:

- `identical`: Boolean indicating if the data is identical
- `folder1_only`: Items that exist only in the first folder
- `folder2_only`: Items that exist only in the second folder
- `different`: Items that exist in both but have different content
- `summary`: Count statistics and differences found

## Common Use Cases

### Backup Verification

```python
# Verify that a backup matches the original workspace
is_identical = compare_workspace(
    folder1_path="original_workspace", 
    folder2_path="backup_workspace",
    write_to_file=True
)

if is_identical:
    print("Backup is identical to original")
else:
    print("Backup differs from original - check diff files")
```

### Migration Validation

```python
# Validate data after migrating between systems
compare_workspace(
    folder1_path="old_system_export", 
    folder2_path="new_system_export",
    write_to_file=True
)
# Review generated diff_*.json files to understand differences
```

### Version Comparison

```python
# Compare different versions of the same workspace
compare_workspace(
    folder1_path="workspace_v1", 
    folder2_path="workspace_v2",
    write_to_file=True
)
# Use diff files to understand what changed between versions
```

