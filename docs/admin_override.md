# Label Pizza -- Admin Override

This guide explains how to rename or delete data in the database.

# ⚠️ Danger Zone: Rename or Delete Data (Use with Caution)

By default, **Label Pizza does not allow deleting** users, videos, questions, question groups, schemas, or projects through standard syncing. These records are meant to be **archived**, not removed. This prevents accidental data loss and avoids breaking dependencies—for example, deleting a schema that is still used by a project.

Similarly, **you cannot rename core identifiers** like a video UID, question text, schema name, or project name through the web interface. These are used as stable, unique identifiers when syncing and updating the database.

That said, if you **really** need to delete or rename something, you can use the force override functions below. These bypass all safeguards and give you full control.

> **Warning:** These functions are irreversible. Always back up your database first.


### Before Using This

All functions in this module provide automatic backup functionality. You can configure the backup settings at the beginning of the file:

```
# Backup configuration
BACKUP_DIR = "./db_backups"
MAX_BACKUPS = 10
```

**Important:** All destructive operations automatically create timestamped backups before execution. The backup system requires `backup_restore.py` to be available.

#### setup

```python
from label_pizza.db import init_database
init_database()  # Uses DBURL environment variable
from label_pizza.models import *  # Import all models
```

### Functions

#### 1. change_question_text(original_text, new_text)

Updates `text` in `Question` table.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.force_override import change_question_text

change_question_text(
  original_text="original_text",
  new_text="new_text"
)
```

> **Important:** This will update the question across all question groups and projects that use it.

**Database Operations:**

- **Table:** `Question`
- **Updates:** `text` and `display_text` fields
- **Query:** `UPDATE questions SET text = ?, display_text = ? WHERE text = ?`
- **Validation:** Checks if question exists and new text doesn't conflict

#### 2. update_question_group_titles(group_id, new_title, new_display_title=None)

Update the `title` and `display_title` in `QuestionGroup` table.

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.force_override import update_question_group_titles

update_question_group_titles(
    group_id=5,
    new_title="Human Detection V2",
    new_display_title="Human Detection (Updated)"
)
```

**Database Operations:**

- **Table:** `QuestionGroup`
- **Updates:** `title` and `display_title` fields
- **Query:** `UPDATE question_groups SET title = ?, display_title = ? WHERE id = ?`
- **Validation:** Checks if group exists and new title doesn't conflict with existing groups

#### 3. change_project_schema_simple(project_id, new_schema_id)

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.force_override import change_project_schema_simple

result = change_project_schema_simple(
    project_id=3,
    new_schema_id=7
)
```

> Changes a project's schema and automatically cleans up incompatible data.

**Database Operations:**

1. **Finds removed questions** by comparing old vs new schema
2. Deletes ALL custom displays:
   - `DELETE FROM project_video_question_displays WHERE project_id = ?`
3. Deletes incompatible answers:
   - `DELETE FROM annotator_answers WHERE project_id = ? AND question_id IN (?)`
4. Deletes incompatible ground truth:
   - `DELETE FROM reviewer_ground_truth WHERE project_id = ? AND question_id IN (?)`
5. Updates project schema:
   - `UPDATE projects SET schema_id = ? WHERE id = ?`
6. Resets user completion status: 
   - `UPDATE project_user_roles SET completed_at = NULL WHERE project_id = ?`

## ⚠️ Force Delete Functions

### 1. Delete from Table `User`

#### `delete_user_by_id`

```
from label_pizza.force_override import delete_user_by_id

user_id = 3
delete_user_by_id(user_id=user_id)
```

#### `delete_user_by_user_id_str`

```
from label_pizza.force_override import delete_user_by_user_id_str

user_id_str = 'User 1'
delete_user_by_user_id_str(user_id=user_id_str)
```

### 2. Delete from Table `Video`

#### `delete_video_by_id`

```
from label_pizza.force_override import delete_video_by_id

video_id = 1
delete_video_by_id(video_id=video_id)
```

#### `delete_video_by_uid`

```
from label_pizza.force_override import delete_video_by_uid

video_uid = 'pizza.mp4'
delete_video_by_id(video_uid=video_uid)
```

### 3. Delete from Table `VideoTag`

#### `delete_video_tag_by_video_id`

```
from label_pizza.force_override import delete_video_tag_by_video_id

video_id = 1
delete_video_tag_by_video_id(video_id=video_id)
```

### 4. Delete from Table `QuestionGroup`

#### `delete_question_group_by_id`

```
from label_pizza.force_override import delete_question_group_by_id

question_group_id = 1
delete_question_group_by_id(question_group_id=question_group_id)
```

#### `delete_question_group_by_title`

```
from label_pizza.force_override import delete_question_group_by_title

title = 'Pizza'
delete_question_group_by_title(title=title)
```

### 5. Delete from table `Question`

#### `delete_question_by_id`

```
from label_pizza.force_override import delete_question_by_id

question_id = 1
delete_question_by_id(question_id=question_id)
```

#### `delete_question_by_text`

```
from label_pizza.force_override import delete_question_by_text

text = 'Number of pizzas?'
delete_question_by_text(text=text)
```

**`delete_question_group_question_by_question_id`**

python

```python
from label_pizza.force_override import delete_question_group_questions_by_question_id

question_id = 1
delete_question_group_questions_by_question_id(question_id=question_id)
```

**`delete_question_group_question_by_group_id`**

python

```python
from label_pizza.force_override import delete_question_group_questions_by_group_id

question_group_id = 1
delete_question_group_questions_by_group_id(question_group_id=question_group_id)
```

**`delete_question_group_question_by_both_ids`**

python

```python
from label_pizza.force_override import delete_question_group_questions_by_both_ids

question_group_id = 1
question_id = 5
delete_question_group_questions_by_both_ids(question_group_id=question_group_id, question_id=question_id)
```

### 6. Delete from Table `Schema`

#### **`delete_schema_by_id`**

```python
from label_pizza.force_override import delete_schema_by_id

schema_id = 1
delete_schema_by_id(schema_id=schema_id)
```

**`delete_schema_by_name`**

python

```python
from label_pizza.force_override import delete_schema_by_name

name = 'Default Schema'
delete_schema_by_name(name=name)
```

### 7. Delete from Table `SchemaQuestionGroup`

#### **`delete_schema_question_group_by_schema_id`**

```python
from label_pizza.force_override import delete_schema_question_groups_by_schema_id

schema_id = 1
delete_schema_question_groups_by_schema_id(schema_id=schema_id)
```

#### **`delete_schema_question_group_by_question_group_id`**

```python
from label_pizza.force_override import delete_schema_question_groups_by_question_group_id

question_group_id = 1
delete_schema_question_groups_by_question_group_id(question_group_id=question_group_id)
```

#### **`delete_schema_question_group_by_both_ids`**

```python
from label_pizza.force_override import delete_schema_question_groups_by_both_ids

schema_id = 1
question_group_id = 5
delete_schema_question_groups_by_both_ids(schema_id=schema_id, question_group_id=question_group_id)
```

### 8. Delete from Table `Project`

#### **`delete_project_by_id`**

```python
from label_pizza.force_override import delete_project_by_id

project_id = 1
delete_project_by_id(project_id=project_id)
```

#### **`delete_project_by_name`**

```python
from label_pizza.force_override import delete_project_by_name

name = 'My Project'
delete_project_by_name(name=name)
```

### 9. Delete from Table `ProjectVideo`

#### **`delete_project_video_by_project_id`**

```python
from label_pizza.force_override import delete_project_video_by_project_id

project_id = 1
delete_project_video_by_project_id(project_id=project_id)
```

#### **`delete_project_video_by_video_id`**

```python
from label_pizza.force_override import delete_project_video_by_video_id

video_id = 1
delete_project_video_by_video_id(video_id=video_id)
```

#### **`delete_project_video_by_both_ids`**

```python
from label_pizza.force_override import delete_project_video_by_both_ids

project_id = 1
video_id = 5
delete_project_video_by_both_ids(project_id=project_id, video_id=video_id)
```

### 10. Delete from Table `ProjectUserRole`

#### **`delete_project_user_role_by_project_id`**

```python
from label_pizza.force_override import delete_project_user_role_by_project_id

project_id = 1
delete_project_user_role_by_project_id(project_id=project_id)
```

#### **`delete_project_user_role_by_user_id`**

```python
from label_pizza.force_override import delete_project_user_role_by_user_id

user_id = 1
delete_project_user_role_by_user_id(user_id=user_id)
```

#### **`delete_project_user_role_by_both_ids`**

```python
from label_pizza.force_override import delete_project_user_role_by_both_ids

project_id = 1
user_id = 5
delete_project_user_role_by_both_ids(project_id=project_id, user_id=user_id)
```

#### 11. Delete from Table `ProjectGroup`

#### **`delete_project_group_by_id`**

```python
from label_pizza.force_override import delete_project_group_by_id

project_group_id = 1
delete_project_group_by_id(project_group_id=project_group_id)
```

#### **`delete_project_group_by_name`**

```python
from label_pizza.force_override import delete_project_group_by_name

name = 'Project Group 1'
delete_project_group_by_name(name=name)
```

### 12. Delete from Table `ProjectGroupProject`

#### **`delete_project_group_project_by_group_id`**

```python
from label_pizza.force_override import delete_project_group_project_by_group_id

project_group_id = 1
delete_project_group_project_by_group_id(project_group_id=project_group_id)
```

#### **`delete_project_group_project_by_project_id`**

```python
from label_pizza.force_override import delete_project_group_project_by_project_id

project_id = 1
delete_project_group_project_by_project_id(project_id=project_id)
```

#### **`delete_project_group_project_by_both_ids`**

```python
from label_pizza.force_override import delete_project_group_project_by_both_ids

project_group_id = 1
project_id = 5
delete_project_group_project_by_both_ids(project_group_id=project_group_id, project_id=project_id)
```

### 13. Delete from Table `ProjectVideoQuestionDisplay`

#### **`delete_project_video_question_display_by_project_id`**

```python
from label_pizza.force_override import delete_project_video_question_display_by_project_id

project_id = 1
delete_project_video_question_display_by_project_id(project_id=project_id)
```

#### **`delete_project_video_question_displays_by_video_id`**

```python
from label_pizza.force_override import delete_project_video_question_displays_by_video_id

video_id = 1
delete_project_video_question_displays_by_video_id(video_id=video_id)
```

#### **`delete_project_video_question_displays_by_question_id`**

```python
from label_pizza.force_override import delete_project_video_question_displays_by_question_id

question_id = 1
delete_project_video_question_displays_by_question_id(question_id=question_id)
```

#### **`delete_project_video_question_display_by_ids`**

```python
from label_pizza.force_override import delete_project_video_question_display_by_ids

project_id = 1
video_id = 5
question_id = 10
delete_project_video_question_display_by_ids(project_id=project_id, video_id=video_id, question_id=question_id)
```

### 14. Delete from Table `AnnotatorAnswer`

#### **`delete_annotator_answer_by_project_id`**

```python
from label_pizza.force_override import delete_annotator_answer_by_project_id

project_id = 1
delete_annotator_answer_by_project_id(project_id=project_id)
```

#### **`delete_annotator_answers_by_video_id`**

```python
from label_pizza.force_override import delete_annotator_answers_by_video_id

video_id = 1
delete_annotator_answers_by_video_id(video_id=video_id)
```

#### **`delete_annotator_answers_by_user_id`**

```python
from label_pizza.force_override import delete_annotator_answers_by_user_id

user_id = 1
delete_annotator_answers_by_user_id(user_id=user_id)
```

### 15. Delete from Table `ReviewerGroundTruth`

#### **`delete_reviewer_ground_truth_by_project_id`**

```python
from label_pizza.force_override import delete_reviewer_ground_truth_by_project_id

project_id = 1
delete_reviewer_ground_truth_by_project_id(project_id=project_id)
```

#### **`delete_reviewer_ground_truth_by_video_id`**

```python
from label_pizza.force_override import delete_reviewer_ground_truth_by_video_id

video_id = 1
delete_reviewer_ground_truth_by_video_id(video_id=video_id)
```

#### **`delete_reviewer_ground_truth_by_reviewer_id`**

```python
from label_pizza.force_override import delete_reviewer_ground_truth_by_reviewer_id

reviewer_id = 1
delete_reviewer_ground_truth_by_reviewer_id(reviewer_id=reviewer_id)
```

---

[← Back to start](start_here.md) 