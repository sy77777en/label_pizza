## Export your own Workspace from existing database

##### All the exports could be directly used to sync to a existing database

### 0. Export `workspace` folder

> Export the whole folder that contains everything for running `sync_from_folder.py`

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_workspace

workspace_folder = './workspace_export'
with label_pizza.db.SessionLocal() as sess:
    export_workspace(session=sess, output_folder=workspace_folder)
```

### 1. Export `users.json`

> Export the `users.json` that contains all the user information

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_users

users_path = './workspace_export/users.json'
with label_pizza.db.SessionLocal() as sess:
    export_users(session=sess, output_path=users_path)
```

### 2. Export `videos.json`

> Export the `videos.json` that contains all the video information

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_videos

videos_path = './workspace_export/videos.json'
with label_pizza.db.SessionLocal() as sess:
    export_videos(session=sess, output_path=videos_path)
```

### 3. Export `question_groups` folder

> Export the `question_groups` folder, each file inside this folder represents a question group

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_question_groups

question_groups_folder = './workspace_export/question_groups'
with label_pizza.db.SessionLocal() as sess:
    export_videos(session=sess, output_folder=question_groups_folder)
```

### 4. Export `schemas.json`

> Export the `schemas.json` that contains all the schema information

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_schemas

schemas_path = './workspace_export/schemas.json'
with label_pizza.db.SessionLocal() as sess:
    export_videos(session=sess, output_path=schemas_path)
```

### 5. Export `projects.json`

> Export the `projects.json` that contains all the project information

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_schemas

schemas_path = './workspace_export/schemas.json'
with label_pizza.db.SessionLocal() as sess:
    export_videos(session=sess, output_path=schemas_path)
```

### 6. Export `project_groups.json`

> Export the `project_groups.json` that contains all the project group information

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_project_groups

project_groups_path = './workspace_export/project_groups.json'
with label_pizza.db.SessionLocal() as sess:
    export_videos(session=sess, output_path=project_groups_path)
```

### 7. Export `assignments.json`

> Export the `assignments.json` that contains all the assignment information

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_assignments

assignments_path = './workspace_export/project_groups.json'
with label_pizza.db.SessionLocal() as sess:
    export_videos(session=sess, output_path=assignments_path)
```

### 8. Export `annotations` folder

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_annotations

annotations_folder = './workspace_export/annotations'
with label_pizza.db.SessionLocal() as sess:
    export_workspace(session=sess, output_folder=annotations_folder)
```

### 9. Export `ground_truths` folder

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_to_workspace import export_ground_truths

ground_truths_folder = './workspace_export/ground_truths'
with label_pizza.db.SessionLocal() as sess:
    export_workspace(session=sess, output_folder=ground_truths_folder)
```

