## Quick Start

#### Folders Structure

> This directory provides a compact, end‑to‑end example of the files required to set up a video‑annotation workflow.  It is intended as a reference: copy the pieces you need, adjust the JSON to match your own questions and videos, then import the data with the project‑creation scripts.

```
example/
├── assignments.json
├── projects.json
├── schemas.json
├── users.json
├── videos.json
├── annotations/
│   ├── human_annotations.json
│   └── pizza_annotations.json
├── question_groups/
│   ├── nsfw.json
│   ├── subjects.json
│   └── pizzas.json
└── reviews/
    ├── human_annotations.json
    └── pizza_annotations.json
```

## File‑by‑file guide

### `videos.json`

Contains one entry per video that should be available to projects.

```
[
  {
    "url": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/d0yGdNEWdn0.0.7.mp4",
    "metadata": {
      "original_name": "d0yGdNEWdn0",
      "license": "youtube educational license"
    }
  },
  {
    "url": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/oVXs1Lo_4pk_2400_4200.0.0.mp4",
    "metadata": {
      "original_name": "oVXs1Lo_4pk",
      "license": "youtube educational license"
    }
  }
]
```

The **`url` **field must be directly downloadable by your annotation platform.  Everything under **`metadata`** is passed through untouched; store provenance or licence notes here.

### `users.json`

Defines every account that should be present before projects are created.  Accepted `user_type` values are `admin`, `human`, and `model`.

```
[
    {
        "user_id": "Admin 1",
        "email": "admin1@example.com",
        "password": "admin111",
        "user_type": "admin"
    },
    {
        "user_id": "User 1",
        "email": "user1@example.com",
        "password": "user111",
        "user_type": "human"
    },
    {
        "user_id": "User 2",
        "email": "user2@example.com",
        "password": "user222",
        "user_type": "human"
    },
    {
        "user_id": "Chancharik's Robot",
        "password": "cmitraRobot",
        "user_type": "model"
    }
]
```

### `assignments.json`

Grants a **role** (annotator, reviewer, etc.) to a user within a specific project.

```
[
  {
    "user_email": "admin1@example.com",
    "project_name": "Pizza test 0",
    "role": "reviewer"
  },
  {
    "user_email": "admin1@example.com",
    "project_name": "Human test 0",
    "role": "reviewer"
  },
  {
    "user_email": "user1@example.com",
    "project_name": "Pizza test 0",
    "role": "reviewer"
  },
  {
    "user_email": "user1@example.com",
    "project_name": "Human test 0",
    "role": "reviewer"
  }
]
```

### `question_groups/`

Each file defines a *single* group of questions that can be reused across multiple schemas.  The example below shows a trimmed‑down `weather.json`.

```
{
    "title": "Human Questions",
    "description": "Detect and describe all humans in the video.",
    "is_reusable": false,
    "is_auto_submit": false,
    "verification_function": "check_human_description",
    "questions": [
        {
            "text": "Number of people?",
            "qtype": "single",
            "required": true,
            "options": [
                "0",
                "1",
                "2",
                "Complex (others)"
            ],
            "display_values": [
                "0",
                "1",
                "2",
                "Complex (others)"
            ],
            "default_option": "0",
            "display_text": "Number of people?"
        },
        {
            "text": "If there are people, describe them.",
            "qtype": "description",
            "required": false,
            "options": null,
            "display_values": null,
            "default_option": null,
            "display_text": "If there are people, describe them."
        }
    ]
}
```

### `schemas.json`

A schema is simply a list of question‑group titles.  Projects reference schemas by name.

```
[
  {
    "schema_name": "Humans in Video",
    "question_group_names": [
      "Human Questions", "NSFW Question"
    ]
  },
  {
    "schema_name": "Pizzas in Video",
    "question_group_names": [
      "Pizza Questions", "NSFW Question"
    ]
  }
]
```

### `projects.json`

Binds a schema to a collection of videos.  Video filenames must match the `original_name` (plus extension) in `videos.json`.

```
[
  {
    "project_name": "Human test 0",
    "schema_name": "Humans in Video",
    "videos": [
      "d0yGdNEWdn0.0.7.mp4",
      "oVXs1Lo_4pk_2400_4200.0.0.mp4"
    ]
  },
  {
    "project_name": "Pizza test 0",
    "schema_name": "Pizzas in Video",
    "videos": [
      "d0yGdNEWdn0.0.7.mp4",
      "oVXs1Lo_4pk_2400_4200.0.0.mp4"
    ]
  }
]
```

### `annotations/` and `reviews/`

Both directories share the same JSON structure.  Use `annotations/` for initial answers and `reviews/` for ground‑truth or adjudicated results.

```
[
  {
    "question_group_title": "Human Questions",
    "project_name": "Human test 0",
    "user_email": "admin1@example.com",
    "video_uid": "d0yGdNEWdn0.0.7.mp4",
    "answers": {
      "Number of people?": "0",
      "If there are people, describe them.": ""
    }
  },
  {
    "question_group_title": "Human Questions",
    "project_name": "Human test 0",
    "user_email": "user1@example.com",
    "video_uid": "oVXs1Lo_4pk_2400_4200.0.0.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "The person is tall and slim."
    }
  }
]
```

```
[
  {
    "question_group_title": "Human Questions",
    "project_name": "Human test 0",
    "user_email": "admin1@example.com",
    "reviewer_email": "user1@example.com",
    "video_uid": "d0yGdNEWdn0.0.7.mp4",
    "answers": {
      "Number of people?": "0",
      "If there are people, describe them.": ""
    }
  },
  {
    "question_group_title": "Human Questions",
    "project_name": "Human test 0",
    "user_email": "user1@example.com",
    "reviewer_email": "admin1@example.com",
    "video_uid": "oVXs1Lo_4pk_2400_4200.0.0.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "The person is tall and slim."
    }
  }
]
```



## Getting Started

Follow the steps **in order** so that every dependency (users → question groups → schemas → videos → projects → assignments → annotations) is satisfied.

#### Step 1 Upload Videos

> Upload all the videos from `./videos` folder. Videos should be stored in `.json` file.

```
from scripts.upload_utils import upload_videos
import json

# quickest: point to the JSON file
upload_videos(videos_path="./example/videos.json")

# alternative: pre-load the file yourself
with open("./example/videos.json") as f:
    videos = json.load(f)
upload_videos(videos_data=videos)
```

#### Step 2 Upload Users

> Upload all the users from `./users` folder. Users should be stored in `.json` file.

```
from scripts.upload_utils import upload_users
import json

upload_users(users_path="./example/users.json")

# or
with open("./example/users.json") as f:
    users = json.load(f)
upload_users(users_data=users)
```

#### Step 3 Register question groups and schemas

> Upload all the schemas / question groups / questions from `./schemas` and `./question_groups` folders. They should be stored in `.json` file.

```
from scripts.upload_utils import create_schemas

create_schemas(
    schemas_path="./example/schemas.json",
    question_groups_folder="./example/question_groups"
)
```

#### Optional: (Optional) Build `projects.json` from annotations

> Skip this step if `projects.json` is already prepared.

```
from import_annotations import get_project_from_annotations
import itertools, json

projects = list(itertools.chain.from_iterable([
    get_project_from_annotations(
        "./example/annotations/subjects/subject_annotations.json",
        schema_name="Subjects in Video"
    ),
    get_project_from_annotations(
        "./example/annotations/weather/weather_annotations.json",
        schema_name="Weather in Video"
    ),
]))

with open("./example/projects.json", "w") as f:
    json.dump(projects, f, indent=2)
```

#### Step 4 Create Projects from Annotations

>Create Projects from existing annotations / reviews (This is somehow complex now). You could just look it for reference.

```
from scripts.upload_utils import create_projects
import json

create_projects(projects_path="./example/projects.json")

# or
with open("./example/projects.json") as f:
    projects = json.load(f)
create_projects(projects_data=projects)
```

#### Step 5 Assign users to projects

> Assign Users to Projects from `./assignment` folder. Assignment should be stored in `.json` file.

```
from scripts.upload_utils import bulk_assign_users
import json

bulk_assign_users(assignment_path="./example/assignments.json")

# or
with open("./example/assignments.json") as f:
    assignments = json.load(f)
bulk_assign_users(assignments_data=assignments)
```

#### Step 6 Import annotations and reviews

> Import all the annotations from `./annotations` folder.

```
from import_annotations import import_annotations, import_reviews

import_annotations(annotations_folder="./example/annotations")
import_reviews(reviews_folder="./example/reviews")
```
