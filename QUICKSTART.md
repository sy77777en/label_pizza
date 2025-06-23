### Quick Start



#### Step 0 Reset DB & Add Seed Admin

#### Step 1 Upload Videos

> Upload all the videos from `./videos` folder. Videos should be stored in `.json` file.

```
from scripts.upload_utils import upload_videos

upload_videos()
```

**`.json` file format should be:**

```
 [
  {
    "url": str,
    "metadata": {
      key: value,
      key: value
    }
  },
  {
    "url": str,
    "metadata": {
      key: value,
      key: value
    }
  }
  ...
]
```



#### Step 2 Upload Users

> Upload all the users from `./users` folder. Users should be stored in `.json` file.

```
from scripts.upload_utils import upload_users

upload_users()
```

**`.json` file format should be:**

```
# "user_type" could only be one of ['human', 'admin', 'model']
[
    {
        "user_id": "User1",
        "email": "user1@example.com",
        "password": "user111",
        "user_type": "human"
    },
    {
        "user_id": "Admin1",
        "email": "admin1@gmail.com",
        "password": "admin111",
        "user_type": "admin"
    },
    {
        "user_id": "Robot1",
        "password": "robot111",
        "user_type": "model"
    }
    ...
]
```

#### Step 3 Upload Questions / Question Groups / Schemas

> Upload all the schemas / question groups / questions from `./schemas` and `./question_groups` folders. They should be stored in `.json` file.

```
from scripts.upload_utils import create_schemas

create_schemas()
```

**Schema `.json` file format should be:**

```
{
  "schema_name": str,
  "question_group_names": [
    "question group 1", "question group 2", ...
  ]
}
```

**Question Group `.json` file format should be:**

```
{
    "title": Group Title,
    "description": Description for This Group,
    "verification_function": Verification Function Name,
    "is_reusable": false,
    "is_auto_submit": false,
    "verification_function": "",
    "questions": [
        {
            "text": Question Text,
            "qtype": "single",
            "required": true,
            "options": [
                "Opt 1",
                "Opt 2",
								"Opt 3"
            ],
            "display_values": [
                "Opt 1",
                "Opt 2",
								"Opt 3"
            ],
            "default_option": "Opt 1",
            "display_text": Question Display Text
        },
        {
            "text": Question Text,
            "qtype": "description",
            "required": false,
            "options": null,
            "display_values": null,
            "default_option": null,
            "display_text": Question Text
        },
        ...
    ]
}
```

#### Step 4 Create Projects from Annotations

>Create Projects from existing annotations / reviews (This is somehow complex now). You could just look it for reference.

```
from scripts.upload_utils import create_projects_from_annotations_json

create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_color.json', schema_name='Color Grading')
```

**`.json` file format should be:**

```
[
  {
    "question_group_title": "Atmospheric Effects",
    "project_name": "Cinematic Effects Lightingtest_0 1",
    "user_email": "whan55751@gmail.com",
    "video_uid": "7jUW96CiEKA.0.6.mp4",
    "answers": {
      "Question 1": "Answer 1",
      "Question 2": "Answer 2",
      "Question 3": "Answer 3"
    },
    "confidence_score": {
      "Question 1": score 1,
      "Question 2": score 2,
      "Question 3": score 3
    }
  },
  ...
]
```

#### Step 5 Bulk Assignment

> Assign Users to Projects from `./assignment` folder. Assignment should be stored in `.json` file.

```
from upload_utils import bulk_assign_users

bulk_assign_users()
```

**`.json` file format should be:**

```
[
  {
    "user_email": "ttiffanyyllingg@gmail.com",
    "project_name": "Color Grading Lightingtest_2_1 2",
    "role": "annotator"
  },
  {
    "user_email": "thebluesoil@hotmail.com",
    "project_name": "Color Grading Lightingtest_2_0 1",
    "role": "reviewer"
  }
  ...
]
```

#### Step 6 Import Annotations

> Import all the annotations from `./annotations` folder.

```
from import_annotations import import_annotations

import_annotations()
```

**`.json` file format should be:**

```
[
  {
    "question_group_title": "Atmospheric Effects",
    "project_name": "Cinematic Effects Lightingtest_0 1",
    "user_email": "whan55751@gmail.com",
    "video_uid": "7jUW96CiEKA.0.6.mp4",
    "answers": {
      "Question 1": "Answer 1",
      "Question 2": "Answer 2",
      "Question 3": "Answer 3"
    },
    "confidence_score": {
      "Question 1": score 1,
      "Question 2": score 2,
      "Question 3": score 3
    }
  },
  ...
]
```

#### Step 7 Import Reviews

> Import all the reviews from `./reviews` folder.

```
from import_annotations import import_reviews

import_reviews()
```

**`.json` file format should be:**

```
[
  {
    "question_group_title": "Atmospheric Effects",
    "project_name": "Cinematic Effects Lightingtest_0 1",
    "user_email": "whan55751@gmail.com",
    "video_uid": "7jUW96CiEKA.0.6.mp4",
    "answers": {
      "Question 1": "Answer 1",
      "Question 2": "Answer 2",
      "Question 3": "Answer 3"
    },
    "reviewer_email": "siyuancen096@gmail.com"
  },
  ...
]
```

