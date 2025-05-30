Below is a **single reference document (`docs/service_api.md`)** that describes every service-layer API you now have **plus the functions you still need to add** (tagged **🚧 Pending**).
Copy this file into your repo and keep it updated as you implement new helpers.

---

# Service-Layer API Spec (v 0.2)

> **Scope** – pure-Python helpers that wrap SQLAlchemy sessions.
> **Goal** – allow UI (Streamlit, REST, CLI) and tests to call business logic without writing SQL.

---

## Index of Namespaces

| Namespace                                                  | Purpose                                     |
| ---------------------------------------------------------- | ------------------------------------------- |
| [`db`](#db)                                                | Engine & `SessionLocal` factory             |
| [`Resolver Helpers`](#resolver-helpers-optional)           | Name → ID convenience                       |
| [`VideoService`](#videoservice)                            | CRUD for `videos` & progress per video      |
| [`ProjectService`](#projectservice)                        | CRUD for `projects`, assignments & progress |
| [`SchemaService`](#schemaservice)                          | CRUD for `schemas` and their questions      |
| [`QuestionService`](#questionservice)                      | CRUD for individual `questions`             |
| [`QuestionGroupService`](#questiongroupservice)            | CRUD for question groups & reuse rule       |
| [`AuthService`](#authservice)                              | Users, roles, login, project assignments    |
| [`AnnotatorService`](#annotatorservice)                    | Core answer submission & retrieval          |
| [`GroundTruthService`](#groundtruthservice)                | Ground truth management & accuracy metrics  |
| [`MetricsService`](#metricsservice--🚧)                    | Accuracy, consensus, trends                 |
| [`NotificationService`](#notificationservice--optional-🚧) | Feedback to annotators                      |

**Legend**

| Mark                                                          | Meaning |
| ------------------------------------------------------------- | ------- |
| ✔︎ Implemented in `services.py`                               |         |
| 🚧 Planned / not yet coded                                    |         |
| 🛡️ Enforces critical business rule (should be in test-suite) |         |

---

### db

| Function         | Returns                  | Notes                                                            |
| ---------------- | ------------------------ | ---------------------------------------------------------------- |
| `SessionLocal()` | `sqlalchemy.orm.Session` | Connects with `DBURL` from `.env`; `expire_on_commit=False`. 🛡️ |

---

### Resolver Helpers (optional)

*(Put these in `services/helpers.py` if you need them.)*

| Function                            | Returns | Notes               |
| ----------------------------------- | ------- | ------------------- |
| `project_id_by_name(name, session)` | `int`   | Raises if not found |
| `schema_id_by_name(name, session)`  | `int`   | —                   |

---

## VideoService

| Function                           | Status | Parameters                               | Returns            | Business rules enforced                              |
| ---------------------------------- | ------ | ---------------------------------------- | ------------------ | ---------------------------------------------------- |
| `get_all_videos(session)`          | ✔︎     | —                                        | `pandas.DataFrame` | Uses left joins to list each video + projects + GT ✓ |
| `add_video(url, session)`          | ✔︎     | `url:str`, `metadata:dict` (optional)    | `None`             | • `video_uid` unique 🛡️<br>• URL validation 🛡️<br>• Metadata validation 🛡️ |

---

## ProjectService

| Function                                              | Status | Parameters | Returns     | Rules enforced                                         |
| ----------------------------------------------------- | ------ | ---------- | ----------- | ------------------------------------------------------ |
| `get_all_projects(session)`                           | ✔︎     | —          | `DataFrame` | shows vids, schema, % GT                               |
| `create_project(name, schema_id, video_ids, session)` | ✔︎     | —          | `Project`   | • schema not archived 🛡️<br>• videos not archived 🛡️ |
| `get_video_ids_by_uids(uids, session)`                | ✔︎     | —          | `list[int]` | —                                                      |
| `archive_project(project_id, session)`                | ✔︎     | —          | `None`      | • Sets `is_archived=True`<br>• blocks new answers 🛡️  |
| `progress(project_id, session)`                       | ✔︎     | —          | `dict`      | returns videos × questions × answers                   |
| `add_videos_to_project(project_id, video_ids, session)` | 🚫     | —          | —           | **Deprecated: Projects are immutable after creation.** |
| `get_project_by_id(project_id, session)`              | ✔︎     | —          | `Project`   |                                                        |

---

## SchemaService

| Function                                                  | Status | Purpose / rules            |
| --------------------------------------------------------- | ------ | -------------------------- |
| `get_all_schemas(session)`                                | ✔︎     | List                       |
| `get_schema_questions(schema_id, session)`                | ✔︎     | List questions             |
| `get_schema_id_by_name(name, session)`                    | ✔︎     | Resolver                   |
| `create_schema(name, question_group_ids, session)` | ✔︎ | Create new schema |
| `archive_schema(schema_id, session)` | ✔︎ | Archive schema |
| `unarchive_schema(schema_id, session)` | ✔︎ | Unarchive schema |
| `get_question_group_order(schema_id, session)` | ✔︎ | Get ordered list of group IDs |
| `update_question_group_order(schema_id, group_ids, session)` | ✔︎ | Update group display order |

Note: Schemas are immutable after creation to maintain data integrity. The display order of question groups can be modified as it only affects UI presentation.

---

## QuestionGroupService

| Function                                                  | Status | Notes                             |
| --------------------------------------------------------- | ------ | --------------------------------- |
| `get_all_groups(session)`                                 | ✔︎     | includes stats                    |
| `get_group_questions(group_id, session)`                  | ✔︎     | List questions in group           |
| `get_group_details(group_id, session)`                    | ✔︎     | Full group info                   |
| `create_group(title, desc, is_reusable, session)`         | ✔︎     | unique title 🛡️                  |
| `get_group_by_name(name, session)`                        | ✔︎     | resolver                          |
| `edit_group(group_id, new_title,…, is_reusable, session)` | ✔︎     | reuse rule when toggling 🛡️      |
| `archive_group(group_id, session)`                        | ✔︎     | archive cascades to questions 🛡️ |
| `unarchive_group(group_id, session)`                      | ✔︎     | Restore group                     |

---

## QuestionService

| Function                                                 | Status | Rules                      |
| -------------------------------------------------------- | ------ | -------------------------- |
| `get_all_questions(session)`                             | ✔︎     | —                          |
| `add_question(text,qtype,group,options,default,session)` | ✔︎     | • default in options 🛡️   |
| `get_question_by_text(text, session)`                    | ✔︎     | Find by text               |
| `edit_question(...)`                                     | ✔︎     | • cannot change `type` 🛡️ |
| `archive_question(question_id, session)`                 | ✔︎     | Soft delete                |
| `unarchive_question(question_id, session)`               | ✔︎     | Restore question           |

---

## AuthService

| Function                                                        | Status | Rules                                 |
| --------------------------------------------------------------- | ------ | ------------------------------------- |
| `seed_admin(session)`                                           | ✔︎     | inserts hard-coded admin              |
| `authenticate(email,pwd,role,session)`                          | ✔︎     | • disabled = reject 🛡️               |
| `get_all_users(session)`                                        | ✔︎     | —                                     |
| `create_user(user_id,email,pwd_hash,user_type,session)`         | ✔︎     | unique id/email 🛡️                   |
| `update_user_id(user_id,new_user_id,session)`                   | ✔︎     | unique id 🛡️                         |
| `update_user_email(user_id,new_email,session)`                  | ✔︎     | unique email 🛡️                      |
| `update_user_password(user_id,new_password,session)`            | ✔︎     | —                                     |
| `update_user_role(user_id,new_role,session)`                    | ✔︎     | —                                     |
| `toggle_user_active(user_id,session)`                           | ✔︎     | —                                     |
| `assign_user_to_project(user_id, project_id, role, session)`    | ✔︎     | • upsert<br>• admin auto reviewer 🛡️ |
| `remove_user_from_project(...)`                                 | ✔︎     | —                                     |
| `bulk_assign_users_to_project / bulk_remove_users_from_project` | ✔︎     | —                                     |
| `get_project_assignments(session)`                              | ✔︎     | df                                    |
| `assign_admin_to_all_projects(user_id, session)`                | ✔︎     | Auto-assign admin to all projects     |

---

## AnnotatorService

| Function                                                                                                                  | Status | Rules                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| `submit_answer(video_id, question_id, project_id, user_id, answer_value, session, confidence_score=None, notes=None)`     | ✔︎     | • scope UQ🛡️ (upsert)<br>• project not archived 🛡️<br>• user role assigned 🛡️<br>• type & option validation 🛡️ |
| `get_answers(video_id, project_id, session)`                                                                              | ✔︎     | Get all answers for video/project                                                                                  |
| `get_question_answers(question_id, project_id, session)`                                                                  | ✔︎     | Get all answers for a question in a project                                                                        |

---

## GroundTruthService

The GroundTruthService provides methods for managing ground truth answers in the system. It includes functionality for submitting, retrieving, and overriding ground truth answers.

| Function                                                                                                                  | Status | Rules                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| `submit_ground_truth_to_question_group(video_id, project_id, reviewer_id, question_group_id, answers, session, confidence_scores=None, notes=None)` | ✔︎ | • reviewer role required 🛡️<br>• type & option validation 🛡️ |
| `get_ground_truth(video_id, project_id, session)`                                                                         | ✔︎     | Get ground truth answers for video/project                                                                         |
| `override_ground_truth_to_question_group(video_id, project_id, question_group_id, admin_id, answers, session)`           | ✔︎     | • admin role required 🛡️<br>• tracks modifications 🛡️ |
| `get_reviewer_accuracy(reviewer_id, project_id, session)`                                                                 | ✔︎     | Calculate accuracy based on admin modifications                                                                    |
| `get_annotator_accuracy(project_id, question_id, session)`                                                                | ✔︎     | Calculate annotator accuracy for a specific question                                                               |

### Notes
- The single-question `override_ground_truth()` method has been removed in favor of the question group-based `override_ground_truth_to_question_group()` method
- All ground truth operations now work at the question group level for better consistency and atomicity
- The service maintains backward compatibility with existing data while enforcing the new group-based approach
- Admin overrides are tracked with timestamps and admin IDs for audit purposes

### Validation Rules

1. Project and user must exist and be active
2. User must have appropriate role (reviewer for submission, admin for override)
3. Question group must exist and not be archived
4. Answers must match questions in the group
5. Answer values must be valid for their question types
6. For single-choice questions, answers must be one of the defined options
7. For description questions, answers must be strings
8. Confidence scores must be floats if provided

---

## MetricsService  🚧

| Function                                            | Rule enforced                    |
| --------------------------------------------------- | -------------------------------- |
| `accuracy_by_user(project_id, session)`             | ignores questions lacking GT 🛡️ |
| `accuracy_by_question(project_id, session)`         | —                                |
| `five_day_trend(project_id, user_id=None, session)` | rolling window                   |

---

## NotificationService  (optional 🚧)

| Function                                                            | Purpose                 |
| ------------------------------------------------------------------- | ----------------------- |
| `record_correction(annotator_id, answer_id, new_gt_value, session)` | insert notification row |
| `get_unread(user_id, session)`                                      | fetch & mark-read       |

*(needs a simple `notifications` table: id, user\_id, message, created\_at, is\_read)*

---

## ProjectGroupService

| Function                                                                                                   | Status | Notes                                                                                                    |
| ---------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------- |
| `create_project_group(name, description, project_ids, session)`                             | ✔︎     | Enforces uniqueness of (video, question) pairs across projects in group                                  |
| `edit_project_group(group_id, name, description, add_project_ids, remove_project_ids, session)`            | ✔︎     | Enforces uniqueness constraint when adding projects                                                      |
| `get_project_group_by_id(group_id, session)`                                                               | ✔︎     | Returns group and its projects                                                                           |
| `list_project_groups(session)`                                                                             | ✔︎     | Lists all project groups                                                                                 |

**Uniqueness Rule:** For any two projects in a group, if their schemas have overlapping questions, they must not have any overlapping (non-archived) videos. If schemas have no overlapping questions, any videos are allowed.

---

## Package Layout Suggestion

```
label_pizza/
├ models.py
├ db.py
├ services/
│   ├ __init__.py          # re-export classes
│   ├ videos.py            # VideoService
│   ├ projects.py          # ProjectService
│   ├ schemas.py           # SchemaService
│   ├ questions.py         # QuestionService
│   ├ qgroups.py           # QuestionGroupService
│   ├ auth.py              # AuthService
│   ├ annotators.py        # AnnotatorService
│   ├ ground_truth.py      # GroundTruthService
│   ├ metrics.py           # MetricsService (🚧)
│   └ notifications.py     # NotificationService (optional)
└ docs/
    ├ database_design.md
    └ service_api.md   ← this file
```

---

### Next Steps

1. **Move** each helper block from your existing `services.py` into the files above.
2. **Implement** the 🚧 functions (start with `MetricsService`).
3. **Write tests** using the full rule checklist.

Once those pieces are in place, your backend will have a stable, documented contract that UI and future micro-services can rely on.
