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
| [`AnswerService`](#answerservice)                          | Core answer upsert, ground-truth, history   |
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

---

## SchemaService

| Function                                                  | Status | Purpose / rules            |
| --------------------------------------------------------- | ------ | -------------------------- |
| `get_all_schemas(session)`                                | ✔︎     | List                       |
| `get_schema_questions(schema_id, session)`                | ✔︎     | List questions             |
| `get_schema_id_by_name(name, session)`                    | ✔︎     | Resolver                   |
| `create_schema(name, rules_json, session)`                | ✔︎     | • Validate group reuse 🛡️ |
| `add_question_group_to_schema(schema_id, group_id, display_order, session)` | ✔︎ | Add group to schema |
| `remove_question_group_from_schema(schema_id, group_id, session)` | ✔︎ | Remove group from schema |
| `archive_schema(schema_id, session)`                      | ✔︎     | Soft delete                |
| `unarchive_schema(schema_id, session)`                    | ✔︎     | Restore schema             |

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

## AnswerService

| Function                                                                                                                  | Status | Rules                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| `submit_answer(video_id, question_id, project_id, user_id, answer_value, session, is_ground_truth=False)`                 | ✔︎     | • scope UQ🛡️ (upsert)<br>• project not archived 🛡️<br>• user role assigned 🛡️<br>• type & option validation 🛡️ |
| `get_answers(video_id, project_id, session)`                                                                              | ✔︎     | Get all answers for video/project                                                                                  |
| `get_ground_truth(video_id, project_id, session)`                                                                         | ✔︎     | Get ground truth answers                                                                                           |
| `submit_review(answer_id, reviewer_id, status, comment, session)`                                                         | ✔︎     | Submit answer review                                                                                               |
| `get_reviews(answer_id, session)`                                                                                         | ✔︎     | Get all reviews for an answer                                                                                      |
| `get_pending_reviews(project_id, session)`                                                                                | ✔︎     | Get pending reviews for a project                                                                                  |

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
│   ├ answers.py           # AnswerService
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
