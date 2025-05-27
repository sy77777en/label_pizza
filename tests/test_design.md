## 1 Users & Authentication

| #   | Rule to test                                                                              | Minimal fixture state | Suggested test name                      |
| --- | ----------------------------------------------------------------------------------------- | --------------------- | ---------------------------------------- |
| 1.1 | Only **admins** can create / deactivate any user or change another user’s password.       | admin A, human H      | `test_only_admin_can_manage_users`       |
| 1.2 | Soft-disabled user (`is_active=False`) cannot authenticate or submit answers.             | disabled D            | `test_disabled_user_cannot_login_submit` |
| 1.3 | Global `admin` role implied in all projects (even if not listed in `project_user_roles`). | admin A, project P    | `test_admin_inherits_project_admin`      |
| 1.4 | `authenticate()` rejects wrong role tab (e.g., Reviewer login with human account).        | human H               | `test_login_rejects_wrong_role`          |

---

## 2 Question Groups & Questions

| #   | Rule                                                                    | Fixture                              | Test name                                          |
| --- | ----------------------------------------------------------------------- | ------------------------------------ | -------------------------------------------------- |
| 2.1 | Non-reusable group may be linked to **≤ 1** active schema.              | group G (non-reusable) + two schemas | `test_duplicate_use_non_reusable_group_fails`      |
| 2.2 | Reusable group can be imported by unlimited schemas.                    | group R (reusable)                   | `test_reusable_group_multi_schema_ok`              |
| 2.3 | `default_option` (if present) must be one of `options` array.           | single-choice Q                      | `test_default_option_validation`                   |
| 2.4 | Question **type** cannot change once created.                           | existing Q                           | `test_cannot_change_question_type`                 |
| 2.5 | Making a reusable group non-reusable fails if >1 schema already use it. | group R in two schemas               | `test_toggle_reusable_false_multiple_schema_fails` |

---

## 3 Schemas

| #   | Rule                                                     | Fixture                               | Test name                                  |
| --- | -------------------------------------------------------- | ------------------------------------- | ------------------------------------------ |
| 3.1 | Creating schema with illegal group reuse is blocked.     | duplicate non-reusable group          | `test_schema_creation_group_reuse_blocked` |
| 3.2 | Archiving schema does **not** archive existing projects. | schema S + project P                  | `test_archive_schema_keeps_projects_live`  |
| 3.3 | `rules_json` validator blocks invalid answer combos.     | schema rule sunny∧overcast impossible | `test_rules_json_blocks_invalid_combo`     |

---

## 4 Projects & Roles

| #   | Rule                                                              | Fixture                 | Test name                                           |
| --- | ----------------------------------------------------------------- | ----------------------- | --------------------------------------------------- |
| 4.1 | `create_project` fails if schema or videos archived.              | archived schema / video | `test_create_project_with_archived_resources_fails` |
| 4.2 | `(project_id, video_id)` must be unique.                          | same video twice        | `test_duplicate_project_video_fail`                 |
| 4.3 | `assign_user_to_project` upserts role (no duplicates).            | existing assignment     | `test_assign_role_upsert`                           |
| 4.4 | Archiving project hides it from list APIs and blocks new answers. | project P archived      | `test_archived_project_hidden_and_read_only`        |
| 4.5 | Admin auto-upgraded to reviewer if added as annotator.            | admin user              | `test_admin_role_auto_upgraded`                     |

---

## 5 Videos

| #   | Rule                                      | Fixture        | Test name                                   |
| --- | ----------------------------------------- | -------------- | ------------------------------------------- |
| 5.1 | `video_uid` globally unique.              | duplicate uid  | `test_duplicate_video_uid_rejected`         |
| 5.2 | Invalid JSON in `video_meta` rejected.    | bad meta       | `test_invalid_video_meta`                   |
| 5.3 | Cannot add archived video to new project. | archived video | `test_cannot_add_archived_video_to_project` |

---

## 6 Answers (core)

| #   | Rule                                                                        | Fixture                     | Test name                               |
| --- | --------------------------------------------------------------------------- | --------------------------- | --------------------------------------- |
| 6.1 | Unique per `(video, question, user, project)` (scope).                      | try second insert           | `test_scope_unique_upsert`              |
| 6.2 | Only **one** GT row per `(video, question, project)`.                       | two GT inserts              | `test_single_gt_per_scope`              |
| 6.3 | `answer_type` must equal `Question.type`.                                   | mismatch                    | `test_answer_type_mismatch`             |
| 6.4 | For single-choice, value must appear in `options`.                          | bad value                   | `test_answer_value_not_in_options`      |
| 6.5 | Submit to archived project rejected.                                        | archived project            | `test_submit_to_archived_project_fails` |
| 6.6 | Disabled user cannot submit.                                                | disabled user               | `test_disabled_user_submit_fails`       |
| 6.7 | Reviewer GT overwrite triggers notification (if notification table exists). | stub notifier mock          | `test_notification_on_gt_change`        |
| 6.8 | Upsert updates timestamp, not duplicate row.                                | compare `created_at` vs new | `test_upsert_updates_timestamp`         |

---

## 7 Metrics & Accuracy

| #   | Rule                                             | Fixture             | Test name                          |
| --- | ------------------------------------------------ | ------------------- | ---------------------------------- |
| 7.1 | `accuracy_by_user` ignores questions lacking GT. | half GT present     | `test_accuracy_ignores_unreviewed` |
| 7.2 | Five-day trend covers only last 5 days.          | answers over 7 days | `test_five_day_trend_window`       |
| 7.3 | Consensus uses latest answer per user.           | user edits answer   | `test_consensus_latest_only`       |

---

## 8 Project Groups (Export)

| #   | Rule                                                    | Fixture                      | Test name                                     |
| --- | ------------------------------------------------------- | ---------------------------- | --------------------------------------------- |
| 8.1 | Group names unique unless archived.                     | duplicate name after archive | `test_project_group_name_reuse_after_archive` |
| 8.2 | Export API returns union of non-archived projects only. | group with archived project  | `test_export_skips_archived_projects`         |

---

## 9 Soft Delete Consistency

| #   | Rule                                       | Fixture                | Test name                        |
| --- | ------------------------------------------ | ---------------------- | -------------------------------- |
| 9.1 | All `list_*` helpers filter `is_archived`. | create archived & live | `test_list_filters_archived`     |
| 9.2 | `QueryHelper.active()` adds filter.        | raw query vs active()  | `test_queryhelper_active_filter` |

---

## 10 Security / Auth Flow

| #    | Rule                                                      | Fixture         | Test name                             |
| ---- | --------------------------------------------------------- | --------------- | ------------------------------------- |
| 10.1 | Wrong password rejected.                                  | user            | `test_login_wrong_password`           |
| 10.2 | Non-admin user blocked from admin functions.              | user role human | `test_non_admin_cannot_manage_users`  |
| 10.3 | User not assigned to project cannot submit answers there. | unassigned user | `test_unassigned_user_submit_blocked` |

---

## 11 Seed / Loader Utilities

| #    | Rule                                                       | Fixture        | Test name                            |
| ---- | ---------------------------------------------------------- | -------------- | ------------------------------------ |
| 11.1 | Video CSV loader skips duplicates gracefully.              | dup rows       | `test_video_loader_skips_duplicates` |
| 11.2 | Question-group JSON loader validates required keys/fields. | malformed JSON | `test_json_loader_validation`        |

---

### Directory Skeleton

```
tests/
├ conftest.py      # db fixture, fresh schema
├ test_auth.py
├ test_users.py
├ test_videos.py
├ test_qgroups.py
├ test_questions.py
├ test_schemas.py
├ test_projects.py
├ test_answers.py
├ test_metrics.py
├ test_softdelete.py
└ test_loader_utils.py
```

* `conftest.py` should spin up a test database (local Postgres or
  `psycopg2`-backed in-memory) and yield a `Session` fixture that rolls back
  after each test.