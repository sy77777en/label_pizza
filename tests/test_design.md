## 1 Users & Authentication

| #   | Rule to test                                                                              | Minimal fixture state | Suggested test name                      |
| --- | ----------------------------------------------------------------------------------------- | --------------------- | ---------------------------------------- |
| 1.1 | Only **admins** can create / deactivate any user or change another user's password.       | admin A, human H      | `test_only_admin_can_manage_users`       |
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
| 4.6 | `name` globally unique.                   | duplicate name | `test_duplicate_project_name_rejected`      |
| 4.7 | `schema_id` must exist.                   | bad schema     | `test_create_project_bad_schema`            |
| 4.8 | `video_ids` must exist.                   | bad video      | `test_create_project_bad_video`             |
| 4.9 | Cannot use archived schema.               | archived schema| `test_create_project_with_archived_resources_fails` |
| 4.10 | Cannot use archived video.                | archived video | `test_create_project_with_archived_resources_fails` |
| 4.11 | No duplicate videos in project.           | duplicate video| `test_duplicate_project_video_fail`         |
| 4.12 | Archived projects are hidden.             | archived project| `test_archived_project_hidden_and_read_only` |
| 4.13 | Archived projects block new answers.      | archived project| `test_archived_project_hidden_and_read_only` |
| 4.14 | Progress with no videos/questions.        | empty project  | `test_project_service_progress_empty`       |
| 4.15 | Progress with partial ground truth.       | partial gt     | `test_project_service_progress_with_data`   |
| 4.16 | Progress with all ground truth.           | all gt         | `test_project_service_progress_with_data`   |
| 4.17 | Progress for non-existent project.        | bad project    | `test_project_service_progress_nonexistent_project` |
| 4.18 | Archive non-existent project.             | bad project    | `test_project_service_archive_nonexistent_project` |

---

## 5 Videos

| #   | Rule                                      | Fixture        | Test name                                   |
| --- | ----------------------------------------- | -------------- | ------------------------------------------- |
| 5.1 | `video_uid` globally unique.              | duplicate uid  | `test_video_service_add_video_duplicate`    |
| 5.2 | Invalid JSON in `video_meta` rejected.    | bad meta       | `test_video_service_add_video_with_invalid_metadata` |
| 5.3 | Cannot add archived video to new project. | archived video | `test_cannot_add_archived_video_to_project` |
| 5.4 | URL must have valid protocol.             | invalid url    | `test_video_service_add_video_with_invalid_protocol` |
| 5.5 | URL must have file extension.             | missing ext    | `test_video_service_add_video_with_missing_extension` |
| 5.6 | URL length must be within limits.         | long url       | `test_video_service_add_video_with_very_long_url` |
| 5.7 | Metadata must be valid dictionary.        | invalid meta   | `test_video_service_add_video_with_invalid_metadata` |
| 5.8 | Empty metadata is allowed.                | empty meta     | `test_video_service_add_video_with_empty_metadata` |
| 5.9 | Partial ground truth is marked incomplete.| partial gt     | `test_video_service_get_all_videos_with_partial_ground_truth` |
| 5.10 | Special characters in URL are handled.    | special chars  | `test_video_service_add_video_special_chars` |
| 5.11 | Query parameters in URL are preserved.    | query params   | `test_video_service_add_video_query_params` |
| 5.12 | Empty video list returns empty DataFrame. | empty db       | `test_video_service_get_all_videos_empty`   |
| 5.13 | Video metadata is preserved.              | with meta      | `test_video_service_get_all_videos_with_metadata` |
| 5.14 | Video metadata value types are validated. | invalid meta   | `test_video_metadata_validation`            |
| 5.15 | Video UIDs are case-sensitive.            | case variants  | `test_video_uid_case_sensitivity`           |
| 5.16 | Add video with metadata.                  | with meta      | `test_video_service_add_video_with_metadata`|
| 5.17 | Add video with empty metadata.            | empty meta     | `test_video_service_add_video_with_empty_metadata` |
| 5.18 | Add video with special chars in UID.      | special chars  | `test_video_uid_special_chars`              |
| 5.19 | Get all videos with project assignments.  | videos in projects | `test_video_service_get_all_videos_with_project` |
| 5.20 | Get all videos with ground truth status.  | videos with gt | `test_video_service_get_all_videos_with_ground_truth` |
| 5.21 | Get all videos in multiple projects.      | videos in multiple projects | `test_video_service_get_all_videos_multiple_projects` |
| 5.22 | Get all videos with mixed status.         | mixed status   | `test_video_service_get_all_videos_mixed_status` |

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
| 6.9 | Get answers returns correct DataFrame columns.                              | answers in db               | `test_answer_service_get_answers`       |
| 6.10 | Get ground truth returns correct DataFrame columns.                         | ground truth in db          | `test_answer_service_get_ground_truth`  |

## Answers

### Rules
1. Answers must be associated with a video, question, project, and user
2. Answers must match the question type (single-choice, multiple-choice, etc.)
3. Answers for single-choice questions must be one of the defined options
4. Ground truth answers are marked with is_ground_truth flag
5. Answers can be updated by the same user
6. Answers cannot be submitted to archived projects
7. Disabled users cannot submit answers
8. Answer history is tracked with modified_by_user_id

### Test Cases

| ID | Test Name | Description | Expected Result |
|----|-----------|-------------|-----------------|
| 6.1 | test_answer_service_submit_answer | Submit a valid answer | Answer is created with correct values |
| 6.2 | test_answer_service_submit_ground_truth | Submit a ground truth answer | Answer is created with is_ground_truth=True |
| 6.3 | test_answer_service_submit_invalid_option | Submit invalid option for single-choice | Raises ValueError with appropriate message |
| 6.4 | test_answer_service_submit_to_archived_project | Submit answer to archived project | Raises ValueError with appropriate message |
| 6.5 | test_answer_service_submit_as_disabled_user | Submit answer as disabled user | Raises ValueError with appropriate message |
| 6.6 | test_answer_service_update_existing_answer | Update an existing answer | Answer is updated with new value and modified_by_user_id |
| 6.7 | test_answer_service_get_answers | Get all answers for a video in project | Returns DataFrame with all answers |
| 6.8 | test_answer_service_get_ground_truth | Get ground truth answers for a video | Returns DataFrame with ground truth answers only |

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

* `conftest.py`