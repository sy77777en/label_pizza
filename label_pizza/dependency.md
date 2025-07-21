# Database Table Dependencies Analysis

## API Reference Tables

### Delete APIs (using_id)
| Table | API Function |
|-------|-------------|
| User | delete_user_using_id(id) |
| Video | delete_video_using_id(id) |
| VideoTag | delete_video_tag_using_id(video_id, tag) |
| QuestionGroup | delete_question_group_using_id(id) |
| Question | delete_question_using_id(id) |
| QuestionGroupQuestion | delete_question_group_question_using_id(question_group_id, question_id) |
| Schema | delete_schema_using_id(id) |
| SchemaQuestionGroup | delete_schema_question_group_using_id(schema_id, question_group_id) |
| Project | delete_project_using_id(id) |
| ProjectVideo | delete_project_video_using_id(project_id, video_id) |
| ProjectUserRole | delete_project_user_role_using_id(project_id, user_id, role) |
| ProjectGroup | delete_project_group_using_id(id) |
| ProjectGroupProject | delete_project_group_project_using_id(project_group_id, project_id) |
| ProjectVideoQuestionDisplay | delete_project_video_question_display_using_id(project_id, video_id, question_id) |
| AnnotatorAnswer | delete_annotator_answer_using_id(id) |
| ReviewerGroundTruth | delete_reviewer_ground_truth_using_id(video_id, question_id, project_id) |
| AnswerReview | delete_answer_review_using_id(id) |

### Delete APIs (using names)
| Table | API Function |
|-------|-------------|
| User | delete_user(user_id_str) |
| Video | delete_video(video_uid) |
| VideoTag | delete_video_tag(video_uid, tag) |
| QuestionGroup | delete_question_group(title) |
| Question | delete_question(text) |
| QuestionGroupQuestion | delete_question_group_question(question_group_title, question_text) |
| Schema | delete_schema(name) |
| SchemaQuestionGroup | delete_schema_question_group(schema_name, question_group_title) |
| Project | delete_project(name) |
| ProjectVideo | delete_project_video(project_name, video_uid) |
| ProjectUserRole | delete_project_user_role(project_name, user_id_str, role) |
| ProjectGroup | delete_project_group(name) |
| ProjectGroupProject | delete_project_group_project(project_group_name, project_name) |
| ProjectVideoQuestionDisplay | delete_project_video_question_display(project_name, video_uid, question_text) |
| AnnotatorAnswer | delete_annotator_answer(video_uid, question_text, user_id_str, project_name) |
| ReviewerGroundTruth | delete_reviewer_ground_truth(video_uid, question_text, project_name) |
| AnswerReview | delete_answer_review(answer_id) |

### Get Name APIs (id to name conversion)
| Table | API Function |
|-------|-------------|
| User | get_user_name(id) → user_id_str |
| Video | get_video_name(id) → video_uid |
| QuestionGroup | get_question_group_name(id) → title |
| Question | get_question_name(id) → text |
| Schema | get_schema_name(id) → name |
| Project | get_project_name(id) → name |
| ProjectGroup | get_project_group_name(id) → name |

### Override Name APIs (set name using id)
**Note: The name must be unique, otherwise an error will be raised.**

| Table | API Function |
|-------|-------------|
| User | set_user_name_using_id(id, user_id_str) |
| Video | set_video_name_using_id(id, video_uid) |
| QuestionGroup | set_question_group_name_using_id(id, title) |
| Question | set_question_name_using_id(id, text) |
| Schema | set_schema_name_using_id(id, name) |
| Project | set_project_name_using_id(id, name) |
| ProjectGroup | set_project_group_name_using_id(id, name) |

## Direct Dependency Graph

This shows what tables would be directly affected (have orphaned/inconsistent data) if we delete a row from each table:

## LEAF NODES (No dependencies - safe to delete)

```
VideoTag → must delete first: []
ProjectGroupProject → must delete first: []
ProjectVideoQuestionDisplay → must delete first: []
ReviewerGroundTruth → must delete first: []
AnswerReview → must delete first: []
```

## LEVEL 1 DEPENDENCIES

### AnnotatorAnswer
**Must delete first:** `[AnswerReview]`
- Call `delete_answer_review_using_id(id)` where `(answer_id = id)` from `AnswerReview`

### ProjectGroup
**Must delete first:** `[ProjectGroupProject]`
- Call `delete_project_group_project_using_id(project_group_id, project_id)` where `(project_group_id = id)` from `ProjectGroupProject`

## LEVEL 2 DEPENDENCIES

### ProjectVideo
**Must delete first:** `[AnnotatorAnswer, ReviewerGroundTruth, ProjectVideoQuestionDisplay]`
- Call `delete_annotator_answer_using_id(id)` where `(project_id = project_id, video_id = video_id)` from `AnnotatorAnswer`
- Call `delete_reviewer_ground_truth_using_id(video_id, question_id, project_id)` where `(project_id = project_id, video_id = video_id)` from `ReviewerGroundTruth`
- Call `delete_project_video_question_display_using_id(project_id, video_id, question_id)` where `(project_id = project_id, video_id = video_id)` from `ProjectVideoQuestionDisplay`

### ProjectUserRole
**Conditional check:** `[AnnotatorAnswer, ReviewerGroundTruth]`

**If deleting admin role only:**
- No need to check
- Call `update_reviewer_ground_truth_using_id(video_id, question_id, project_id)` where `(project_id = project_id, modified_by_admin_id = user_id)` to revert ground truth from `ReviewerGroundTruth` -- importantly, need set `answer_value` to `original_answer_value` and set to `NULL` for `(modified_at, modified_by_admin_id, and modified_by_admin_at)`

**If deleting reviewer role:**
- Must delete first from `ReviewerGroundTruth`; also must delete the user's admin role (if exists) in this project
- Call `delete_reviewer_ground_truth_using_id(video_id, question_id, project_id)` where `(project_id = project_id, reviewer_id = user_id)` from `ReviewerGroundTruth`
- Gather all answers `(answer_id)` from `AnnotatorAnswer` and call `delete_answer_review_using_id(id)` where `(answer_id, reviewer_id = user_id)` from `AnswerReview`

**If deleting annotator/model role:**
- Must delete first from `AnnotatorAnswer`; also must delete the user's reviewer role (if exists) in this project
- Call `delete_annotator_answer_using_id(id)` where `(project_id = project_id, user_id = user_id)` from `AnnotatorAnswer`

### QuestionGroupQuestion
**Must delete first:** `[AnnotatorAnswer, ReviewerGroundTruth, ProjectVideoQuestionDisplay]`

1. **Step 1:** Gather all schemas `(schema_id)` using this question group `(question_group_id)`
2. **Step 2:** Gather all projects `(project_id)` using those schemas `(schema_id)`
3. Call `delete_annotator_answer_using_id(id)`, `delete_reviewer_ground_truth_using_id(video_id, question_id, project_id)`, and `delete_project_video_question_display_using_id(project_id, video_id, question_id)` where `(project_id = project_id, question_id = question_id)` tuple matches

### SchemaQuestionGroup
**Must delete first:** `[AnnotatorAnswer, ReviewerGroundTruth, ProjectVideoQuestionDisplay]`

1. **Step 1:** Gather all projects `(project_id)` using this schema `(schema_id)`
2. **Step 2:** Gather all questions `(question_id)` from `QuestionGroupQuestion` using this question group `(question_group_id)`
3. Call `delete_annotator_answer_using_id(id)`, `delete_reviewer_ground_truth_using_id(video_id, question_id, project_id)`, and `delete_project_video_question_display_using_id(project_id, video_id, question_id)` where `(project_id = project_id, question_id = question_id)` tuple matches

## LEVEL 3 DEPENDENCIES

### Video
**Must delete first:** `[VideoTag, ProjectVideo]`
- Call `delete_video_tag_using_id(video_id, tag)` where `(video_id = video_id)` from `VideoTag`
- Call `delete_project_video_using_id(project_id, video_id)` where `(video_id = video_id)` from `ProjectVideo`

### Question
**Must delete first:** `[QuestionGroupQuestion]`
- Call `delete_question_group_question_using_id(question_group_id, question_id)` where `(question_id = question_id)` from `QuestionGroupQuestion`

### QuestionGroup
**Must delete first:** `[QuestionGroupQuestion, SchemaQuestionGroup]`
- Call `delete_question_group_question_using_id(question_group_id, question_id)` where `(question_group_id = question_group_id)` from `QuestionGroupQuestion`
- Call `delete_schema_question_group_using_id(schema_id, question_group_id)` where `(question_group_id = question_group_id)` from `SchemaQuestionGroup`

### Project
**Must delete first:** `[ProjectVideo, ProjectUserRole, ProjectGroupProject]`
- Call `delete_project_video_using_id(project_id, video_id)` where `(project_id = project_id)` from `ProjectVideo`
- Call `delete_project_user_role_using_id(project_id, user_id, role)` where `(project_id = project_id)` from `ProjectUserRole`
- Call `delete_project_group_project_using_id(project_group_id, project_id)` where `(project_id = project_id)` from `ProjectGroupProject`

### User
**Must delete first:** `[ProjectUserRole]`
- Call `delete_project_user_role_using_id(project_id, user_id, role)` where `(user_id = user_id)` from `ProjectUserRole`

## LEVEL 4 DEPENDENCIES

### Schema
**Must delete first:** `[SchemaQuestionGroup, Project]`
- Call `delete_schema_question_group_using_id(schema_id, question_group_id)` where `(schema_id = schema_id)` from `SchemaQuestionGroup`
- Call `delete_project_using_id(id)` where `(schema_id = schema_id)` from `Project`