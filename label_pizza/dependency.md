# Database Table Dependencies Analysis

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
- Use `(answer_id = id)` to delete from `AnswerReview`

### ProjectGroup
**Must delete first:** `[ProjectGroupProject]`
- Use `(project_group_id = id)` to delete from `ProjectGroupProject`

## LEVEL 2 DEPENDENCIES

### ProjectVideo
**Must delete first:** `[AnnotatorAnswer, ReviewerGroundTruth, ProjectVideoQuestionDisplay]`
- Use `(project_id = project_id, video_id = video_id)` to delete from `AnnotatorAnswer`, `ReviewerGroundTruth`, and `ProjectVideoQuestionDisplay`

### ProjectUserRole
**Conditional check:** `[AnnotatorAnswer, ReviewerGroundTruth]`

**If deleting admin role only:**
- No need to check
- Use `(project_id = project_id, modified_by_admin_id = user_id)` to revert ground truth from `ReviewerGroundTruth` -- importantly, need set `answer_value` to `original_answer_value` and set to `NULL` for `(modified_at, modified_by_admin_id, and modified_by_admin_at)`

**If deleting reviewer role:**
- Must delete first from `ReviewerGroundTruth`; also must delete the user's admin role (if exists) in this project
- Use `(project_id = project_id, reviewer_id = user_id)` to delete from `ReviewerGroundTruth`
- Gather all answers `(answer_id)` from `AnnotatorAnswer` and use `(answer_id, reviewer_id = user_id)` to delete from `AnswerReview`

**If deleting annotator/model role:**
- Must delete first from `AnnotatorAnswer`; also must delete the user's reviewer role (if exists) in this project
- Use `(project_id = project_id, user_id = user_id)` to delete from `AnnotatorAnswer`

### QuestionGroupQuestion
**Must delete first:** `[AnnotatorAnswer, ReviewerGroundTruth, ProjectVideoQuestionDisplay]`

1. **Step 1:** Gather all schemas `(schema_id)` using this question group `(question_group_id)`
2. **Step 2:** Gather all projects `(project_id)` using those schemas `(schema_id)`
3. Use `(project_id = project_id, question_id = question_id)` tuple to delete from `AnnotatorAnswer`, `ReviewerGroundTruth`, and `ProjectVideoQuestionDisplay`

### SchemaQuestionGroup
**Must delete first:** `[AnnotatorAnswer, ReviewerGroundTruth, ProjectVideoQuestionDisplay]`

1. **Step 1:** Gather all projects `(project_id)` using this schema `(schema_id)`
2. **Step 2:** Gather all questions `(question_id)` from `QuestionGroupQuestion` using this question group `(question_group_id)`
3. Use `(project_id = project_id, question_id = question_id)` tuple to delete from `AnnotatorAnswer`, `ReviewerGroundTruth`, and `ProjectVideoQuestionDisplay`

## LEVEL 3 DEPENDENCIES

### Video
**Must delete first:** `[VideoTag, ProjectVideo]`
- Use `(video_id = video_id)` to delete from `VideoTag`, `ProjectVideo`

### Question
**Must delete first:** `[QuestionGroupQuestion]`
- Use `(question_id = question_id)` to delete from `QuestionGroupQuestion`

### QuestionGroup
**Must delete first:** `[QuestionGroupQuestion, SchemaQuestionGroup]`
- Use `(question_group_id = question_group_id)` to delete from `QuestionGroupQuestion`, `SchemaQuestionGroup`

### Project
**Must delete first:** `[ProjectVideo, ProjectUserRole, ProjectGroupProject]`
- Use `(project_id = project_id)` to delete from `ProjectVideo`, `ProjectUserRole`, `ProjectGroupProject`

### User
**Must delete first:** `[ProjectUserRole]`
- Use `(user_id = user_id)` to delete from `ProjectUserRole`

## LEVEL 4 DEPENDENCIES

### Schema
**Must delete first:** `[SchemaQuestionGroup, Project]`
- Use `(schema_id = schema_id)` to delete from `SchemaQuestionGroup`, `Project`