# Video-Labeling Platform · Database Schema v 1.0
> **PostgreSQL + SQLAlchemy** — soft links (no hard FKs); integrity enforced in the service layer.

---

## 0 · Type & Convention Cheat-Sheet

| Doc type       | SQLAlchemy field              | Postgres column              | Why we chose it                                |
| -------------- | ----------------------------- | ---------------------------- | ---------------------------------------------- |
| `INT`          | `Integer`                     | `integer`                    | Compact PK/FK                                  |
| `VARCHAR(n)`   | `String(n)`                   | `character varying(n)`       | Bounded text (emails, IDs)                     |
| `TEXT`         | `Text`                        | `text`                       | Unbounded UTF-8                                |
| `TIMESTAMPTZ`  | `DateTime(timezone=True)`     | `timestamp with time zone`   | Always UTC, DST-safe                           |
| `JSONB`        | `postgresql.JSONB`            | `jsonb`                      | GIN-indexable blobs                            |
| Soft delete    | `is_archived BOOLEAN`         | —                            | Hide rows without losing history               |

`QueryHelper.active(query)` (see *models.py*) auto-filters archived rows.

---

## 1 · `users`

| column | type | description |
| ------ | ---- | ----------- |
| `id`                | INT PK |
| `user_id_str`       | VARCHAR(128) UNIQUE | login / SSO handle |
| `email`             | VARCHAR(255) UNIQUE |
| `password_hash`     | TEXT |
| `password_updated_at` | TIMESTAMPTZ |
| `user_type`         | ENUM (human / model / admin) |
| `created_at`        | TIMESTAMPTZ |
| `is_active`         | BOOL |

**Rationale** – Global identity; `user_type='admin'` bypasses project ACLs.  
Soft-disable via `is_active` keeps audit history.

---

## 2 · `videos`

| column | type    | notes |
| ------ | ------- | ----- |
| `id`             | INT PK |
| `video_uid`      | VARCHAR(180) UNIQUE | file-name or UUID |
| `url`            | TEXT |
| `video_metadata` | JSONB |
| `created_at`     | TIMESTAMPTZ |
| `updated_at`     | TIMESTAMPTZ |
| `is_archived`    | BOOL |

**Rationale** – `video_uid` lets UI find assets without joins; metadata stays searchable.  
Archiving supports takedowns.

---

## 3 · `video_tags`

| column | type | notes |
| ------ | ---- | ----- |
| `video_id` / `tag` | INT, VARCHAR(64) — **composite PK** |
| `tag_source`       | ENUM (model / reviewer) |
| `created_at`       | TIMESTAMPTZ |

*Index*: GIN on `tag`.

**Rationale** – Global flags (NSFW, epilepsy). Only trusted actors write rows.

---

## 4 · `question_groups`

| column | type | default | description |
| ------ | ---- | ------- | ----------- |
| `id`           | INT PK |
| `title`        | TEXT |
| `description`  | TEXT |
| `is_reusable`  | BOOL | **FALSE** | `TRUE` ⇒ can be imported by many schemas (e.g., Shot Transition) |
| `is_archived`  | BOOL | FALSE |

**Rationale** – Most groups one-time; reusable flag allows read-only sharing.

---

## 5 · `questions`

| column | type | notes |
| ------ | ---- | ----- |
| `id`                 | INT PK |
| `text`               | TEXT |
| `type`               | ENUM (single / description) |
| `question_group_id`  | INT nullable |
| `options`            | JSONB array (single-choice) |
| `default_option`     | VARCHAR(120) nullable |
| `is_active`          | BOOL |
| `is_archived`        | BOOL |
| `created_at`         | TIMESTAMPTZ |

**Rationale** – Supports both radio and free-text; single-choice values indexed, descriptions not.

---

## 6 · `schemas`

| column | type | notes |
| ------ | ---- | ----- |
| `id`          | INT PK |
| `name`        | TEXT UNIQUE |
| `rules_json`  | JSONB |
| `created_at`  | TIMESTAMPTZ |
| `updated_at`  | TIMESTAMPTZ |
| `is_archived` | BOOL |

**Link** `schema_questions` – PK `(schema_id, question_id)`, plus `added_at`.

**Rationale** – Reusable question sets; no FK allows forks without cascade.

---

## 7 · `projects`

| column | type | notes |
| ------ | ---- | ----- |
| `id`          | INT PK |
| `name`        | TEXT |
| `schema_id`   | INT |
| `description` | TEXT |
| `created_at`  | TIMESTAMPTZ |
| `updated_at`  | TIMESTAMPTZ |
| `is_archived` | BOOL |

### 7.1 Links

| table | composite PK | notes |
| ----- | ------------ | ----- |
| `project_videos` | `(project_id, video_id)` | plus `added_at` |
| `project_user_roles` | `(project_id, user_id)` | `role`, `assigned_at`, `completed_at`; index on `user_id` |

**Rationale** – A project = schema + video subset + roles; archiving hides it while retaining history.

---

## 8 · `project_groups`

| column | type | notes |
| ------ | ---- | ----- |
| `id`           | INT PK |
| `name`         | TEXT UNIQUE |
| `description`  | TEXT |
| `is_default`   | BOOL |
| `owner_user_id`| INT nullable (NULL ⇒ global) |
| `created_at`   | TIMESTAMPTZ |
| `is_archived`  | BOOL |

### 8.1 Link

| table | composite PK |
| ----- | ------------ |
| `project_group_projects` | `(project_group_id, project_id)` |

**Rationale** – Bundles arbitrary projects for export/report dashboards.

---

## 9 · `answers`

| column | type |
| ------ | ---- |
| `id` | INT PK |
| `video_id`, `question_id`, `user_id`, `project_id` | INT |
| `answer_type`      | ENUM (single / description) |
| `answer_value`     | TEXT |
| `confidence_score` | FLOAT nullable |
| `is_ground_truth`  | BOOL |
| `created_at`       | TIMESTAMPTZ |
| `modified_by_user_id` | INT nullable |
| `notes`            | TEXT |

### 9.1 Constraints & Indexes

| kind | columns / condition |
| ---- | ------------------- |
| Unique | `(video_id, question_id, user_id, project_id)` |
| Partial Unique | `(video_id, question_id, project_id)` **WHERE is_ground_truth** |
| Index | `(question_id)` |
| Index | `(project_id, question_id)` |
| Index | `(project_id, question_id, answer_value)` **WHERE answer_type='single'`** |
| Index | `(video_id, question_id)` |

**Rationale** – One table = full history; indexes make common queries sub-ms.

---

## 10 · `answer_reviews`

| column | type |
| ------ | ---- |
| `id`           | INT PK |
| `answer_id`    | INT |
| `reviewer_id`  | INT |
| `status`       | ENUM (pending / approved / rejected) |
| `comment`      | TEXT |
| `reviewed_at`  | TIMESTAMPTZ |

**Rationale** – Manual moderation of free-text; single-choice rows auto-grade.

---

## Soft-Delete Strategy

* Tables with `is_archived` default to hidden.  
* Use `QueryHelper.active(query)` to auto-filter.  
* Cascade archival (e.g., project ⇒ its answers) inside service code so rules evolve freely.

---

*End of spec — `models.py` mirrors this schema exactly.*
