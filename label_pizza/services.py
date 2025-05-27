# services.py
from sqlalchemy import select, insert, update, func, delete
from sqlalchemy.orm import Session
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, Answer, User, SchemaQuestion
)
from typing import List, Optional, Dict
import pandas as pd
from datetime import datetime
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

# ---------- VIDEO ----------------------------------------------------------
def list_videos(session: Session):
    return session.scalars(select(Video)).all()

def add_video(video_uid: str, url: str, meta: dict, *, session: Session):
    v = Video(video_uid=video_uid, url=url, video_meta=meta)
    session.add(v); session.commit(); return v

# ---------- SCHEMA / QUESTIONS --------------------------------------------
def list_schemas(session: Session):
    return session.scalars(select(Schema)).all()

def list_question_groups(session: Session):
    return session.scalars(select(QuestionGroup)).all()

def list_questions(session: Session):
    return session.scalars(select(Question)).all()

def add_question(text: str, qtype: str, group_id: int | None,
                 options: list[str] | None, default: str | None,
                 *, session: Session):
    q = Question(
        text=text, type=qtype, question_group_id=group_id,
        options=options, default_option=default)
    session.add(q); session.commit(); return q

# ---------- PROJECT --------------------------------------------------------
def list_projects(session: Session):
    return session.scalars(select(Project)).all()

def create_project(name: str, schema_id: int, video_ids: list[int], *,
                   session: Session):
    proj = Project(name=name, schema_id=schema_id)
    session.add(proj); session.flush()
    session.bulk_save_objects([ProjectVideo(
        project_id=proj.id, video_id=vid) for vid in video_ids])
    session.commit(); return proj

def project_progress(project_id: int, *, session: Session):
    total = session.scalar(
        select(Answer).where(Answer.project_id == project_id).count()
    )
    return {"answer_rows": total}

# ---------- USERS / ROLES --------------------------------------------------
def add_role(project_id: int, user_id: int, role: str, *, session: Session):
    stmt = insert(ProjectUserRole).values(
        project_id=project_id, user_id=user_id, role=role
    ).on_conflict_do_update(
        index_elements=["project_id", "user_id"],
        set_=dict(role=role)
    )
    session.execute(stmt); session.commit()

class VideoService:
    @staticmethod
    def get_all_videos(session: Session) -> pd.DataFrame:
        """Get all videos with their project assignments and ground truth status."""
        rows = []
        for v in session.scalars(select(Video)).all():
            # Get all projects this video belongs to
            projects = session.scalars(
                select(Project)
                .join(ProjectVideo, Project.id == ProjectVideo.project_id)
                .where(ProjectVideo.video_id == v.id)
            ).all()
            
            # For each project, check if video has complete ground truth
            project_status = []
            for p in projects:
                # Get total questions in schema
                total_questions = session.scalar(
                    select(func.count())
                    .select_from(SchemaQuestion)
                    .where(SchemaQuestion.schema_id == p.schema_id)
                )
                
                # Get ground truth answers for this video in this project
                gt_answers = session.scalar(
                    select(func.count())
                    .select_from(Answer)
                    .where(
                        Answer.video_id == v.id,
                        Answer.project_id == p.id,
                        Answer.is_ground_truth == True
                    )
                )
                
                status = "✓" if gt_answers == total_questions else "✗"
                project_status.append(f"{p.name}: {status}")
            
            rows.append({
                "Video UID": v.video_uid,
                "URL": v.url,
                "Projects": ", ".join(project_status) if project_status else "No projects",
            })
        return pd.DataFrame(rows)

    @staticmethod
    def add_video(url: str, session: Session) -> None:
        """Add a new video with validation."""
        # Extract video_uid from URL (using the full filename)
        video_uid = url.split("/")[-1]
        if not video_uid:
            raise ValueError("URL must end with a filename")
        
        # Check if video already exists
        existing = session.scalar(
            select(Video).where(Video.video_uid == video_uid)
        )
        if existing:
            raise ValueError(f"Video with UID '{video_uid}' already exists")
        
        session.add(Video(video_uid=video_uid, url=url))
        session.commit()

class ProjectService:
    @staticmethod
    def get_all_projects(session: Session) -> pd.DataFrame:
        rows = []
        for p in session.scalars(select(Project)).all():
            v_total = session.scalar(select(func.count()).select_from(ProjectVideo)
                                .where(ProjectVideo.project_id == p.id))
            q_total = session.scalar(select(func.count()).select_from(SchemaQuestion)
                                .where(SchemaQuestion.schema_id == p.schema_id))
            gt = session.scalar(select(func.count()).select_from(Answer)
                            .where(Answer.project_id == p.id, Answer.is_ground_truth))
            pct = round(gt / (v_total * q_total) * 100, 2) if v_total * q_total else 0.0
            rows.append({
                "Name": p.name,
                "Videos": v_total,
                "Schema ID": p.schema_id,
                "GT %": pct,
            })
        return pd.DataFrame(rows)

    @staticmethod
    def create_project(name: str, schema_id: int, video_ids: List[int], session: Session) -> None:
        p = Project(name=name, schema_id=schema_id)
        session.add(p)
        session.flush()
        session.bulk_save_objects([
            ProjectVideo(project_id=p.id, video_id=v) for v in video_ids
        ])
        session.commit()

    @staticmethod
    def get_video_ids_by_uids(video_uids: List[str], session: Session) -> List[int]:
        return session.scalars(select(Video.id).where(Video.video_uid.in_(video_uids))).all()

class SchemaService:
    @staticmethod
    def get_all_schemas(session: Session) -> pd.DataFrame:
        return pd.DataFrame([
            {"ID": s.id, "Name": s.name, "Rules": s.rules_json}
            for s in session.scalars(select(Schema)).all()
        ])

    @staticmethod
    def get_schema_questions(schema_id: int, session: Session) -> pd.DataFrame:
        qids = session.scalars(select(SchemaQuestion.question_id)
                             .where(SchemaQuestion.schema_id == schema_id)).all()
        qs = session.scalars(select(Question).where(Question.id.in_(qids))).all()
        return pd.DataFrame([{"ID": q.id, "Text": q.text} for q in qs])

    @staticmethod
    def get_schema_id_by_name(name: str, session: Session) -> int:
        schema = session.scalar(select(Schema).where(Schema.name == name))
        if not schema:
            raise ValueError(f"Schema '{name}' not found")
        return schema.id

    @staticmethod
    def create_schema(name: str, rules_json: dict, session: Session) -> Schema:
        schema = Schema(name=name, rules_json=rules_json)
        session.add(schema)
        session.commit()
        return schema

    @staticmethod
    def add_question_to_schema(schema_id: int, question_id: int, session: Session) -> None:
        sq = SchemaQuestion(schema_id=schema_id, question_id=question_id)
        session.add(sq)
        session.commit()

class QuestionService:
    @staticmethod
    def get_all_questions(session: Session) -> pd.DataFrame:
        qs = session.scalars(select(Question)).all()
        return pd.DataFrame([
            {
                "ID": q.id, 
                "Text": q.text, 
                "Type": q.type,
                "Group": session.scalar(
                    select(QuestionGroup.title)
                    .where(QuestionGroup.id == q.question_group_id)
                ) if q.question_group_id else None,
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            } for q in qs
        ])

    @staticmethod
    def add_question(text: str, qtype: str, group_name: Optional[str],
                    options: Optional[List[str]], default: Optional[str], 
                    session: Session) -> None:
        # Validate default option for single-choice questions
        if qtype == "single":
            if not options:
                raise ValueError("Single-choice questions must have options")
            if default and default not in options:
                raise ValueError(f"Default option '{default}' must be one of the available options: {', '.join(options)}")

        # Always create or use a group
        if not group_name:
            group_name = text  # Use question text as group name if none provided
        group = QuestionGroupService.get_group_by_name(group_name, session)
        if not group:
            group = QuestionGroupService.create_group(
                title=group_name,
                description="",
                is_reusable=False,
                session=session
            )

        q = Question(
            text=text, 
            type=qtype, 
            question_group_id=group.id,
            options=options, 
            default_option=default
        )
        session.add(q)
        session.commit()

    @staticmethod
    def get_question_by_text(text: str, session: Session) -> Optional[Question]:
        return session.scalar(select(Question).where(Question.text == text))

    @staticmethod
    def edit_question(text: str, new_text: str, new_group: Optional[str],
                     new_opts: Optional[List[str]], new_default: Optional[str],
                     session: Session) -> None:
        q = QuestionService.get_question_by_text(text, session)
        if not q:
            raise ValueError(f"Question with text '{text}' not found")
        
        q.text = new_text
        
        # Always create or use a group
        if not new_group:
            new_group = new_text  # Use new question text as group name if none provided
        group = QuestionGroupService.get_group_by_name(new_group, session)
        if not group:
            group = QuestionGroupService.create_group(
                title=new_group,
                description="",
                is_reusable=False,
                session=session
            )
        q.question_group_id = group.id

        if q.type == "single":
            if not new_opts:
                raise ValueError("Single-choice questions must have options")
            if new_default and new_default not in new_opts:
                raise ValueError(f"Default option '{new_default}' must be one of the available options: {', '.join(new_opts)}")
            q.options = new_opts
            q.default_option = new_default

        session.commit()

    @staticmethod
    def archive_question(question_id: int, session: Session) -> None:
        q = session.get(Question, question_id)
        if not q:
            raise ValueError(f"Question with ID {question_id} not found")
        q.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_question(question_id: int, session: Session) -> None:
        q = session.get(Question, question_id)
        if not q:
            raise ValueError(f"Question with ID {question_id} not found")
        q.is_archived = False
        session.commit()

class AuthService:
    @staticmethod
    def authenticate(email: str, pwd: str, role: str, session: Session) -> Optional[dict]:
        u = session.scalar(select(User).where(User.email == email, 
                                           User.password_hash == pwd, 
                                           User.is_active))
        if not u:
            return None
        if role != "admin" and u.user_type != role:
            return None
        return {"id": u.id, "name": u.user_id_str, "role": u.user_type}

    @staticmethod
    def seed_admin(session: Session) -> None:
        """Create hard‑coded admin if not present."""
        if not session.scalar(select(User).where(User.email == "zhiqiulin98@gmail.com")):
            session.add(User(user_id_str="admin", 
                           email="zhiqiulin98@gmail.com",
                           password_hash="zhiqiulin98", 
                           user_type="admin", 
                           is_active=True))
            session.commit()

    @staticmethod
    def get_all_users(session: Session) -> pd.DataFrame:
        """Get all users in a DataFrame format."""
        users = session.scalars(select(User)).all()
        return pd.DataFrame([
            {
                "ID": u.id,
                "User ID": u.user_id_str,
                "Email": u.email,
                "Role": u.user_type,
                "Active": u.is_active,
                "Created At": u.created_at
            } for u in users
        ])

    @staticmethod
    def update_user_role(user_id: int, new_role: str, session: Session) -> None:
        """Update a user's role."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if new_role not in ["human", "model", "admin"]:
            raise ValueError(f"Invalid role: {new_role}")
        user.user_type = new_role
        session.commit()

    @staticmethod
    def toggle_user_active(user_id: int, session: Session) -> None:
        """Toggle a user's active status."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        user.is_active = not user.is_active
        session.commit()

    @staticmethod
    def get_project_assignments(session: Session) -> pd.DataFrame:
        """Get all project assignments in a DataFrame format."""
        assignments = session.scalars(
            select(ProjectUserRole)
            .join(Project, ProjectUserRole.project_id == Project.id)
            .join(User, ProjectUserRole.user_id == User.id)
        ).all()
        
        return pd.DataFrame([
            {
                "Project ID": a.project_id,
                "Project Name": session.get(Project, a.project_id).name,
                "User ID": a.user_id,
                "User Name": session.get(User, a.user_id).user_id_str,
                "Role": a.role,
                "Assigned At": a.assigned_at,
                "Completed At": a.completed_at
            } for a in assignments
        ])

    @staticmethod
    def create_user(user_id: str, email: str, password_hash: str, user_type: str, session: Session) -> User:
        """Create a new user with validation."""
        if user_type not in ["human", "model", "admin"]:
            raise ValueError("Invalid user type. Must be one of: human, model, admin")
        
        # Check if user already exists
        existing_user = session.scalar(
            select(User).where(
                (User.user_id_str == user_id) | (User.email == email)
            )
        )
        if existing_user:
            raise ValueError(f"User with ID '{user_id}' or email '{email}' already exists")
        
        user = User(
            user_id_str=user_id,
            email=email,
            password_hash=password_hash,
            user_type=user_type,
            is_active=True
        )
        session.add(user)
        session.commit()
        return user

    @staticmethod
    def assign_user_to_project(user_id: int, project_id: int, role: str, session: Session) -> None:
        """Assign a user to a project with role validation and admin privileges."""
        if role not in ["annotator", "reviewer", "admin", "model"]:
            raise ValueError("Invalid role. Must be one of: annotator, reviewer, admin, model")
        
        # Get user and project
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # If user is an admin, they automatically get reviewer role
        if user.user_type == "admin" and role != "admin":
            role = "reviewer"
        
        # Check if assignment already exists
        existing = session.scalar(
            select(ProjectUserRole).where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id
            )
        )
        
        if existing:
            existing.role = role
        else:
            assignment = ProjectUserRole(
                project_id=project_id,
                user_id=user_id,
                role=role
            )
            session.add(assignment)
        
        session.commit()

    @staticmethod
    def remove_user_from_project(user_id: int, project_id: int, session: Session) -> None:
        """Remove a user's assignment from a project."""
        assignment = session.scalar(
            select(ProjectUserRole).where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id
            )
        )
        
        if not assignment:
            raise ValueError(f"No assignment found for user {user_id} in project {project_id}")
        
        session.delete(assignment)
        session.commit()

    @staticmethod
    def bulk_assign_users_to_project(user_ids: List[int], project_id: int, role: str, session: Session) -> None:
        """Assign multiple users to a project with the same role."""
        for user_id in user_ids:
            try:
                AuthService.assign_user_to_project(user_id, project_id, role, session)
            except ValueError as e:
                # Log error but continue with other assignments
                print(f"Error assigning user {user_id}: {str(e)}")

    @staticmethod
    def bulk_remove_users_from_project(user_ids: List[int], project_id: int, session: Session) -> None:
        """Remove multiple users from a project."""
        session.execute(
            delete(ProjectUserRole).where(
                ProjectUserRole.user_id.in_(user_ids),
                ProjectUserRole.project_id == project_id
            )
        )
        session.commit()

class QuestionGroupService:
    @staticmethod
    def get_all_groups(session: Session) -> pd.DataFrame:
        groups = session.scalars(select(QuestionGroup)).all()
        rows = []
        for g in groups:
            # Get all questions in this group
            questions = session.scalars(
                select(Question)
                .where(Question.question_group_id == g.id)
            ).all()
            
            # Get all schemas using this group
            schemas = session.scalars(
                select(Schema)
                .join(SchemaQuestion, Schema.id == SchemaQuestion.schema_id)
                .join(Question, SchemaQuestion.question_id == Question.id)
                .where(Question.question_group_id == g.id)
                .distinct()
            ).all()
            
            # Format questions as a list of strings
            question_list = []
            for q in questions:
                q_str = f"- {q.text} ({q.type})"
                if q.type == "single" and q.options:
                    q_str += f" [Options: {', '.join(q.options)}"
                    if q.default_option:
                        q_str += f", Default: {q.default_option}"
                    q_str += "]"
                if q.is_archived:
                    q_str += " [ARCHIVED]"
                question_list.append(q_str)
            
            rows.append({
                "ID": g.id,
                "Name": g.title,
                "Description": g.description,
                "Questions": "\n".join(question_list) if question_list else "No questions",
                "Reusable": g.is_reusable,
                "Archived": g.is_archived,
                "Question Count": len(questions),
                "Archived Questions": sum(1 for q in questions if q.is_archived),
                "Used in Schemas": ", ".join(s.name for s in schemas) if schemas else "None"
            })
        return pd.DataFrame(rows)

    @staticmethod
    def get_group_questions(group_id: int, session: Session) -> pd.DataFrame:
        questions = session.scalars(
            select(Question)
            .where(Question.question_group_id == group_id)
        ).all()
        
        return pd.DataFrame([
            {
                "ID": q.id,
                "Text": q.text,
                "Type": q.type,
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            } for q in questions
        ])

    @staticmethod
    def get_group_details(group_id: int, session: Session) -> dict:
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        return {
            "title": group.title,
            "description": group.description,
            "is_reusable": group.is_reusable,
            "is_archived": group.is_archived
        }

    @staticmethod
    def create_group(title: str, description: str, is_reusable: bool, session: Session) -> QuestionGroup:
        """Create a new question group with validation."""
        # Check if group with same title exists
        existing = session.scalar(
            select(QuestionGroup).where(QuestionGroup.title == title)
        )
        if existing:
            raise ValueError(f"Question group with title '{title}' already exists")
        
        group = QuestionGroup(
            title=title,
            description=description,
            is_reusable=is_reusable
        )
        session.add(group)
        session.commit()
        return group

    @staticmethod
    def get_group_by_name(name: str, session: Session) -> Optional[QuestionGroup]:
        return session.scalar(select(QuestionGroup).where(QuestionGroup.title == name))

    @staticmethod
    def edit_group(group_id: int, new_title: str, new_description: str, is_reusable: bool, session: Session) -> None:
        """Edit a question group with validation."""
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        # If making a group non-reusable, check if it's used in multiple schemas
        if not is_reusable and group.is_reusable:
            schemas = session.scalars(
                select(Schema)
                .join(SchemaQuestion, Schema.id == SchemaQuestion.schema_id)
                .join(Question, SchemaQuestion.question_id == Question.id)
                .where(Question.question_group_id == group_id)
                .distinct()
            ).all()
            
            if len(schemas) > 1:
                raise ValueError(
                    f"Cannot make group non-reusable as it is used in multiple schemas: "
                    f"{', '.join(s.name for s in schemas)}"
                )
        
        # Check if new title conflicts with existing group
        if new_title != group.title:
            existing = session.scalar(
                select(QuestionGroup).where(QuestionGroup.title == new_title)
            )
            if existing:
                raise ValueError(f"Question group with title '{new_title}' already exists")
        
        group.title = new_title
        group.description = new_description
        group.is_reusable = is_reusable
        session.commit()

    @staticmethod
    def archive_group(group_id: int, session: Session) -> None:
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        group.is_archived = True
        # Also archive all questions in this group
        questions = session.scalars(
            select(Question)
            .where(Question.question_group_id == group_id)
        ).all()
        for q in questions:
            q.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_group(group_id: int, session: Session) -> None:
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        group.is_archived = False
        session.commit()
