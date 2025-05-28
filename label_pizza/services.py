# services.py
from sqlalchemy import select, insert, update, func, delete, exists
from sqlalchemy.orm import Session
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, Answer, User, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup
)
from typing import List, Optional, Dict
import pandas as pd
from datetime import datetime, timezone
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

# ---------- VIDEO ----------------------------------------------------------
def list_videos(session: Session):
    return session.scalars(select(Video)).all()

def add_video(video_uid: str, url: str, meta: dict, *, session: Session):
    v = Video(video_uid=video_uid, url=url, video_metadata=meta)
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
    # Create question
    q = Question(
        text=text, 
        type=qtype,
        options=options, 
        default_option=default
    )
    session.add(q)
    session.flush()  # Get the question ID
    
    # Add question to group if group_id is provided
    if group_id:
        session.add(QuestionGroupQuestion(
            question_group_id=group_id,
            question_id=q.id,
            display_order=0  # Default order
        ))
    
    session.commit()
    return q

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
        for v in session.scalars(select(Video).where(Video.is_archived == False)).all():
            # Get all non-archived projects this video belongs to
            projects = session.scalars(
                select(Project)
                .join(ProjectVideo, Project.id == ProjectVideo.project_id)
                .where(
                    ProjectVideo.video_id == v.id,
                    Project.is_archived == False
                )
            ).all()
            
            # Skip videos that only belong to archived projects
            if not projects:
                continue
            
            # For each project, check if video has complete ground truth
            project_status = []
            for p in projects:
                # Get total questions in schema through question groups
                total_questions = session.scalar(
                    select(func.count())
                    .select_from(Question)
                    .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                    .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                    .where(SchemaQuestionGroup.schema_id == p.schema_id)
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
                
                # Get any answers (ground truth or not) for this video in this project
                any_answers = session.scalar(
                    select(func.count())
                    .select_from(Answer)
                    .where(
                        Answer.video_id == v.id,
                        Answer.project_id == p.id
                    )
                )
                
                # Determine status based on answers
                if total_questions == 0:
                    status = "No questions"
                elif gt_answers == total_questions:
                    status = "✓"
                else:
                    status = "✗"
                project_status.append(f"{p.name}: {status}")
            
            rows.append({
                "Video UID": v.video_uid,
                "URL": v.url,
                "Projects": ", ".join(project_status) if project_status else "No projects",
            })
        return pd.DataFrame(rows)

    @staticmethod
    def add_video(url: str, session: Session, metadata: dict = None) -> None:
        """Add a new video to the database.
        
        Args:
            url: The URL of the video
            session: Database session
            metadata: Optional dictionary containing video metadata
            
        Raises:
            ValueError: If URL is invalid, video already exists, or metadata is invalid
        """
        if not url.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        
        # Extract filename and check for extension
        filename = url.split("/")[-1]
        if not filename or "." not in filename:
            raise ValueError("URL must end with a filename with extension")
        
        if len(filename) > 255:
            raise ValueError("Video UID is too long")
        
        # Validate metadata type - must be None or a dictionary
        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError("Metadata must be a dictionary")
            if not metadata:
                raise ValueError("Metadata must be a non-empty dictionary")
            
            # Validate metadata value types if metadata is provided
            for key, value in metadata.items():
                if not isinstance(value, (str, int, float, bool, list, dict)):
                    raise ValueError(f"Invalid metadata value type for key '{key}': {type(value)}")
                if isinstance(value, list):
                    # Validate list elements
                    for item in value:
                        if not isinstance(item, (str, int, float, bool, dict)):
                            raise ValueError(f"Invalid list element type in metadata key '{key}': {type(item)}")
                elif isinstance(value, dict):
                    # Validate nested dictionary values
                    for k, v in value.items():
                        if not isinstance(v, (str, int, float, bool, list, dict)):
                            raise ValueError(f"Invalid nested metadata value type for key '{key}.{k}': {type(v)}")
        
        # Check if video already exists
        existing = session.scalar(
            select(Video).where(Video.video_uid == filename)
        )
        if existing:
            raise ValueError(f"Video with UID '{filename}' already exists")
        
        # Create video
        video = Video(
            video_uid=filename,
            url=url,
            video_metadata=metadata or {}
        )
        session.add(video)
        session.commit()

class ProjectService:
    @staticmethod
    def get_all_projects(session: Session) -> pd.DataFrame:
        """Get all non-archived projects with their video counts and ground truth percentages."""
        rows = []
        for p in session.scalars(select(Project).where(Project.is_archived == False)).all():
            # Get schema
            schema = session.get(Schema, p.schema_id)
            if schema.is_archived:
                continue
            
            # Count videos in project
            video_count = session.scalar(
                select(func.count())
                .select_from(ProjectVideo)
                .where(ProjectVideo.project_id == p.id)
            )
            
            # Get total questions in schema
            total_questions = session.scalar(
                select(func.count())
                .select_from(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == p.schema_id)
            )
            
            # Get ground truth answers
            gt_answers = session.scalar(
                select(func.count())
                .select_from(Answer)
                .where(
                    Answer.project_id == p.id,
                    Answer.is_ground_truth == True
                )
            )
            
            # Calculate percentage
            gt_percentage = (gt_answers / total_questions * 100) if total_questions > 0 else 0.0
            
            rows.append({
                "ID": p.id,
                "Name": p.name,
                "Videos": video_count,
                "Schema ID": p.schema_id,
                "GT %": gt_percentage
            })
        return pd.DataFrame(rows)

    @staticmethod
    def create_project(name: str, schema_id: int, video_ids: List[int], session: Session) -> None:
        """Create a new project and assign all admin users to it.
        
        Args:
            name: Project name
            schema_id: ID of the schema to use
            video_ids: List of video IDs to include in the project
            session: Database session
            
        Raises:
            ValueError: If schema or any video is archived, or if any video is already in another project
        """
        # Check if schema exists and is not archived
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        if schema.is_archived:
            raise ValueError(f"Schema with ID {schema_id} is archived")
        
        # Create project
        project = Project(name=name, schema_id=schema_id)
        session.add(project)
        session.flush()  # Get the project ID
        
        # Add videos to project
        for vid in video_ids:
            # Check if video exists and is not archived
            video = session.get(Video, vid)
            if not video:
                raise ValueError(f"Video with ID {vid} not found")
            if video.is_archived:
                raise ValueError(f"Video with ID {vid} is archived")
            
            # Check if video is already in any project
            existing = session.scalar(
                select(ProjectVideo).where(
                    ProjectVideo.video_id == vid
                )
            )
            if existing:
                raise ValueError(f"Video {vid} is already in project {existing.project_id}")
            
            session.add(ProjectVideo(project_id=project.id, video_id=vid))
        
        # Assign all admin users to the project
        admin_users = session.scalars(
            select(User).where(User.user_type == "admin", User.is_archived == False)
        ).all()
        
        for admin in admin_users:
            session.add(ProjectUserRole(
                user_id=admin.id,
                project_id=project.id,
                role="admin"
            ))
        
        session.commit()

    @staticmethod
    def get_video_ids_by_uids(video_uids: List[str], session: Session) -> List[int]:
        """Get video IDs from their UIDs.
        
        Args:
            video_uids: List of video UIDs
            session: Database session
            
        Returns:
            List of video IDs
        """
        return session.scalars(select(Video.id).where(Video.video_uid.in_(video_uids))).all()

    @staticmethod
    def archive_project(project_id: int, session: Session) -> None:
        """Archive a project and block new answers.
        
        Args:
            project_id: The ID of the project to archive
            session: Database session
            
        Raises:
            ValueError: If project not found
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        project.is_archived = True
        session.commit()

    @staticmethod
    def progress(project_id: int, session: Session) -> dict:
        """Get project progress statistics.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Dictionary containing:
            - total_videos: Number of videos in project
            - total_questions: Number of questions in schema
            - total_answers: Total number of answers submitted
            - ground_truth_answers: Number of ground truth answers
            - completion_percentage: Percentage of questions with ground truth answers
            
        Raises:
            ValueError: If project not found
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get total videos in project
        total_videos = session.scalar(
            select(func.count())
            .select_from(ProjectVideo)
            .where(ProjectVideo.project_id == project_id)
        )
        
        # Get total questions in schema through question groups
        total_questions = session.scalar(
            select(func.count())
            .select_from(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == project.schema_id)
        )
        
        # Get total answers
        total_answers = session.scalar(
            select(func.count())
            .select_from(Answer)
            .where(Answer.project_id == project_id)
        )
        
        # Get ground truth answers
        ground_truth_answers = session.scalar(
            select(func.count())
            .select_from(Answer)
            .where(
                Answer.project_id == project_id,
                Answer.is_ground_truth == True
            )
        )
        
        # Calculate completion percentage
        total_possible_answers = total_videos * total_questions
        completion_percentage = round(
            (ground_truth_answers / total_possible_answers * 100) if total_possible_answers > 0 else 0,
            2
        )
        
        return {
            "total_videos": total_videos,
            "total_questions": total_questions,
            "total_answers": total_answers,
            "ground_truth_answers": ground_truth_answers,
            "completion_percentage": completion_percentage
        }

class SchemaService:
    @staticmethod
    def get_all_schemas(session: Session) -> pd.DataFrame:
        """Get all schemas with their question groups.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing schemas with columns:
            - ID: Schema ID
            - Name: Schema name
            - Rules: Schema rules JSON
            - Question Groups: List of question groups in schema
        """
        schemas = session.scalars(select(Schema)).all()
        rows = []
        for s in schemas:
            # Get question groups for this schema
            groups = session.scalars(
                select(QuestionGroup)
                .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == s.id)
            ).all()
            
            rows.append({
                "ID": s.id,
                "Name": s.name,
                "Rules": s.rules_json,
                "Question Groups": ", ".join(g.title for g in groups) if groups else "No groups"
            })
        return pd.DataFrame(rows)

    @staticmethod
    def get_schema_questions(schema_id: int, session: Session) -> pd.DataFrame:
        """Get all questions in a schema through its question groups.
        
        Args:
            schema_id: The ID of the schema
            session: Database session
            
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text
            - Group: Question group name
            - Type: Question type
            - Options: Available options for single-choice questions
        """
        # Get questions through question groups
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == schema_id)
        ).all()
        
        return pd.DataFrame([
            {
                "ID": q.id,
                "Text": q.text,
                "Group": session.scalar(
                    select(QuestionGroup.title)
                    .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                    .where(QuestionGroupQuestion.question_id == q.id)
                ),
                "Type": q.type,
                "Options": ", ".join(q.options or []) if q.options else ""
            }
            for q in questions
        ])

    @staticmethod
    def get_schema_id_by_name(name: str, session: Session) -> int:
        """Get schema ID by name.
        
        Args:
            name: Schema name
            session: Database session
            
        Returns:
            Schema ID
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.scalar(select(Schema).where(Schema.name == name))
        if not schema:
            raise ValueError(f"Schema '{name}' not found")
        return schema.id

    @staticmethod
    def create_schema(name: str, rules_json: dict, session: Session) -> Schema:
        """Create a new schema.
        
        Args:
            name: Schema name
            rules_json: Schema rules as JSON
            session: Database session
            
        Returns:
            Created schema
            
        Raises:
            ValueError: If schema with same name exists
        """
        # Check if schema with same name exists
        existing = session.scalar(select(Schema).where(Schema.name == name))
        if existing:
            raise ValueError(f"Schema with name '{name}' already exists")
            
        schema = Schema(name=name, rules_json=rules_json)
        session.add(schema)
        session.commit()
        return schema

    @staticmethod
    def add_question_group_to_schema(schema_id: int, group_id: int, display_order: int, session: Session) -> None:
        """Add a question group to a schema.
        
        Args:
            schema_id: Schema ID
            group_id: Question group ID
            display_order: Display order in schema
            session: Database session
            
        Raises:
            ValueError: If schema or group not found, or if group already in schema
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        # Check if group exists
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
            
        # Check if group is already in schema
        existing = session.scalar(
            select(SchemaQuestionGroup).where(
                SchemaQuestionGroup.schema_id == schema_id,
                SchemaQuestionGroup.question_group_id == group_id
            )
        )
        if existing:
            raise ValueError(f"Question group {group_id} already in schema {schema_id}")
            
        # Add group to schema
        sqg = SchemaQuestionGroup(
            schema_id=schema_id,
            question_group_id=group_id,
            display_order=display_order
        )
        session.add(sqg)
        session.commit()

    @staticmethod
    def remove_question_group_from_schema(schema_id: int, group_id: int, session: Session) -> None:
        """Remove a question group from a schema.
        
        Args:
            schema_id: Schema ID
            group_id: Question group ID
            session: Database session
            
        Raises:
            ValueError: If schema or group not found, or if group not in schema
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        # Check if group exists
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
            
        # Check if group is in schema
        existing = session.scalar(
            select(SchemaQuestionGroup).where(
                SchemaQuestionGroup.schema_id == schema_id,
                SchemaQuestionGroup.question_group_id == group_id
            )
        )
        if not existing:
            raise ValueError(f"Question group {group_id} not in schema {schema_id}")
            
        session.delete(existing)
        session.commit()

    @staticmethod
    def archive_schema(schema_id: int, session: Session) -> None:
        """Archive a schema and prevent its use in new projects.
        
        Args:
            schema_id: The ID of the schema to archive
            session: Database session
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        schema.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_schema(schema_id: int, session: Session) -> None:
        """Unarchive a schema to allow its use in new projects.
        
        Args:
            schema_id: The ID of the schema to unarchive
            session: Database session
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        schema.is_archived = False
        session.commit()

class QuestionService:
    @staticmethod
    def get_all_questions(session: Session) -> pd.DataFrame:
        """Get all questions with their group information.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text
            - Type: Question type
            - Group: Question group name
            - Options: Available options for single-choice questions
            - Default: Default option for single-choice questions
            - Archived: Whether the question is archived
        """
        qs = session.scalars(select(Question)).all()
        return pd.DataFrame([
            {
                "ID": q.id, 
                "Text": q.text, 
                "Type": q.type,
                "Group": session.scalar(
                    select(QuestionGroup.title)
                    .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                    .where(QuestionGroupQuestion.question_id == q.id)
                ),
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            } for q in qs
        ])

    @staticmethod
    def add_question(text: str, qtype: str, group_name: Optional[str],
                    options: Optional[List[str]], default: Optional[str], 
                    session: Session) -> None:
        """Add a new question to a group.
        
        Args:
            text: Question text
            qtype: Question type ('single' or 'description')
            group_name: Name of the question group
            options: List of options for single-choice questions
            default: Default option for single-choice questions
            session: Database session
            
        Raises:
            ValueError: If question text already exists or validation fails
        """
        # Check if question text already exists
        existing = session.scalar(select(Question).where(Question.text == text))
        if existing:
            raise ValueError(f"Question with text '{text}' already exists")
            
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

        # Create question
        q = Question(
            text=text, 
            type=qtype, 
            options=options, 
            default_option=default
        )
        session.add(q)
        session.flush()  # Get the question ID
        
        # Add question to group
        session.add(QuestionGroupQuestion(
            question_group_id=group.id,
            question_id=q.id,
            display_order=0  # Default order
        ))
        session.commit()

    @staticmethod
    def get_question_by_text(text: str, session: Session) -> Optional[Question]:
        """Get a question by its text.
        
        Args:
            text: Question text
            session: Database session
            
        Returns:
            Question object if found, None otherwise
        """
        return session.scalar(select(Question).where(Question.text == text))

    @staticmethod
    def edit_question(text: str, new_text: str, new_group: Optional[str],
                     new_opts: Optional[List[str]], new_default: Optional[str],
                     session: Session) -> None:
        """Edit an existing question.
        
        Args:
            text: Current question text
            new_text: New question text
            new_group: New question group name
            new_opts: New options for single-choice questions
            new_default: New default option for single-choice questions
            session: Database session
            
        Raises:
            ValueError: If question not found or validation fails
        """
        q = QuestionService.get_question_by_text(text, session)
        if not q:
            raise ValueError(f"Question with text '{text}' not found")
        
        # Check if new text conflicts with existing question
        if new_text != text:
            existing = session.scalar(select(Question).where(Question.text == new_text))
            if existing:
                raise ValueError(f"Question with text '{new_text}' already exists")
        
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
            
        # Update question group assignment
        existing_assignment = session.scalar(
            select(QuestionGroupQuestion)
            .where(QuestionGroupQuestion.question_id == q.id)
        )
        if existing_assignment:
            existing_assignment.question_group_id = group.id
        else:
            session.add(QuestionGroupQuestion(
                question_group_id=group.id,
                question_id=q.id,
                display_order=0  # Default order
            ))

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
        """Archive a question.
        
        Args:
            question_id: Question ID
            session: Database session
            
        Raises:
            ValueError: If question not found
        """
        q = session.get(Question, question_id)
        if not q:
            raise ValueError(f"Question with ID {question_id} not found")
        q.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_question(question_id: int, session: Session) -> None:
        """Unarchive a question.
        
        Args:
            question_id: Question ID
            session: Database session
            
        Raises:
            ValueError: If question not found
        """
        q = session.get(Question, question_id)
        if not q:
            raise ValueError(f"Question with ID {question_id} not found")
        q.is_archived = False
        session.commit()

class AuthService:
    @staticmethod
    def authenticate(email: str, pwd: str, role: str, session: Session) -> Optional[dict]:
        """Authenticate a user.
        
        Args:
            email: User's email
            pwd: User's password
            role: Required role
            session: Database session
            
        Returns:
            Dictionary containing user info if authenticated, None otherwise
        """
        u = session.scalar(select(User).where(
            User.email == email, 
            User.password_hash == pwd, 
            User.is_active == True
        ))
        if not u:
            return None
        if role != "admin" and u.user_type != role:
            return None
        return {"id": u.id, "name": u.user_id_str, "role": u.user_type}

    @staticmethod
    def seed_admin(session: Session) -> None:
        """Create hard‑coded admin if not present."""
        if not session.scalar(select(User).where(User.email == "zhiqiulin98@gmail.com")):
            session.add(User(
                user_id_str="admin", 
                email="zhiqiulin98@gmail.com",
                password_hash="zhiqiulin98", 
                user_type="admin", 
                is_active=True
            ))
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
                "Password Hash": u.password_hash,
                "Role": u.user_type,
                "Active": u.is_active,
                "Created At": u.created_at
            } for u in users
        ])

    @staticmethod
    def update_user_id(user_id: int, new_user_id: str, session: Session) -> None:
        """Update a user's ID."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        # Check if new user ID already exists
        existing = session.scalar(
            select(User).where(User.user_id_str == new_user_id)
        )
        if existing and existing.id != user_id:
            raise ValueError(f"User ID '{new_user_id}' already exists")
        
        user.user_id_str = new_user_id
        session.commit()

    @staticmethod
    def update_user_email(user_id: int, new_email: str, session: Session) -> None:
        """Update a user's email."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        # Check if new email already exists
        existing = session.scalar(
            select(User).where(User.email == new_email)
        )
        if existing and existing.id != user_id:
            raise ValueError(f"Email '{new_email}' already exists")
        
        user.email = new_email
        session.commit()

    @staticmethod
    def update_user_password(user_id: int, new_password: str, session: Session) -> None:
        """Update a user's password."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        user.password_hash = new_password  # Note: In production, this should be hashed
        user.password_updated_at = datetime.now(timezone.utc)
        session.commit()

    @staticmethod
    def update_user_role(user_id: int, new_role: str, session: Session) -> None:
        """Update a user's role and handle admin project assignments."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if new_role not in ["human", "model", "admin"]:
            raise ValueError(f"Invalid role: {new_role}")
        
        # If changing to admin role, assign to all projects
        if new_role == "admin" and user.user_type != "admin":
            # Get all non-archived projects
            projects = session.scalars(
                select(Project).where(Project.is_archived == False)
            ).all()
            
            # Assign user as admin to each project
            for project in projects:
                # Check if assignment already exists
                existing = session.scalar(
                    select(ProjectUserRole).where(
                        ProjectUserRole.user_id == user_id,
                        ProjectUserRole.project_id == project.id
                    )
                )
                if existing:
                    existing.role = "admin"
                else:
                    session.add(ProjectUserRole(
                        user_id=user_id,
                        project_id=project.id,
                        role="admin"
                    ))
        
        # If changing from admin to human, update all project roles
        if user.user_type == "admin" and new_role == "human":
            # Get all project assignments for this user
            assignments = session.scalars(
                select(ProjectUserRole)
                .where(ProjectUserRole.user_id == user_id)
            ).all()
            
            # Update each assignment to human role
            for assignment in assignments:
                assignment.role = "human"
        
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
        if not user.is_active:
            raise ValueError(f"User with ID {user_id} is not active")
        
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        if project.is_archived:
            raise ValueError(f"Project with ID {project_id} is archived")
        
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

    @staticmethod
    def assign_admin_to_all_projects(user_id: int, session: Session) -> None:
        """Assign a user as admin to all existing and future projects."""
        # Get all non-archived projects
        projects = session.scalars(
            select(Project).where(Project.is_archived == False)
        ).all()
        
        # Assign user as admin to each project
        for project in projects:
            # Check if assignment already exists
            existing = session.scalar(
                select(ProjectUserRole).where(
                    ProjectUserRole.user_id == user_id,
                    ProjectUserRole.project_id == project.id
                )
            )
            if not existing:
                session.add(ProjectUserRole(
                    user_id=user_id,
                    project_id=project.id,
                    role="admin"
                ))
        session.commit()

class QuestionGroupService:
    @staticmethod
    def get_all_groups(session: Session) -> pd.DataFrame:
        """Get all question groups with their questions and schema usage.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing groups with columns:
            - ID: Group ID
            - Name: Group name
            - Description: Group description
            - Questions: List of questions in the group
            - Reusable: Whether the group is reusable
            - Archived: Whether the group is archived
            - Question Count: Number of questions
            - Archived Questions: Number of archived questions
            - Used in Schemas: List of schemas using this group
        """
        groups = session.scalars(select(QuestionGroup)).all()
        rows = []
        for g in groups:
            # Get all questions in this group
            questions = session.scalars(
                select(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .where(QuestionGroupQuestion.question_group_id == g.id)
            ).all()
            
            # Get all schemas using this group
            schemas = session.scalars(
                select(Schema)
                .join(SchemaQuestionGroup, Schema.id == SchemaQuestionGroup.schema_id)
                .where(SchemaQuestionGroup.question_group_id == g.id)
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
        """Get all questions in a group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text
            - Type: Question type
            - Options: Available options for single-choice questions
            - Default: Default option for single-choice questions
            - Archived: Whether the question is archived
        """
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == group_id)
        ).all()
        
        return pd.DataFrame([
            {
                "ID": q.id,
                "Text": q.text,
                "Type": q.type,
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            }
            for q in questions
        ])

    @staticmethod
    def get_group_details(group_id: int, session: Session) -> dict:
        """Get details of a question group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            Dictionary containing group details
            
        Raises:
            ValueError: If group not found
        """
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
        """Create a new question group.
        
        Args:
            title: Group title
            description: Group description
            is_reusable: Whether the group is reusable
            session: Database session
            
        Returns:
            Created question group
            
        Raises:
            ValueError: If group with same title exists
        """
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
        """Get a question group by its name.
        
        Args:
            name: Group name
            session: Database session
            
        Returns:
            Question group if found, None otherwise
        """
        return session.scalar(select(QuestionGroup).where(QuestionGroup.title == name))

    @staticmethod
    def edit_group(group_id: int, new_title: str, new_description: str, is_reusable: bool, session: Session) -> None:
        """Edit a question group.
        
        Args:
            group_id: Group ID
            new_title: New group title
            new_description: New group description
            is_reusable: Whether the group is reusable
            session: Database session
            
        Raises:
            ValueError: If group not found or validation fails
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        # If making a group non-reusable, check if it's used in multiple schemas
        if not is_reusable and group.is_reusable:
            schemas = session.scalars(
                select(Schema)
                .join(SchemaQuestionGroup, Schema.id == SchemaQuestionGroup.schema_id)
                .where(SchemaQuestionGroup.question_group_id == group_id)
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
        """Archive a question group and its questions.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        group.is_archived = True
        # Also archive all questions in this group
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == group_id)
        ).all()
        for q in questions:
            q.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_group(group_id: int, session: Session) -> None:
        """Unarchive a question group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        group.is_archived = False
        session.commit()

class AnswerService:
    @staticmethod
    def submit_answer(video_id: int, question_id: int, project_id: int, user_id: int, 
                     answer_value: str, session: Session, is_ground_truth: bool = False) -> None:
        """Submit an answer for a video question.
        
        Args:
            video_id: The ID of the video
            question_id: The ID of the question
            project_id: The ID of the project
            user_id: The ID of the user submitting the answer
            answer_value: The answer value
            session: Database session
            is_ground_truth: Whether this is a ground truth answer
            
        Raises:
            ValueError: If project is archived, user is disabled, or answer validation fails
        """
        # Check if project is archived
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        if project.is_archived:
            raise ValueError("Project is archived")
            
        # Check if user is active
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if user.is_archived:
            raise ValueError("User is disabled")
            
        # Get question to validate answer type
        question = session.get(Question, question_id)
        if not question:
            raise ValueError(f"Question with ID {question_id} not found")
            
        # Validate answer value for single-choice questions
        if question.type == "single":
            if not question.options:
                raise ValueError("Question has no options defined")
            if answer_value not in question.options:
                raise ValueError(f"Answer value '{answer_value}' not in options: {', '.join(question.options)}")
        
        # Check for existing answer
        existing = session.scalar(
            select(Answer).where(
                Answer.video_id == video_id,
                Answer.question_id == question_id,
                Answer.user_id == user_id,
                Answer.project_id == project_id
            )
        )
        
        if existing:
            # Update existing answer
            existing.answer_value = answer_value
            existing.answer_type = question.type
            existing.is_ground_truth = is_ground_truth
            existing.modified_by_user_id = user_id
        else:
            # Create new answer
            answer = Answer(
                video_id=video_id,
                question_id=question_id,
                project_id=project_id,
                user_id=user_id,
                answer_type=question.type,
                answer_value=answer_value,
                is_ground_truth=is_ground_truth
            )
            session.add(answer)
            
        session.commit()

    @staticmethod
    def get_answers(video_id: int, project_id: int, session: Session) -> pd.DataFrame:
        """Get all answers for a video in a project.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            session: Database session
            
        Returns:
            DataFrame containing answers with columns:
            - Question ID
            - User ID
            - Answer Value
            - Is Ground Truth
            - Created At
            - Modified By User ID
        """
        answers = session.scalars(
            select(Answer)
            .where(
                Answer.video_id == video_id,
                Answer.project_id == project_id
            )
        ).all()
        
        return pd.DataFrame([
            {
                "Question ID": a.question_id,
                "User ID": a.user_id,
                "Answer Value": a.answer_value,
                "Is Ground Truth": a.is_ground_truth,
                "Created At": a.created_at,
                "Modified By User ID": a.modified_by_user_id
            }
            for a in answers
        ])

    @staticmethod
    def get_ground_truth(video_id: int, project_id: int, session: Session) -> pd.DataFrame:
        """Get ground truth answers for a video in a project.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            session: Database session
            
        Returns:
            DataFrame containing ground truth answers with columns:
            - Question ID
            - Answer Value
            - Is Ground Truth
            - Created At
            - Modified By User ID
        """
        answers = session.scalars(
            select(Answer)
            .where(
                Answer.video_id == video_id,
                Answer.project_id == project_id,
                Answer.is_ground_truth == True
            )
        ).all()
        
        return pd.DataFrame([
            {
                "Question ID": a.question_id,
                "Answer Value": a.answer_value,
                "Is Ground Truth": a.is_ground_truth,
                "Created At": a.created_at,
                "Modified By User ID": a.modified_by_user_id
            }
            for a in answers
        ])

    @staticmethod
    def submit_review(answer_id: int, reviewer_id: int, status: str, comment: str, session: Session) -> None:
        """Submit a review for an answer.
        
        Args:
            answer_id: The ID of the answer to review
            reviewer_id: The ID of the reviewer
            status: Review status ('pending', 'approved', or 'rejected')
            comment: Review comment
            session: Database session
            
        Raises:
            ValueError: If answer not found, reviewer not found, or invalid status
        """
        # Validate status
        if status not in ['pending', 'approved', 'rejected']:
            raise ValueError("Invalid review status. Must be one of: pending, approved, rejected")
            
        # Check if answer exists
        answer = session.get(Answer, answer_id)
        if not answer:
            raise ValueError(f"Answer with ID {answer_id} not found")
            
        # Check if reviewer exists and is active
        reviewer = session.get(User, reviewer_id)
        if not reviewer:
            raise ValueError(f"Reviewer with ID {reviewer_id} not found")
        if not reviewer.is_active:
            raise ValueError("Reviewer is disabled")
            
        # Check for existing review
        existing = session.scalar(
            select(AnswerReview).where(AnswerReview.answer_id == answer_id)
        )
        
        if existing:
            # Update existing review
            existing.reviewer_id = reviewer_id
            existing.status = status
            existing.comment = comment
            existing.reviewed_at = datetime.now(timezone.utc)
        else:
            # Create new review
            review = AnswerReview(
                answer_id=answer_id,
                reviewer_id=reviewer_id,
                status=status,
                comment=comment,
                reviewed_at=datetime.now(timezone.utc)
            )
            session.add(review)
            
        session.commit()

    @staticmethod
    def get_reviews(answer_id: int, session: Session) -> pd.DataFrame:
        """Get all reviews for an answer.
        
        Args:
            answer_id: The ID of the answer
            session: Database session
            
        Returns:
            DataFrame containing reviews with columns:
            - Reviewer ID
            - Status
            - Comment
            - Reviewed At
        """
        reviews = session.scalars(
            select(AnswerReview)
            .where(AnswerReview.answer_id == answer_id)
        ).all()
        
        return pd.DataFrame([
            {
                "Reviewer ID": r.reviewer_id,
                "Status": r.status,
                "Comment": r.comment,
                "Reviewed At": r.reviewed_at
            }
            for r in reviews
        ])

    @staticmethod
    def get_pending_reviews(project_id: int, session: Session) -> pd.DataFrame:
        """Get all pending reviews for a project.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            DataFrame containing pending reviews with columns:
            - Answer ID
            - Video ID
            - Question ID
            - User ID
            - Answer Value
            - Created At
        """
        # Get all answers in the project that need review
        answers = session.scalars(
            select(Answer)
            .where(
                Answer.project_id == project_id,
                Answer.answer_type == 'description'  # Only description answers need review
            )
            .outerjoin(AnswerReview)  # Left join to find unreviewed answers
            .where(AnswerReview.id == None)  # No review exists
        ).all()
        
        return pd.DataFrame([
            {
                "Answer ID": a.id,
                "Video ID": a.video_id,
                "Question ID": a.question_id,
                "User ID": a.user_id,
                "Answer Value": a.answer_value,
                "Created At": a.created_at
            }
            for a in answers
        ])
