from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, Float, Enum,
    UniqueConstraint, Index, create_engine, JSON, func, PrimaryKeyConstraint
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, Session
import json

Base = declarative_base()
now = lambda: datetime.utcnow()

# ---------- Helper --------------------------------------------------------
class QueryHelper:
    @staticmethod
    def active(query):
        mdl = query.column_descriptions[0]["entity"]
        if hasattr(mdl, "is_archived"):
            return query.filter(mdl.is_archived.is_(False))
        return query
# --------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    user_id_str = Column(String(128), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=True)
    password_hash = Column(Text, nullable=False)
    user_type = Column(Enum("human", "model", "admin", name="user_types"), default="human")
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now)
    is_archived = Column(Boolean, default=False)

class Video(Base):
    __tablename__ = "videos"
    id = Column(Integer, primary_key=True)
    video_uid = Column(String(255), unique=True, nullable=False)
    url = Column(Text)
    video_metadata = Column(JSONB)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)

class VideoTag(Base):
    __tablename__ = "video_tags"
    video_id = Column(Integer, primary_key=True)
    tag = Column(String(64), primary_key=True)
    tag_source = Column(Enum("model", "reviewer", name="tag_sources"), default="model")
    created_at = Column(DateTime(timezone=True), default=now)
    __table_args__ = (Index("ix_tag_lookup", "tag"),)

class QuestionGroup(Base):
    __tablename__ = "question_groups"
    id = Column(Integer, primary_key=True)
    title = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    is_reusable = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)
    verification_function = Column(String(255), nullable=True)  # Name of verification function in verify.py

class Question(Base):
    __tablename__ = "questions"
    id = Column(Integer, primary_key=True)
    text = Column(Text, unique=True)
    type = Column(Enum("single", "description", name="question_type"))
    options = Column(JSONB, nullable=True)  # Actual option values used in answers
    display_values = Column(JSONB, nullable=True)  # Display text for options in UI
    default_option = Column(String(120), nullable=True)

    is_archived = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=now)

class QuestionGroupQuestion(Base):
    __tablename__ = "question_group_questions"
    question_group_id = Column(Integer, nullable=False)
    question_id = Column(Integer, nullable=False)
    display_order = Column(Integer, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('question_group_id', 'question_id'),
    )

class Schema(Base):
    __tablename__ = "schemas"
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    rules_json = Column(JSONB)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)


class SchemaQuestionGroup(Base):
    __tablename__ = "schema_question_groups"
    schema_id = Column(Integer, nullable=False)
    question_group_id = Column(Integer, nullable=False)
    display_order = Column(Integer, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('schema_id', 'question_group_id'),
    )

class Project(Base):
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    schema_id = Column(Integer, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)

class ProjectVideo(Base):
    __tablename__ = "project_videos"
    project_id = Column(Integer, primary_key=True)
    video_id = Column(Integer, primary_key=True)
    added_at = Column(DateTime(timezone=True), default=now)

class ProjectUserRole(Base):
    __tablename__ = "project_user_roles"
    project_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    role = Column(Enum("annotator", "reviewer", "admin", "model", name="project_roles"), nullable=False)
    assigned_at = Column(DateTime(timezone=True), default=now)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    is_archived = Column(Boolean, default=False)
    __table_args__ = (
        PrimaryKeyConstraint('project_id', 'user_id', 'role'),
        Index("ix_user_projects", "user_id"),
    )

class ProjectGroup(Base):
    __tablename__ = "project_groups"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now)
    is_archived = Column(Boolean, default=False)

class ProjectGroupProject(Base):
    __tablename__ = "project_group_projects"
    project_group_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, primary_key=True)

class AnnotatorAnswer(Base):
    __tablename__ = "annotator_answers"
    id = Column(Integer, primary_key=True)
    video_id = Column(Integer, nullable=False)
    question_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)  # Annotator who submitted
    project_id = Column(Integer, nullable=False)
    answer_type = Column(Enum("single", "description", name="answer_value_types"), default="single", nullable=False)
    answer_value = Column(Text, nullable=False)
    confidence_score = Column(Float)
    created_at = Column(DateTime(timezone=True), default=now)
    modified_at = Column(DateTime(timezone=True), default=now)
    notes = Column(Text)

    __table_args__ = (
        # Same unique constraint pattern as original Answer table
        UniqueConstraint("video_id", "question_id", "user_id", "project_id", name="uq_annotator_answer_scope"),
        
        # Same index patterns as original Answer table
        Index("ix_annotator_question", "question_id"),  # Query answers by question
        Index("ix_annotator_proj_q", "project_id", "question_id"),  # Query project+question combinations
        Index("ix_proj_q_val_single", "project_id", "question_id", "answer_value", 
              postgresql_where=(answer_type == "single")),  # Fast lookups for single-choice answers
        Index("ix_annotator_vid_q", "video_id", "question_id"),  # Query answers for specific video+question

        Index("ix_annotator_user_proj", "user_id", "project_id"),  # Query annotator's answers for a project
        Index("ix_annotator_user_proj_q", "user_id", "project_id", "question_id"),  # Query annotator's answers for a project+question
    )

class ReviewerGroundTruth(Base):
    __tablename__ = "reviewer_ground_truth"
    
    # Composite primary key ensures exactly one ground truth per (video, question, project)
    video_id = Column(Integer, primary_key=True)
    question_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, primary_key=True)
    
    # Ground truth metadata
    reviewer_id = Column(Integer, nullable=False)  # Reviewer who created GT
    answer_type = Column(Enum("single", "description", name="answer_value_types"), default="single", nullable=False)
    answer_value = Column(Text, nullable=False)  # Current GT answer
    original_answer_value = Column(Text, nullable=False)  # Original GT answer (for accuracy)
    
    # Modification tracking - two types
    modified_at = Column(DateTime(timezone=True))  # When reviewer modified their own GT (self-correction)
    modified_by_admin_id = Column(Integer)  # Admin who overrode GT (affects accuracy)
    modified_by_admin_at = Column(DateTime(timezone=True))  # When admin overrode GT
    
    # Additional metadata
    confidence_score = Column(Float)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    notes = Column(Text)

    __table_args__ = (
        # Since (video_id, question_id, project_id) is already the PK, we get unique constraint for free
        
        # Similar index patterns adapted for ground truth queries
        Index("ix_gt_question", "question_id"),  # Query GT by question
        Index("ix_gt_proj_q", "project_id", "question_id"),  # Query GT by project+question
        Index("ix_proj_q_val_single_gt", "project_id", "question_id", "answer_value",
              postgresql_where=(answer_type == "single")),  # Fast GT lookups for single-choice
        Index("ix_gt_vid_q", "video_id", "question_id"),  # Query GT for video+question
        
        # Additional indexes specific to ground truth operations
        Index("ix_gt_reviewer", "project_id", "reviewer_id"),  # Calculate reviewer accuracy
        Index("ix_gt_admin_modified", "project_id", "modified_by_admin_id"),  # Find admin-modified GTs
    )

class AnswerReview(Base):
    __tablename__ = "answer_reviews"
    id = Column(Integer, primary_key=True)
    answer_id = Column(Integer, nullable=False, unique=True)
    reviewer_id = Column(Integer, nullable=False)
    status = Column(Enum("pending", "approved", "rejected", name="review_status"), default="pending")
    comment = Column(Text)
    reviewed_at = Column(DateTime(timezone=True), default=now)

# ---------------- create & smoke-test -------------------------------------
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    engine = create_engine(os.environ["DBURL"], echo=False)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(User(user_id_str="alice", email="alice@example.com", password_hash="x"))
        session.commit()
