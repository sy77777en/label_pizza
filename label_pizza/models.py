from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, Float, Enum,
    UniqueConstraint, Index, create_engine, JSON
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
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(Text, nullable=False)
    password_updated_at = Column(DateTime(timezone=True), default=now)
    user_type = Column(Enum("human", "model", "admin", name="user_types"), default="human")
    created_at = Column(DateTime(timezone=True), default=now)
    is_active = Column(Boolean, default=True)

class Video(Base):
    __tablename__ = "videos"
    id = Column(Integer, primary_key=True)
    video_uid = Column(String(180), unique=True, nullable=False)
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
    title = Column(String, unique=True, nullable=False)
    description = Column(Text)
    is_reusable = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)

class Question(Base):
    __tablename__ = "questions"
    id = Column(Integer, primary_key=True)
    text = Column(Text, nullable=False)
    type = Column(Enum("single", "description", name="question_types"), nullable=False)
    question_group_id = Column(Integer)
    options = Column(JSONB)
    default_option = Column(String(120))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=now)
    is_archived = Column(Boolean, default=False)

class Schema(Base):
    __tablename__ = "schemas"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    rules_json = Column(JSONB)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)

class SchemaQuestion(Base):
    __tablename__ = "schema_questions"
    schema_id = Column(Integer, primary_key=True)
    question_id = Column(Integer, primary_key=True)
    added_at = Column(DateTime(timezone=True), default=now)

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
    project_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, primary_key=True)
    role = Column(Enum("annotator", "reviewer", "admin", "model", name="project_roles"), nullable=False)
    assigned_at = Column(DateTime(timezone=True), default=now)
    completed_at = Column(DateTime(timezone=True))
    __table_args__ = (Index("ix_user_projects", "user_id"),)

class ProjectGroup(Base):
    __tablename__ = "project_groups"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    is_default = Column(Boolean, default=False)
    owner_user_id = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=now)
    is_archived = Column(Boolean, default=False)

class ProjectGroupProject(Base):
    __tablename__ = "project_group_projects"
    project_group_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, primary_key=True)

class Answer(Base):
    __tablename__ = "answers"
    id = Column(Integer, primary_key=True)
    video_id = Column(Integer, nullable=False)
    question_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    project_id = Column(Integer, nullable=False)
    answer_type = Column(Enum("single", "description", name="answer_value_types"), default="single", nullable=False)
    answer_value = Column(Text, nullable=False)
    confidence_score = Column(Float)
    is_ground_truth = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=now)
    modified_by_user_id = Column(Integer)
    notes = Column(Text)

    __table_args__ = (
        UniqueConstraint("video_id", "question_id", "user_id", "project_id", name="uq_answer_scope"),
        UniqueConstraint("video_id", "question_id", "project_id", name="uq_gt_row", deferrable=True, initially="DEFERRED"),
        Index("ix_answer_question", "question_id"),
        Index("ix_answer_proj_q", "project_id", "question_id"),
        Index("ix_proj_q_val_single", "project_id", "question_id", "answer_value", postgresql_where=(answer_type == "single")),
        Index("ix_answer_vid_q", "video_id", "question_id"),
    )

class AnswerReview(Base):
    __tablename__ = "answer_reviews"
    id = Column(Integer, primary_key=True)
    answer_id = Column(Integer, nullable=False)
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
