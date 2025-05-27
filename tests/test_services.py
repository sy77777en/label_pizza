import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite import JSON
from label_pizza.models import Base, User, Video, Project, Schema, QuestionGroup, Question, SchemaQuestion, ProjectUserRole, ProjectVideo
from label_pizza.services import (
    VideoService, ProjectService, SchemaService, QuestionService,
    QuestionGroupService, AuthService
)
import pandas as pd
from datetime import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Test database setup
@pytest.fixture(scope="function")
def engine():
    # Use the test database URL from environment
    db_url = os.getenv("TEST_DBURL")
    if not db_url:
        raise ValueError("TEST_DBURL environment variable not set")
    return create_engine(db_url)

@pytest.fixture(scope="function")
def tables(engine):
    # Drop all tables first to ensure clean state
    Base.metadata.drop_all(engine)
    # Create all tables
    Base.metadata.create_all(engine)
    yield
    # Clean up after tests
    Base.metadata.drop_all(engine)

@pytest.fixture(scope="function")
def session(engine, tables):
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    
    yield session
    
    session.close()
    transaction.rollback()
    connection.close()

# Test data setup
@pytest.fixture
def test_user(session):
    user = User(
        user_id_str="test_user",
        email="test@example.com",
        password_hash="test_hash",
        user_type="admin",
        is_active=True
    )
    session.add(user)
    session.commit()
    return user

@pytest.fixture
def test_video(session):
    video = Video(
        video_uid="test_video",
        url="http://example.com/test.mp4",
        video_metadata={"test": "data"}
    )
    session.add(video)
    session.commit()
    return video

@pytest.fixture
def test_schema(session):
    schema = Schema(
        name="test_schema",
        rules_json={"test": "rules"}
    )
    session.add(schema)
    session.commit()
    return schema

@pytest.fixture
def test_question_group(session):
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    return group

@pytest.fixture
def test_question(session, test_question_group):
    question = Question(
        text="test question",
        type="single",
        question_group_id=test_question_group.id,
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.commit()
    return question

@pytest.fixture
def test_project(session, test_schema):
    project = Project(
        name="test_project",
        schema_id=test_schema.id
    )
    session.add(project)
    session.commit()
    return project

# VideoService Tests
def test_video_service_get_all_videos(session, test_video):
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test_video"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert df.iloc[0]["Projects"] == "No projects"

def test_video_service_get_all_videos_with_project(session, test_video, test_project):
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert "test_project" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # No ground truth yet

def test_video_service_add_video(session):
    VideoService.add_video("http://example.com/new_video.mp4", session)
    video = session.query(Video).filter_by(video_uid="new_video.mp4").first()
    assert video is not None
    assert video.url == "http://example.com/new_video.mp4"

def test_video_service_add_video_duplicate(session, test_video):
    with pytest.raises(ValueError, match="already exists"):
        VideoService.add_video(f"http://example.com/{test_video.video_uid}", session)

def test_video_service_add_video_invalid_url(session):
    with pytest.raises(ValueError, match="must end with a filename"):
        VideoService.add_video("http://example.com/", session)

# ProjectService Tests
def test_project_service_get_all_projects(session, test_schema):
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    df = ProjectService.get_all_projects(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "test_project"

def test_project_service_create_project(session, test_schema, test_video):
    ProjectService.create_project(
        "test_project",
        test_schema.id,
        [test_video.id],
        session
    )
    project = session.query(Project).first()
    assert project.name == "test_project"
    assert project.schema_id == test_schema.id

# SchemaService Tests
def test_schema_service_get_all_schemas(session, test_schema):
    df = SchemaService.get_all_schemas(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "test_schema"

def test_schema_service_get_schema_questions(session, test_schema, test_question):
    schema_question = SchemaQuestion(
        schema_id=test_schema.id,
        question_id=test_question.id
    )
    session.add(schema_question)
    session.commit()
    
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"

# QuestionService Tests
def test_question_service_get_all_questions(session, test_question):
    df = QuestionService.get_all_questions(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"

def test_question_service_add_question(session, test_question_group):
    QuestionService.add_question(
        "new question",
        "single",
        test_question_group.title,
        ["option1", "option2"],
        "option1",
        session
    )
    question = session.query(Question).filter_by(text="new question").first()
    assert question is not None
    assert question.type == "single"
    assert question.options == ["option1", "option2"]

def test_question_service_archive_question(session, test_question):
    QuestionService.archive_question(test_question.id, session)
    question = session.get(Question, test_question.id)
    assert question.is_archived is True

def test_question_service_unarchive_question(session, test_question):
    # First archive the question
    QuestionService.archive_question(test_question.id, session)
    # Then unarchive it
    QuestionService.unarchive_question(test_question.id, session)
    question = session.get(Question, test_question.id)
    assert question.is_archived is False

# QuestionGroupService Tests
def test_question_group_service_get_all_groups(session, test_question_group):
    df = QuestionGroupService.get_all_groups(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "test_group"

def test_question_group_service_create_group(session):
    group = QuestionGroupService.create_group(
        "new group",
        "new description",
        True,
        session
    )
    assert group.title == "new group"
    assert group.description == "new description"
    assert group.is_reusable is True

def test_question_group_service_archive_group(session, test_question_group):
    QuestionGroupService.archive_group(test_question_group.id, session)
    group = session.get(QuestionGroup, test_question_group.id)
    assert group.is_archived is True

def test_question_group_service_unarchive_group(session, test_question_group):
    # First archive the group
    QuestionGroupService.archive_group(test_question_group.id, session)
    # Then unarchive it
    QuestionGroupService.unarchive_group(test_question_group.id, session)
    group = session.get(QuestionGroup, test_question_group.id)
    assert group.is_archived is False

def test_question_group_service_get_group_questions(session, test_question_group, test_question):
    df = QuestionGroupService.get_group_questions(test_question_group.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"

def test_question_group_service_get_group_details(session, test_question_group):
    details = QuestionGroupService.get_group_details(test_question_group.id, session)
    assert details["title"] == "test_group"
    assert details["description"] == "test description"
    assert details["is_reusable"] is True
    assert details["is_archived"] is False

# AuthService Tests
def test_auth_service_authenticate(session, test_user):
    result = AuthService.authenticate(
        "test@example.com",
        "test_hash",
        "admin",
        session
    )
    assert result is not None
    assert result["name"] == "test_user"
    assert result["role"] == "admin"

def test_auth_service_seed_admin(session):
    AuthService.seed_admin(session)
    admin = session.query(User).filter_by(email="zhiqiulin98@gmail.com").first()
    assert admin is not None
    assert admin.user_type == "admin"

def test_auth_service_get_all_users(session, test_user):
    df = AuthService.get_all_users(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["User ID"] == "test_user"
    assert df.iloc[0]["Email"] == "test@example.com"
    assert df.iloc[0]["Role"] == "admin"
    assert df.iloc[0]["Active"] == True

def test_auth_service_update_user_role(session, test_user):
    AuthService.update_user_role(test_user.id, "model", session)
    df = AuthService.get_all_users(session)
    assert df.iloc[0]["Role"] == "model"

def test_auth_service_toggle_user_active(session, test_user):
    # Test deactivation
    AuthService.toggle_user_active(test_user.id, session)
    df = AuthService.get_all_users(session)
    assert df.iloc[0]["Active"] == False
    
    # Test reactivation
    AuthService.toggle_user_active(test_user.id, session)
    df = AuthService.get_all_users(session)
    assert df.iloc[0]["Active"] == True

def test_auth_service_get_project_assignments(session, test_project, test_user):
    # Create a project assignment
    assignment = ProjectUserRole(
        project_id=test_project.id,
        user_id=test_user.id,
        role="annotator"
    )
    session.add(assignment)
    session.commit()
    
    df = AuthService.get_project_assignments(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Project ID"] == test_project.id
    assert df.iloc[0]["Project Name"] == "test_project"
    assert df.iloc[0]["User ID"] == test_user.id
    assert df.iloc[0]["User Name"] == "test_user"
    assert df.iloc[0]["Role"] == "annotator"

def test_auth_service_assign_user_to_project(session, test_project, test_user):
    AuthService.assign_user_to_project(test_user.id, test_project.id, "reviewer", session)
    df = AuthService.get_project_assignments(session)
    assert len(df) == 1
    assert df.iloc[0]["Role"] == "reviewer"

def test_auth_service_remove_user_from_project(session, test_project, test_user):
    # First assign the user
    AuthService.assign_user_to_project(test_user.id, test_project.id, "annotator", session)
    assert len(AuthService.get_project_assignments(session)) == 1
    
    # Then remove the assignment
    AuthService.remove_user_from_project(test_user.id, test_project.id, session)
    assert len(AuthService.get_project_assignments(session)) == 0

def test_auth_service_invalid_operations(session, test_project, test_user):
    # Test invalid role update
    with pytest.raises(ValueError, match="Invalid role"):
        AuthService.update_user_role(test_user.id, "invalid_role", session)
    
    # Test invalid project assignment role
    with pytest.raises(ValueError, match="Invalid role"):
        AuthService.assign_user_to_project(test_user.id, test_project.id, "invalid_role", session)
    
    # Test removing non-existent assignment
    with pytest.raises(ValueError, match="No assignment found"):
        AuthService.remove_user_from_project(test_user.id, test_project.id, session)

def test_auth_service_create_user(session):
    user = AuthService.create_user(
        "new_user",
        "new@example.com",
        "hash123",
        "human",
        session
    )
    assert user.user_id_str == "new_user"
    assert user.email == "new@example.com"
    assert user.user_type == "human"
    assert user.is_active is True

def test_auth_service_create_user_duplicate(session, test_user):
    with pytest.raises(ValueError, match="already exists"):
        AuthService.create_user(
            test_user.user_id_str,
            "different@example.com",
            "hash123",
            "human",
            session
        )
    
    with pytest.raises(ValueError, match="already exists"):
        AuthService.create_user(
            "different_user",
            test_user.email,
            "hash123",
            "human",
            session
        )

def test_auth_service_create_user_invalid_type(session):
    with pytest.raises(ValueError, match="Invalid user type"):
        AuthService.create_user(
            "new_user",
            "new@example.com",
            "hash123",
            "invalid_type",
            session
        )

def test_auth_service_admin_auto_reviewer(session, test_project):
    # Create an admin user
    admin = AuthService.create_user(
        "admin_user",
        "admin@example.com",
        "hash123",
        "admin",
        session
    )
    
    # Try to assign as annotator
    AuthService.assign_user_to_project(admin.id, test_project.id, "annotator", session)
    
    # Should be assigned as reviewer instead
    df = AuthService.get_project_assignments(session)
    assert len(df) == 1
    assert df.iloc[0]["Role"] == "reviewer"

def test_auth_service_bulk_assignments(session, test_project):
    # Create multiple users
    users = []
    for i in range(3):
        user = AuthService.create_user(
            f"user{i}",
            f"user{i}@example.com",
            "hash123",
            "human",
            session
        )
        users.append(user)
    
    # Bulk assign users
    AuthService.bulk_assign_users_to_project(
        [u.id for u in users],
        test_project.id,
        "annotator",
        session
    )
    
    # Verify assignments
    df = AuthService.get_project_assignments(session)
    assert len(df) == 3
    assert all(df["Role"] == "annotator")
    
    # Bulk remove users
    AuthService.bulk_remove_users_from_project(
        [u.id for u in users],
        test_project.id,
        session
    )
    assert len(AuthService.get_project_assignments(session)) == 0

def test_question_group_reusable_validation(session, test_schema):
    # Create a reusable group
    group = QuestionGroupService.create_group(
        "reusable_group",
        "test description",
        True,
        session
    )
    
    # Add a question to the group
    question = Question(
        text="test question",
        type="single",
        question_group_id=group.id,
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.commit()
    
    # Add question to first schema
    SchemaService.add_question_to_schema(test_schema.id, question.id, session)
    
    # Create second schema
    schema2 = Schema(name="test_schema2", rules_json={})
    session.add(schema2)
    session.commit()
    
    # Add question to second schema
    SchemaService.add_question_to_schema(schema2.id, question.id, session)
    
    # Try to make group non-reusable
    with pytest.raises(ValueError, match="used in multiple schemas"):
        QuestionGroupService.edit_group(
            group.id,
            "reusable_group",
            "test description",
            False,
            session
        )

def test_question_group_title_uniqueness(session, test_question_group):
    with pytest.raises(ValueError, match="already exists"):
        QuestionGroupService.create_group(
            test_question_group.title,
            "different description",
            True,
            session
        )

# Error Cases
def test_question_service_add_question_invalid_default(session, test_question_group):
    with pytest.raises(ValueError):
        QuestionService.add_question(
            "invalid question",
            "single",
            test_question_group.title,
            ["option1", "option2"],
            "invalid_option",
            session
        )

def test_question_group_service_get_nonexistent_group(session):
    with pytest.raises(ValueError):
        QuestionGroupService.get_group_details(999, session)

def test_schema_service_get_nonexistent_schema(session):
    with pytest.raises(ValueError):
        SchemaService.get_schema_id_by_name("nonexistent", session)

def test_question_service_archive_nonexistent_question(session):
    with pytest.raises(ValueError):
        QuestionService.archive_question(999, session)

def test_question_group_service_archive_nonexistent_group(session):
    with pytest.raises(ValueError):
        QuestionGroupService.archive_group(999, session) 