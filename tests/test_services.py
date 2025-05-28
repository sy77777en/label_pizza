import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite import JSON
from label_pizza.models import Base, User, Video, Project, Schema, QuestionGroup, Question, SchemaQuestion, ProjectUserRole, ProjectVideo, Answer
from label_pizza.services import (
    VideoService, ProjectService, SchemaService, QuestionService,
    QuestionGroupService, AuthService, AnswerService
)
import pandas as pd
from datetime import datetime
import json
import os
from dotenv import load_dotenv
from sqlalchemy import select

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
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert "test_project" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # No ground truth yet

def test_video_service_get_all_videos_with_ground_truth(session, test_video, test_project, test_schema, test_user):
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    
    # Add a question to schema
    question = Question(
        text="test question",
        type="single",
        question_group_id=None,
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    sq = SchemaQuestion(schema_id=test_schema.id, question_id=question.id)
    session.add(sq)
    
    # Add ground truth answer
    answer = Answer(
        video_id=test_video.id,
        project_id=test_project.id,
        question_id=question.id,
        answer_value="option1",
        is_ground_truth=True,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert "test_project" in df.iloc[0]["Projects"]
    assert "✓" in df.iloc[0]["Projects"]  # Has ground truth

def test_video_service_get_all_videos_multiple_projects(session, test_video):
    # Create multiple projects
    projects = []
    for i in range(3):
        schema = Schema(name=f"test_schema_{i}", rules_json={})
        session.add(schema)
        session.commit()
        
        project = Project(name=f"test_project_{i}", schema_id=schema.id)
        session.add(project)
        session.commit()
        
        pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
        session.add(pv)
        projects.append(project)
    
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    projects_str = df.iloc[0]["Projects"]
    for i in range(3):
        assert f"test_project_{i}" in projects_str
        assert "✗" in projects_str  # No ground truth in any project

def test_video_service_get_all_videos_mixed_status(session, test_video, test_project, test_schema, test_user):
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    
    # Create second project
    schema2 = Schema(name="test_schema2", rules_json={})
    session.add(schema2)
    session.commit()
    
    project2 = Project(name="test_project2", schema_id=schema2.id)
    session.add(project2)
    session.commit()
    
    pv2 = ProjectVideo(project_id=project2.id, video_id=test_video.id)
    session.add(pv2)
    
    # Add a question to first schema
    question = Question(
        text="test question",
        type="single",
        question_group_id=None,
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.commit()
    
    # Add question to first schema
    sq = SchemaQuestion(schema_id=test_schema.id, question_id=question.id)
    session.add(sq)
    
    # Add ground truth answer for first project
    answer = Answer(
        video_id=test_video.id,
        project_id=test_project.id,
        question_id=question.id,
        answer_value="option1",
        is_ground_truth=True,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    projects_str = df.iloc[0]["Projects"]
    assert "test_project: ✓" in projects_str  # First project has ground truth
    assert "test_project2: ✗" in projects_str  # Second project has no ground truth

def test_video_service_add_video(session):
    VideoService.add_video("http://example.com/new_video.mp4", session)
    video = session.query(Video).filter_by(video_uid="new_video.mp4").first()
    assert video is not None
    assert video.url == "http://example.com/new_video.mp4"

def test_video_service_add_video_duplicate(session, test_video):
    # First ensure test_video has a proper extension
    test_video.video_uid = "test_video.mp4"
    session.commit()
    
    with pytest.raises(ValueError, match="already exists"):
        VideoService.add_video(f"http://example.com/{test_video.video_uid}", session)

def test_video_service_add_video_invalid_url(session):
    with pytest.raises(ValueError, match="must end with a filename"):
        VideoService.add_video("http://example.com/", session)

def test_video_service_add_video_special_chars(session):
    # Test URL with special characters in filename
    VideoService.add_video("http://example.com/video with spaces & special chars!.mp4", session)
    video = session.query(Video).filter_by(video_uid="video with spaces & special chars!.mp4").first()
    assert video is not None
    assert video.url == "http://example.com/video with spaces & special chars!.mp4"

def test_video_service_add_video_query_params(session):
    # Test URL with query parameters
    VideoService.add_video("http://example.com/video.mp4?param=value", session)
    video = session.query(Video).filter_by(video_uid="video.mp4?param=value").first()
    assert video is not None
    assert video.url == "http://example.com/video.mp4?param=value"

def test_video_service_get_all_videos_empty(session):
    """Test getting all videos when database is empty"""
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_video_service_get_all_videos_with_metadata(session):
    """Test getting videos with metadata"""
    video = Video(
        video_uid="test_video",
        url="http://example.com/test.mp4",
        video_metadata={"duration": 120, "resolution": "1080p"}
    )
    session.add(video)
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test_video"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"

def test_video_service_get_all_videos_with_partial_ground_truth(session, test_video, test_project, test_schema, test_user):
    """Test getting videos where only some questions have ground truth answers"""
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    
    # Add two questions to schema
    questions = []
    for i in range(2):
        question = Question(
            text=f"test question {i}",
            type="single",
            question_group_id=None,
            options=["option1", "option2"],
            default_option="option1"
        )
        session.add(question)
        questions.append(question)
    
    session.commit()
    
    # Add questions to schema
    for question in questions:
        sq = SchemaQuestion(schema_id=test_schema.id, question_id=question.id)
        session.add(sq)
    
    # Add ground truth answer for only one question
    answer = Answer(
        video_id=test_video.id,
        project_id=test_project.id,
        question_id=questions[0].id,
        answer_value="option1",
        is_ground_truth=True,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert "test_project" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # Should show incomplete since only one question has ground truth

def test_video_service_add_video_with_metadata(session):
    """Test adding a video with metadata"""
    metadata = {"duration": 120, "resolution": "1080p"}
    VideoService.add_video("http://example.com/new_video.mp4", session, metadata)
    video = session.query(Video).filter_by(video_uid="new_video.mp4").first()
    assert video is not None
    assert video.url == "http://example.com/new_video.mp4"
    assert video.video_metadata == metadata

def test_video_service_add_video_with_empty_metadata(session):
    """Test adding a video with empty metadata"""
    VideoService.add_video("http://example.com/new_video.mp4", session, {})
    video = session.query(Video).filter_by(video_uid="new_video.mp4").first()
    assert video is not None
    assert video.video_metadata == {}

def test_video_service_add_video_with_invalid_metadata(session):
    """Test adding a video with invalid metadata type"""
    with pytest.raises(ValueError, match="must be a dictionary"):
        VideoService.add_video("http://example.com/new_video.mp4", session, "invalid_metadata")

def test_video_service_add_video_with_very_long_url(session):
    """Test adding a video with a very long URL"""
    long_url = "http://example.com/" + "a" * 1000 + ".mp4"
    with pytest.raises(ValueError, match="URL is too long"):
        VideoService.add_video(long_url, session)

def test_video_service_add_video_with_invalid_protocol(session):
    """Test adding a video with invalid URL protocol"""
    with pytest.raises(ValueError, match="must start with http:// or https://"):
        VideoService.add_video("ftp://example.com/video.mp4", session)

def test_video_service_add_video_with_missing_extension(session):
    """Test adding a video without file extension"""
    with pytest.raises(ValueError, match="must end with a filename"):
        VideoService.add_video("http://example.com/video", session)

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

def test_project_service_archive_project(session, test_project):
    # Archive project
    ProjectService.archive_project(test_project.id, session)
    
    # Verify project is archived
    project = session.get(Project, test_project.id)
    assert project.is_archived == True

def test_project_service_archive_nonexistent_project(session):
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        ProjectService.archive_project(999, session)

def test_project_service_progress_empty(session, test_project):
    # Get progress for empty project
    progress = ProjectService.progress(test_project.id, session)
    assert progress["total_videos"] == 0
    assert progress["total_questions"] == 0
    assert progress["total_answers"] == 0
    assert progress["ground_truth_answers"] == 0
    assert progress["completion_percentage"] == 0.0

def test_project_service_progress_with_data(session, test_project, test_schema, test_video, test_user):
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    
    # Add questions to schema
    questions = []
    for i in range(2):
        question = Question(
            text=f"test question {i}",
            type="single",
            question_group_id=None,
            options=["option1", "option2"],
            default_option="option1"
        )
        session.add(question)
        questions.append(question)
    
    session.commit()
    
    # Add questions to schema
    for question in questions:
        sq = SchemaQuestion(schema_id=test_schema.id, question_id=question.id)
        session.add(sq)
    
    # Add some answers
    for i, question in enumerate(questions):
        answer = Answer(
            video_id=test_video.id,
            project_id=test_project.id,
            question_id=question.id,
            answer_value="option1",
            is_ground_truth=(i == 0),  # Only first answer is ground truth
            user_id=test_user.id
        )
        session.add(answer)
    
    session.commit()
    
    # Get progress
    progress = ProjectService.progress(test_project.id, session)
    assert progress["total_videos"] == 1
    assert progress["total_questions"] == 2
    assert progress["total_answers"] == 2
    assert progress["ground_truth_answers"] == 1
    assert progress["completion_percentage"] == 50.0  # 1 out of 2 questions have ground truth

def test_project_service_progress_nonexistent_project(session):
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        ProjectService.progress(999, session)

def test_create_project_with_archived_resources_fails(session, test_schema, test_video):
    # Archive schema
    test_schema.is_archived = True
    session.commit()
    
    # Try to create project with archived schema
    with pytest.raises(ValueError, match="Schema is archived"):
        ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)
    
    # Unarchive schema and archive video
    test_schema.is_archived = False
    test_video.is_archived = True
    session.commit()
    
    # Try to create project with archived video
    with pytest.raises(ValueError, match="Video is archived"):
        ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)

def test_duplicate_project_video_fail(session, test_project, test_video):
    # Try to add the same video to project again
    with pytest.raises(ValueError, match="Video already in project"):
        ProjectService.create_project(
            "test_project2",
            test_project.schema_id,
            [test_video.id, test_video.id],  # Duplicate video ID
            session
        )

def test_archived_project_hidden_and_read_only(session, test_project, test_video, test_user):
    # Archive project
    ProjectService.archive_project(test_project.id, session)
    
    # Verify project is not in get_all_projects
    df = ProjectService.get_all_projects(session)
    assert len(df) == 0
    
    # Try to add answer to archived project
    question = Question(
        text="test question",
        type="single",
        question_group_id=None,
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.commit()
    
    with pytest.raises(ValueError, match="Project is archived"):
        AnswerService.submit_answer(
            test_video.id,
            question.id,
            test_project.id,
            test_user.id,
            "option1",
            session=session
        )

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

def test_schema_service_create_schema(session):
    # Test creating a new schema
    schema = SchemaService.create_schema("test_schema", {"rule": "test"}, session)
    assert schema.name == "test_schema"
    assert schema.rules_json == {"rule": "test"}
    
    # Test duplicate schema name
    with pytest.raises(ValueError, match="already exists"):
        SchemaService.create_schema("test_schema", {"rule": "test"}, session)

def test_schema_service_add_question_to_schema(session, test_schema, test_question):
    # Test adding a question to schema
    SchemaService.add_question_to_schema(test_schema.id, test_question.id, session)
    
    # Verify question was added
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) == 1
    assert df.iloc[0]["ID"] == test_question.id
    assert df.iloc[0]["Text"] == test_question.text
    
    # Test adding same question again
    with pytest.raises(ValueError, match="already in schema"):
        SchemaService.add_question_to_schema(test_schema.id, test_question.id, session)
    
    # Test adding to non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.add_question_to_schema(999, test_question.id, session)
    
    # Test adding non-existent question
    with pytest.raises(ValueError, match="not found"):
        SchemaService.add_question_to_schema(test_schema.id, 999, session)

def test_schema_service_remove_question_from_schema(session, test_schema, test_question):
    # Add question to schema first
    SchemaService.add_question_to_schema(test_schema.id, test_question.id, session)
    
    # Test removing question
    SchemaService.remove_question_from_schema(test_schema.id, test_question.id, session)
    
    # Verify question was removed
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) == 0
    
    # Test removing non-existent question
    with pytest.raises(ValueError, match="not in schema"):
        SchemaService.remove_question_from_schema(test_schema.id, test_question.id, session)
    
    # Test removing from non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.remove_question_from_schema(999, test_question.id, session)

def test_schema_service_archive_unarchive(session, test_schema):
    # Test archiving schema
    SchemaService.archive_schema(test_schema.id, session)
    schema = session.get(Schema, test_schema.id)
    assert schema.is_archived
    
    # Test unarchiving schema
    SchemaService.unarchive_schema(test_schema.id, session)
    schema = session.get(Schema, test_schema.id)
    assert not schema.is_archived
    
    # Test archiving non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.archive_schema(999, session)
    
    # Test unarchiving non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.unarchive_schema(999, session)

def test_schema_service_get_schema_id_by_name(session, test_schema):
    # Test getting schema ID by name
    schema_id = SchemaService.get_schema_id_by_name(test_schema.name, session)
    assert schema_id == test_schema.id
    
    # Test getting non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.get_schema_id_by_name("non_existent", session)

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

def test_cannot_add_archived_video_to_project(session, test_video):
    # Archive video
    test_video.is_archived = True
    session.commit()
    
    # Create new project
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    # Try to add archived video to project
    with pytest.raises(ValueError, match="Video is archived"):
        ProjectService.create_project("test_project", schema.id, [test_video.id], session)

def test_video_metadata_validation(session):
    """Test validation of video metadata."""
    # Test invalid metadata types
    invalid_metadatas = [
        "not a dict",
        123,
        [1, 2, 3],
        {}  # Empty dict instead of None
    ]
    
    for metadata in invalid_metadatas:
        with pytest.raises(ValueError, match="Metadata must be a non-empty dictionary"):
            VideoService.add_video("http://example.com/test.mp4", session, metadata)
    
    # Test metadata with invalid value types
    invalid_value_metadata = {
        "duration": "not a number",
        "resolution": 1080,  # Should be string
        "tags": "not a list"  # Should be list
    }
    
    with pytest.raises(ValueError, match="Invalid metadata value type"):
        VideoService.add_video("http://example.com/test.mp4", session, invalid_value_metadata)
    
    # Test valid metadata
    valid_metadata = {
        "duration": 120,
        "resolution": "1080p",
        "tags": ["action", "sports"]
    }
    
    VideoService.add_video("http://example.com/test.mp4", session, valid_metadata)
    video = session.query(Video).filter_by(video_uid="test.mp4").first()
    assert video is not None
    assert video.video_metadata == valid_metadata

def test_video_uid_special_chars(session):
    """Test handling of special characters in video UIDs."""
    # Test various special characters in video UIDs
    special_chars = [
        "test video with spaces.mp4",
        "test-video-with-dashes.mp4",
        "test.video.with.dots.mp4",
        "test_video_with_underscores.mp4",
        "test@video.mp4",
        "test#video.mp4",
        "test$video.mp4",
        "test%video.mp4",
        "test&video.mp4",
        "test*video.mp4",
        "test+video.mp4",
        "test=video.mp4",
        "test[video].mp4",
        "test{video}.mp4",
        "test(video).mp4",
        "test<video>.mp4",
        "test>video.mp4",
        "test|video.mp4",
        "test\\video.mp4",
        "test:video.mp4",
        "test;video.mp4",
        "test'video.mp4",
        "test\"video.mp4",
        "test`video.mp4",
        "test~video.mp4",
        "test!video.mp4",
        "test?video.mp4",
        "test,video.mp4"
    ]
    
    # Remove problematic characters that can't be used in filenames
    special_chars = [uid for uid in special_chars if not any(c in uid for c in ['/', '\\', ':', '*', '?', '"', '<', '>', '|'])]
    
    for uid in special_chars:
        url = f"http://example.com/{uid}"
        try:
            # Add video
            VideoService.add_video(url, session)
            
            # Verify video was added
            video = session.query(Video).filter_by(video_uid=uid).first()
            assert video is not None, f"Failed to find video with UID: {uid}"
            assert video.url == url, f"URL mismatch for UID: {uid}"
            
            # Clean up
            session.delete(video)
            session.commit()
        except Exception as e:
            pytest.fail(f"Failed to handle special character in UID '{uid}': {str(e)}")

def test_video_uid_case_sensitivity(session):
    # Test case sensitivity in video UIDs
    base_uid = "TestVideo.mp4"
    variations = [
        "testvideo.mp4",
        "TESTVIDEO.mp4",
        "TestVideo.mp4",
        "testVideo.mp4",
        "TESTvideo.mp4"
    ]
    
    # Add first video
    url = f"http://example.com/{base_uid}"
    VideoService.add_video(url, session)
    
    # Try to add variations
    for variation in variations[1:]:  # Skip first variation as it's the same
        url = f"http://example.com/{variation}"
        with pytest.raises(ValueError, match="already exists"):
            VideoService.add_video(url, session) 

def test_answer_service_submit_answer(session):
    """Test submitting a valid answer."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    # Create question group
    group = QuestionGroup(title="test_group", description="test description", is_reusable=True)
    session.add(group)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"],
        question_group_id=group.id
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Add video to project
    project_video = ProjectVideo(project_id=project.id, video_id=video.id)
    session.add(project_video)
    session.commit()
    
    # Submit answer
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option1",
        session=session
    )
    
    # Verify answer was created
    answer = session.scalar(
        select(Answer).where(
            Answer.video_id == video.id,
            Answer.question_id == question.id,
            Answer.user_id == user.id
        )
    )
    assert answer is not None
    assert answer.answer_value == "option1"
    assert answer.answer_type == "single"
    assert not answer.is_ground_truth

def test_answer_service_submit_ground_truth(session):
    """Test submitting a ground truth answer."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    # Create question group
    group = QuestionGroup(title="test_group", description="test description", is_reusable=True)
    session.add(group)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"],
        question_group_id=group.id
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Add video to project
    project_video = ProjectVideo(project_id=project.id, video_id=video.id)
    session.add(project_video)
    session.commit()
    
    # Submit ground truth answer
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option1",
        session=session,
        is_ground_truth=True
    )
    
    # Verify answer was created as ground truth
    answer = session.scalar(
        select(Answer).where(
            Answer.video_id == video.id,
            Answer.question_id == question.id,
            Answer.user_id == user.id
        )
    )
    assert answer is not None
    assert answer.answer_value == "option1"
    assert answer.is_ground_truth

def test_answer_service_submit_invalid_option(session):
    """Test submitting an invalid option for single-choice question."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"]
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Try to submit invalid option
    with pytest.raises(ValueError, match="Answer value 'invalid' not in options"):
        AnswerService.submit_answer(
            video_id=video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=user.id,
            answer_value="invalid",
            session=session
        )

def test_answer_service_submit_to_archived_project(session):
    """Test submitting answer to archived project."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id, is_archived=True)
    session.add(project)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"]
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Try to submit answer to archived project
    with pytest.raises(ValueError, match="Project is archived"):
        AnswerService.submit_answer(
            video_id=video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=user.id,
            answer_value="option1",
            session=session
        )

def test_answer_service_submit_as_disabled_user(session):
    """Test submitting answer as disabled user."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", is_active=False, password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"]
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Try to submit answer as disabled user
    with pytest.raises(ValueError, match="User is disabled"):
        AnswerService.submit_answer(
            video_id=video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=user.id,
            answer_value="option1",
            session=session
        )

def test_answer_service_update_existing_answer(session):
    """Test updating an existing answer."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"]
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Submit initial answer
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option1",
        session=session
    )
    
    # Update answer
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option2",
        session=session
    )
    
    # Verify answer was updated
    answer = session.scalar(
        select(Answer).where(
            Answer.video_id == video.id,
            Answer.question_id == question.id,
            Answer.user_id == user.id
        )
    )
    assert answer is not None
    assert answer.answer_value == "option2"
    assert answer.modified_by_user_id == user.id

def test_answer_service_get_answers(session):
    """Test retrieving answers for a video in a project."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    question1 = Question(
        text="Question 1?",
        type="single",
        options=["option1", "option2"]
    )
    question2 = Question(
        text="Question 2?",
        type="single",
        options=["option1", "option2"]
    )
    session.add_all([question1, question2])
    session.commit()
    
    # Add questions to schema
    schema_question1 = SchemaQuestion(schema_id=schema.id, question_id=question1.id)
    schema_question2 = SchemaQuestion(schema_id=schema.id, question_id=question2.id)
    session.add_all([schema_question1, schema_question2])
    session.commit()
    
    # Submit answers
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question1.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option1",
        session=session
    )
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question2.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option2",
        session=session
    )
    
    # Get answers
    answers_df = AnswerService.get_answers(video.id, project.id, session)
    
    # Verify answers
    assert len(answers_df) == 2
    assert answers_df.iloc[0]["Question ID"] == question1.id
    assert answers_df.iloc[0]["Answer Value"] == "option1"
    assert answers_df.iloc[1]["Question ID"] == question2.id
    assert answers_df.iloc[1]["Answer Value"] == "option2"

def test_answer_service_get_ground_truth(session):
    """Test retrieving ground truth answers for a video in a project."""
    # Create test data
    user = User(user_id_str="test_user", email="test@example.com", password_hash="dummy_hash")
    session.add(user)
    session.commit()
    
    video = Video(video_uid="test_video.mp4", url="http://example.com/test_video.mp4")
    session.add(video)
    session.commit()
    
    # Create schema first
    schema = Schema(name="test_schema", rules_json={})
    session.add(schema)
    session.commit()
    
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    question = Question(
        text="Test question?",
        type="single",
        options=["option1", "option2"]
    )
    session.add(question)
    session.commit()
    
    # Add question to schema
    schema_question = SchemaQuestion(schema_id=schema.id, question_id=question.id)
    session.add(schema_question)
    session.commit()
    
    # Submit ground truth answer
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option1",
        session=session,
        is_ground_truth=True
    )
    
    # Get ground truth answers
    ground_truth_df = AnswerService.get_ground_truth(video.id, project.id, session)
    
    # Verify ground truth answer
    assert len(ground_truth_df) == 1
    assert ground_truth_df.iloc[0]["Question ID"] == question.id
    assert ground_truth_df.iloc[0]["Answer Value"] == "option1"
    assert ground_truth_df.iloc[0]["Is Ground Truth"]

def test_question_text_uniqueness(session):
    """Test that question text must be unique."""
    # Add first question
    QuestionService.add_question(
        text="What is your favorite color?",
        qtype="single",
        group_name="Colors",
        options=["Red", "Blue", "Green"],
        default="Red",
        session=session
    )
    
    # Try to add question with same text
    with pytest.raises(ValueError, match="Question with text 'What is your favorite color\\?' already exists"):
        QuestionService.add_question(
            text="What is your favorite color?",
            qtype="single",
            group_name="Colors",
            options=["Red", "Blue", "Green"],
            default="Red",
            session=session
        )
    
    # Try to edit question to use existing text
    QuestionService.add_question(
        text="What is your favorite fruit?",
        qtype="single",
        group_name="Fruits",
        options=["Apple", "Banana", "Orange"],
        default="Apple",
        session=session
    )
    
    with pytest.raises(ValueError, match="Question with text 'What is your favorite color\\?' already exists"):
        QuestionService.edit_question(
            text="What is your favorite fruit?",
            new_text="What is your favorite color?",
            new_group="Fruits",
            new_opts=["Apple", "Banana", "Orange"],
            new_default="Apple",
            session=session
        ) 