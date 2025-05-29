import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite import JSON
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, Answer, User, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup
)
from label_pizza.services import (
    VideoService, ProjectService, SchemaService, QuestionService,
    QuestionGroupService, AuthService, AnswerService
)
from label_pizza.db import Base, test_engine, TestSessionLocal, init_test_db, get_test_session
import pandas as pd
from datetime import datetime
import json
import os
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.sql import text

load_dotenv()

# Test database setup
@pytest.fixture(scope="function")
def engine():
    # Use the test database configuration
    return test_engine

@pytest.fixture(scope="function")
def tables(engine):
    # Initialize test database
    init_test_db()
    yield
    # Clean up after tests
    Base.metadata.drop_all(engine)

@pytest.fixture(scope="function")
def session(engine, tables):
    # Get a new test session
    session = get_test_session()
    yield session
    session.close()

# Test data setup
@pytest.fixture
def test_user(session):
    AuthService.create_user(
        user_id="test_user",
        email="test@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session,
        is_archived=False
    )
    return AuthService.get_user_by_id("test_user", session)

@pytest.fixture
def test_video(session):
    VideoService.add_video("http://example.com/test.mp4", session)
    return VideoService.get_video_by_uid("test.mp4", session)

@pytest.fixture
def test_schema(session):
    schema = SchemaService.create_schema("test_schema", {"test": "rules"}, session)
    return schema

@pytest.fixture
def test_question_group(session):
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    return group

@pytest.fixture
def test_question(session, test_question_group):
    QuestionService.add_question(
        text="test question",
        qtype="single",
        group_name=test_question_group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    return QuestionService.get_question_by_text("test question", session)

@pytest.fixture
def test_project(session, test_schema, test_video):
    ProjectService.create_project(
        name="test_project",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    return ProjectService.get_project_by_name("test_project", session)

# VideoService Tests
def test_video_service_get_all_videos(session):
    """Test getting all videos."""
    # Create a video
    video = VideoService.add_video("http://example.com/test.mp4", session)
    
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert df.iloc[0]["Archived"] == False

def test_video_service_get_all_videos_with_project(session, test_video, test_schema, test_question_group):
    # Add video to project through service layer
    ProjectService.create_project(
        name="test_project_with_status",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    
    # Add a question to schema through question group
    question_text = "test question"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=test_question_group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    assert "test_project_with_status" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # No ground truth yet

def test_video_service_get_all_videos_with_ground_truth(session, test_video, test_schema, test_user, test_question_group):
    # Add video to project through service layer
    ProjectService.create_project(
        name="test_project_with_gt",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    project = ProjectService.get_project_by_name("test_project_with_gt", session)
    
    # Add a question to schema through question group
    question_text = "test question"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=test_question_group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
    # Add ground truth answer
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option1",
        session=session,
        is_ground_truth=True
    )
    
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    assert "test_project_with_gt" in df.iloc[0]["Projects"]
    assert "✓" in df.iloc[0]["Projects"]  # Has ground truth

def test_video_service_get_all_videos_multiple_projects(session, test_video):
    # Create multiple projects through service layer
    projects = []
    for i in range(2):
        schema = SchemaService.create_schema(f"test_schema{i}", {}, session)
        
        ProjectService.create_project(
            name=f"test_project{i}",
            schema_id=schema.id,
            video_ids=[test_video.id],
            session=session
        )
        project = ProjectService.get_project_by_name(f"test_project{i}", session)
        projects.append(project)
        
        # Create question group
        group = QuestionGroupService.create_group(
            title=f"test_group{i}",
            description="test description",
            is_reusable=True,
            session=session
        )
        
        # Add a question to schema through question group
        question_text = f"test question {i}"
        QuestionService.add_question(
            text=question_text,
            qtype="single",
            group_name=group.title,
            options=["option1", "option2"],
            default="option1",
            session=session
        )
        
        # Add question group to schema
        SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    projects_str = df.iloc[0]["Projects"]
    assert "test_project0" in projects_str
    assert "test_project1" in projects_str
    assert "✗" in projects_str  # No ground truth in any project

def test_video_service_get_all_videos_with_metadata(session):
    """Test getting videos with metadata"""
    video = VideoService.add_video("http://example.com/test.mp4", session, {"duration": 120, "resolution": "1080p"})
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert df.iloc[0]["Archived"] == False

def test_video_service_get_all_videos_with_partial_ground_truth(session, test_video, test_schema, test_user, test_question_group):
    """Test getting videos where only some questions have ground truth answers"""
    # Add video to project through service layer
    ProjectService.create_project(
        name="test_project_partial_gt",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    project = ProjectService.get_project_by_name("test_project_partial_gt", session)
    
    # Add two questions to schema through question group
    questions = []
    for i in range(2):
        question_text = f"test question {i}"
        QuestionService.add_question(
            text=question_text,
            qtype="single",
            group_name=test_question_group.title,
            options=["option1", "option2"],
            default="option1",
            session=session
        )
        question = QuestionService.get_question_by_text(question_text, session)
        questions.append(question)
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Add ground truth answer for only one question
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=questions[0].id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option1",
        session=session,
        is_ground_truth=True
    )
    
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    assert "test_project_partial_gt" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # Not all questions have ground truth

def test_video_service_get_all_videos_with_review(session, test_video, test_schema, test_user, test_question_group):
    # Add video to project through service layer
    ProjectService.create_project(
        name="test_project_with_review",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    project = ProjectService.get_project_by_name("test_project_with_review", session)
    
    # Add questions to group through service layer
    questions = []
    for i in range(2):
        question_text = f"test question {i}"
        QuestionService.add_question(
            text=question_text,
            qtype="single",
            group_name=test_question_group.title,
            options=["option1", "option2"],
            default="option1",
            session=session
        )
        # Get the question after creating it
        question = QuestionService.get_question_by_text(question_text, session)
        questions.append(question)
    
    # Add question group to schema through service layer
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Add some answers
    for i, question in enumerate(questions):
        AnswerService.submit_answer(
            video_id=test_video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=test_user.id,
            answer_value="option1",
            session=session,
            is_ground_truth=(i == 0)  # Only first answer is ground truth
        )
    
    # Get progress
    progress = ProjectService.progress(project.id, session)
    assert progress["total_videos"] == 1
    assert progress["total_questions"] == 2
    assert progress["total_answers"] == 2
    assert progress["ground_truth_answers"] == 1
    assert progress["completion_percentage"] == 50.0  # 1 out of 2 questions have ground truth

def test_video_service_add_video(session):
    """Test adding a new video."""
    VideoService.add_video("http://example.com/new_video.mp4", session)
    video = VideoService.get_video_by_uid("new_video.mp4", session)
    assert video is not None
    assert VideoService.get_video_url(video.id, session) == "http://example.com/new_video.mp4"

def test_video_service_add_video_duplicate(session, test_video):
    # Try to add duplicate video with same UID
    url = "http://example.com/test.mp4"
    
    # Try to add duplicate video
    with pytest.raises(ValueError, match="already exists"):
        VideoService.add_video(url, session)

def test_video_service_add_video_invalid_url(session):
    with pytest.raises(ValueError, match="must end with a filename"):
        VideoService.add_video("http://example.com/", session)

def test_video_service_add_video_special_chars(session):
    # Test URL with special characters in filename
    url = "http://example.com/video with spaces & special chars!.mp4"
    VideoService.add_video(url, session)
    video = VideoService.get_video_by_uid("video with spaces & special chars!.mp4", session)
    assert video is not None
    assert VideoService.get_video_url(video.id, session) == url

def test_video_service_add_video_query_params(session):
    # Test URL with query parameters
    url = "http://example.com/video.mp4?param=value"
    VideoService.add_video(url, session)
    video = VideoService.get_video_by_uid("video.mp4?param=value", session)
    assert video is not None
    assert VideoService.get_video_url(video.id, session) == url

def test_video_service_get_all_videos_empty(session):
    """Test getting all videos when database is empty"""
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_project_service_progress_nonexistent_project(session):
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        ProjectService.progress(999, session)

def test_create_project_with_archived_resources_fails(session, test_schema, test_video):
    # Archive schema
    test_schema.is_archived = True
    session.commit()
    
    # Try to create project with archived schema
    with pytest.raises(ValueError, match="Schema with ID 1 is archived"):
        ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)
    
    # Unarchive schema and archive video
    test_schema.is_archived = False
    VideoService.archive_video(test_video.id, session)
    session.commit()
    
    # Try to create project with archived video
    with pytest.raises(ValueError, match="Video with ID 1 is archived"):
        ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)

def test_video_can_be_in_multiple_projects(session, test_video, test_schema):
    """Test that a video can be added to multiple projects."""
    # Create first project
    ProjectService.create_project(
        name="test_project1",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    project1 = ProjectService.get_project_by_name("test_project1", session)
    
    # Create second project
    ProjectService.create_project(
        name="test_project2",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    project2 = ProjectService.get_project_by_name("test_project2", session)
    
    # Verify video is in both projects through service layer
    projects = ProjectService.get_all_projects(session)
    assert len(projects) == 2
    assert project1.id in projects["ID"].values
    assert project2.id in projects["ID"].values

def test_archived_project_hidden_and_read_only(session, test_project, test_video, test_user):
    # Archive project
    ProjectService.archive_project(test_project.id, session)
    
    # Verify project is not in get_all_projects
    df = ProjectService.get_all_projects(session)
    assert len(df) == 0
    
    # Create a question group and add it to schema
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(test_project.schema_id, group.id, 0, session)
    
    # Add a question through the service layer
    question_text = "test question"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Get the question using the service layer
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
    # Try to add answer to archived project
    with pytest.raises(ValueError, match="Project is archived"):
        AnswerService.submit_answer(
            video_id=test_video.id,
            question_id=question.id,
            project_id=test_project.id,
            user_id=test_user.id,
            answer_value="option1",
            session=session
        )

# SchemaService Tests
def test_schema_service_get_all_schemas(session, test_schema):
    df = SchemaService.get_all_schemas(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "test_schema"

def test_schema_service_get_schema_questions(session, test_schema, test_question_group, test_question):
    """Test getting questions from a schema through its question groups."""
    # Add question group to schema through service layer
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Get schema questions through service layer
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"
    assert df.iloc[0]["Type"] == "single"
    assert df.iloc[0]["Options"] == "option1, option2"

def test_schema_service_create_schema(session):
    # Test creating a new schema
    schema = SchemaService.create_schema("test_schema", {"rule": "test"}, session)
    assert schema.name == "test_schema"
    assert schema.rules_json == {"rule": "test"}
    
    # Test duplicate schema name
    with pytest.raises(ValueError, match="already exists"):
        SchemaService.create_schema("test_schema", {"rule": "test"}, session)

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

def test_schema_service_add_question_group_to_schema(session, test_schema, test_question_group):
    # Add a question to the question group first
    QuestionService.add_question(
        text="test question",
        qtype="single",
        group_name=test_question_group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Test adding a question group to schema through service layer
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Verify the relationship was created by checking schema questions
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) > 0  # Should have questions from the group
    assert df.iloc[0]["Text"] == "test question"
    assert df.iloc[0]["Type"] == "single"
    assert df.iloc[0]["Options"] == "option1, option2"

def test_schema_service_remove_question_group_from_schema(session, test_schema, test_question_group):
    # Add question group to schema through service layer
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Remove the question group through service layer
    SchemaService.remove_question_group_from_schema(test_schema.id, test_question_group.id, session)
    
    # Verify the relationship was removed by checking schema questions
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) == 0  # Should have no questions after removing group

def test_schema_service_question_group_operations(session, test_schema, test_question_group):
    """Test adding and removing question groups from schema."""
    # Add a question to the question group first
    QuestionService.add_question(
        text="test question",
        qtype="single",
        group_name=test_question_group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    # Verify question group was added
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) > 0
    assert df.iloc[0]["Text"] == "test question"
    
    # Remove question group from schema
    SchemaService.remove_question_group_from_schema(test_schema.id, test_question_group.id, session)
    
    # Verify question group was removed
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) == 0
    
    # Test adding to non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.add_question_group_to_schema(999, test_question_group.id, 0, session)
    
    # Test removing from non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.remove_question_group_from_schema(999, test_question_group.id, session)

# QuestionService Tests
def test_question_service_get_all_questions(session, test_question):
    df = QuestionService.get_all_questions(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"
    assert df.iloc[0]["Type"] == "single"
    assert df.iloc[0]["Options"] == "option1, option2"
    assert df.iloc[0]["Default"] == "option1"
    assert df.iloc[0]["Group"] == "test_group"

def test_question_service_add_question(session, test_question_group):
    # Test adding a new question
    QuestionService.add_question(
        text="new question",
        qtype="single",
        group_name=test_question_group.title,
        options=["opt1", "opt2"],
        default="opt1",
        session=session
    )
    
    # Verify question was created and added to group
    question = QuestionService.get_question_by_text("new question", session)
    assert question is not None
    assert question.type == "single"
    assert question.options == ["opt1", "opt2"]
    assert question.default_option == "opt1"
    
    # Verify question was added to group
    group_questions = QuestionGroupService.get_group_questions(test_question_group.id, session)
    assert len(group_questions) == 1
    assert group_questions.iloc[0]["Text"] == "new question"

def test_question_service_edit_question(session, test_question, test_question_group):
    # Test editing an existing question
    QuestionService.edit_question(
        text="test question",
        new_text="edited question",
        new_group=test_question_group.title,
        new_opts=["new1", "new2"],
        new_default="new1",
        session=session
    )
    
    # Verify question was updated
    question = QuestionService.get_question_by_text("edited question", session)
    assert question is not None
    assert question.type == "single"
    assert question.options == ["new1", "new2"]
    assert question.default_option == "new1"
    
    # Verify question is still in group
    group_questions = QuestionGroupService.get_group_questions(test_question_group.id, session)
    assert len(group_questions) == 1
    assert group_questions.iloc[0]["Text"] == "edited question"

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
def test_question_group_service_get_all_groups(session, test_question_group, test_question):
    df = QuestionGroupService.get_all_groups(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "test_group"
    assert df.iloc[0]["Description"] == "test description"
    assert df.iloc[0]["Reusable"] == True
    assert df.iloc[0]["Question Count"] == 1
    assert df.iloc[0]["Archived Questions"] == 0
    assert "test question" in df.iloc[0]["Questions"]

def test_question_group_service_get_group_questions(session, test_question_group, test_question):
    df = QuestionGroupService.get_group_questions(test_question_group.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"
    assert df.iloc[0]["Type"] == "single"
    assert df.iloc[0]["Options"] == "option1, option2"
    assert df.iloc[0]["Default"] == "option1"

def test_question_group_service_archive_group(session, test_question_group, test_question):
    # Archive the group
    QuestionGroupService.archive_group(test_question_group.id, session)
    
    # Verify group is archived
    group = session.get(QuestionGroup, test_question_group.id)
    assert group.is_archived == True
    
    # Verify question is also archived
    question = session.get(Question, test_question.id)
    assert question.is_archived == True

def test_question_group_service_unarchive_group(session, test_question_group):
    # First archive the group
    test_question_group.is_archived = True
    session.commit()
    
    # Unarchive the group
    QuestionGroupService.unarchive_group(test_question_group.id, session)
    
    # Verify group is unarchived
    group = session.get(QuestionGroup, test_question_group.id)
    assert group.is_archived == False

def test_question_group_reusable_validation(session, test_schema):
    # Create a reusable group
    group = QuestionGroupService.create_group(
        title="reusable_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add group to first schema
    SchemaService.add_question_group_to_schema(test_schema.id, group.id, 0, session)
    
    # Create second schema
    schema2 = SchemaService.create_schema("test_schema2", {}, session)
    
    # Try to add same group to second schema - should succeed since group is reusable
    SchemaService.add_question_group_to_schema(schema2.id, group.id, 0, session)
    
    # Create non-reusable group
    group2 = QuestionGroupService.create_group(
        title="non_reusable_group",
        description="test description",
        is_reusable=False,
        session=session
    )
    
    # Add non-reusable group to first schema
    SchemaService.add_question_group_to_schema(test_schema.id, group2.id, 0, session)
    
    # Try to add non-reusable group to second schema - should fail
    with pytest.raises(ValueError, match="Question group non_reusable_group is not reusable and is already used in schema test_schema"):
        SchemaService.add_question_group_to_schema(schema2.id, group2.id, 0, session)

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
    assert df.iloc[0]["Archived"] == False  # New users are not archived by default

def test_auth_service_update_user_role(session, test_user):
    AuthService.update_user_role(test_user.id, "model", session)
    df = AuthService.get_all_users(session)
    assert df.iloc[0]["Role"] == "model"

def test_auth_service_toggle_user_active(session, test_user):
    # Test archiving
    AuthService.toggle_user_archived(test_user.id, session)
    df = AuthService.get_all_users(session)
    assert df.iloc[0]["Archived"] == True  # Archived = True
    
    # Test unarchiving
    AuthService.toggle_user_archived(test_user.id, session)
    df = AuthService.get_all_users(session)
    assert df.iloc[0]["Archived"] == False  # Archived = False

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
    assert user.is_archived is False

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
    VideoService.archive_video(test_video.id, session)
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    
    # Try to add archived video to project
    with pytest.raises(ValueError, match="Video with ID 1 is archived"):
        ProjectService.create_project("test_project", schema.id, [test_video.id], session)

def test_video_metadata_validation(session):
    # Test invalid metadata types
    with pytest.raises(ValueError, match="Metadata must be a dictionary"):
        VideoService.add_video("http://example.com/test.mp4", session, metadata="invalid")
    
    with pytest.raises(ValueError, match="Metadata must be a non-empty dictionary"):
        VideoService.add_video("http://example.com/test.mp4", session, metadata={})
    
    with pytest.raises(ValueError, match="Invalid metadata value type for key 'invalid'"):
        VideoService.add_video("http://example.com/test.mp4", session, metadata={"invalid": object()})
    
    with pytest.raises(ValueError, match="Invalid list element type in metadata key 'invalid'"):
        VideoService.add_video("http://example.com/test.mp4", session, metadata={"invalid": [object()]})
    
    with pytest.raises(ValueError, match="Invalid nested metadata value type for key 'invalid.nested'"):
        VideoService.add_video("http://example.com/test.mp4", session, metadata={"invalid": {"nested": object()}})
    
    # Test valid metadata
    metadata = {
        "duration": 120,
        "resolution": "1080p",
        "tags": ["action", "sports"],
        "info": {"fps": 30, "codec": "h264"}
    }
    VideoService.add_video("http://example.com/test.mp4", session, metadata=metadata)

def test_video_uid_special_chars(session):
    # Test various special characters in video UIDs
    special_chars = [
        "test.video.with.dots.mp4",
        "test-video-with-dashes.mp4",
        "test_video_with_underscores.mp4",
        "test video with spaces.mp4",
        "test@video.mp4",
        "test#video.mp4",
        "test$video.mp4",
        "test%video.mp4",
        "test&video.mp4",
        "test*video.mp4",
        "test+video.mp4",
        "test=video.mp4",
        "test^video.mp4",
        "test<video.mp4",
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
    
    # Filter out problematic characters that can't be used in filenames
    valid_uids = [uid for uid in special_chars if not any(c in uid for c in '/\\:*?"<>|')]
    
    for uid in valid_uids:
        url = f"http://example.com/{uid}"
        try:
            video = VideoService.add_video(url, session)
            retrieved_video = VideoService.get_video_by_uid(uid, session)
            assert retrieved_video is not None
            assert VideoService.get_video_url(retrieved_video.id, session) == url, f"URL mismatch for UID: {uid}"
        except Exception as e:
            pytest.fail(f"Failed to add video with UID {uid}: {str(e)}")

def test_video_uid_case_sensitivity(session):
    # Test that case-insensitive video UIDs are allowed
    VideoService.add_video("http://example.com/Test.mp4", session)
    VideoService.add_video("http://example.com/test.mp4", session)
    
    # Verify both videos exist
    video1 = VideoService.get_video_by_uid("Test.mp4", session)
    video2 = VideoService.get_video_by_uid("test.mp4", session)
    assert video1 is not None
    assert video2 is not None
    assert video1.id != video2.id  # They should be different videos

def test_answer_service_submit_answer(session):
    """Test submitting a valid answer."""
    # Create test data
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Add video and get the video object
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Assign user to project
    AuthService.assign_user_to_project(user.id, project.id, "annotator", session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
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
    answers_df = AnswerService.get_answers(video.id, project.id, session)
    assert len(answers_df) == 1
    assert answers_df.iloc[0]["Answer Value"] == "option1"
    assert not answers_df.iloc[0]["Is Ground Truth"]

def test_answer_service_submit_ground_truth(session):
    """Test submitting a ground truth answer."""
    # Create test data
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Assign user to project
    AuthService.assign_user_to_project(user.id, project.id, "annotator", session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
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
    ground_truth_df = AnswerService.get_ground_truth(video.id, project.id, session)
    assert len(ground_truth_df) == 1
    assert ground_truth_df.iloc[0]["Answer Value"] == "option1"
    assert ground_truth_df.iloc[0]["Is Ground Truth"]

def test_answer_service_submit_invalid_option(session):
    """Test submitting an invalid option for single-choice question."""
    # Create test data
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Add video and get the video object
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Assign user to project
    AuthService.assign_user_to_project(user.id, project.id, "annotator", session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
    # Try to submit invalid option
    with pytest.raises(ValueError, match="Answer value 'invalid' not in options: option1, option2"):
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
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Add video and get the video object
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Archive project
    ProjectService.archive_project(project.id, session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
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
    """Test submitting answer as archived user."""
    # Create test data
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Archive user
    AuthService.toggle_user_archived(user.id, session)
    
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
    # Try to submit answer as archived user
    with pytest.raises(ValueError, match="User is archived"):
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
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Add video and get the video object
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Assign user to project
    AuthService.assign_user_to_project(user.id, project.id, "annotator", session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
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
    answers_df = AnswerService.get_answers(video.id, project.id, session)
    assert len(answers_df) == 1
    assert answers_df.iloc[0]["Answer Value"] == "option2"
    assert answers_df.iloc[0]["Modified By User ID"] == user.id

def test_answer_service_get_answers(session):
    """Test retrieving answers for a video in a project."""
    # Create test data
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Add video and get the video object
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Assign user to project
    AuthService.assign_user_to_project(user.id, project.id, "annotator", session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add questions through service layer
    questions = []
    for i in range(2):
        question_text = f"Question {i}?"
        QuestionService.add_question(
            text=question_text,
            qtype="single",
            group_name=group.title,
            options=["option1", "option2"],
            default="option1",
            session=session
        )
        question = QuestionService.get_question_by_text(question_text, session)
        questions.append(question)
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Submit answers
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=questions[0].id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option1",
        session=session
    )
    AnswerService.submit_answer(
        video_id=video.id,
        question_id=questions[1].id,
        project_id=project.id,
        user_id=user.id,
        answer_value="option2",
        session=session
    )
    
    # Get answers
    answers_df = AnswerService.get_answers(video.id, project.id, session)
    
    # Verify answers
    assert len(answers_df) == 2
    assert answers_df.iloc[0]["Question ID"] == questions[0].id
    assert answers_df.iloc[0]["Answer Value"] == "option1"
    assert answers_df.iloc[1]["Question ID"] == questions[1].id
    assert answers_df.iloc[1]["Answer Value"] == "option2"

def test_answer_service_get_ground_truth(session):
    """Test retrieving ground truth answers for a video in a project."""
    # Create test data
    user = AuthService.create_user(
        "test_user",
        "test@example.com",
        "dummy_hash",
        "human",
        session
    )
    
    # Add video and get the video object
    VideoService.add_video("http://example.com/test_video.mp4", session)
    video = VideoService.get_video_by_uid("test_video.mp4", session)
    assert video is not None
    
    # Create schema and project
    schema = SchemaService.create_schema("test_schema", {}, session)
    ProjectService.create_project("Test Project", schema.id, [video.id], session)
    project = ProjectService.get_project_by_name("Test Project", session)
    assert project is not None
    
    # Assign user to project
    AuthService.assign_user_to_project(user.id, project.id, "annotator", session)
    
    # Create question group
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        session=session
    )
    
    # Add question through service layer
    question_text = "Test question?"
    QuestionService.add_question(
        text=question_text,
        qtype="single",
        group_name=group.title,
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add question group to schema
    SchemaService.add_question_group_to_schema(schema.id, group.id, 0, session)
    
    # Get the question
    question = QuestionService.get_question_by_text(question_text, session)
    assert question is not None
    
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

def test_project_service_add_videos(session, test_project, test_video):
    """Test adding videos to a project."""
    # Add another video
    VideoService.add_video("http://example.com/test2.mp4", session)
    video2 = VideoService.get_video_by_uid("test2.mp4", session)
    assert video2 is not None
    
    # Add both videos to project
    ProjectService.add_videos_to_project(test_project.id, [test_video.id, video2.id], session)
    
    # Verify videos were added
    project = ProjectService.get_project_by_name("test_project", session)
    assert project is not None
    video_count = session.scalar(
        select(func.count())
        .select_from(ProjectVideo)
        .where(ProjectVideo.project_id == project.id)
    )
    assert video_count == 2

def test_project_service_add_videos_validation(session, test_project, test_video):
    """Test validation when adding videos to a project."""
    # Try to add non-existent video
    with pytest.raises(ValueError, match="Video with ID 999 not found"):
        ProjectService.add_videos_to_project(test_project.id, [999], session)
    
    # Archive video
    VideoService.archive_video(test_video.id, session)
    
    # Try to add archived video
    with pytest.raises(ValueError, match=f"Video with ID {test_video.id} is archived"):
        ProjectService.add_videos_to_project(test_project.id, [test_video.id], session)
    
    # Archive project
    ProjectService.archive_project(test_project.id, session)
    
    # Try to add video to archived project
    with pytest.raises(ValueError, match=f"Project with ID {test_project.id} is archived"):
        ProjectService.add_videos_to_project(test_project.id, [test_video.id], session) 