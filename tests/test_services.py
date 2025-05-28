import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.types import JSON
from label_pizza.models import (
    Base, User, Video, Project, Schema, QuestionGroup, Question,
    ProjectUserRole, ProjectVideo, Answer, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup
)
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
        is_archived=False
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
    # Create question
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()  # Get the question ID
    
    # Add question to group
    session.add(QuestionGroupQuestion(
        question_group_id=test_question_group.id,
        question_id=question.id,
        display_order=0
    ))
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
def test_video_service_get_all_videos(session):
    """Test getting all videos when database is empty."""
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_video_service_get_all_videos_with_project_assignments(session):
    """Test getting all videos with their project assignments."""
    # Create test data
    video1 = Video(video_uid="test1.mp4", url="https://example.com/test1.mp4")
    video2 = Video(video_uid="test2.mp4", url="https://example.com/test2.mp4")
    session.add_all([video1, video2])
    session.flush()
    
    # Create schema with questions
    schema = Schema(name="Test Schema")
    session.add(schema)
    session.flush()
    
    # Create question group
    group = QuestionGroup(title="Test Group")
    session.add(group)
    session.flush()
    
    # Create questions
    q1 = Question(text="Q1", type="single", options=["A", "B"])
    q2 = Question(text="Q2", type="description")
    session.add_all([q1, q2])
    session.flush()
    
    # Add questions to group
    session.add_all([
        QuestionGroupQuestion(question_group_id=group.id, question_id=q1.id, display_order=1),
        QuestionGroupQuestion(question_group_id=group.id, question_id=q2.id, display_order=2)
    ])
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=schema.id,
        question_group_id=group.id,
        display_order=1
    ))
    
    # Create project
    project = Project(name="Test Project", schema_id=schema.id)
    session.add(project)
    session.flush()
    
    # Add videos to project
    session.add_all([
        ProjectVideo(project_id=project.id, video_id=video1.id),
        ProjectVideo(project_id=project.id, video_id=video2.id)
    ])
    
    # Add ground truth answers for video1
    session.add_all([
        Answer(
            video_id=video1.id,
            question_id=q1.id,
            project_id=project.id,
            user_id=1,
            answer_type="single",
            answer_value="A",
            is_ground_truth=True
        ),
        Answer(
            video_id=video1.id,
            question_id=q2.id,
            project_id=project.id,
            user_id=1,
            answer_type="description",
            answer_value="Test answer",
            is_ground_truth=True
        )
    ])
    
    session.commit()
    
    # Get all videos
    df = VideoService.get_all_videos(session)
    
    # Verify results
    assert len(df) == 2
    assert df.iloc[0]["Video UID"] == "test1.mp4"
    assert df.iloc[1]["Video UID"] == "test2.mp4"
    assert "Test Project: ✓" in df.iloc[0]["Projects"]
    assert "Test Project: ✗" in df.iloc[1]["Projects"]

def test_video_service_get_all_videos_with_ground_truth(session, test_user):
    """Test getting all videos with complete ground truth answers."""
    # Create a video
    video = Video(
        video_uid="test_video.mp4",
        url="http://example.com/test.mp4",
        video_metadata={"test": "data"}
    )
    session.add(video)
    session.commit()
    
    # Create a schema
    schema = Schema(
        name="test_schema",
        rules_json={"test": "rules"}
    )
    session.add(schema)
    session.commit()
    
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add questions to group
    questions = []
    for i in range(2):
        question = Question(
            text=f"test question {i}",
            type="single",
            options=["option1", "option2"],
            default_option="option1"
        )
        session.add(question)
        questions.append(question)
    session.flush()
    
    for i, question in enumerate(questions):
        session.add(QuestionGroupQuestion(
            question_group_id=group.id,
            question_id=question.id,
            display_order=i
        ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=video.id)
    session.add(pv)
    
    # Add ground truth answers for all questions
    for question in questions:
        answer = Answer(
            video_id=video.id,
            project_id=project.id,
            question_id=question.id,
            answer_value="option1",
            is_ground_truth=True,
            user_id=test_user.id
        )
        session.add(answer)
    session.commit()
    
    # Get all videos
    df = VideoService.get_all_videos(session)
    
    # Verify results
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test_video.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert "test_project: ✓" in df.iloc[0]["Projects"]  # Complete ground truth

def test_video_service_get_all_videos_with_single_ground_truth(session, test_user):
    """Test getting all videos with a single ground truth answer."""
    # Create a video
    video = Video(
        video_uid="test_video.mp4",
        url="http://example.com/test.mp4",
        video_metadata={"test": "data"}
    )
    session.add(video)
    session.commit()
    
    # Create a schema
    schema = Schema(
        name="test_schema",
        rules_json={"test": "rules"}
    )
    session.add(schema)
    session.commit()
    
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add questions to group
    questions = []
    for i in range(2):
        question = Question(
            text=f"test question {i}",
            type="single",
            options=["option1", "option2"],
            default_option="option1"
        )
        session.add(question)
        questions.append(question)
    session.flush()
    
    for i, question in enumerate(questions):
        session.add(QuestionGroupQuestion(
            question_group_id=group.id,
            question_id=question.id,
            display_order=i
        ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=video.id)
    session.add(pv)
    
    # Add ground truth answer for only one question
    answer = Answer(
        video_id=video.id,
        project_id=project.id,
        question_id=questions[0].id,
        answer_value="option1",
        is_ground_truth=True,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    # Get all videos
    df = VideoService.get_all_videos(session)
    
    # Verify results
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test_video.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert "test_project: ✗" in df.iloc[0]["Projects"]  # Incomplete ground truth

def test_video_service_get_all_videos_mixed_status(session, test_user):
    """Test getting all videos with mixed ground truth status across projects."""
    # Create a video
    video = Video(
        video_uid="test_video.mp4",
        url="http://example.com/test.mp4",
        video_metadata={"test": "data"}
    )
    session.add(video)
    session.commit()
    
    # Create schemas
    schemas = []
    for i in range(2):
        schema = Schema(
            name=f"test_schema_{i}",
            rules_json={"test": "rules"}
        )
        session.add(schema)
        schemas.append(schema)
    session.commit()
    
    # Create question groups
    groups = []
    for i in range(2):
        group = QuestionGroup(
            title=f"test_group_{i}",
            description="test description",
            is_reusable=True
        )
        session.add(group)
        groups.append(group)
    session.commit()
    
    # Add questions to groups
    questions = []
    for i in range(2):
        question = Question(
            text=f"test question {i}",
            type="single",
            options=["option1", "option2"],
            default_option="option1"
        )
        session.add(question)
        questions.append(question)
    session.flush()
    
    # Add questions to both groups
    for group in groups:
        for i, question in enumerate(questions):
            session.add(QuestionGroupQuestion(
                question_group_id=group.id,
                question_id=question.id,
                display_order=i
            ))
    
    # Add groups to schemas
    for i, schema in enumerate(schemas):
        session.add(SchemaQuestionGroup(
            schema_id=schema.id,
            question_group_id=groups[i].id,
            display_order=0
        ))
    
    # Create projects
    projects = []
    for i, schema in enumerate(schemas):
        project = Project(name=f"test_project_{i}", schema_id=schema.id)
        session.add(project)
        projects.append(project)
    session.commit()
    
    # Add video to projects
    for project in projects:
        pv = ProjectVideo(project_id=project.id, video_id=video.id)
        session.add(pv)
    
    # Add ground truth answers for first project only
    for question in questions:
        answer = Answer(
            video_id=video.id,
            project_id=projects[0].id,
            question_id=question.id,
            answer_value="option1",
            is_ground_truth=True,
            user_id=test_user.id
        )
        session.add(answer)
    session.commit()
    
    # Get all videos
    df = VideoService.get_all_videos(session)
    
    # Verify results
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test_video.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    projects_str = df.iloc[0]["Projects"]
    assert "test_project_0: ✓" in projects_str  # First project has complete ground truth
    assert "test_project_1: ✗" in projects_str  # Second project has no ground truth

def test_video_service_add_video(session):
    """Test adding a video with various scenarios."""
    # Test valid video addition
    url = "https://example.com/video.mp4"
    metadata = {"duration": 120, "resolution": "1080p"}
    VideoService.add_video(url, session, metadata)
    
    # Verify video was added
    video = session.scalar(select(Video).where(Video.url == url))
    assert video is not None
    assert video.video_uid == "video.mp4"
    assert video.video_metadata == metadata
    
    # Test duplicate video
    with pytest.raises(ValueError, match="already exists"):
        VideoService.add_video(url, session)
    
    # Test invalid URL
    with pytest.raises(ValueError, match="must start with http:// or https://"):
        VideoService.add_video("invalid-url", session)
    
    # Test URL without filename
    with pytest.raises(ValueError, match="must end with a filename"):
        VideoService.add_video("https://example.com/", session)
    
    # Test URL without extension
    with pytest.raises(ValueError, match="must end with a filename"):
        VideoService.add_video("https://example.com/video", session)
    
    # Test long video UID
    long_filename = "x" * 256
    with pytest.raises(ValueError, match="Video UID is too long"):
        VideoService.add_video(f"https://example.com/{long_filename}.mp4", session)
    
    # Test empty metadata
    with pytest.raises(ValueError, match="must be a non-empty dictionary"):
        VideoService.add_video("https://example.com/video2.mp4", session, {})
    
    # Test invalid metadata type
    with pytest.raises(ValueError, match="must be a dictionary"):
        VideoService.add_video("https://example.com/video3.mp4", session, "invalid_metadata")
    
    # Test invalid metadata value type
    with pytest.raises(ValueError, match="Invalid metadata value type"):
        VideoService.add_video("https://example.com/video4.mp4", session, {"invalid": object()})

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

def test_video_service_get_all_videos_archived_project(session, test_video, test_project):
    """Test getting all videos when project is archived."""
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Verify video shows up before archiving
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == test_video.video_uid
    assert "test_project" in df.iloc[0]["Projects"]
    
    # Archive project
    test_project.is_archived = True
    session.commit()
    
    # Get all videos again
    df = VideoService.get_all_videos(session)
    
    # Verify results - video should not show up because project is archived
    assert len(df) == 0

def test_video_service_get_all_videos_archived_video(session, test_video, test_project):
    # Archive video
    test_video.is_archived = True
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 0  # Archived videos should not show up

def test_video_service_get_all_videos_with_answers(session, test_video, test_project, test_schema, test_user):
    """Test getting all videos with non-ground truth answers."""
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    # Add question to group
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    session.commit()
    
    # Add non-ground truth answer
    answer = Answer(
        video_id=test_video.id,
        project_id=test_project.id,
        question_id=question.id,
        answer_value="option1",
        is_ground_truth=False,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    # Get all videos
    df = VideoService.get_all_videos(session)
    
    # Verify results
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == test_video.video_uid
    assert "test_project" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # No ground truth

def test_video_service_get_all_videos_with_review(session, test_video, test_project, test_schema, test_user):
    # Add video to project
    pv = ProjectVideo(project_id=test_project.id, video_id=test_video.id)
    session.add(pv)
    
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Add answer with review
    answer = Answer(
        video_id=test_video.id,
        project_id=test_project.id,
        question_id=question.id,
        answer_value="option1",
        is_ground_truth=False,
        user_id=test_user.id
    )
    session.add(answer)
    session.flush()
    
    review = AnswerReview(
        answer_id=answer.id,
        reviewer_id=test_user.id,
        status="approved",
        comment="test review"
    )
    session.add(review)
    session.commit()
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert "test_project" in df.iloc[0]["Projects"]
    assert "✗" in df.iloc[0]["Projects"]  # No ground truth, even with review

# ProjectService Tests
def test_project_service_get_all_projects(session, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    df = ProjectService.get_all_projects(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ID"] == project.id
    assert df.iloc[0]["Name"] == "test_project"
    assert df.iloc[0]["Videos"] == 0
    assert df.iloc[0]["Schema ID"] == test_schema.id
    assert df.iloc[0]["GT %"] == 0.0

def test_project_service_get_all_projects_with_videos(session, test_schema, test_video):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    df = ProjectService.get_all_projects(session)
    assert len(df) == 1
    assert df.iloc[0]["Videos"] == 1

def test_project_service_get_all_projects_with_ground_truth(session, test_schema, test_video, test_user):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    
    # Add ground truth answer
    answer = Answer(
        video_id=test_video.id,
        project_id=project.id,
        question_id=question.id,
        answer_value="option1",
        is_ground_truth=True,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    df = ProjectService.get_all_projects(session)
    assert len(df) == 1
    assert df.iloc[0]["GT %"] == 100.0

def test_project_service_get_all_projects_archived(session, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Archive project
    project.is_archived = True
    session.commit()
    
    df = ProjectService.get_all_projects(session)
    assert len(df) == 0  # Archived projects should not show up

def test_project_service_get_all_projects_archived_schema(session, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Archive schema
    test_schema.is_archived = True
    session.commit()
    
    df = ProjectService.get_all_projects(session)
    assert len(df) == 0  # Projects with archived schemas should not show up

def test_project_service_get_all_projects_no_projects(session):
    df = ProjectService.get_all_projects(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_project_service_create_project(session, test_schema, test_video):
    ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)
    
    project = session.query(Project).filter_by(name="test_project").first()
    assert project is not None
    assert project.schema_id == test_schema.id
    
    pv = session.query(ProjectVideo).filter_by(project_id=project.id, video_id=test_video.id).first()
    assert pv is not None

def test_project_service_create_project_duplicate_video(session):
    """Test creating a project with a video that's already in another project."""
    # Create first project
    schema = Schema(name="Test Schema", rules_json={})
    session.add(schema)
    session.flush()
    
    video = Video(video_uid="test_video", url="http://example.com/video.mp4")
    session.add(video)
    session.flush()
    
    # Create first project with the video
    ProjectService.create_project("Test Project 1", schema.id, [video.id], session)
    
    # Try to create second project with same video
    with pytest.raises(ValueError, match=f"Video {video.id} is already in project"):
        ProjectService.create_project("Test Project 2", schema.id, [video.id], session)

def test_project_service_create_project_archived_schema(session, test_schema, test_video):
    # Archive schema
    test_schema.is_archived = True
    session.commit()
    
    with pytest.raises(ValueError):
        ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)

def test_project_service_create_project_archived_video(session, test_schema, test_video):
    # Archive video
    test_video.is_archived = True
    session.commit()
    
    with pytest.raises(ValueError):
        ProjectService.create_project("test_project", test_schema.id, [test_video.id], session)

def test_project_service_get_video_ids_by_uids(session, test_video):
    video_ids = ProjectService.get_video_ids_by_uids([test_video.video_uid], session)
    assert len(video_ids) == 1
    assert video_ids[0] == test_video.id

def test_project_service_get_video_ids_by_uids_nonexistent(session):
    video_ids = ProjectService.get_video_ids_by_uids(["nonexistent"], session)
    assert len(video_ids) == 0

def test_project_service_archive_project(session, test_schema):
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    ProjectService.archive_project(project.id, session)
    
    project = session.query(Project).filter_by(id=project.id).first()
    assert project.is_archived == True

def test_project_service_archive_project_nonexistent(session):
    with pytest.raises(ValueError):
        ProjectService.archive_project(999, session)

def test_project_service_progress(session, test_schema, test_video, test_user):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    
    # Add ground truth answer
    answer = Answer(
        video_id=test_video.id,
        project_id=project.id,
        question_id=question.id,
        answer_value="option1",
        is_ground_truth=True,
        user_id=test_user.id
    )
    session.add(answer)
    session.commit()
    
    progress = ProjectService.progress(project.id, session)
    assert progress["total_videos"] == 1
    assert progress["total_questions"] == 1
    assert progress["total_answers"] == 1
    assert progress["ground_truth_answers"] == 1
    assert progress["completion_percentage"] == 100.0

def test_project_service_progress_no_answers(session, test_schema, test_video):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    progress = ProjectService.progress(project.id, session)
    assert progress["total_videos"] == 1
    assert progress["total_questions"] == 1
    assert progress["total_answers"] == 0
    assert progress["ground_truth_answers"] == 0
    assert progress["completion_percentage"] == 0.0

def test_project_service_progress_nonexistent(session):
    with pytest.raises(ValueError):
        ProjectService.progress(999, session)

# AnswerService Tests
def test_answer_service_submit_answer(session, test_user, test_video, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Submit answer
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option1",
        session=session
    )
    
    # Verify answer was created
    answer = session.scalar(
        select(Answer).where(
            Answer.video_id == test_video.id,
            Answer.question_id == question.id,
            Answer.user_id == test_user.id
        )
    )
    assert answer is not None
    assert answer.answer_value == "option1"
    assert answer.answer_type == "single"
    assert not answer.is_ground_truth

def test_answer_service_submit_ground_truth(session, test_user, test_video, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Submit ground truth answer
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option1",
        session=session,
        is_ground_truth=True
    )
    
    # Verify answer was created as ground truth
    answer = session.scalar(
        select(Answer).where(
            Answer.video_id == test_video.id,
            Answer.question_id == question.id,
            Answer.user_id == test_user.id
        )
    )
    assert answer is not None
    assert answer.answer_value == "option1"
    assert answer.is_ground_truth

def test_answer_service_submit_invalid_option(session, test_user, test_video, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Try to submit invalid option
    with pytest.raises(ValueError, match="Answer value 'invalid' not in options"):
        AnswerService.submit_answer(
            video_id=test_video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=test_user.id,
            answer_value="invalid",
            session=session
        )

def test_answer_service_submit_to_archived_project(session, test_user, test_video, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Archive project
    project.is_archived = True
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Try to submit answer to archived project
    with pytest.raises(ValueError, match="Project is archived"):
        AnswerService.submit_answer(
            video_id=test_video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=test_user.id,
            answer_value="option1",
            session=session
        )

def test_answer_service_submit_as_disabled_user(session, test_video, test_schema):
    # Create disabled user
    user = User(
        user_id_str="disabled_user",
        email="disabled@example.com",
        password_hash="test_hash",
        user_type="human",
        is_archived=True
    )
    session.add(user)
    session.commit()
    
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Try to submit answer as disabled user
    with pytest.raises(ValueError, match="User is disabled"):
        AnswerService.submit_answer(
            video_id=test_video.id,
            question_id=question.id,
            project_id=project.id,
            user_id=user.id,
            answer_value="option1",
            session=session
        )

def test_answer_service_update_existing_answer(session, test_user, test_video, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Submit initial answer
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option1",
        session=session
    )
    
    # Submit updated answer
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option2",
        session=session
    )
    
    # Verify answer was updated
    answer = session.scalar(
        select(Answer).where(
            Answer.video_id == test_video.id,
            Answer.question_id == question.id,
            Answer.user_id == test_user.id
        )
    )
    assert answer is not None
    assert answer.answer_value == "option2"
    assert answer.answer_type == "single"
    assert not answer.is_ground_truth

def test_answer_service_get_answers(session, test_user, test_video, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create project
    project = Project(name="test_project", schema_id=test_schema.id)
    session.add(project)
    session.commit()
    
    # Add video to project
    pv = ProjectVideo(project_id=project.id, video_id=test_video.id)
    session.add(pv)
    session.commit()
    
    # Submit answer
    AnswerService.submit_answer(
        video_id=test_video.id,
        question_id=question.id,
        project_id=project.id,
        user_id=test_user.id,
        answer_value="option1",
        session=session
    )
    
    # Get answers
    df = AnswerService.get_answers(test_video.id, project.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Question ID"] == question.id
    assert df.iloc[0]["User ID"] == test_user.id
    assert df.iloc[0]["Answer Value"] == "option1"
    assert not df.iloc[0]["Is Ground Truth"]

# SchemaService Tests
def test_schema_service_get_all_schemas(session, test_schema):
    df = SchemaService.get_all_schemas(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "test_schema"

def test_schema_service_get_schema_questions(session, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
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

def test_schema_service_add_question_group_to_schema(session, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Test adding a question group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, group.id, 0, session)
    
    # Verify question group was added
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"
    
    # Test adding same question group again
    with pytest.raises(ValueError, match="already in schema"):
        SchemaService.add_question_group_to_schema(test_schema.id, group.id, 0, session)
    
    # Test adding to non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.add_question_group_to_schema(999, group.id, 0, session)
    
    # Test adding non-existent question group
    with pytest.raises(ValueError, match="not found"):
        SchemaService.add_question_group_to_schema(test_schema.id, 999, 0, session)

def test_schema_service_remove_question_group_from_schema(session, test_schema):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to schema first
    SchemaService.add_question_group_to_schema(test_schema.id, group.id, 0, session)
    
    # Test removing question group
    SchemaService.remove_question_group_from_schema(test_schema.id, group.id, session)
    
    # Verify question group was removed
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(df) == 0
    
    # Test removing non-existent question group
    with pytest.raises(ValueError, match="not in schema"):
        SchemaService.remove_question_group_from_schema(test_schema.id, group.id, session)
    
    # Test removing from non-existent schema
    with pytest.raises(ValueError, match="not found"):
        SchemaService.remove_question_group_from_schema(999, group.id, session)

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
def test_question_service_get_all_questions(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    session.commit()
    
    df = QuestionService.get_all_questions(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"

def test_question_service_add_question(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    QuestionService.add_question(
        "new question",
        "single",
        group.title,
        ["option1", "option2"],
        "option1",
        session
    )
    question = session.query(Question).filter_by(text="new question").first()
    assert question is not None
    assert question.type == "single"
    assert question.options == ["option1", "option2"]
    
    # Verify question was added to group
    qgq = session.query(QuestionGroupQuestion).filter_by(question_id=question.id).first()
    assert qgq is not None
    assert qgq.question_group_id == group.id

def test_question_service_archive_question(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    session.commit()
    
    QuestionService.archive_question(question.id, session)
    question = session.get(Question, question.id)
    assert question.is_archived is True

def test_question_service_unarchive_question(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    session.commit()
    
    # First archive the question
    QuestionService.archive_question(question.id, session)
    # Then unarchive it
    QuestionService.unarchive_question(question.id, session)
    question = session.get(Question, question.id)
    assert question.is_archived is False

def test_question_service_add_question_invalid_default(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    with pytest.raises(ValueError):
        QuestionService.add_question(
            "invalid question",
            "single",
            group.title,
            ["option1", "option2"],
            "invalid_option",
            session
        )

def test_question_service_archive_nonexistent_question(session):
    with pytest.raises(ValueError):
        QuestionService.archive_question(999, session)

def test_question_service_unarchive_nonexistent_question(session):
    with pytest.raises(ValueError):
        QuestionService.unarchive_question(999, session)

# QuestionGroupService Tests
def test_question_group_service_get_all_groups(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    session.commit()
    
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

def test_question_group_service_archive_group(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    QuestionGroupService.archive_group(group.id, session)
    group = session.get(QuestionGroup, group.id)
    assert group.is_archived is True

def test_question_group_service_unarchive_group(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # First archive the group
    QuestionGroupService.archive_group(group.id, session)
    # Then unarchive it
    QuestionGroupService.unarchive_group(group.id, session)
    group = session.get(QuestionGroup, group.id)
    assert group.is_archived is False

def test_question_group_service_get_group_questions(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    # Add question to group
    question = Question(
        text="test question",
        type="single",
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    session.commit()
    
    df = QuestionGroupService.get_group_questions(group.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Text"] == "test question"

def test_question_group_service_get_group_details(session):
    # Create a question group
    group = QuestionGroup(
        title="test_group",
        description="test description",
        is_reusable=True
    )
    session.add(group)
    session.commit()
    
    details = QuestionGroupService.get_group_details(group.id, session)
    assert details["title"] == "test_group"
    assert details["description"] == "test description"
    assert details["is_reusable"] is True
    assert details["is_archived"] is False

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
        options=["option1", "option2"],
        default_option="option1"
    )
    session.add(question)
    session.flush()
    
    session.add(QuestionGroupQuestion(
        question_group_id=group.id,
        question_id=question.id,
        display_order=0
    ))
    
    # Add group to first schema
    session.add(SchemaQuestionGroup(
        schema_id=test_schema.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Create second schema
    schema2 = Schema(name="test_schema2", rules_json={})
    session.add(schema2)
    session.commit()
    
    # Add group to second schema
    session.add(SchemaQuestionGroup(
        schema_id=schema2.id,
        question_group_id=group.id,
        display_order=0
    ))
    
    # Try to make group non-reusable
    with pytest.raises(ValueError, match="used in multiple schemas"):
        QuestionGroupService.edit_group(
            group.id,
            "reusable_group",
            "test description",
            False,
            session
        )

def test_question_group_title_uniqueness(session):
    # Create first group
    group1 = QuestionGroupService.create_group(
        "test_group",
        "test description",
        True,
        session
    )
    
    # Try to create second group with same title
    with pytest.raises(ValueError, match="already exists"):
        QuestionGroupService.create_group(
            "test_group",
            "different description",
            True,
            session
        )

def test_question_group_service_get_nonexistent_group(session):
    with pytest.raises(ValueError):
        QuestionGroupService.get_group_details(999, session)

def test_question_group_service_archive_nonexistent_group(session):
    with pytest.raises(ValueError):
        QuestionGroupService.archive_group(999, session)

def test_question_group_service_unarchive_nonexistent_group(session):
    with pytest.raises(ValueError):
        QuestionGroupService.unarchive_group(999, session) 