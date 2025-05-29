import pytest
from label_pizza.services import ProjectService, SchemaService, QuestionService, QuestionGroupService

def test_project_service_create_project(session, test_schema, test_video):
    """Test creating a new project."""
    project = ProjectService.create_project(
        name="test_project",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    assert project.name == "test_project"
    assert project.schema_id == test_schema.id
    assert not project.is_archived

def test_project_service_create_project_duplicate(session, test_project):
    """Test creating a project with duplicate name."""
    with pytest.raises(ValueError, match="already exists"):
        ProjectService.create_project(
            name="test_project",
            schema_id=test_project.schema_id,
            video_ids=[test_project.videos[0].id],
            session=session
        )

def test_project_service_create_project_invalid_schema(session, test_video):
    """Test creating a project with invalid schema."""
    with pytest.raises(ValueError, match="Schema not found"):
        ProjectService.create_project(
            name="test_project",
            schema_id=999,  # Non-existent schema ID
            video_ids=[test_video.id],
            session=session
        )

def test_project_service_create_project_invalid_video(session, test_schema):
    """Test creating a project with invalid video."""
    with pytest.raises(ValueError, match="Video not found"):
        ProjectService.create_project(
            name="test_project",
            schema_id=test_schema.id,
            video_ids=[999],  # Non-existent video ID
            session=session
        )

def test_project_service_get_project_by_name(session, test_project):
    """Test getting a project by name."""
    project = ProjectService.get_project_by_name("test_project", session)
    assert project.id == test_project.id
    assert project.name == "test_project"

def test_project_service_get_project_by_name_not_found(session):
    """Test getting a non-existent project by name."""
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_by_name("non_existent_project", session)

def test_project_service_get_project_by_name_archived(session, test_project):
    """Test getting an archived project by name."""
    ProjectService.archive_project(test_project.id, session)
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_by_name("test_project", session)

def test_project_service_archive_project(session, test_project):
    """Test archiving a project."""
    ProjectService.archive_project(test_project.id, session)
    project = ProjectService.get_project_by_id(test_project.id, session)
    assert project.is_archived

def test_project_service_archive_project_not_found(session):
    """Test archiving a non-existent project."""
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.archive_project(999, session)

def test_project_service_get_project_by_id(session, test_project):
    """Test getting a project by ID."""
    project = ProjectService.get_project_by_id(test_project.id, session)
    assert project.name == "test_project"
    assert project.schema_id == test_project.schema_id

def test_project_service_get_project_by_id_not_found(session):
    """Test getting a non-existent project by ID."""
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_by_id(999, session)

def test_project_service_get_project_by_id_archived(session, test_project):
    """Test getting an archived project by ID."""
    ProjectService.archive_project(test_project.id, session)
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_by_id(test_project.id, session)

def test_project_service_get_all_projects(session, test_project):
    """Test getting all projects."""
    projects = ProjectService.get_all_projects(session)
    assert len(projects) == 1
    assert projects[0].id == test_project.id
    assert projects[0].name == "test_project"

def test_project_service_get_all_projects_empty(session):
    """Test getting all projects when none exist."""
    projects = ProjectService.get_all_projects(session)
    assert len(projects) == 0

def test_project_service_get_all_projects_with_archived(session, test_project):
    """Test getting all projects including archived ones."""
    ProjectService.archive_project(test_project.id, session)
    projects = ProjectService.get_all_projects(session, include_archived=True)
    assert len(projects) == 1
    assert projects[0].id == test_project.id
    assert projects[0].is_archived

def test_project_service_get_project_videos(session, test_project):
    """Test getting videos in a project."""
    videos = ProjectService.get_project_videos(test_project.id, session)
    assert len(videos) == 1
    assert videos[0].video_uid == "test.mp4"

def test_project_service_get_project_videos_not_found(session):
    """Test getting videos for a non-existent project."""
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_videos(999, session)

def test_project_service_get_project_videos_archived(session, test_project):
    """Test getting videos for an archived project."""
    ProjectService.archive_project(test_project.id, session)
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_videos(test_project.id, session)

def test_project_service_get_project_schema(session, test_project, test_schema):
    """Test getting schema for a project."""
    schema = ProjectService.get_project_schema(test_project.id, session)
    assert schema.id == test_schema.id
    assert schema.name == "test_schema"

def test_project_service_get_project_schema_not_found(session):
    """Test getting schema for a non-existent project."""
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_schema(999, session)

def test_project_service_get_project_schema_archived(session, test_project):
    """Test getting schema for an archived project."""
    ProjectService.archive_project(test_project.id, session)
    with pytest.raises(ValueError, match="Project not found"):
        ProjectService.get_project_schema(test_project.id, session) 