import pytest
from label_pizza.services import ProjectGroupService
from label_pizza.models import ProjectGroup, ProjectGroupProject, Project, Video, Schema, Question, QuestionGroup, QuestionGroupQuestion, SchemaQuestionGroup

def test_project_group_service_create_project_group(session, test_project):
    """Test creating a project group."""
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Test group description",
        project_ids=[test_project.id],
        session=session
    )
    
    assert group.name == "test_group"
    assert group.description == "Test group description"
    
    # Verify project assignment
    project_assignments = session.scalars(
        select(ProjectGroupProject)
        .where(ProjectGroupProject.project_group_id == group.id)
    ).all()
    assert len(project_assignments) == 1
    assert project_assignments[0].project_id == test_project.id

def test_project_group_service_create_project_group_duplicate(session, test_project):
    """Test creating a project group with duplicate name."""
    # Create first group
    ProjectGroupService.create_project_group(
        name="test_group",
        description="Test group description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Try to create second group with same name
    with pytest.raises(ValueError, match="Project group with name 'test_group' already exists"):
        ProjectGroupService.create_project_group(
            name="test_group",
            description="Another description",
            project_ids=[],
            session=session
        )

def test_project_group_service_create_project_group_invalid_project(session):
    """Test creating a project group with invalid project ID."""
    with pytest.raises(ValueError, match="Project with ID None not found"):
        ProjectGroupService.create_project_group(
            name="test_group",
            description="Test group description",
            project_ids=[999],  # Non-existent project ID
            session=session
        )

def test_project_group_service_edit_project_group(session, test_project):
    """Test editing a project group."""
    # Create initial group
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Initial description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Edit group
    updated_group = ProjectGroupService.edit_project_group(
        group_id=group.id,
        name="updated_group",
        description="Updated description",
        add_project_ids=None,
        remove_project_ids=None,
        session=session
    )
    
    assert updated_group.name == "updated_group"
    assert updated_group.description == "Updated description"
    
    # Verify project assignments unchanged
    project_assignments = session.scalars(
        select(ProjectGroupProject)
        .where(ProjectGroupProject.project_group_id == group.id)
    ).all()
    assert len(project_assignments) == 1
    assert project_assignments[0].project_id == test_project.id

def test_project_group_service_edit_project_group_not_found(session):
    """Test editing a non-existent project group."""
    with pytest.raises(ValueError, match="Project group with ID 999 not found"):
        ProjectGroupService.edit_project_group(
            group_id=999,
            name="new_name",
            description="New description",
            add_project_ids=None,
            remove_project_ids=None,
            session=session
        )

def test_project_group_service_edit_project_group_duplicate_name(session, test_project):
    """Test editing a project group to have duplicate name."""
    # Create two groups
    group1 = ProjectGroupService.create_project_group(
        name="group1",
        description="First group",
        project_ids=[],
        session=session
    )
    group2 = ProjectGroupService.create_project_group(
        name="group2",
        description="Second group",
        project_ids=[],
        session=session
    )
    
    # Try to rename group2 to group1
    with pytest.raises(ValueError, match="Project group with name 'group1' already exists"):
        ProjectGroupService.edit_project_group(
            group_id=group2.id,
            name="group1",
            description="Updated description",
            add_project_ids=None,
            remove_project_ids=None,
            session=session
        )

def test_project_group_service_edit_project_group_add_remove_projects(session, test_project):
    """Test adding and removing projects from a project group."""
    # Create another project
    schema = Schema(name="test_schema")
    session.add(schema)
    session.flush()
    
    project2 = Project(name="test_project2", schema_id=schema.id)
    session.add(project2)
    session.flush()
    
    # Create initial group with one project
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Test description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Add second project
    updated_group = ProjectGroupService.edit_project_group(
        group_id=group.id,
        name=None,
        description=None,
        add_project_ids=[project2.id],
        remove_project_ids=None,
        session=session
    )
    
    # Verify both projects are assigned
    project_assignments = session.scalars(
        select(ProjectGroupProject)
        .where(ProjectGroupProject.project_group_id == group.id)
    ).all()
    assert len(project_assignments) == 2
    project_ids = {a.project_id for a in project_assignments}
    assert project_ids == {test_project.id, project2.id}
    
    # Remove first project
    updated_group = ProjectGroupService.edit_project_group(
        group_id=group.id,
        name=None,
        description=None,
        add_project_ids=None,
        remove_project_ids=[test_project.id],
        session=session
    )
    
    # Verify only second project remains
    project_assignments = session.scalars(
        select(ProjectGroupProject)
        .where(ProjectGroupProject.project_group_id == group.id)
    ).all()
    assert len(project_assignments) == 1
    assert project_assignments[0].project_id == project2.id

def test_project_group_service_get_project_group_by_id(session, test_project):
    """Test getting a project group by ID."""
    # Create group
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Test description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Get group
    result = ProjectGroupService.get_project_group_by_id(group.id, session)
    
    assert result["group"].name == "test_group"
    assert result["group"].description == "Test description"
    assert len(result["projects"]) == 1
    assert result["projects"][0].id == test_project.id

def test_project_group_service_get_project_group_by_id_not_found(session):
    """Test getting a non-existent project group."""
    with pytest.raises(ValueError, match="Project group with ID 999 not found"):
        ProjectGroupService.get_project_group_by_id(999, session)

def test_project_group_service_list_project_groups(session, test_project):
    """Test listing all project groups."""
    # Create multiple groups
    group1 = ProjectGroupService.create_project_group(
        name="group1",
        description="First group",
        project_ids=[test_project.id],
        session=session
    )
    group2 = ProjectGroupService.create_project_group(
        name="group2",
        description="Second group",
        project_ids=[],
        session=session
    )
    
    # List groups
    groups = ProjectGroupService.list_project_groups(session)
    
    assert len(groups) == 2
    group_names = {g.name for g in groups}
    assert group_names == {"group1", "group2"}

def test_project_group_service_validate_project_group_uniqueness(session):
    """Test validation of project group uniqueness."""
    # Create schema
    schema = Schema(name="test_schema")
    session.add(schema)
    session.flush()
    
    # Create question group
    question_group = QuestionGroup(title="test_group", description="Test group")
    session.add(question_group)
    session.flush()
    
    # Create question
    question = Question(text="test_question", type="single", options=["yes", "no"])
    session.add(question)
    session.flush()
    
    # Add question to group
    session.add(QuestionGroupQuestion(question_group_id=question_group.id, question_id=question.id))
    
    # Add group to schema
    session.add(SchemaQuestionGroup(schema_id=schema.id, question_group_id=question_group.id))
    
    # Create video
    video = Video(video_uid="test_video", url="http://example.com/video")
    session.add(video)
    session.flush()
    
    # Create two projects with same schema and video
    project1 = Project(name="project1", schema_id=schema.id)
    project2 = Project(name="project2", schema_id=schema.id)
    session.add_all([project1, project2])
    session.flush()
    
    session.add_all([
        ProjectVideo(project_id=project1.id, video_id=video.id),
        ProjectVideo(project_id=project2.id, video_id=video.id)
    ])
    session.commit()
    
    # Try to create group with both projects
    with pytest.raises(ValueError, match="Projects .* have overlapping questions and videos"):
        ProjectGroupService.create_project_group(
            name="test_group",
            description="Test description",
            project_ids=[project1.id, project2.id],
            session=session
        ) 