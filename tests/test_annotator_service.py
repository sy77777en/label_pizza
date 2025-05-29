import pytest
from label_pizza.services import AnnotatorService

def test_annotator_service_create_annotator(session, test_user, test_project):
    """Test creating a new annotator."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    assert annotator.user_id == test_user.id
    assert annotator.project_id == test_project.id
    assert not annotator.is_archived

def test_annotator_service_create_annotator_duplicate(session, test_user, test_project):
    """Test creating a duplicate annotator."""
    AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    with pytest.raises(ValueError, match="already exists"):
        AnnotatorService.create_annotator(
            user_id=test_user.id,
            project_id=test_project.id,
            session=session
        )

def test_annotator_service_create_annotator_invalid_user(session, test_project):
    """Test creating an annotator with invalid user."""
    with pytest.raises(ValueError, match="User not found"):
        AnnotatorService.create_annotator(
            user_id=999,
            project_id=test_project.id,
            session=session
        )

def test_annotator_service_create_annotator_invalid_project(session, test_user):
    """Test creating an annotator with invalid project."""
    with pytest.raises(ValueError, match="Project not found"):
        AnnotatorService.create_annotator(
            user_id=test_user.id,
            project_id=999,
            session=session
        )

def test_annotator_service_get_annotator(session, test_user, test_project):
    """Test getting an annotator."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    result = AnnotatorService.get_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    assert result.id == annotator.id
    assert result.user_id == test_user.id
    assert result.project_id == test_project.id

def test_annotator_service_get_annotator_not_found(session, test_user, test_project):
    """Test getting a non-existent annotator."""
    with pytest.raises(ValueError, match="Annotator not found"):
        AnnotatorService.get_annotator(
            user_id=test_user.id,
            project_id=test_project.id,
            session=session
        )

def test_annotator_service_get_annotator_archived(session, test_user, test_project):
    """Test getting an archived annotator."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    AnnotatorService.archive_annotator(annotator.id, session)
    with pytest.raises(ValueError, match="Annotator not found"):
        AnnotatorService.get_annotator(
            user_id=test_user.id,
            project_id=test_project.id,
            session=session
        )

def test_annotator_service_archive_annotator(session, test_user, test_project):
    """Test archiving an annotator."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    AnnotatorService.archive_annotator(annotator.id, session)
    result = AnnotatorService.get_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session,
        include_archived=True
    )
    assert result.is_archived

def test_annotator_service_archive_annotator_not_found(session):
    """Test archiving a non-existent annotator."""
    with pytest.raises(ValueError, match="Annotator not found"):
        AnnotatorService.archive_annotator(999, session)

def test_annotator_service_get_annotator_include_archived(session, test_user, test_project):
    """Test getting an annotator including archived ones."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    AnnotatorService.archive_annotator(annotator.id, session)
    result = AnnotatorService.get_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session,
        include_archived=True
    )
    assert result.id == annotator.id
    assert result.is_archived

def test_annotator_service_get_all_annotators(session, test_user, test_project):
    """Test getting all annotators."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    results = AnnotatorService.get_all_annotators(session)
    assert len(results) == 1
    assert results[0].id == annotator.id
    assert results[0].user_id == test_user.id
    assert results[0].project_id == test_project.id

def test_annotator_service_get_all_annotators_empty(session):
    """Test getting all annotators when none exist."""
    results = AnnotatorService.get_all_annotators(session)
    assert len(results) == 0

def test_annotator_service_get_all_annotators_with_archived(session, test_user, test_project):
    """Test getting all annotators including archived ones."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    AnnotatorService.archive_annotator(annotator.id, session)
    results = AnnotatorService.get_all_annotators(session, include_archived=True)
    assert len(results) == 1
    assert results[0].id == annotator.id
    assert results[0].is_archived

def test_annotator_service_get_annotators_by_project(session, test_user, test_project):
    """Test getting annotators by project."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    results = AnnotatorService.get_annotators_by_project(test_project.id, session)
    assert len(results) == 1
    assert results[0].id == annotator.id
    assert results[0].project_id == test_project.id

def test_annotator_service_get_annotators_by_project_empty(session, test_project):
    """Test getting annotators by project when none exist."""
    results = AnnotatorService.get_annotators_by_project(test_project.id, session)
    assert len(results) == 0

def test_annotator_service_get_annotators_by_project_with_archived(session, test_user, test_project):
    """Test getting annotators by project including archived ones."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    AnnotatorService.archive_annotator(annotator.id, session)
    results = AnnotatorService.get_annotators_by_project(test_project.id, session, include_archived=True)
    assert len(results) == 1
    assert results[0].id == annotator.id
    assert results[0].is_archived

def test_annotator_service_get_annotators_by_user(session, test_user, test_project):
    """Test getting annotators by user."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    results = AnnotatorService.get_annotators_by_user(test_user.id, session)
    assert len(results) == 1
    assert results[0].id == annotator.id
    assert results[0].user_id == test_user.id

def test_annotator_service_get_annotators_by_user_empty(session, test_user):
    """Test getting annotators by user when none exist."""
    results = AnnotatorService.get_annotators_by_user(test_user.id, session)
    assert len(results) == 0

def test_annotator_service_get_annotators_by_user_with_archived(session, test_user, test_project):
    """Test getting annotators by user including archived ones."""
    annotator = AnnotatorService.create_annotator(
        user_id=test_user.id,
        project_id=test_project.id,
        session=session
    )
    AnnotatorService.archive_annotator(annotator.id, session)
    results = AnnotatorService.get_annotators_by_user(test_user.id, session, include_archived=True)
    assert len(results) == 1
    assert results[0].id == annotator.id
    assert results[0].is_archived 