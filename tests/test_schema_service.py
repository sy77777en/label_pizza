import pytest
from label_pizza.services import SchemaService, QuestionService, QuestionGroupService

def test_schema_service_create_schema(session):
    """Test creating a new schema."""
    schema = SchemaService.create_schema("test_schema", [], session)
    assert schema.name == "test_schema"
    assert not schema.is_archived

def test_schema_service_create_schema_duplicate(session, test_schema):
    """Test creating a schema with duplicate name."""
    with pytest.raises(ValueError, match="already exists"):
        SchemaService.create_schema("test_schema", [], session)

def test_schema_service_get_schema_by_name(session, test_schema):
    """Test getting a schema by name."""
    schema = SchemaService.get_schema_by_name("test_schema", session)
    assert schema.id == test_schema.id
    assert schema.name == "test_schema"

def test_schema_service_get_schema_by_name_not_found(session):
    """Test getting a non-existent schema by name."""
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_by_name("non_existent_schema", session)

def test_schema_service_get_schema_by_name_archived(session, test_schema):
    """Test getting an archived schema by name."""
    SchemaService.archive_schema(test_schema.id, session)
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_by_name("test_schema", session)

def test_schema_service_archive_schema(session, test_schema):
    """Test archiving a schema."""
    SchemaService.archive_schema(test_schema.id, session)
    schema = SchemaService.get_schema_by_id(test_schema.id, session)
    assert schema.is_archived

def test_schema_service_archive_schema_not_found(session):
    """Test archiving a non-existent schema."""
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.archive_schema(999, session)

def test_schema_service_get_schema_by_id(session, test_schema):
    """Test getting a schema by ID."""
    schema = SchemaService.get_schema_by_id(test_schema.id, session)
    assert schema.name == "test_schema"

def test_schema_service_get_schema_by_id_not_found(session):
    """Test getting a non-existent schema by ID."""
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_by_id(999, session)

def test_schema_service_get_schema_by_id_archived(session, test_schema):
    """Test getting an archived schema by ID."""
    SchemaService.archive_schema(test_schema.id, session)
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_by_id(test_schema.id, session)

def test_schema_service_get_all_schemas(session, test_schema):
    """Test getting all schemas."""
    schemas = SchemaService.get_all_schemas(session)
    assert len(schemas) == 1
    assert schemas[0].id == test_schema.id
    assert schemas[0].name == "test_schema"

def test_schema_service_get_all_schemas_empty(session):
    """Test getting all schemas when none exist."""
    schemas = SchemaService.get_all_schemas(session)
    assert len(schemas) == 0

def test_schema_service_get_all_schemas_with_archived(session, test_schema):
    """Test getting all schemas including archived ones."""
    SchemaService.archive_schema(test_schema.id, session)
    schemas = SchemaService.get_all_schemas(session, include_archived=True)
    assert len(schemas) == 1
    assert schemas[0].id == test_schema.id
    assert schemas[0].is_archived

def test_schema_service_get_schema_question_groups(session, test_schema, test_question_group):
    """Test getting question groups in a schema."""
    # Add question group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    groups = SchemaService.get_schema_question_groups(test_schema.id, session)
    assert len(groups) == 1
    assert groups[0].id == test_question_group.id
    assert groups[0].title == "test_group"

def test_schema_service_get_schema_question_groups_not_found(session):
    """Test getting question groups for a non-existent schema."""
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_question_groups(999, session)

def test_schema_service_get_schema_question_groups_archived(session, test_schema):
    """Test getting question groups for an archived schema."""
    SchemaService.archive_schema(test_schema.id, session)
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_question_groups(test_schema.id, session)

def test_schema_service_get_schema_questions(session, test_schema, test_question_group, test_question):
    """Test getting questions in a schema."""
    # Add question to group
    QuestionGroupService.add_question_to_group(test_question_group.id, test_question.id, 0, session)
    
    # Add group to schema
    SchemaService.add_question_group_to_schema(test_schema.id, test_question_group.id, 0, session)
    
    questions = SchemaService.get_schema_questions(test_schema.id, session)
    assert len(questions) == 1
    assert questions[0].id == test_question.id
    assert questions[0].text == "test question"

def test_schema_service_get_schema_questions_not_found(session):
    """Test getting questions for a non-existent schema."""
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_questions(999, session)

def test_schema_service_get_schema_questions_archived(session, test_schema):
    """Test getting questions for an archived schema."""
    SchemaService.archive_schema(test_schema.id, session)
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_questions(test_schema.id, session) 