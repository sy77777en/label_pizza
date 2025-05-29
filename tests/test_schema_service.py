import pytest
from label_pizza.services import SchemaService, QuestionService, QuestionGroupService
import pandas as pd

def test_schema_service_create_schema(session, test_question_group):
    """Test creating a new schema."""
    schema = SchemaService.create_schema("test_schema", [test_question_group.id], session)
    assert schema.name == "test_schema"
    assert not schema.is_archived

def test_schema_service_create_schema_duplicate(session, test_schema, test_question_group):
    """Test creating a schema with duplicate name."""
    # Create a new question then group
    QuestionService.add_question(
        text="test question for schema duplicate",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    question = QuestionService.get_question_by_text("test question for schema duplicate", session)
    group = QuestionGroupService.create_group(
        title="test_group_for_schema_duplicate",
        description="test description",
        is_reusable=True,
        question_ids=[question.id],
        verification_function=None,
        session=session
    )
    with pytest.raises(ValueError, match="Schema with name 'test_schema' already exists"):
        SchemaService.create_schema("test_schema", [test_question_group.id, group.id], session)

def test_schema_service_create_schema_invalid_group(session):
    """Test creating a schema with invalid question group."""
    with pytest.raises(ValueError, match="Question group with ID 999 not found"):
        SchemaService.create_schema("test_schema", [999], session)

def test_schema_service_create_schema_archived_group(session, test_question_group):
    """Test creating a schema with archived question group."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    with pytest.raises(ValueError, match="Question group with ID 1 is archived"):
        SchemaService.create_schema("test_schema", [test_question_group.id], session)

def test_schema_service_create_schema_non_reusable_group(session, test_question_group):
    """Test creating a schema with non-reusable question group that's already used."""
    # Create first schema with the group
    # make the group non-reusable
    QuestionGroupService.edit_group(
        group_id=test_question_group.id,
        new_title=test_question_group.title,  # Keep same title
        new_description=test_question_group.description,  # Keep same description
        is_reusable=False,
        session=session
    )
    SchemaService.create_schema("schema1", [test_question_group.id], session)
    
    # Try to create second schema with same group
    with pytest.raises(ValueError, match="Question group test_group is not reusable and is already used in schema schema1"):
        SchemaService.create_schema("schema2", [test_question_group.id], session)

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
    schema = SchemaService.get_schema_by_name("test_schema", session)
    assert schema.is_archived

def test_schema_service_archive_schema_not_found(session):
    """Test archiving a non-existent schema."""
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.archive_schema(999, session)

def test_schema_service_get_schema_by_id(session, test_schema):
    """Test getting a schema by ID."""
    schema = SchemaService.get_schema_by_id(test_schema.id, session)
    assert schema.id == test_schema.id
    assert schema.name == test_schema.name

def test_schema_service_get_schema_by_id_not_found(session):
    """Test getting a non-existent schema by ID."""
    with pytest.raises(ValueError, match="Schema with ID 999 not found"):
        SchemaService.get_schema_by_id(999, session)

def test_schema_service_get_schema_by_id_archived(session, test_schema):
    """Test getting an archived schema by ID."""
    SchemaService.archive_schema(test_schema.id, session)
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_by_id(test_schema.id, session)

def test_schema_service_get_all_schemas(session, test_schema):
    """Test getting all schemas."""
    df = SchemaService.get_all_schemas(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ID"] == test_schema.id
    assert df.iloc[0]["Name"] == test_schema.name
    assert df.iloc[0]["Question Groups"] == "test_group"  # From test_question_group fixture

def test_schema_service_get_all_schemas_empty(session):
    """Test getting all schemas when none exist."""
    schemas = SchemaService.get_all_schemas(session)
    assert isinstance(schemas, pd.DataFrame)
    assert len(schemas) == 0

def test_schema_service_get_all_schemas_with_archived(session, test_schema):
    """Test getting all schemas including archived ones."""
    SchemaService.archive_schema(test_schema.id, session)
    schemas = SchemaService.get_all_schemas(session, include_archived=True)
    assert isinstance(schemas, pd.DataFrame)
    assert len(schemas) == 1
    assert schemas.iloc[0]["ID"] == test_schema.id
    assert schemas.iloc[0]["Name"] == "test_schema"

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

def test_schema_service_get_schema_questions(session, test_schema):
    """Test getting questions in a schema."""
    df = SchemaService.get_schema_questions(test_schema.id, session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1  # One question from test_question fixture
    assert df.iloc[0]["ID"] == 1  # From test_question fixture
    assert df.iloc[0]["Text"] == "test_question"
    assert df.iloc[0]["Group"] == "test_group"
    assert df.iloc[0]["Type"] == "single"
    assert df.iloc[0]["Options"] == "option1, option2"

def test_schema_service_get_schema_questions_not_found(session):
    """Test getting questions for a non-existent schema."""
    with pytest.raises(ValueError, match="Schema with ID 999 not found"):
        SchemaService.get_schema_questions(999, session)

def test_schema_service_get_schema_questions_archived(session, test_schema):
    """Test getting questions for an archived schema."""
    SchemaService.archive_schema(test_schema.id, session)
    with pytest.raises(ValueError, match="Schema not found"):
        SchemaService.get_schema_questions(test_schema.id, session)

def test_schema_service_get_question_group_order(session, test_schema):
    """Test getting question group order in a schema."""
    order = SchemaService.get_question_group_order(test_schema.id, session)
    assert len(order) == 1
    assert order[0] == 1  # From test_question_group fixture

def test_schema_service_get_question_group_order_not_found(session):
    """Test getting question group order for a non-existent schema."""
    with pytest.raises(ValueError, match="Schema with ID 999 not found"):
        SchemaService.get_question_group_order(999, session)

def test_schema_service_update_question_group_order(session, test_schema):
    """Test updating question group order in a schema."""
    # Create another question group
    question = QuestionService.add_question(
        text="test_question2",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    group = QuestionGroupService.create_group(
        title="test_group2",
        description="test description",
        is_reusable=True,
        question_ids=[question.id],
        verification_function=None,
        session=session
    )
    
    # Add group to schema
    SchemaService.add_question_group_to_schema(
        test_schema.id,
        group.id,
        display_order=1,
        session=session
    )
    
    # Update order
    SchemaService.update_question_group_order(
        test_schema.id,
        [group.id, 1],  # Reverse the order
        session=session
    )
    
    # Verify new order
    order = SchemaService.get_question_group_order(test_schema.id, session)
    assert order == [group.id, 1]

def test_schema_service_update_question_group_order_not_found(session):
    """Test updating question group order for a non-existent schema."""
    with pytest.raises(ValueError, match="Schema with ID 999 not found"):
        SchemaService.update_question_group_order(999, [1], session)

def test_schema_service_update_question_group_order_invalid_group(session, test_schema):
    """Test updating question group order with an invalid group."""
    with pytest.raises(ValueError, match="Question group 999 not in schema"):
        SchemaService.update_question_group_order(test_schema.id, [999], session)

def test_schema_service_edit_group(session, test_schema, test_question_group):
    """Test editing a question group in a schema."""
    # Edit the group
    QuestionGroupService.edit_group(
        group_id=test_question_group.id,
        new_title="edited_group",
        new_description="edited description",
        is_reusable=True,
        session=session
    )
    
    # Verify group was updated
    group = QuestionGroupService.get_group_by_id(test_question_group.id, session)
    assert group.title == "edited_group"
    assert group.description == "edited description"
    assert group.is_reusable == True

def test_schema_service_edit_group_duplicate_title(session, test_schema, test_question_group):
    """Test editing a group with a duplicate title."""
    # Create another group
    QuestionService.add_question(
        text="test question 2",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    question2 = QuestionService.get_question_by_text("test question 2", session)
    
    group2 = QuestionGroupService.create_group(
        title="test_group2",
        description="test description",
        is_reusable=True,
        question_ids=[question2.id],
        verification_function=None,
        session=session
    )
    
    # Try to edit first group to have same title as second group
    with pytest.raises(ValueError, match="already exists"):
        QuestionGroupService.edit_group(
            group_id=test_question_group.id,
            new_title="test_group2",
            new_description="edited description",
            is_reusable=True,
            session=session
        )

def test_schema_service_edit_group_not_found(session):
    """Test editing a non-existent group."""
    with pytest.raises(ValueError, match="not found"):
        QuestionGroupService.edit_group(
            group_id=999,
            new_title="edited_group",
            new_description="edited description",
            is_reusable=True,
            session=session
        )

def test_schema_service_edit_group_make_non_reusable(session, test_schema, test_question_group):
    """Test making a group non-reusable when it's used in multiple schemas."""
    # Create another schema
    schema2 = SchemaService.create_schema("test_schema2", [test_question_group.id], session)
    
    # Try to make group non-reusable
    with pytest.raises(ValueError, match="used in multiple schemas"):
        QuestionGroupService.edit_group(
            group_id=test_question_group.id,
            new_title="edited_group",
            new_description="edited description",
            is_reusable=False,
            session=session
        ) 