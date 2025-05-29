import pytest
from label_pizza.services import QuestionGroupService, QuestionService

def test_question_group_service_create_group(session):
    """Test creating a new question group."""
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        question_ids=[],
        verification_function=None,
        session=session
    )
    assert group.title == "test_group"
    assert group.description == "test description"
    assert group.is_reusable
    assert not group.is_archived
    assert group.verification_function is None

def test_question_group_service_create_group_duplicate(session, test_question_group):
    """Test creating a group with duplicate title."""
    with pytest.raises(ValueError, match="already exists"):
        QuestionGroupService.create_group(
            title="test_group",
            description="test description",
            is_reusable=True,
            question_ids=[],
            verification_function=None,
            session=session
        )

def test_question_group_service_create_group_with_questions(session, test_question):
    """Test creating a group with questions."""
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        question_ids=[test_question.id],
        verification_function=None,
        session=session
    )
    assert group.title == "test_group"
    questions = QuestionGroupService.get_group_questions(group.id, session)
    assert len(questions) == 1
    assert questions[0].id == test_question.id

def test_question_group_service_create_group_with_invalid_question(session):
    """Test creating a group with invalid question ID."""
    with pytest.raises(ValueError, match="Question not found"):
        QuestionGroupService.create_group(
            title="test_group",
            description="test description",
            is_reusable=True,
            question_ids=[999],  # Non-existent question ID
            verification_function=None,
            session=session
        )

def test_question_group_service_get_group_by_title(session, test_question_group):
    """Test getting a group by title."""
    group = QuestionGroupService.get_group_by_title("test_group", session)
    assert group.id == test_question_group.id
    assert group.title == "test_group"

def test_question_group_service_get_group_by_title_not_found(session):
    """Test getting a non-existent group by title."""
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_group_by_title("non_existent_group", session)

def test_question_group_service_get_group_by_title_archived(session, test_question_group):
    """Test getting an archived group by title."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_group_by_title("test_group", session)

def test_question_group_service_archive_group(session, test_question_group):
    """Test archiving a group."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    group = QuestionGroupService.get_group_by_id(test_question_group.id, session)
    assert group.is_archived

def test_question_group_service_archive_group_not_found(session):
    """Test archiving a non-existent group."""
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.archive_group(999, session)

def test_question_group_service_get_group_by_id(session, test_question_group):
    """Test getting a group by ID."""
    group = QuestionGroupService.get_group_by_id(test_question_group.id, session)
    assert group.title == "test_group"
    assert group.description == "test description"
    assert group.is_reusable

def test_question_group_service_get_group_by_id_not_found(session):
    """Test getting a non-existent group by ID."""
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_group_by_id(999, session)

def test_question_group_service_get_group_by_id_archived(session, test_question_group):
    """Test getting an archived group by ID."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_group_by_id(test_question_group.id, session)

def test_question_group_service_get_all_groups(session, test_question_group):
    """Test getting all groups."""
    groups = QuestionGroupService.get_all_groups(session)
    assert len(groups) == 1
    assert groups[0].id == test_question_group.id
    assert groups[0].title == "test_group"

def test_question_group_service_get_all_groups_empty(session):
    """Test getting all groups when none exist."""
    groups = QuestionGroupService.get_all_groups(session)
    assert len(groups) == 0

def test_question_group_service_get_all_groups_with_archived(session, test_question_group):
    """Test getting all groups including archived ones."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    groups = QuestionGroupService.get_all_groups(session, include_archived=True)
    assert len(groups) == 1
    assert groups[0].id == test_question_group.id
    assert groups[0].is_archived

def test_question_group_service_get_group_questions(session, test_question_group, test_question):
    """Test getting questions in a group."""
    # Add question to group
    QuestionGroupService.add_question_to_group(test_question_group.id, test_question.id, 0, session)
    
    questions = QuestionGroupService.get_group_questions(test_question_group.id, session)
    assert len(questions) == 1
    assert questions[0].id == test_question.id
    assert questions[0].text == "test question"

def test_question_group_service_get_group_questions_not_found(session):
    """Test getting questions for a non-existent group."""
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_group_questions(999, session)

def test_question_group_service_get_group_questions_archived(session, test_question_group):
    """Test getting questions for an archived group."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_group_questions(test_question_group.id, session)

def test_question_group_service_get_question_order(session, test_question_group, test_question):
    """Test getting question order in a group."""
    # Add question to group
    QuestionGroupService.add_question_to_group(test_question_group.id, test_question.id, 0, session)
    
    order = QuestionGroupService.get_question_order(test_question_group.id, session)
    assert len(order) == 1
    assert order[0] == test_question.id

def test_question_group_service_get_question_order_not_found(session):
    """Test getting question order for a non-existent group."""
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_question_order(999, session)

def test_question_group_service_get_question_order_archived(session, test_question_group):
    """Test getting question order for an archived group."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.get_question_order(test_question_group.id, session)

def test_question_group_service_update_question_order(session, test_question_group, test_question):
    """Test updating question order in a group."""
    # Add question to group
    QuestionGroupService.add_question_to_group(test_question_group.id, test_question.id, 0, session)
    
    # Create another question
    question2 = QuestionService.add_question(
        text="test question 2",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Add second question to group
    QuestionGroupService.add_question_to_group(test_question_group.id, question2.id, 1, session)
    
    # Update order
    QuestionGroupService.update_question_order(
        test_question_group.id,
        [question2.id, test_question.id],
        session
    )
    
    # Verify new order
    order = QuestionGroupService.get_question_order(test_question_group.id, session)
    assert len(order) == 2
    assert order[0] == question2.id
    assert order[1] == test_question.id

def test_question_group_service_update_question_order_not_found(session):
    """Test updating question order for a non-existent group."""
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.update_question_order(999, [1, 2], session)

def test_question_group_service_update_question_order_archived(session, test_question_group):
    """Test updating question order for an archived group."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    with pytest.raises(ValueError, match="Question group not found"):
        QuestionGroupService.update_question_order(test_question_group.id, [1], session)

def test_question_group_service_update_question_order_invalid_question(session, test_question_group):
    """Test updating question order with invalid question ID."""
    with pytest.raises(ValueError, match="Question not found"):
        QuestionGroupService.update_question_order(test_question_group.id, [999], session)

def test_question_group_service_update_question_order_question_not_in_group(session, test_question_group):
    """Test updating question order with question not in group."""
    # Create a question but don't add it to the group
    question = QuestionService.add_question(
        text="test question 2",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    with pytest.raises(ValueError, match="not in group"):
        QuestionGroupService.update_question_order(test_question_group.id, [question.id], session) 