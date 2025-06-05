import pytest
from label_pizza.services import QuestionGroupService, QuestionService
import pandas as pd

def test_question_group_service_create_group(session):
    """Test creating a new question group."""
    # Create a question first
    QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_text="Test Question"
    )
    question = QuestionService.get_question_by_text("test question", session)
    
    group = QuestionGroupService.create_group(
        title="test_group",
        description="test description",
        is_reusable=True,
        question_ids=[question.id],  # Add the question to the group
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
    # Create a new question
    QuestionService.add_question(
        text="test question new",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_text="Test Question"
    )
    new_question = QuestionService.get_question_by_text("test question", session)
    with pytest.raises(ValueError, match="Question group with title 'test_group' already exists"):
        QuestionGroupService.create_group(
            title="test_group",
            description="test description",
            is_reusable=True,
            question_ids=[new_question.id],  # Use test_question.id instead of test_question_group.id
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
    assert questions.iloc[0]["ID"] == test_question.id
    assert questions.iloc[0]["Text"] == "test question"
    assert questions.iloc[0]["Display Text"] == "Test Question"

def test_question_group_service_create_group_with_invalid_question(session):
    """Test creating a group with invalid question ID."""
    with pytest.raises(ValueError, match="Question with ID 999 not found"):
        QuestionGroupService.create_group(
            title="test_group",
            description="test description",
            is_reusable=True,
            question_ids=[999],  # Non-existent question ID
            verification_function=None,
            session=session
        )

def test_question_group_service_get_group_by_name(session, test_question_group):
    """Test getting a group by name."""
    group = QuestionGroupService.get_group_by_name("test_group", session)
    assert group.id == test_question_group.id
    assert group.title == "test_group"

def test_question_group_service_get_group_by_name_not_found(session):
    """Test getting a non-existent group by name."""
    with pytest.raises(ValueError, match="Question group with title 'non_existent_group' not found"):
        QuestionGroupService.get_group_by_name("non_existent_group", session)

def test_question_group_service_get_group_by_name_archived(session, test_question_group):
    """Test getting an archived group by name."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    group = QuestionGroupService.get_group_by_name("test_group", session)
    assert group.is_archived

def test_question_group_service_get_group_by_id(session, test_question_group):
    """Test getting a group by ID."""
    group = QuestionGroupService.get_group_by_id(test_question_group.id, session)
    assert group.title == "test_group"
    assert group.description == "test description"
    assert group.is_reusable
    assert not group.is_archived

def test_question_group_service_get_group_by_id_not_found(session):
    """Test getting a non-existent group by ID."""
    with pytest.raises(ValueError, match="Question group with ID 999 not found"):
        QuestionGroupService.get_group_by_id(999, session)

def test_question_group_service_get_all_groups(session, test_question_group):
    """Test getting all groups."""
    groups = QuestionGroupService.get_all_groups(session)
    assert isinstance(groups, pd.DataFrame)
    assert len(groups) == 1
    assert groups.iloc[0]["ID"] == test_question_group.id
    assert groups.iloc[0]["Name"] == "test_group"

def test_question_group_service_get_group_questions(session, test_question_group):
    """Test getting questions in a group."""
    questions = QuestionGroupService.get_group_questions(test_question_group.id, session)
    assert isinstance(questions, pd.DataFrame)
    assert len(questions) == 1
    assert questions.iloc[0]["Text"] == "test question"
    assert questions.iloc[0]["Display Text"] == "Test Question"
    assert questions.iloc[0]["Type"] == "single"
    assert questions.iloc[0]["Options"] == "option1, option2"
    assert questions.iloc[0]["Default"] == "option1"

def test_question_group_service_get_group_questions_not_found(session):
    """Test getting questions for a non-existent group."""
    with pytest.raises(ValueError, match="Question group with ID 999 not found"):
        QuestionGroupService.get_group_questions(999, session)

def test_question_group_service_archive_group(session, test_question_group):
    """Test archiving a group."""
    QuestionGroupService.archive_group(test_question_group.id, session)
    group = QuestionGroupService.get_group_by_id(test_question_group.id, session)
    assert group.is_archived

def test_question_group_service_unarchive_group(session, test_question_group):
    """Test unarchiving a group."""
    # First archive the group
    QuestionGroupService.archive_group(test_question_group.id, session)
    # Then unarchive it
    QuestionGroupService.unarchive_group(test_question_group.id, session)
    group = QuestionGroupService.get_group_by_id(test_question_group.id, session)
    assert not group.is_archived

def test_question_group_service_get_question_order(session, test_question_group):
    """Test getting question order in a group."""
    order = QuestionGroupService.get_question_order(test_question_group.id, session)
    assert len(order) == 1
    assert order[0] == test_question_group.id

def test_question_group_service_update_question_order(session, test_question_group):
    """Test updating question order in a group."""
    # Create a second question
    QuestionService.add_question(
        text="test question 2",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_text="Test Question 2"
    )
    question2 = QuestionService.get_question_by_text("test question 2", session)
    
    # Create a new group with both questions
    group = QuestionGroupService.create_group(
        title="test_group2",
        description="test description",
        is_reusable=True,
        question_ids=[test_question_group.id, question2.id],
        verification_function=None,
        session=session
    )
    
    # Update order
    QuestionGroupService.update_question_order(
        group.id,
        [question2.id, test_question_group.id],
        session
    )
    
    # Verify new order
    order = QuestionGroupService.get_question_order(group.id, session)
    assert len(order) == 2
    assert order[0] == question2.id
    assert order[1] == test_question_group.id

def test_question_group_service_get_all_groups_by_reusable(session, test_question_group):
    """Test getting all groups filtered by reusable status."""
    # Create a non-reusable group
    QuestionGroupService.create_group(
        title="non_reusable_group",
        description="test description",
        is_reusable=False,
        question_ids=[test_question_group.id],
        verification_function=None,
        session=session
    )
    
    
    # Get all groups
    all_groups = QuestionGroupService.get_all_groups(session)
    assert len(all_groups) == 2 

    reusable_groups = all_groups[all_groups["Reusable"] == True]
    assert len(reusable_groups) == 1
    assert reusable_groups.iloc[0]["Name"] == "test_group"

    non_reusable_groups = all_groups[all_groups["Reusable"] == False]
    assert len(non_reusable_groups) == 1
    assert non_reusable_groups.iloc[0]["Name"] == "non_reusable_group"