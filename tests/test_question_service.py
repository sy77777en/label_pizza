import pytest
from label_pizza.services import QuestionService
import pandas as pd

def test_question_service_add_question(session):
    """Test adding a new question."""
    QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_values=["Option 1", "Option 2"]
    )
    question = QuestionService.get_question_by_text("test question", session)
    assert question.text == "test question"
    assert question.type == "single"
    assert question.options == ["option1", "option2"]
    assert question.display_values == ["Option 1", "Option 2"]
    assert question.default_option == "option1"

def test_question_service_add_question_default_display_values(session):
    """Test adding a question without display values (should use options as display values)."""
    QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    question = QuestionService.get_question_by_text("test question", session)
    assert question.display_values == ["option1", "option2"]

def test_question_service_add_question_mismatched_display_values(session):
    """Test adding a question with mismatched display values length."""
    with pytest.raises(ValueError, match="Number of display values must match number of options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=["option1", "option2"],
            default="option1",
            session=session,
            display_values=["Option 1"]  # Only one display value for two options
        )

def test_question_service_add_question_description_no_display_values(session):
    """Test adding a description question (should have no display values)."""
    QuestionService.add_question(
        text="test question",
        qtype="description",
        options=None,
        default=None,
        session=session,
        display_values=["Should be ignored"]  # Should be ignored for description type
    )
    question = QuestionService.get_question_by_text("test question", session)
    assert question.text == "test question"
    assert question.type == "description"
    assert question.options is None
    assert question.display_values is None
    assert question.default_option is None

def test_question_service_add_question_duplicate(session, test_question):
    """Test adding a question with duplicate text."""
    with pytest.raises(ValueError, match="already exists"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=["option1", "option2"],
            default="option1",
            session=session
        )

def test_question_service_add_question_single_choice_no_options(session):
    """Test adding a single-choice question without options."""
    with pytest.raises(ValueError, match="must have options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=None,
            default=None,
            session=session
        )

def test_question_service_add_question_single_choice_empty_options(session):
    """Test adding a single-choice question with empty options."""
    with pytest.raises(ValueError, match="must have options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=[],
            default="option1",
            session=session
        )

def test_question_service_add_question_invalid_default(session):
    """Test adding a question with invalid default value."""
    with pytest.raises(ValueError, match="must be one of the available options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=["option1", "option2"],
            default="invalid",
            session=session
        )

def test_question_service_get_question_by_text(session, test_question):
    """Test getting a question by text."""
    question = QuestionService.get_question_by_text("test question", session)
    assert question.id == test_question.id
    assert question.text == "test question"

def test_question_service_get_question_by_text_not_found(session):
    """Test getting a non-existent question by text."""
    with pytest.raises(ValueError, match="Question with text 'non_existent_question' not found"):
        QuestionService.get_question_by_text("non_existent_question", session)

def test_question_service_archive_question(session, test_question):
    """Test archiving a question."""
    QuestionService.archive_question(test_question.id, session)
    question = QuestionService.get_question_by_id(test_question.id, session)
    assert question.is_archived

def test_question_service_archive_question_not_found(session):
    """Test archiving a non-existent question."""
    with pytest.raises(ValueError, match="Question with ID 999 not found"):
        QuestionService.archive_question(999, session)

def test_question_service_get_question_by_id(session, test_question):
    """Test getting a question by ID."""
    question = QuestionService.get_question_by_id(test_question.id, session)
    assert question.text == "test question"
    assert question.type == "single"
    assert question.options == ["option1", "option2"]
    assert question.default_option == "option1"

def test_question_service_get_all_questions(session, test_question):
    """Test getting all questions."""
    questions = QuestionService.get_all_questions(session)
    assert isinstance(questions, pd.DataFrame)
    assert len(questions) == 1
    assert questions.iloc[0]["ID"] == test_question.id
    assert questions.iloc[0]["Text"] == "test question"
    assert questions.iloc[0]["Type"] == "single"
    assert questions.iloc[0]["Options"] == "option1, option2"
    assert questions.iloc[0]["Default"] == "option1"

def test_question_service_get_all_questions_empty(session):
    """Test getting all questions when none exist."""
    questions = QuestionService.get_all_questions(session)
    assert len(questions) == 0

def test_question_service_get_all_questions_with_archived(session, test_question):
    """Test getting all questions including archived ones."""
    QuestionService.archive_question(test_question.id, session)
    questions = QuestionService.get_all_questions(session)
    assert len(questions) == 1
    assert questions.iloc[0]["ID"] == test_question.id
    assert questions.iloc[0]["Archived"] == True

def test_question_service_edit_question(session, test_question):
    """Test editing a question."""
    QuestionService.edit_question(
        question_id=test_question.id,
        new_text="updated question",
        new_opts=["option1", "option2", "option3"],
        new_default="option3",
        session=session,
        new_display_values=["Option 1", "Option 2", "Option 3"]
    )
    question = QuestionService.get_question_by_id(test_question.id, session)
    assert question.text == "updated question"
    assert question.type == "single"
    assert question.options == ["option1", "option2", "option3"]
    assert question.display_values == ["Option 1", "Option 2", "Option 3"]
    assert question.default_option == "option3"

def test_question_service_edit_question_maintain_display_values(session, test_question):
    """Test editing a question while maintaining existing display values."""
    # First add display values
    QuestionService.edit_question(
        question_id=test_question.id,
        new_text="test question",
        new_opts=["option1", "option2"],
        new_default="option1",
        session=session,
        new_display_values=["Option 1", "Option 2"]
    )
    
    # Then edit options while maintaining display values
    QuestionService.edit_question(
        question_id=test_question.id,
        new_text="test question",
        new_opts=["option1", "option2", "option3"],
        new_default="option1",
        session=session
    )
    question = QuestionService.get_question_by_id(test_question.id, session)
    assert question.options == ["option1", "option2", "option3"]
    assert question.display_values == ["Option 1", "Option 2", "option3"]  # New option uses its value as display

def test_question_service_edit_question_mismatched_display_values(session, test_question):
    """Test editing a question with mismatched display values length."""
    with pytest.raises(ValueError, match="Number of display values must match number of options"):
        QuestionService.edit_question(
            question_id=test_question.id,
            new_text="test question",
            new_opts=["option1", "option2", "option3"],
            new_default="option1",
            session=session,
            new_display_values=["Option 1", "Option 2"]  # Only two display values for three options
        )

def test_question_service_edit_question_description_no_display_values(session, test_question):
    """Test that we cannot change a question's type."""
    # Try to edit a single-type question to be a description-type question
    with pytest.raises(ValueError, match="Cannot change question type"):
        QuestionService.edit_question(
            question_id=test_question.id,
            new_text="test question edited",
            new_opts=None,  # Try to remove options
            new_default=None,  # Try to remove default
            session=session,
            new_display_values=None  # Try to remove display values
        )
    
    # Verify the question remains unchanged
    updated_question = QuestionService.get_question_by_id(test_question.id, session)
    assert updated_question.type == "single"
    assert updated_question.options == ["option1", "option2"]
    assert updated_question.display_values == ["Option 1", "Option 2"]
    assert updated_question.default_option == "option1"

def test_question_service_edit_question_not_found(session):
    """Test editing a non-existent question."""
    with pytest.raises(ValueError, match="not found"):
        QuestionService.edit_question(
            999,
            new_text="updated question",
            new_opts=["option1", "option2"],
            new_default="option1",
            session=session
        )

def test_question_service_edit_question_archived(session, test_question):
    """Test editing an archived question."""
    QuestionService.archive_question(test_question.id, session)
    with pytest.raises(ValueError, match="is archived"):
        QuestionService.edit_question(
            test_question.id,
            new_text="updated question",
            new_opts=["option1", "option2"],
            new_default="option1",
            session=session
        )

def test_question_service_edit_question_duplicate_text(session, test_question):
    """Test editing a question to have duplicate text."""
    # Create another question
    QuestionService.add_question(
        text="other question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Try to edit first question to have same text
    with pytest.raises(ValueError, match="already exists"):
        QuestionService.edit_question(
            test_question.id,
            new_text="other question",
            new_opts=["option1", "option2"],
            new_default="option1",
            session=session
        )

def test_question_service_edit_question_single_choice_no_options(session, test_question):
    """Test editing a single-choice question without options."""
    with pytest.raises(ValueError, match="must have options"):
        QuestionService.edit_question(
            test_question.id,
            new_text="test question",
            new_opts=[],
            new_default=None,
            session=session
        )

def test_question_service_edit_question_single_choice_empty_options(session, test_question):
    """Test editing a single-choice question with empty options."""
    with pytest.raises(ValueError, match="must have options"):
        QuestionService.edit_question(
            test_question.id,
            new_text="test question",
            new_opts=None,
            new_default=None,
            session=session
        )

def test_question_service_edit_question_single_choice_invalid_default(session, test_question):
    """Test editing a single-choice question with invalid default option."""
    with pytest.raises(ValueError, match="must be one of the available options"):
        QuestionService.edit_question(
            test_question.id,
            new_text="test question",
            new_opts=["option1", "option2"],
            new_default="invalid",
            session=session
        )