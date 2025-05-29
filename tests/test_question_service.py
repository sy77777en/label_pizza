import pytest
from label_pizza.services import QuestionService

def test_question_service_add_question(session):
    """Test adding a new question."""
    question = QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    assert question.text == "test question"
    assert question.type == "single"
    assert question.options == ["option1", "option2"]
    assert question.default_option == "option1"
    assert not question.is_archived

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

def test_question_service_add_question_invalid_type(session):
    """Test adding a question with invalid type."""
    with pytest.raises(ValueError, match="must be one of"):
        QuestionService.add_question(
            text="test question",
            qtype="invalid",
            options=["option1", "option2"],
            default="option1",
            session=session
        )

def test_question_service_add_question_single_choice_no_options(session):
    """Test adding a single-choice question without options."""
    with pytest.raises(ValueError, match="must provide options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=None,
            default="option1",
            session=session
        )

def test_question_service_add_question_single_choice_empty_options(session):
    """Test adding a single-choice question with empty options."""
    with pytest.raises(ValueError, match="must provide options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=[],
            default="option1",
            session=session
        )

def test_question_service_add_question_single_choice_invalid_default(session):
    """Test adding a single-choice question with invalid default option."""
    with pytest.raises(ValueError, match="must be one of the options"):
        QuestionService.add_question(
            text="test question",
            qtype="single",
            options=["option1", "option2"],
            default="invalid",
            session=session
        )

def test_question_service_add_question_description(session):
    """Test adding a description question."""
    question = QuestionService.add_question(
        text="test question",
        qtype="description",
        options=None,
        default=None,
        session=session
    )
    assert question.text == "test question"
    assert question.type == "description"
    assert question.options is None
    assert question.default_option is None

def test_question_service_get_question_by_text(session, test_question):
    """Test getting a question by text."""
    question = QuestionService.get_question_by_text("test question", session)
    assert question.id == test_question.id
    assert question.text == "test question"

def test_question_service_get_question_by_text_not_found(session):
    """Test getting a non-existent question by text."""
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.get_question_by_text("non_existent_question", session)

def test_question_service_get_question_by_text_archived(session, test_question):
    """Test getting an archived question by text."""
    QuestionService.archive_question(test_question.id, session)
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.get_question_by_text("test question", session)

def test_question_service_archive_question(session, test_question):
    """Test archiving a question."""
    QuestionService.archive_question(test_question.id, session)
    question = QuestionService.get_question_by_id(test_question.id, session)
    assert question.is_archived

def test_question_service_archive_question_not_found(session):
    """Test archiving a non-existent question."""
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.archive_question(999, session)

def test_question_service_get_question_by_id(session, test_question):
    """Test getting a question by ID."""
    question = QuestionService.get_question_by_id(test_question.id, session)
    assert question.text == "test question"
    assert question.type == "single"
    assert question.options == ["option1", "option2"]
    assert question.default_option == "option1"

def test_question_service_get_question_by_id_not_found(session):
    """Test getting a non-existent question by ID."""
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.get_question_by_id(999, session)

def test_question_service_get_question_by_id_archived(session, test_question):
    """Test getting an archived question by ID."""
    QuestionService.archive_question(test_question.id, session)
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.get_question_by_id(test_question.id, session)

def test_question_service_get_all_questions(session, test_question):
    """Test getting all questions."""
    questions = QuestionService.get_all_questions(session)
    assert len(questions) == 1
    assert questions[0].id == test_question.id
    assert questions[0].text == "test question"

def test_question_service_get_all_questions_empty(session):
    """Test getting all questions when none exist."""
    questions = QuestionService.get_all_questions(session)
    assert len(questions) == 0

def test_question_service_get_all_questions_with_archived(session, test_question):
    """Test getting all questions including archived ones."""
    QuestionService.archive_question(test_question.id, session)
    questions = QuestionService.get_all_questions(session, include_archived=True)
    assert len(questions) == 1
    assert questions[0].id == test_question.id
    assert questions[0].is_archived

def test_question_service_edit_question(session, test_question):
    """Test editing a question."""
    updated_question = QuestionService.edit_question(
        question_id=test_question.id,
        text="updated question",
        options=["option1", "option2", "option3"],
        default="option2",
        session=session
    )
    assert updated_question.text == "updated question"
    assert updated_question.options == ["option1", "option2", "option3"]
    assert updated_question.default_option == "option2"

def test_question_service_edit_question_not_found(session):
    """Test editing a non-existent question."""
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.edit_question(
            question_id=999,
            text="updated question",
            options=["option1", "option2"],
            default="option1",
            session=session
        )

def test_question_service_edit_question_archived(session, test_question):
    """Test editing an archived question."""
    QuestionService.archive_question(test_question.id, session)
    with pytest.raises(ValueError, match="Question not found"):
        QuestionService.edit_question(
            question_id=test_question.id,
            text="updated question",
            options=["option1", "option2"],
            default="option1",
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
            question_id=test_question.id,
            text="other question",
            options=["option1", "option2"],
            default="option1",
            session=session
        ) 