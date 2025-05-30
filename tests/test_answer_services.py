import pytest
from label_pizza.services import AnnotatorService, GroundTruthService, ProjectService, AuthService, QuestionService, QuestionGroupService
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import select
from label_pizza.models import Question, QuestionGroupQuestion, SchemaQuestionGroup, Project, AnnotatorAnswer, AnswerReview

def test_annotator_service_submit_answer_to_question_group(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers to a question group."""
    # Create answers
    answers = {
        "test question": "option1"
    }
    
    # Submit answers
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Verify answers were submitted
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Answer Value"] == "option1"

def test_annotator_service_submit_answer_with_confidence(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with confidence scores."""
    answers = {
        "test question": "option1"
    }
    confidence_scores = {
        "test question": 0.8
    }
    
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        confidence_scores=confidence_scores,
        session=session
    )
    
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Confidence Score"] == 0.8

def test_annotator_service_submit_answer_with_notes(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with notes."""
    answers = {
        "test question": "option1"
    }
    notes = {
        "test question": "Note 1"
    }
    
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        notes=notes,
        session=session
    )
    
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Notes"] == "Note 1"

def test_annotator_service_submit_answer_invalid_user(session, test_project, test_video, test_question_group):
    """Test submitting answers with invalid user."""
    answers = {"test question": "option1"}
    
    with pytest.raises(ValueError, match="User with ID 999 not found"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=999,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_annotator_service_submit_answer_invalid_project(session, test_user, test_video, test_question_group):
    """Test submitting answers with invalid project."""
    answers = {"test question": "option1"}
    
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=999,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_annotator_service_submit_answer_invalid_question_group(session, test_user, test_project, test_video):
    """Test submitting answers with invalid question group."""
    answers = {"test question": "option1"}
    
    with pytest.raises(ValueError, match="Question group with ID 999 not found"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=999,
            answers=answers,
            session=session
        )

def test_annotator_service_submit_answer_invalid_option(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with invalid option."""
    answers = {"test question": "invalid_option"}
    
    with pytest.raises(ValueError, match="Answer value 'invalid_option' not in options"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_annotator_service_submit_answer_invalid_question_text(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with invalid question text."""
    answers = {
        "Invalid Question": "option1"
    }
    with pytest.raises(ValueError, match="Answers do not match questions in group"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_annotator_service_submit_answer_missing_questions(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with missing questions."""
    answers = {}  # Empty answers
    with pytest.raises(ValueError, match="Answers do not match questions in group"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_annotator_service_submit_answer_invalid_confidence(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with invalid confidence scores."""
    answers = {
        "test question": "option1"
    }
    confidence_scores = {
        "test question": "1.5" # invalid string format (should be float)
    }
    with pytest.raises(ValueError, match="Confidence score for question 'test question' must be a float"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            confidence_scores=confidence_scores,
            session=session
        )

def test_annotator_service_update_existing_answer(session, test_user, test_project, test_video, test_question_group):
    """Test updating an existing answer."""
    # Submit initial answer
    initial_answers = {
        "test question": "option1"
    }
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=initial_answers,
        session=session
    )
    
    # Update answer
    updated_answers = {
        "test question": "option2"
    }
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=updated_answers,
        session=session
    )
    
    # Verify update
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Answer Value"] == "option2"

def test_annotator_service_get_question_answers(session, test_user, test_project, test_video, test_question_group):
    """Test getting answers for a specific question."""
    # Submit answers
    answers = {
        "test question": "option1"
    }
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Get the question using QuestionService
    question = QuestionService.get_question_by_text("test question", session)
    
    # Get answers for the question
    result = AnnotatorService.get_question_answers(
        question_id=question.id,
        project_id=test_project.id,
        session=session
    )
    assert len(result) == 1
    assert result.iloc[0]["Answer Value"] == "option1"
    assert result.iloc[0]["User ID"] == test_user.id

def test_ground_truth_service_submit_ground_truth(session, test_user, test_project, test_video, test_question_group):
    """Test submitting ground truth answers."""
    answers = {
        "test question": "option1"
    }
    
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    result = GroundTruthService.get_ground_truth(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Answer Value"] == "option1"

def test_ground_truth_service_override_ground_truth(session, test_user, test_project, test_video, test_question_group):
    """Test overriding ground truth answers."""
    # First submit initial ground truth
    initial_answers = {"test question": "option1"}
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=initial_answers,
        session=session
    )
    
    # Then override it
    override_answers = {"test question": "option2"}
    GroundTruthService.override_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        question_group_id=test_question_group.id,
        admin_id=test_user.id,
        answers=override_answers,
        session=session
    )
    
    result = GroundTruthService.get_ground_truth(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Answer Value"] == "option2"
    assert result.iloc[0]["Modified By Admin"] == test_user.id

def test_ground_truth_service_override_ground_truth_invalid_admin(session, test_project, test_video, test_question_group):
    """Test overriding ground truth with invalid admin."""
    # Create a new non-admin user
    AuthService.create_user(
        user_id="2",
        email="test@test.com",
        password_hash="test",
        user_type="human",
        session=session
    )
    user = AuthService.get_user_by_id("2", session)

    # Add the user to the project
    ProjectService.add_user_to_project(
        user_id=user.id,
        project_id=test_project.id,
        role="reviewer",
        session=session
    )

    # submit ground truth
    answers = {"test question": "option1"}
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )

    override_answers = {"test question": "option2"}
    with pytest.raises(ValueError, match="User 1 does not have admin role in project 1"):
        GroundTruthService.override_ground_truth_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            question_group_id=test_question_group.id,
            admin_id=user.id,
            answers=override_answers,
            session=session
        )

    AuthService.update_user_role(
        user_id=user.id,
        new_role="admin",
        session=session
    )
    # Admin can override ground truth
    GroundTruthService.override_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        question_group_id=test_question_group.id,
        admin_id=user.id,
        answers=override_answers,
        session=session
    )

    assert GroundTruthService.get_ground_truth(test_video.id, test_project.id, session).iloc[0]["Answer Value"] == "option2"

def test_ground_truth_service_override_ground_truth_invalid_option(session, test_user, test_project, test_video, test_question_group):
    """Test overriding ground truth with invalid option."""
    override_answers = {"test question": "invalid_option"}
    with pytest.raises(ValueError, match="Answer value 'invalid_option' not in options"):
        GroundTruthService.override_ground_truth_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            question_group_id=test_question_group.id,
            admin_id=test_user.id,
            answers=override_answers,
            session=session
        )

def test_ground_truth_service_get_reviewer_accuracy(session, test_user, test_project, test_video, test_question_group):
    """Test getting reviewer accuracy."""
    # Submit some ground truth answers
    initial_answers = {"test question": "option1"}
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=initial_answers,
        session=session
    )
    
    # Override all answers
    override_answers = {"test question": "option2"}
    GroundTruthService.override_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        question_group_id=test_question_group.id,
        admin_id=test_user.id,
        answers=override_answers,
        session=session
    )
    
    accuracy = GroundTruthService.get_reviewer_accuracy(test_user.id, test_project.id, session)
    assert accuracy == 0.0  # All answers were modified

def test_ground_truth_service_get_annotator_accuracy(session, test_user, test_project, test_video, test_question_group):
    """Test getting annotator accuracy."""
    # Submit annotator answer
    answers = {"test question": "option1"}
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Submit matching ground truth
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Get the question using QuestionService
    question = QuestionService.get_question_by_text("test question", session)
    
    accuracy = GroundTruthService.get_annotator_accuracy(
        project_id=test_project.id,
        question_id=question.id,
        session=session
    )
    assert len(accuracy) == 1
    assert accuracy.iloc[0]["Correct"] == 1  # Answer matches ground truth

def test_ground_truth_service_submit_ground_truth_invalid_question_text(session, test_user, test_project, test_video, test_question_group):
    """Test submitting ground truth with invalid question text."""
    answers = {
        "Invalid Question": "option1"
    }
    with pytest.raises(ValueError, match="Answers do not match questions in group"):
        GroundTruthService.submit_ground_truth_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            reviewer_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_ground_truth_service_override_same_value(session, test_user, test_project, test_video, test_question_group):
    """Test overriding ground truth with same value (should not update)."""
    # Submit initial ground truth
    initial_answers = {
        "test question": "option1"
    }
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=initial_answers,
        session=session
    )
    
    # Get initial ground truth
    initial_gt = GroundTruthService.get_ground_truth(test_video.id, test_project.id, session)
    initial_modified_at = initial_gt.iloc[0]["Modified At"]
    
    # Override with same value
    GroundTruthService.override_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        question_group_id=test_question_group.id,
        admin_id=test_user.id,
        answers=initial_answers,
        session=session
    )
    
    # Verify no update occurred
    updated_gt = GroundTruthService.get_ground_truth(test_video.id, test_project.id, session)
    assert updated_gt.iloc[0]["Modified At"] == initial_modified_at

def test_ground_truth_service_reviewer_accuracy_no_answers(session, test_user, test_project):
    """Test reviewer accuracy calculation with no answers."""
    accuracy = GroundTruthService.get_reviewer_accuracy(test_user.id, test_project.id, session)
    assert accuracy == 0.0

def test_ground_truth_service_reviewer_accuracy_all_modified(session, test_user, test_project, test_video, test_question_group):
    """Test reviewer accuracy calculation with all modified answers."""
    # Submit initial ground truth
    initial_answers = {
        "test question": "option1"
    }
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=initial_answers,
        session=session
    )
    
    # Override all answers
    override_answers = {
        "test question": "option2"
    }
    GroundTruthService.override_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        question_group_id=test_question_group.id,
        admin_id=test_user.id,
        answers=override_answers,
        session=session
    )
    
    # Verify accuracy is 0
    accuracy = GroundTruthService.get_reviewer_accuracy(test_user.id, test_project.id, session)
    assert accuracy == 0.0

def test_ground_truth_service_annotator_accuracy_mixed(session, test_user, test_project, test_video, test_question_group):
    """Test annotator accuracy calculation with mixed correct/incorrect answers."""
    # Submit ground truth
    gt_answers = {
        "test question": "option1"
    }
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=gt_answers,
        session=session
    )
    
    # Submit annotator answers (one correct, one incorrect)
    annotator_answers = {
        "test question": "option2"  # Incorrect
    }
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=annotator_answers,
        session=session
    )
    
    # Get the question using QuestionService
    question = QuestionService.get_question_by_text("test question", session)
    
    # Verify accuracy is 0%
    accuracy_df = GroundTruthService.get_annotator_accuracy(
        project_id=test_project.id,
        question_id=question.id,
        session=session
    )
    assert len(accuracy_df) == 1
    assert accuracy_df.iloc[0]["Correct"] == 0

def test_answer_services_verification_function(session, test_user, test_project, test_video, test_question_group):
    """Test answer submission with verification function."""
    # Add verification function to question group
    test_question_group.verification_function = "verify_answers"
    session.commit()
    
    # Submit answers that should fail verification
    answers = {
        "test question": "invalid_option"
    }
    with pytest.raises(ValueError, match="Verification function 'verify_answers' not found in verify.py"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_answer_services_completion_status(session, test_user, test_project, test_video, test_question_group):
    """Test completion status updates."""
    # Get the question group id
    question_group_id = test_question_group.id
    # Submit answers for all questions
    # Get all questions in the question group
    questions = QuestionGroupService.get_group_questions(question_group_id, session)
    answers = {q["Text"]: "option1" for _, q in questions.iterrows()}
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=question_group_id,
        answers=answers,
        session=session
    )
    
    # Get user's role using AuthService
    assignments = AuthService.get_project_assignments(session)
    user_role = assignments[
        (assignments["User ID"] == test_user.id) & 
        (assignments["Project ID"] == test_project.id) &
        (assignments["Role"] == "annotator") &
        (assignments["Archived"] == False)
    ].iloc[0]
    
    # Verify completion status
    assert user_role["Completed At"] is not None
    assert user_role["Completed At"] <= datetime.now(timezone.utc)

def test_ground_truth_service_submit_answer_review(session, test_user, test_project, test_video, test_question_group):
    """Test submitting an answer review."""
    # Create a description question
    QuestionService.add_question(
        text="description question",
        qtype="description",
        options=None,
        default=None,
        session=session
    )
    question = QuestionService.get_question_by_text("description question", session)
    
    # Create a new question group with the description question
    description_group = QuestionGroupService.create_group(
        title="Description Group",
        description="Group for description questions",
        is_reusable=True,
        question_ids=[question.id],
        verification_function=None,
        session=session
    )
    
    # Submit an answer
    answers = {"description question": "test answer"}
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=description_group.id,
        answers=answers,
        session=session
    )
    
    # Get the answer using AnnotatorService
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    answer_id = int(result.iloc[0]["Answer ID"])  # Convert to Python int
    
    # Submit review
    GroundTruthService.submit_answer_review(
        answer_id=answer_id,
        reviewer_id=test_user.id,
        status="approved",
        comment="Good answer",
        session=session
    )
    
    # Verify review was created using GroundTruthService
    review_result = GroundTruthService.get_answer_review(answer_id, session)
    assert review_result is not None
    assert review_result["status"] == "approved"
    assert review_result["comment"] == "Good answer"
    assert review_result["reviewer_id"] == test_user.id
    assert review_result["reviewed_at"] is not None
    
    # Test overriding existing review
    GroundTruthService.submit_answer_review(
        answer_id=answer_id,
        reviewer_id=test_user.id,
        status="rejected",
        comment="Changed my mind",
        session=session
    )
    
    # Verify review was updated
    review_result = GroundTruthService.get_answer_review(answer_id, session)
    assert review_result is not None
    assert review_result["status"] == "rejected"
    assert review_result["comment"] == "Changed my mind"
    assert review_result["reviewer_id"] == test_user.id
    assert review_result["reviewed_at"] is not None

def test_ground_truth_service_submit_answer_review_invalid_answer(session, test_user):
    """Test submitting review for non-existent answer."""
    with pytest.raises(ValueError, match="Answer with ID 999 not found"):
        GroundTruthService.submit_answer_review(
            answer_id=999,
            reviewer_id=test_user.id,
            status="approved",
            session=session
        )

def test_ground_truth_service_submit_answer_review_invalid_status(session, test_user, test_project, test_video, test_question_group):
    """Test submitting review with invalid status."""
    # Create a description question
    QuestionService.add_question(
        text="description question",
        qtype="description",
        options=None,
        default=None,
        session=session
    )
    question = QuestionService.get_question_by_text("description question", session)
    
    # Create a new question group with the description question
    description_group = QuestionGroupService.create_group(
        title="Description Group",
        description="Group for description questions",
        is_reusable=True,
        question_ids=[question.id],
        verification_function=None,
        session=session
    )
    
    # Submit an answer
    answers = {"description question": "test answer"}
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=description_group.id,
        answers=answers,
        session=session
    )
    
    # Get the answer using AnnotatorService
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    answer_id = int(result.iloc[0]["Answer ID"])  # Convert to Python int
    
    # Try to submit review with invalid status
    with pytest.raises(ValueError, match="Invalid review status: invalid"):
        GroundTruthService.submit_answer_review(
            answer_id=answer_id,
            reviewer_id=test_user.id,
            status="invalid",
            session=session
        )

def test_ground_truth_service_submit_answer_review_single_choice(session, test_user, test_project, test_video, test_question_group):
    """Test submitting review for single-choice question."""
    # Submit an answer
    answers = {"test question": "option1"}
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        user_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Get the answer using AnnotatorService
    result = AnnotatorService.get_answers(test_video.id, test_project.id, session)
    answer_id = int(result.iloc[0]["Answer ID"])  # Convert to Python int
    
    # Try to submit review for single-choice question
    with pytest.raises(ValueError, match="Question 'test question' is not a description type question"):
        GroundTruthService.submit_answer_review(
            answer_id=answer_id,
            reviewer_id=test_user.id,
            status="approved",
            session=session
        )

def test_ground_truth_service_get_answer_review_nonexistent(session):
    """Test getting review for non-existent answer."""
    review_result = GroundTruthService.get_answer_review(999, session)
    assert review_result is None