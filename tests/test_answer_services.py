import pytest
from label_pizza.services import AnnotatorService, GroundTruthService, ProjectService
import pandas as pd
from datetime import datetime, timezone

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

def test_annotator_service_submit_answer_to_question_group(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers to a question group."""
    # Create answers
    answers = {
        "Test Question 1": "option1",
        "Test Question 2": "option2"
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
    assert len(result) == 2
    assert result.iloc[0]["Answer Value"] == "option1"
    assert result.iloc[1]["Answer Value"] == "option2"

def test_annotator_service_submit_answer_with_confidence(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with confidence scores."""
    answers = {
        "Test Question 1": "option1",
        "Test Question 2": "option2"
    }
    confidence_scores = {
        "Test Question 1": 0.8,
        "Test Question 2": 0.9
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
    assert len(result) == 2
    assert result.iloc[0]["Confidence Score"] == 0.8
    assert result.iloc[1]["Confidence Score"] == 0.9

def test_annotator_service_submit_answer_with_notes(session, test_user, test_project, test_video, test_question_group):
    """Test submitting answers with notes."""
    answers = {
        "Test Question 1": "option1",
        "Test Question 2": "option2"
    }
    notes = {
        "Test Question 1": "Note 1",
        "Test Question 2": "Note 2"
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
    assert len(result) == 2
    assert result.iloc[0]["Notes"] == "Note 1"
    assert result.iloc[1]["Notes"] == "Note 2"

def test_annotator_service_submit_answer_invalid_user(session, test_project, test_video, test_question_group):
    """Test submitting answers with invalid user."""
    answers = {"Test Question 1": "option1"}
    
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
    answers = {"Test Question 1": "option1"}
    
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
    answers = {"Test Question 1": "option1"}
    
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
    answers = {"Test Question 1": "invalid_option"}
    
    with pytest.raises(ValueError, match="Answer value 'invalid_option' not in options"):
        AnnotatorService.submit_answer_to_question_group(
            video_id=test_video.id,
            project_id=test_project.id,
            user_id=test_user.id,
            question_group_id=test_question_group.id,
            answers=answers,
            session=session
        )

def test_ground_truth_service_submit_ground_truth(session, test_user, test_project, test_video, test_question_group):
    """Test submitting ground truth answers."""
    answers = {
        "Test Question 1": "option1",
        "Test Question 2": "option2"
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
    assert len(result) == 2
    assert result.iloc[0]["Answer Value"] == "option1"
    assert result.iloc[1]["Answer Value"] == "option2"

def test_ground_truth_service_override_ground_truth(session, test_user, test_project, test_video, test_question_group):
    """Test overriding ground truth answers."""
    # First submit initial ground truth
    answers = {"Test Question 1": "option1"}
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Then override it
    GroundTruthService.override_ground_truth(
        video_id=test_video.id,
        question_id=test_question_group.questions[0].id,
        project_id=test_project.id,
        admin_id=test_user.id,
        new_answer_value="option2",
        session=session
    )
    
    result = GroundTruthService.get_ground_truth(test_video.id, test_project.id, session)
    assert len(result) == 1
    assert result.iloc[0]["Answer Value"] == "option2"
    assert result.iloc[0]["Modified By Admin"] == test_user.id

def test_ground_truth_service_override_ground_truth_invalid_admin(session, test_project, test_video, test_question_group):
    """Test overriding ground truth with invalid admin."""
    with pytest.raises(ValueError, match="User 999 is not an admin"):
        GroundTruthService.override_ground_truth(
            video_id=test_video.id,
            question_id=test_question_group.questions[0].id,
            project_id=test_project.id,
            admin_id=999,
            new_answer_value="option2",
            session=session
        )

def test_ground_truth_service_override_ground_truth_invalid_option(session, test_user, test_project, test_video, test_question_group):
    """Test overriding ground truth with invalid option."""
    with pytest.raises(ValueError, match="Answer value 'invalid_option' not in options"):
        GroundTruthService.override_ground_truth(
            video_id=test_video.id,
            question_id=test_question_group.questions[0].id,
            project_id=test_project.id,
            admin_id=test_user.id,
            new_answer_value="invalid_option",
            session=session
        )

def test_ground_truth_service_get_reviewer_accuracy(session, test_user, test_project, test_video, test_question_group):
    """Test getting reviewer accuracy."""
    # Submit some ground truth answers
    answers = {"Test Question 1": "option1"}
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=test_project.id,
        reviewer_id=test_user.id,
        question_group_id=test_question_group.id,
        answers=answers,
        session=session
    )
    
    # Override one answer
    GroundTruthService.override_ground_truth(
        video_id=test_video.id,
        question_id=test_question_group.questions[0].id,
        project_id=test_project.id,
        admin_id=test_user.id,
        new_answer_value="option2",
        session=session
    )
    
    accuracy = GroundTruthService.get_reviewer_accuracy(test_user.id, test_project.id, session)
    assert accuracy == 0.0  # All answers were modified

def test_ground_truth_service_get_annotator_accuracy(session, test_user, test_project, test_video, test_question_group):
    """Test getting annotator accuracy."""
    # Submit annotator answer
    answers = {"Test Question 1": "option1"}
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
    
    accuracy = GroundTruthService.get_annotator_accuracy(
        project_id=test_project.id,
        question_id=test_question_group.questions[0].id,
        session=session
    )
    assert len(accuracy) == 1
    assert accuracy.iloc[0]["Correct"] == 1  # Answer matches ground truth 