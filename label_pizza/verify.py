from typing import Dict

def verify_non_empty(answers: Dict[str, str]) -> None:
    """Verify that answers are non-empty.
    
    Args:
        answers: Dictionary mapping question text to answer value
        
    Raises:
        ValueError: If any answer is empty
    """
    for question_text, answer in answers.items():
        if not answer.strip():
            raise ValueError(f"Answer for '{question_text}' cannot be empty") 
        

def check_subject_description(answers: Dict[str, str]) -> None:
    """Check if the subject description is valid.
    
    Args:
        answers: Dictionary mapping question text to answer value
    """
    descriptions = answers.get("If there is at least one person in the video, please describe them.", "")
    number_of_people = answers.get("How many people are there in the video", None)
    if number_of_people is None:
        raise ValueError("Answer for 'How many people are there in the video' cannot be empty")
    if number_of_people.strip() != "0" and descriptions == "":
        raise ValueError("Answer for 'How many people are there in the video' cannot be empty when there is at least one person!")
    elif number_of_people.strip() == "0" and descriptions != "":
        raise ValueError("Answer for 'How many people are there in the video' cannot be non-empty when there is no person!")
            
def check_action_description(answers: Dict[str, str]) -> None:
    """Check if the action description is valid.
    
    Args:
        answers: Dictionary mapping question text to answer value
    """
    descriptions = answers.get("Provide a brief description of the observed action.", "")
    action = answers.get("What is the primary action in the video?", None)
    if action is None:
        raise ValueError("Answer for 'What is the primary action in the video?' cannot be empty")
    if action.strip() != "Other (please specify)" and descriptions == "":
        raise ValueError("Answer for 'What is the primary action in the video?' cannot be empty when not selecting 'Other (please specify)'")
    elif action.strip() == "Other (please specify)" and descriptions != "":
        raise ValueError("Answer for 'What is the primary action in the video?' cannot be non-empty when selecting 'Other (please specify)'")
            
