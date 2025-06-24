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
    descriptions = ""
    for question_text, answer in answers.items():
        if question_text == "If there is at least one person in the video, please describe them.":
            descriptions = answer
            break
    for question_text, answer in answers.items():
        if question_text == "How many people are there in the video":
            if answer.strip() != "0" and descriptions == "":
                raise ValueError(f"Answer for '{question_text}' cannot be empty when there is at least one person!") 
            elif answer.strip() == "0" and descriptions != "":
                raise ValueError(f"Answer for '{question_text}' cannot be non-empty when there is no person!") 
            
def check_weather_description(answers: Dict[str, str]) -> None:
    """Check if the weather description is valid.
    
    Args:
        answers: Dictionary mapping question text to answer value
    """
    descriptions = ""
    for question_text, answer in answers.items():
        if question_text == "If the weathers change during the video, please describe them.":
            descriptions = answer
            break
    
    for question_text, answer in answers.items():
        if question_text == "Is the weather sunny?":
            if answer.strip() == "Complex (others)" and descriptions == "":
                raise ValueError(f"Answer for '{question_text}' cannot be empty when selecting compelx weather!") 
            elif answer.strip() != "Complex (others)" and descriptions != "":
                raise ValueError(f"Answer for '{question_text}' cannot be non-empty when not selecting compelx weather!") 
            
