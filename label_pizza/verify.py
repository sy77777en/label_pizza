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