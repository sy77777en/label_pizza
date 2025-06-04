from typing import Dict

def verify_non_empty_description(answers: Dict[str, str]) -> None:
    """Verify that description answers are non-empty.
    
    Args:
        answers: Dictionary mapping question text to answer value
        
    Raises:
        ValueError: If any description answer is empty
    """
    for question_text, answer in answers.items():
        if "Provide a brief caption or description of the video content" in question_text and not answer.strip():
            raise ValueError(f"Description answer for '{question_text}' cannot be empty") 