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
        

def check_human_description(answers: Dict[str, str]) -> None:
    """Check if the subject description is valid.
    
    Args:
        answers: Dictionary mapping question text to answer value
    """
    descriptions = answers.get("If there are people, describe them.", "")
    number_of_people = answers.get("Number of people?", None)
    if number_of_people is None:
        raise ValueError("Answer for 'Number of people?' cannot be empty")
    if number_of_people.strip() != "0" and descriptions == "":
        raise ValueError("Answer for 'If there are people, describe them.' cannot be empty when there is at least one person!")
    elif number_of_people.strip() == "0" and descriptions != "":
        raise ValueError("Answer for 'If there are people, describe them.' cannot be non-empty when there is no person!")
            
def check_pizza_description(answers: Dict[str, str]) -> None:
    """Check if the action description is valid.
    
    Args:
        answers: Dictionary mapping question text to answer value
    """
    descriptions = answers.get("If there are pizzas, describe them.", "")
    number_of_pizzas = answers.get("Number of pizzas?", None)
    if number_of_pizzas is None:
        raise ValueError("Answer for 'What is the primary action in the video?' cannot be empty")
    if number_of_pizzas.strip() != "0" and descriptions == "":
        raise ValueError("Answer for 'If there are pizzas, describe them.' cannot be empty when there is at least one pizza!")
    elif number_of_pizzas.strip() == "0" "Other (please specify)" and descriptions != "":
        raise ValueError("Answer for 'If there are pizzas, describe them.' cannot be non-empty when there is no pizza!")
            
