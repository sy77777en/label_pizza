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
            
def check_camera_movement(answers: Dict[str, str]) -> None:
    """Check if the camera movement is valid.
    
    Args:
        answers: Dictionary mapping question text to answer value
    """
    
    steadiness = answers.get("What is the camera steadiness?", None)
    camera_movement = answers.get("Is there any camera movement other than shaking?", None)
    description = answers.get("If the camera motion is too complex, how would you describe it?", None)
    
    if steadiness == "Static (Fixed Camera)" and camera_movement in ["Yes with major, simple motion", "Yes with minor motion"]:
        raise ValueError("When steadiness is 'static', camera_movement cannot be 'major_simple' or 'minor'.")
    
    if camera_movement == "Yes with major, complex motion":
        if not description:
            raise ValueError("When camera_movement is 'major_complex', complex_motion_description must not be empty.")
    else:
        if description:
            raise ValueError("When camera_movement is not 'major_complex', complex_motion_description must be empty.")
    
    movements = []
    movement_questions = ["Is the camera moving forward or backward?", "Is the camera zooming?", "Is the camera moving (trucking) to the left or right?", "Is the camera panning?", "Is the camera moving up or down?", "Is the camera tilting?", "Is the camera moving in an arc?", "Is the camera rolling?"]
    for movement_question in movement_questions:
        answer = answers.get(movement_question, None)
        movements.append(answer)
    
    if camera_movement == "Yes with major, simple motion":
        if all(movement == "No" for movement in movements):
            raise ValueError("When camera_movement is 'major_simple', at least one direction movement must be specified.")
        
    if camera_movement == "No":
        if(any(movement != "No" for movement in movements)):
            raise ValueError("When camera_movement is 'no', all direction movements must be 'no'.")
        

    
    