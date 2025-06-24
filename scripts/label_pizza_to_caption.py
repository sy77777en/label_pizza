import glob
import json
import os

# Only for single questions
QUESTION_CHOICE_MAP = {
    
    # Camera Movement Attributes
    "Is there any camera movement other than shaking?": "camera_movement",
    "How fast is the camera movement? (e.g., crash zoom, whip pan)?": "camera_motion_speed",
    "What is the camera steadiness?": "steadiness",
    "Is the camera moving forward or backward?": "camera_forward_backward",
    "Is the camera zooming?": "camera_zoom",
    "Is the camera moving (trucking) to the left or right?": "camera_left_right",
    "Is the camera panning?": "camera_pan",
    "Is the camera moving up or down?": "camera_up_down",
    "Is the camera tilting?": "camera_tilt",
    "Is the camera moving in an arc?": "camera_arc",
    "Is the camera rolling?": "camera_roll",
    "If the camera motion is too complex, how would you describe it?": "complex_motion_description",
    
    # Camera Movement Effects
    "Is there a frame-freezing effect in this video?": "frame_freezing",
    "Is there a dolly-zoom effect in this video?": "dolly_zoom",
    "Is there a motion blur effect in this video?": "motion_blur",
    "Is there a cinemagraph effect in this video?": "cinemagraph",
    
    # Camera Tracking Shot
    "Does the camera track the moving subject(s)?": "is_tracking",
    "Does the size of the subject change during tracking?": "subject_size_change"
}

OPTION_MAP = {
    "What is the camera steadiness?": {
        "Static (Fixed Camera)": "static",
        "Very Smooth / No Shaking (e.g., Drone shot with no shaking at all)": "very_smooth",
        "Smooth / Minimal Shaking (e.g., Steadicam shot or stabilized handheld shot)": "smooth",
        "Unsteady (e.g., Somewhat shaky handheld shot)": "unsteady",
        "Very Unsteady (e.g., Shaky shot)": "very_unsteady"
    },
    "Is there any camera movement other than shaking?": {
        "Yes with major, complex motion": "major_complex",
        "Yes with major, simple motion": "major_simple",
        "Yes with minor motion": "minor",
        "No": "no"
    },
    "How fast is the camera movement? (e.g., crash zoom, whip pan)?": {
        "Slow": "slow",
        "Regular": "regular",
        "Fast": "fast"
    },
    "Is the camera tail-tracking (following behind the subject)?": {
        "No": "no",
        "Yes": "yes"
    },
    "Is the camera moving forward or backward?": {
        "No": "no",
        "Forward (e.g., Dolly-in / Push-in)": "forward",
        "Backward (e.g., Dolly-out / Pull-out)": "backward",
        "Unclear": "no"
    },
    "Is the camera zooming?": {
        "No": "no",
        "Zooming In": "in",
        "Zooming Out": "out",
        "Unclear": "no"
    },
    "Is the camera moving (trucking) to the left or right?": {
        "No": "no",
        "Left-to-Right (--->)": "left_to_right",
        "Right-to-Left (<---)": "right_to_left",
        "Unclear": "no"
    },
    "Is the camera panning?": {
        "No": "no",
        "Left-to-Right (-->)": "left_to_right",
        "Right-to-Left (<--)": "right_to_left",
        "Unclear": "no"
    },
    "Is the camera moving up or down?": {
        "No": "no",
        "Up (e.g., Pedestal up)": "up",
        "Down (e.g., Pedestal down)": "down",
        "Unclear": "no"
    },
    "Is the camera tilting?": {
        "No": "no",
        "Up": "up",
        "Down": "down",
        "Unclear": "no"
    },
    "Is the camera moving in an arc?": {
        "No": "no",
        "Clockwise (e.g., Arc clockwise)": "clockwise",
        "Counter-clockwise (e.g., Arc counter-clockwise)": "counter_clockwise",
        "Crane Up": "crane_up",
        "Crane Down": "crane_down"
    },
    "Is the camera rolling?": {
        "No": "no",
        "Clockwise": "clockwise",
        "Counter-clockwise": "counter_clockwise",
        "Unclear": "no"
    },
    "Is there a frame-freezing effect in this video?": {
        "No": False,
        "Yes": True
    },
    "Is there a dolly-zoom effect in this video?": {
        "No": False,
        "Yes": True
    },
    "Is there a motion blur effect in this video?": {
        "No": False,
        "Yes": True
    },
    "Is there a cinemagraph effect in this video?": {
        "No": False,
        "Yes": True
    }
}

def mapping_label_pizza_to_caption_motion(label_pizza_path: str = None, caption_path: str = None) -> None:
    with open(label_pizza_path, 'r') as f:
        data = json.load(f)
    res = []
    for item in data:
        new_item = {}
        new_item['video_uid'] = item['video_uid']
        new_item['cam_motion'] = {}
        answers = item['answers']
        new_item['cam_motion']['tracking_shot_types'] = []
        for question, answer in answers.items():
            question_attribute = QUESTION_CHOICE_MAP.get(question, None)
            if question_attribute is None:
                # Process the only checklist questions (tracking type)
                # TBD
                if question in ["Is the camera tail-tracking (following behind the subject)?", "Is the camera side-tracking (moving alongside the subject)?", "Is the camera lead-tracking (moving ahead of the subject)?", "Is the camera arc-tracking (arcing around the subject)?", "Is the camera tilt-tracking (tilting to follow the subject)?", "Is the camera aerial-tracking (from above, e.g., drone or crane)?", "Is the camera pan-tracking (panning to follow the subject)?"]:
                    if question == 'Is the camera tail-tracking (following behind the subject)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('tail')
                    if question == 'Is the camera side-tracking (moving alongside the subject)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('side')
                    if question == 'Is the camera lead-tracking (moving ahead of the subject)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('lead')
                    if question == 'Is the camera arc-tracking (arcing around the subject)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('arc')
                    if question == 'Is the camera tilt-tracking (tilting to follow the subject)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('tilt')
                    if question == 'Is the camera aerial-tracking (from above, e.g., drone or crane)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('aerial')
                    if question == 'Is the camera pan-tracking (panning to follow the subject)?' and answer == 'Yes':
                        new_item['cam_motion']['tracking_shot_types'].append('pan')
                continue
            if question_attribute == "complex_motion_description":
                answer_attribute = answer
            else:
                answer_attribute = OPTION_MAP.get(question, {}).get(answer, None)
            new_item['cam_motion'][question_attribute] = answer_attribute
        res.append(new_item)
    # Create caption folder if not exists
    os.makedirs(os.path.dirname(caption_path), exist_ok=True)
    with open(caption_path, 'w') as f:
        json.dump(res, f, indent=2)
            

if __name__ == "__main__":
    mapping_label_pizza_to_caption_motion(label_pizza_path='./annotations/movement/Camera_Movement_motion_attributes.json', caption_path='./captions/movement/Camera_Movement_motion_attributes.json')
            
            
            
            
        
