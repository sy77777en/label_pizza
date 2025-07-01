import glob
import json
import os

# Only for single questions
QUESTION_CHOICE_MAP = {
    # Shot Transition
    "Are there any shot transitions?": "shot_transition",
    
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
    "Does the size of the subject change during tracking?": "subject_size_change",
    
    # Shot Composition
    "Lens distortion:": "lens_distortion",
    "Text/watermarks present?": "has_overlays",
    "Video speed:": "video_speed",
    "Camera POV:": "camera_pov",
    "Shot type:": "shot_type",
    "Complex shot:": "complex_shot_type",
    "Reason for text description:": "shot_size_description_type",
    "Initial shot size:": "shot_size_start",
    "Ending shot size:": "shot_size_end",
    "Describe complex shot size:": "shot_size_description",
    "Initial relative height:": "subject_height_start",
    "Ending relative height:": "subject_height_end",
    "Describe complex relative height:": "subject_height_description",
    "Initial overall height:": "overall_height_start",
    "Ending overall height:": "overall_height_end",
    "Describe complex overall height:": "overall_height_description",
    "Initial camera angle:": "camera_angle_start",
    "Ending camera angle:": "camera_angle_end",
    "Describe complex angle:": "camera_angle_description",
    "Dutch angle (>15) present?": "dutch_angle",
    "Focus type:": "camera_focus",
    "Focus depth (start):": "focus_plane_start",
    "Focus depth (end):": "focus_plane_end",
    "Reason for focus change:": "focus_change_reason",
    "Describe complex focus:": "camera_focus_description",

    "Color tones?": "color_temperature",
    "Colorfulness?": "color_saturation",
    "Brightness and Exposure?": "brightness",
    "Describe the color grading": "color_grading_description",
    "Indoors or outdoors?": "scene_type",
    "Sunlight level?": "sunlight_level",
    "Light quality across the entire scene?": "light_quality",
    "Describe the scene and lighting setup": "lighting_setup_description",
    "Dominant light direction on subject(s) is?": "subject_light_direction",
    "Describe the subject lighting": "subject_lighting_description",
    "Subject lighting apply?": "subject_condition",
    "Light contrast?": "subject_contrast_ratio",
    "Regular lens flares?": "lens_flares_regular",
    "Anamorphic lens flares?": "lens_flares_anamorphic",
    "Mist diffusion?": "mist_diffusion",
    "Bokeh?": "bokeh",
    "Water reflection?": "reflection_from_water",
    "Glossy surface reflection?": "reflection_from_glossy_surface",
    "Mirror reflection?": "reflection_from_mirror",
    "Aerial / atmospheric Perspective?": "aerial_perspective",
    "Rainbow?": "rainbow",
    "Aurora?": "aurora",
    "Heat waves or heat haze?": "heat_haze",
    "Lightning?": "lightning",
    "Wave light or water caustics?": "water_caustics",
    "Volumetric beam light?": "volumetric_beam_light",
    "Volumetric spot light?": "volumetric_spot_light",
    "God rays?": "god_rays",
    "Volumetric light through medium?": "light_through_medium",
    "Volumetric light (others)?": "volumetric_light_others",
    "Venetian blinds?": "venetian_blinds",
    "Subject shape?": "subject_shape",
    "Window frame?": "window_frames",
    "Foliage?": "foliage",
    "Gobo lighting (others)?": "shadow_patterns_gobo_others",
    "Color shifting (smooth)?": "color_shifting_smooth",
    "Color shifting (sudden)?": "color_shifting_sudden",
    "Flashing?": "flashing",
    "Moving light?": "moving_light",
    "Pulsing or flickering?": "pulsing_flickering",
    "Professional or portrait lighting?": "professional_lighting",
    "Colored or neon lighting?": "colored_neon_lighting",
    "Headlight or flashlight?": "headlight_flashlight",
    "Vignette?": "vignette",
    "City light?": "city_light",
    "Street light?": "street_light",
    "Describe special lighting effects": "special_lighting_description",
    "Describe volumetric lighting effects": "volumetric_lighting_description",
    "Describe shadows and gobes": "shadow_pattern_description",
    "Describe dynamic effects": "lighting_dynamics_description",
    "Revealing shot?": "revealing_shot",
    "Transformation or morphing?": "transformation_morphing",
    "Levitation or floating?": "levitation_floating",
    "Explosion?": "explosion",
    "Shattering or breaking?": "shattering_breaking",
    "Diffusion?": "diffusion",
    "Splashing or waves?": "splashing_waves",
    "Sunlight?": "sunlight_source",
    "Moonlight / starlight? (moon/star visible)": "moonlight_starlight_source",
    "Firelight?": "firelight_source",
    "Artificial / Practical light?": "artificial_light_source",
    "Non-visible light source?": "non_visible_light_source",
    "Abstract light source / N/A?": "abstract_light_source",
    "Light source complex (others)?": "complex_light_source",
    "Back light?": "subject_back_light",
    "Front light?": "subject_front_light",
    "Top light?": "subject_top_light",
    "Bottom light?": "subject_bottom_light",
    "Right-side light?": "subject_right_side_light",
    "Left-side light?": "subject_left_side_light",
    "Ambient / no dominant side?": "subject_ambient_light",
    "Rembrandt lighting?": "rembrandt_lighting",
    "Subject lit as a silhouette?": "silhouette",
    "Rim lighting on the subject?": "rim_light"
}

OPTION_MAP = {
    # Camera Movement
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
    },
    
    # Shot Composition
    "Initial camera angle:": {
        "N/A": "unknown",
        "Bird's eye": "bird_eye_angle",
        "High angle (Looking down)": "high_angle",
        "Level Angle": "level_angle",
        "Low angle (Looking up)": "low_angle",
        "Worm's eye": "worm_eye_angle"
    },

    "Ending camera angle:": {
        "N/A (e.g., no change)": "unknown",
        "Bird's eye": "bird_eye_angle",
        "High angle (Looking down)": "high_angle",
        "Level Angle": "level_angle",
        "Low angle (Looking up)": "low_angle",
        "Worm's eye": "worm_eye_angle"
    },

    "Dutch angle (>15) present?": {
        "No": "no",
        "Varying": "varying",
        "Yes": "yes"
    },

    "Focus type:": {
        "Deep focus": "deep_focus",
        "Shallow focus": "shallow_focus",
        "Ultra shallow focus": "ultra_shallow_focus",
        "N/A": "unknown"
    },

    "Focus depth (start):": {
        "Foreground": "foreground",
        "Middleground": "middle_ground",
        "Background": "background",
        "Out of focus": "out_of_focus",
        "N/A": "unknown"
    },

    "Focus depth (end):": {
        "Foreground": "foreground",
        "Middleground": "middle_ground",
        "Background": "background",
        "Out of focus": "out_of_focus",
        "N/A": "unknown"
    },

    "Reason for focus change:": {
        "No Change": "no_change",
        "Camera or Subject Movement": "camera_subject_movement",
        "Rack Focus": "rack_focus",
        "Pull Focus": "pull_focus",
        "Focus Tracking": "focus_tracking",
        "Others (please specify)": "others"
    },

    "Initial relative height:": {
        "N/A (no subject)": "unknown",
        "Above the subject": "above_subject",
        "At the subject": "at_subject",
        "Below the subject": "below_subject"
    },

    "Ending relative height:": {
        "N/A (no subject / no change)": "unknown",
        "Above the subject": "above_subject",
        "At the subject": "at_subject",
        "Below the subject": "below_subject"
    },

    "Initial overall height:": {
        "N/A": "unknown",
        "Aerial-level": "aerial_level",
        "Overhead-level": "overhead_level",
        "Eye-level": "eye_level",
        "Hip-level": "hip_level",
        "Ground-level": "ground_level",
        "Water-level": "water_level",
        "Underwater": "underwater_level"
    },

    "Ending overall height:": {
        "N/A (e.g., no changes)": "unknown",
        "Aerial-level": "aerial_level",
        "Overhead-level": "overhead_level",
        "Eye-level": "eye_level",
        "Hip-level": "hip_level",
        "Ground-level": "ground_level",
        "Water-level": "water_level",
        "Underwater": "underwater_level"
    },

    "Camera POV:": {
        "N/A": "unknown",
        "First-person POV": "first_person",
        "Drone POV": "drone_pov",
        "Third-person Full-body POV (Gaming only)": "third_person_full_body",
        "Third-person Over-the-shoulder POV (Gaming / Film)": "third_person_over_shoulder",
        "Third-person Over-the-hip POV (Gaming only)": "third_person_over_hip",
        "Third-person Side view POV (Gaming only)": "third_person_side_view",
        "Third-person Isometric POV (Gaming only)": "third_person_isometric",
        "Third-person Top-down/Oblique POV (Gaming only)": "third_person_top_down",
        "Broadcast POV (Television station)": "broadcast_pov",
        "Overhead POV (Hands-on demonstration)": "overhead_pov",
        "Selfie POV": "selfie_pov",
        "Screen Recording (software tutorials, zoom calls)": "screen_recording",
        "Dashcam POV": "dashcam_pov",
        "Locked-on POV": "locked_on_pov"
    },

    "Shot type:": {
        "Clear human subject(s)": "human",
        "Clear non-human subject(s)": "non_human",
        "Change of subject(s)": "change_of_subject",
        "Scenery shot": "scenery",
        "Complex shot": "complex"
    },

    "Complex shot:": {
        "Clear Subject with Dynamic Size (subject)": "clear_subject_dynamic_size",
        "Different Subjects in Focus (subject)": "different_subject_in_focus",
        "Clear yet Atypical Subject (subject)": "clear_subject_atypical",
        "Many Subject(s) with One Clear Focus (subject)": "many_subject_one_focus",
        "Many Subject(s) with No Clear Focus (scenery)": "many_subject_no_focus",
        "Description (text)": "description",
        "N/A (e.g., abstract/FPS with body parts/screenshot/etc.)": "unknown"
    },

    "Reason for text description:": {
        "Subject-Scene Size Mismatch": "subject_scene_mismatch",
        "Back-and-Forth Size Changes": "back_and_forth_change",
        "Others": "others"
    },

    "Initial shot size:": {
        "N/A (no subject)": "unknown",
        "Extreme wide/long shot": "extreme_wide",
        "Wide/long shot": "wide",
        "Full shot (Subject Shot Only)": "full",
        "Medium-Full shot (Human Subject Shot Only)": "medium_full",
        "Medium shot (Subject Shot Only)": "medium",
        "Medium Close-up shot (Human Subject Shot Only)": "medium_close_up",
        "Close-up shot": "close_up",
        "Extreme Close-up shot": "extreme_close_up"
    },

    "Ending shot size:": {
        "N/A (no subject / no change)": "unknown",
        "Extreme wide/long shot": "extreme_wide",
        "Wide/long shot": "wide",
        "Full shot (Subject Shot Only)": "full",
        "Medium-Full shot (Human Subject Shot Only)": "medium_full",
        "Medium shot (Subject Shot Only)": "medium",
        "Medium Close-up shot (Human Subject Shot Only)": "medium_close_up",
        "Close-up shot": "close_up",
        "Extreme Close-up shot": "extreme_close_up"
    },

    "Lens distortion:": {
        "Regular Lens": "regular",
        "Barrel Distortion (e.g., Wide-angle lens)": "barrel",
        "Fisheye Distortion (e.g., Fisheye lens)": "fisheye"
    },

    "Text/watermarks present?": {
        "No": False,
        "Yes": True
    },

    "Video speed:": {
        "Time-lapse": "time_lapse",
        "Fast-Motion": "fast_motion",
        "Regular": "regular",
        "Slow-Motion": "slow_motion",
        "Stop-Motion": "stop_motion",
        "Speed-Ramp": "speed_ramp",
        "Time-Reversed": "time_reversed"
    },
    
    
}

def mapping_label_pizza_to_caption_motion(label_pizza_path: str = None, caption_path: str = None) -> None:
    with open(label_pizza_path, 'r') as f:
        data = json.load(f)
    res = []
    for item in data:
        new_item = {}
        new_item['video_uid'] = item['video_uid']
        new_item['shotcomp'] = {}
        answers = item['answers']
        # new_item['cam_motion']['tracking_shot_types'] = []
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
            if question_attribute in ["complex_motion_description", "shot_size_description", "subject_height_description", "overall_height_description", "camera_angle_description", "camera_focus_description"]:
                answer_attribute = answer
            else:
                answer_attribute = OPTION_MAP.get(question, {}).get(answer, None)
            # if new_item['video_uid'] == 'v75NeIqdD74.0.0.mp4':
            #     print(question, answer_attribute)
            #     print('-'*100)
            new_item['shotcomp'][question_attribute] = answer_attribute
        res.append(new_item)
    # Create caption folder if not exists
    os.makedirs(os.path.dirname(caption_path), exist_ok=True)
    with open(caption_path, 'w') as f:
        json.dump(res, f, indent=2)
            

if __name__ == "__main__":
    import glob
    paths = glob.glob('./CameraShotcomp/annotations/Shotcomp1_Trusted_*.json')
    for path in paths:
        target_path = path.replace('annotations', 'captions')
        mapping_label_pizza_to_caption_motion(label_pizza_path=path, caption_path=target_path)
    
    # label_pizza_path = './CameraShotcomp/annotations/Shotcomp1_Trusted_Camera_Angle.json'
    # caption_path = './CameraShotcomp/captions/Shotcomp1_Trusted_Camera_Angle.json'
    # mapping_label_pizza_to_caption_motion(label_pizza_path=label_pizza_path, caption_path=caption_path)
            
            
            
            
        
