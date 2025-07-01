import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from label_pizza.db import init_database
init_database()
import json
import pdb
from pathlib import Path
from typing import List, Dict, Optional, Any
from label_pizza.services import QuestionGroupService, QuestionService
from label_pizza.db import SessionLocal

from scripts.upload_utils import *

PROJECT_KEY = "projects"        # adjust if your NDJSON schema differs

# ──────────────────────────────
# 1.  Checklist → yes / no map
#     raw-checklist-name  →  { raw_option : yes/no question }
# ──────────────────────────────
CHECKLIST_EXPANSION: dict[str, dict[str, str]] = {
    "what_is_the_major_light_source": {
        "sunlight":  "Sunlight?",
        "moonlight_starlight": "Moonlight / starlight? (moon/star visible)",
        "firelight": "Firelight?",
        "artificial_lighting_practical_visible": "Artificial / Practical light?",
        "non_visible_light_sources": "Non-visible light source?",
        "n_a_abstract": "Abstract light source / N/A?",
        "complex_others": "Light source complex (others)?",
    },
    "select_light_directions": {
        "back_light": "Back light?",
        "front_light": "Front light?",
        "top_light": "Top light?",
        "bottom_light": "Bottom light?",
        "right_side_light": "Right-side light?",
        "left_side_light": "Left-side light?",
        "ambient_light": "Ambient / no dominant side?",
    },
    "special_lighting_on_subject_s_is": {
        "rembrandt_lighting": "Rembrandt lighting?",
        "silhouette_lighting_subject_not_always_required": "Subject lit as a silhouette?",
        "rim_light_subject_not_always_required": "Rim lighting on the subject?",
    },
    "select_the_type_of_tracking_shot": {
        "side_moving_alongside_the_subject": "Is the camera side-tracking (moving alongside the subject)?",
        "tail_following_behind_the_subject": "Is the camera tail-tracking (following behind the subject)?",
        "lead_moving_ahead_of_the_subject": "Is the camera lead-tracking (moving ahead of the subject)?",
        "aerial_tracking_from_above_usually_with_a_drone_or_crane": "Is the camera aerial-tracking (from above, e.g., drone or crane)?",
        "arc_moving_around_the_subject": "Is the camera arc-tracking (arcing around the subject)?",
        "pan_panning_to_follow_the_subject": "Is the camera pan-tracking (panning to follow the subject)?",
        "tilt_tilting_to_follow_the_subject": "Is the camera tilt-tracking (tilting to follow the subject)?"
    },
    "are_there_any_of_the_following_camera_motion_effects": {
        "frame_freezing": "Is there a frame-freezing effect in this video?",
        "dolly_zoom": "Is there a dolly-zoom effect in this video?",
        "motion_blur": "Is there a motion blur effect in this video?",
        "cinemagraph": "Is there a cinemagraph effect in this video?"
    }
    # add more checklist questions here if needed
}

def _expand_checklist(q_name: str, selected: List[str]) -> Dict[str, str]:
    """Convert one checklist answer into several yes/no single-choice answers."""
    mapping = CHECKLIST_EXPANSION.get(q_name, {})
    return {
        target_q: "Yes" if option in selected else "No"
        for option, target_q in mapping.items()
    }

# ──────────────────────────────
# 2.  Option-name mapping
#     Fill this dict as you need
# ──────────────────────────────
OPTION_MAPPING: dict[str, dict[str, str]] = {
    
    # Shot Composition
    "Initial camera angle:": {
        "n/a": "N/A",
        "birds_eye": "Bird's eye",
        "high_angle": "High angle (Looking down)",
        "level_angle": "Level Angle",
        "low_angle": "Low angle (Looking up)",
        "worms_eye": "Worm's eye"
    },

    "Ending camera angle:": {
        "n/a_no_change": "N/A (e.g., no change)",
        "birds_eye": "Bird's eye",
        "high_angle": "High angle (Looking down)",
        "level_angle": "Level Angle",
        "low_angle": "Low angle (Looking up)",
        "worms_eye": "Worm's eye"
    },

    "Dutch angle (>15) present?": {
        "no": "No",
        "varying": "Varying",
        "yes": "Yes"
    },

    "Focus type:": {
        "deep_focus": "Deep focus",
        "shallow_focus": "Shallow focus",
        "ultra_shallow_focus": "Ultra shallow focus",
        "n_a": "N/A"
    },

    "Focus depth (start):": {
        "foreground": "Foreground",
        "middleground": "Middleground",
        "background": "Background",
        "out_of_focus": "Out of focus",
        "n_a": "N/A"
    },

    "Focus depth (end):": {
        "foreground": "Foreground",
        "middleground": "Middleground",
        "background": "Background",
        "out_of_focus": "Out of focus",
        "n_a": "N/A"
    },

    "Reason for focus change:": {
        "no_change": "No Change",
        "camera_or_subject_movement": "Camera or Subject Movement",
        "rack_focus": "Rack Focus",
        "pull_focus": "Pull Focus",
        "focus_tracking": "Focus Tracking",
        "others_please_specify": "Others (please specify)"
    },

    "Initial relative height:": {
        "n/a_no_subject": "N/A (no subject)",
        "above_subject": "Above the subject",
        "at_subject": "At the subject",
        "below_subject": "Below the subject"
    },

    "Ending relative height:": {
        "n/a_no_change_subject_changes": "N/A (no subject / no change)",
        "above_subject": "Above the subject",
        "at_subject": "At the subject",
        "below_subject": "Below the subject"
    },

    "Initial overall height:": {
        "n_a": "N/A",
        "aerial_level": "Aerial-level",
        "overhead_level": "Overhead-level",
        "eye_level": "Eye-level",
        "body_level": "Hip-level",
        "ground_level": "Ground-level",
        "water_level": "Water-level",
        "underwater": "Underwater"
    },

    "Ending overall height:": {
        "n_a_e_g_no_changes": "N/A (e.g., no changes)",
        "aerial_level": "Aerial-level",
        "overhead_level": "Overhead-level",
        "eye_level": "Eye-level",
        "body_level": "Hip-level",
        "ground_level": "Ground-level",
        "water_level": "Water-level",
        "underwater": "Underwater"
    },

    "Camera POV:": {
        "n/a": "N/A",
        "first_person_pov": "First-person POV",
        "drone_pov": "Drone POV",
        "third_person_full_body_pov_gaming_only": "Third-person Full-body POV (Gaming only)",
        "third_person_over_the_shoulder_pov_gaming_film": "Third-person Over-the-shoulder POV (Gaming / Film)",
        "third_person_over_the_hip_pov_gaming_only": "Third-person Over-the-hip POV (Gaming only)",
        "third_person_side_view_pov_gaming_only": "Third-person Side view POV (Gaming only)",
        "third_person_isometric_pov_gaming_only": "Third-person Isometric POV (Gaming only)",
        "third_person_top_down_pov_gaming_only": "Third-person Top-down/Oblique POV (Gaming only)",
        "broadcast_pov_television_station": "Broadcast POV (Television station)",
        "overhead_pov_hands_on_demonstration": "Overhead POV (Hands-on demonstration)",
        "selfie_pov": "Selfie POV",
        "screen_recording_software_tutorials_zoom_calls": "Screen Recording (software tutorials, zoom calls)",
        "dashcam_pov": "Dashcam POV",
        "locked_on_pov": "Locked-on POV"
    },

    "Shot type:": {
        "clear_human_subject_s": "Clear human subject(s)",
        "clear_non_human_subject_s": "Clear non-human subject(s)",
        "change_of_subject_s": "Change of subject(s)",
        "scenery_shot": "Scenery shot",
        "complex_shot": "Complex shot"
    },

    "Complex shot:": {
        "clear_subject_with_dynamic_size_subject": "Clear Subject with Dynamic Size (subject)",
        "different_subjects_in_focus_subject": "Different Subjects in Focus (subject)",
        "clear_yet_atypical_subject_subject": "Clear yet Atypical Subject (subject)",
        "many_subject_s_with_one_clear_focus_subject": "Many Subject(s) with One Clear Focus (subject)",
        "many_subject_s_with_no_clear_focus_scenery": "Many Subject(s) with No Clear Focus (scenery)",
        "description_text": "Description (text)",
        "n_a_e_g_abstract_fps_with_body_parts_screenshot_etc": "N/A (e.g., abstract/FPS with body parts/screenshot/etc.)"
    },

    "Reason for text description:": {
        "partial_subjects_in_wide_shots": "Subject-Scene Size Mismatch",
        "back_and_forth_size_changes": "Back-and-Forth Size Changes",
        "others": "Others"
    },

    "Initial shot size:": {
        "n_a_e_g_no_subject_too_many_subjects": "N/A (no subject)",
        "extreme_wide_shot": "Extreme wide/long shot",
        "wide_shot": "Wide/long shot",
        "full_shot": "Full shot (Subject Shot Only)",
        "medium_full_shot_human_only": "Medium-Full shot (Human Subject Shot Only)",
        "medium_shot": "Medium shot (Subject Shot Only)",
        "medium_close_up_shot_human_only": "Medium Close-up shot (Human Subject Shot Only)",
        "close_up_shot": "Close-up shot",
        "extreme_close_up_shot": "Extreme Close-up shot"
    },

    "Ending shot size:": {
        "n_a_e_g_no_subject_too_many_subjects": "N/A (no subject / no change)",
        "extreme_wide_shot": "Extreme wide/long shot",
        "wide_shot": "Wide/long shot",
        "full_shot": "Full shot (Subject Shot Only)",
        "medium_full_shot_human_only": "Medium-Full shot (Human Subject Shot Only)",
        "medium_shot": "Medium shot (Subject Shot Only)",
        "medium_close_up_shot_human_only": "Medium Close-up shot (Human Subject Shot Only)",
        "close_up_shot": "Close-up shot",
        "extreme_close_up_shot": "Extreme Close-up shot"
    },

    "Lens distortion:": {
        "regular_lens": "Regular Lens",
        "barrel_distortion_e_g_wide_angle_lens": "Barrel Distortion (e.g., Wide-angle lens)",
        "fisheye_distortion_e_g_fisheye_lens": "Fisheye Distortion (e.g., Fisheye lens)"
    },

    "Text/watermarks present?": {
        "no": "No",
        "yes": "Yes"
    },

    "Video speed:": {
        "time_lapse": "Time-lapse",
        "fast_motion": "Fast-Motion",
        "regular": "Regular",
        "slow_motion": "Slow-Motion",
        "stop_motion": "Stop-Motion",
        "speed_ramp": "Speed-Ramp",
        "reversed": "Time-Reversed"
    },
    
    "What is the camera steadiness?": {
        "static_fixed_camera": "Static (Fixed Camera)",
        "very_smooth_drone_shot": "Very Smooth / No Shaking (e.g., Drone shot with no shaking at all)",
        "smooth_steadicam_stabilized_handheld_shot": "Smooth / Minimal Shaking (e.g., Steadicam shot or stabilized handheld shot)",
        "unsteady_somewhat_shaky_handheld_shot": "Unsteady (e.g., Somewhat shaky handheld shot)",
        "very_unsteady_extreme_shaky_found_footage_style": "Very Unsteady (e.g., Shaky shot)"
    },
    "Is there any camera movement other than shaking?": {
        "yes_major_complex_motion": "Yes with major, complex motion",
        "yes_major_simple_motion": "Yes with major, simple motion",
        "yes_minor_motion": "Yes with minor motion",
        "no": "No"
    },
    "How fast is the camera movement? (e.g., crash zoom, whip pan)?": {
        "slow": "Slow",
        "regular": "Regular",
        "fast": "Fast"
    },
    "Is the camera tail-tracking (following behind the subject)?": {
        "no": "No",
        "yes": "Yes"
    },
    "Is the camera moving forward or backward?": {
        "no": "No",
        "forward_e_g_dolly_in_push_in": "Forward (e.g., Dolly-in / Push-in)",
        "backward_e_g_dolly_out_pull_out": "Backward (e.g., Dolly-out / Pull-out)"
    },
    "Is the camera zooming?": {
        "no": "No",
        "zooming_in": "Zooming In",
        "zooming_out": "Zooming Out"
    },
    "Is the camera moving (trucking) to the left or right?": {
        "no": "No",
        "left_to_right_panning": "Left-to-Right (--->)",
        "right_to_left_panning": "Right-to-Left (<---)"
    },
    "Is the camera panning?": {
        "no": "No",
        "left_to_right_panning": "Left-to-Right (-->)",
        "right_to_left_panning": "Right-to-Left (<--)"
    },
    "Is the camera moving up or down?": {
        "no": "No",
        "up_pedestal_up": "Up (e.g., Pedestal up)",
        "down_pedestal_down": "Down (e.g., Pedestal down)"
    },
    "Is the camera tilting?": {
        "no": "No",
        "up": "Up",
        "down": "Down"
    },
    "Is the camera moving in an arc?": {
        "no": "No",
        "clockwise_arc_clockwise": "Clockwise (e.g., Arc clockwise)",
        "counter_clockwise_arc_counter_clockwise": "Counter-clockwise (e.g., Arc counter-clockwise)",
        "crane_up": "Crane Up",
        "crane_down": "Crane Down"
    },
    "Is the camera rolling?": {
        "no": "No",
        "clockwise": "Clockwise",
        "counter_clockwise": "Counter-clockwise"
    },
    "Color tones?": {
        "n_a_black_white": "N/A (black-white)",
        "warm": "Changing and Contrasting",
        "cool": "Changing",
        "changing": "Contrast",
        "contrasting": "Warm",
        "changing_and_contrasting": "Cool",
        "neither_warm_nor_cool": "Neither Warm nor Cool"
    },
    "Colorfulness?": {
        "n_a_black_white": "N/A (black-white)",
        "changing_and_contrast": "Changing + Contrast",
        "changing": "Changing",
        "contrast": "Contrast",
        "low_colorfulness": "Low colorfulness",
        "high_colorfulness": "High colorfulness",
        "neither_low_nor_high_colorfulness": "Neither low nor high colorfulness"
    },
    "Brightness and Exposure?": {
        "changing_and_contrasts": "Changing + Contrast",
        "changing": "Changing",
        "contrasting": "Contrast",
        "overexposed_very_bright": "Overexposed / Very Bright",
        "underexposed_very_dark": "Neither too bright nor too dark",
        "neither_too_bright_nor_too_dark": "Underexposed / Very Dark"
    },
    "Indoors or outdoors?": {
        "interior": "Interior",
        "exterior": "Exterior",
        "synthetic_unrealistic": "Synthetic / Unrealistic",
        "complex_others": "Complex (others)"
    },
    "Light quality across the entire scene?": {
        "soft_light": "Unclear",
        "hard_light": "Changing (temporal)",
        "changing_temporal": "Hard Light",
        "unclear": "Soft Light"
    },
    "Sunlight level?": {
        "normal_sunlight": "Normal Sunlight",
        "hard_sunlight_e_g_sunny": "Hard Sunlight (e.g., Sunny)",
        "soft_sunlight_e_g_overcast_dusk_dawn": "Soft Sunlight (e.g., Overcast / Dusk / Dawn)",
        "sunset_sunrise": "Sunset / Sunrise",
        "n_a_e_g_indoors_or_changing_sunlight_conditions": "N/A (indoors or changing sunlight conditions)"
    },
    "Does subject lighting apply?": {
        "unclear_or_light_emitting_subject": "Unclear or light-emitting subject",
        "non_physically_realistic_lighting_on_subjects": "Unclear lighting",
        "inconsistent_subject": "Inconsistent subject",
        "consistent_subject": "Consistent subject"
    },
    "Light contrast?": {
        "unclear": "Unclear",
        "changing_temporal": "Changing (temporal)",
        "mixed_spatial": "Mixed (spatial)",
        "changing_mixed": "Changing + Mixed",
        "high_contrast": "High contrast",
        "normal_contrast_below_1_8_and_above_1_4": "Normal contrast",
        "no_contrast_flat_lighting_below_1_2": "Low contrast"
    },
    "Dominant Light Direction on Subject(s) is?": {
        "unclear": "Unclear",
        "changing_temporal": "Changing (temporal)",
        "mixed_spatial": "Mixed (spatial)",
        "changing_mixed": "Changing + Mixed",
        "consistent": "Consistent"
    },
    "Regular lens flares?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Anamorphic lens flares?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Mist diffusion?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Bokeh?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Water reflection?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Glossy surface reflection?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Mirror reflection?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Aerial / atmospheric Perspective?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Rainbow?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Aurora?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Heat waves or heat haze?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Lightning?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Volumetric beam light?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Volumetric spot light?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "God rays?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Volumetric light through medium?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Volumetric light (others)?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Venetian blinds?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Subject shape?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Window frame?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Foliage?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Gobo lighting (others)?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Color shifting (smooth)?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Color shifting (sudden)?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Flashing?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Moving light?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Pulsing or flickering?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Vignette?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Wave light or water caustics?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "City light?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Street light?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Revealing shot?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Transformation or morphing?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Levitation or floating?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Explosion?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Shattering or breaking?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Diffusion?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Splashing or waves?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Does the size of the subject change during tracking?": {
        "no": "No",
        "subject_gets_larger": "Subject gets larger",
        "subject_gets_smaller": "Subject gets smaller"
    },
    "Professional or portrait lighting?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Colored or neon lighting?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Headlight or flashlight?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    }
}

# key: value --> labelbox question text: labelpizza question text
NAME_MAPPING: dict[str, str] = {
    
    # Shot Composition
    "select_if_there_is_obvious_lens_distortion_otherwise_leave_as_regular": "Lens distortion:",
    "are_there_artificially_added_texts_or_watermarks_in_this_video": "Text/watermarks present?",
    "video_speed": "Video speed:",
    "select_the_camera_point_of_view": "Camera POV:",
    "what_is_the_shot_type": "Shot type:",
    "if_the_shot_is_a_predefined_complex_shot_type_label_the_shot": "Complex shot:",
    "if_the_shot_is_a_text_description_label_the_shot": "Reason for text description:",
    "select_the_shot_size_if_the_shot_size_changes_during_the_video_select_the_shot_size_at_the_beginning_frame": "Initial shot size:",
    "if_the_shot_size_changes_during_the_video_select_the_shot_size_at_the_ending_frame_otherwise_leave_as_n_a": "Ending shot size:",
    "shot_size_text_box": "Describe complex shot size:",
    "select_camera_height_relative_to_subjects_at_start": "Initial relative height:",
    "select_camera_height_relative_to_subjects_at_end": "Ending relative height:",
    "relative_camera_height_text_box": "Describe complex relative height:",
    "select_the_overall_camera_height": "Initial overall height:",
    "1_if_the_overall_camera_height_changes_during_the_video_select_the_height_at_the_ending_frame": "Ending overall height:",
    "overall_camera_height_text_box": "Describe complex overall height:",
    "select_camera_angle_at_start": "Initial camera angle:",
    "select_camera_angle_at_end": "Ending camera angle:",
    "camera_angle_text_box": "Describe complex angle:",
    "is_there_obvious_dutch_angle": "Dutch angle (>15) present?",
    "select_the_type_of_camera_focus": "Focus type:",
    "select_the_depth_of_camera_focus_start_of_video": "Focus depth (start):",
    "select_the_depth_of_camera_focus_end_of_video": "Focus depth (end):",
    "If the focus plane changes, is it due to:": "Reason for focus change:",
    "camera_focus_text_box": "Describe complex focus:",
    
    # Camera motion – steadiness & global movement
    "select_camera_steadiness":                "What is the camera steadiness?",
    "is_there_camera_movement_other_than_shaking":                    "Is there any camera movement other than shaking?",
    "camera_motion_speed":               "How fast is the camera movement? (e.g., crash zoom, whip pan)?",

    # Subject tracking & size
    "is_the_camera_tracking_following_subject":                      "Is the camera tail-tracking (following behind the subject)?",
    # (tracking-shot checklist handled separately → yes / no expansion)

    # Forward / backward etc.
    "is_the_camera_moving_forward_or_backward":    "Is the camera moving forward or backward?",
    "is_the_camera_zooming":                       "Is the camera zooming?",
    "is_the_camera_moving_trucking_to_the_left_or_right":        "Is the camera moving (trucking) to the left or right?",
    "is_the_camera_panning":                       "Is the camera panning?",
    "is_the_camera_moving_up_or_down":             "Is the camera moving up or down?",
    "is_the_camera_tilting":                       "Is the camera tilting?",
    "is_the_camera_moving_in_an_arc":              "Is the camera moving in an arc?",
    "is_the_camera_rolling":                       "Is the camera rolling?",

    # Complex-motion free-text
    "camera_motion_text_box":                      "If the camera motion is too complex, how would you describe it?",
    "does_the_size_of_the_subject_change":         "Does the size of the subject change during tracking?",
    
    # Color / Grading
    "the_color_tones_in_this_video_are":           "Color tones?",
    "how_colorful_is_this_video":                  "Colorfulness?",
    "brightness_and_exposure":                     "Brightness and Exposure?",
    "describe_color_compositions_and_dynamics":    "Describe the color grading",

    # Scene & Lighting setup
    "is_the_scene_indoors_or_outdoors":            "Indoors or outdoors?",
    "select_the_sunlight_level":                   "Sunlight level?",
    "what_is_the_light_quality_across_the_entire_scene": "Light quality across the entire scene?",
    "scene_and_lighting_setup_description":        "Describe the scene and lighting setup",
    
    # Subject lighting
    "dominant_light_direction_on_subject_s_is":    "Dominant light direction on subject(s) is?",
    "subject_lighting_description":                "Describe the subject lighting",
    "does_subject_lighting_apply":                 "Subject lighting apply?",
    "light_contrast_on_subject_s_is":              "Light contrast?",
    
    # Light-effects checklist
    "are_there_any_regular_lens_flares":           "Regular lens flares?",
    "are_there_any_anamorphic_lens_flares":        "Anamorphic lens flares?",
    "are_there_any_mist_diffusion_effect":         "Mist diffusion?",
    "are_there_any_bokeh_effect":                  "Bokeh?",
    "are_there_any_water_reflection_effect":       "Water reflection?",
    "are_there_any_glassy_surface_reflection_effect": "Glossy surface reflection?",
    "are_there_any_mirror_reflection_effect":      "Mirror reflection?",
    "are_there_any_aeiral_perspective_effect":     "Aerial / atmospheric Perspective?",
    "are_there_any_rainbow_effect":                "Rainbow?",
    "are_there_any_aurora_effect":                 "Aurora?",
    "are_there_any_heat_waves_or_heat_haze_effect": "Heat waves or heat haze?",
    "are_there_any_lightning_effect":              "Lightning?",
    "are_there_any_wave_light_or_water_caustics_effect": "Wave light or water caustics?",
    "are_there_any_volumetric_beam_light":         "Volumetric beam light?",
    "are_there_any_volumetric_spot_light":         "Volumetric spot light?",
    "are_ther_any_god_rays":                       "God rays?",
    "are_there_any_volumetric_light_through_medium": "Volumetric light through medium?",
    "are_there_any_volumetric_light_others":       "Volumetric light (others)?",
    "are_there_any_venetian_blinds_lighting":      "Venetian blinds?",
    "are_there_any_subject_shape_lighting":        "Subject shape?",
    "are_there_any_window_frame_lighting":         "Window frame?",
    "are_there_any_foliage_lighting":              "Foliage?",
    "are_there_any_gobo_lighting_others":          "Gobo lighting (others)?",
    "are_there_any_color_shifting_smooth_effect":  "Color shifting (smooth)?",
    "are_there_any_color_shifting_sudden_effect":  "Color shifting (sudden)?",
    "are_there_any_flashing_effect":               "Flashing?",
    "are_there_any_moving_light_effect":           "Moving light?",
    "are_there_any_pulsing_or_flickering_effect":  "Pulsing or flickering?",
    "are_there_any_professional_or_portrait_lighting_effect": "Professional or portrait lighting?",
    "are_there_any_colored_or_neon_lighting_effect": "Colored or neon lighting?",
    "are_there_any_headlight_or_flashlight_effect": "Headlight or flashlight?",
    "are_there_any_vignette_effect":               "Vignette?",
    "are_there_any_city_light_effect":             "City light?",
    "are_there_any_street_light_effect":           "Street light?",

    # Descriptive free-text for special lighting
    "special_lighting_effects_description":        "Describe special lighting effects",
    "volumetric_lighting_description":             "Describe volumetric lighting effects",
    "shadow_patterns_gobo_lighting_description":   "Describe shadows and gobes",
    "dynamic_lighting_effects_description":        "Describe dynamic effects",

    # Dynamic / motion-graphic effects
    "are_there_any_revealing_shot_dynamic":        "Revealing shot?",
    "are_there_any_transformation_or_morphing_dynamic": "Transformation or morphing?",
    "are_there_any_levitation_or_floating_dynamic": "Levitation or floating?",
    "are_there_any_explosion_dynamic":             "Explosion?",
    "are_there_any_shattering_or_breaking_effect": "Shattering or breaking?",
    "are_there_any_diffusion_dynamic":             "Diffusion?",
    "are_there_any_splashing_or_waves_dynamic":    "Splashing or waves?"
}

def _rename_key(raw_key: str) -> str:
    """Return canonical question key; fall back to original if unknown."""
    return NAME_MAPPING.get(raw_key, raw_key)

# ──────────────────────────────
# 3.  Build the answer dict for **one** label
#     steps:
#       1. raw extraction
#       2. checklist expansion
#       3. fill missing questions
#       4. option-name remapping
# ──────────────────────────────
def _answers_from_classifications(
    classifications: List[dict],
    q_meta: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    answers: Dict[str, Any] = {}

    # step 1 + 2 : extract → (optionally) expand
    for cls in classifications:
        raw_key = cls.get("value")
        key = _rename_key(raw_key)                    # ← NEW
        if "checklist_answers" in cls:
            selected = [item["value"] for item in cls["checklist_answers"]]
            answers.update(_expand_checklist(raw_key, selected))

        elif "radio_answer" in cls:
            if raw_key == "is_the_camera_tracking_following_subject" and cls["radio_answer"]["value"] == "yes":
                tracking_questions = cls['radio_answer']['classifications']
                sub_questions = [item['value'] for item in tracking_questions]
                if "does_the_size_of_the_subject_change" in sub_questions:
                    change_size_question = next(
                        (cls for cls in tracking_questions
                        if cls.get("value") == "does_the_size_of_the_subject_change"),
                        None             # ← default returned when nothing matches
                    )
                    tmp_key = _rename_key("does_the_size_of_the_subject_change")
                    answers[tmp_key] = change_size_question["radio_answer"]["value"]
                if "select_the_type_of_tracking_shot" in sub_questions:
                    tracking_question = next(
                        (cls for cls in tracking_questions
                        if cls.get("value") == "select_the_type_of_tracking_shot"),
                        None             # ← default returned when nothing matches
                    )
                    selected = [item["value"] for item in tracking_question["checklist_answers"]]
                    answers.update(_expand_checklist("select_the_type_of_tracking_shot", selected))
            if raw_key == "dominant_light_direction_on_subject_s_is" and cls['radio_answer']['value'] == "consistent":
                subject_lightings = cls['radio_answer']['classifications']
                sub_questions = [item['value'] for item in subject_lightings]
                if "select_light_directions" in sub_questions:
                    subject_light = next(
                        (cls for cls in subject_lightings
                        if cls.get("value") == "select_light_directions"),
                        None             # ← default returned when nothing matches
                    )
                    selected = [item["value"] for item in subject_light["checklist_answers"]]
                    answers.update(_expand_checklist("select_light_directions", selected))
            answers[key] = cls["radio_answer"]["value"]

        elif "text_answer" in cls:
            answers[key] = cls["text_answer"]["content"]

    # step 3 : ensure every group question is present
    for q_text, meta in q_meta.items():
        if q_text in answers:
            continue
        if meta["type"] == "description":
            answers[q_text] = ""
        else:
            # fall back to default-option or first option (if any)
            answers[q_text] = meta["default"] or ""

    # step 4 : map raw option names → canonical option names
    for q_text, raw_val in list(answers.items()):
        transl = OPTION_MAPPING.get(q_text)
        # if q_text == 'Ending camera angle:':
        #     # print(transl)
        #     # print(raw_val)
        #     # print(transl.get(raw_val, raw_val))
        #     print('--------------------------------')
        if transl and isinstance(raw_val, str):
            answers[q_text] = transl.get(raw_val, raw_val)

    # keep only legitimate questions
    return {k: answers[k] for k in q_meta.keys()}

def modify_motion_attributes(motion_attributes_json: str = None):
    with open(motion_attributes_json, 'r') as f:
        motion_attributes = json.load(f)

    new_item = []
    # Check whether should we change the answer of question to "Unsure"
    attributes = ["Is the camera moving forward or backward?", "Is the camera zooming?", "Is the camera moving (trucking) to the left or right?", "Is the camera panning?", "Is the camera moving up or down?", "Is the camera tilting?", "Is the camera rolling?"]
    for item in motion_attributes:
        try:
            answers = item.get("answers", None)
        except ValueError:
            print(item)
            return
        new_answers = {}
        new_answers['Is the camera moving in an arc?'] = answers.get("Is the camera moving in an arc?", None)
        steadiness = answers.get("What is the camera steadiness?", None)
        arcing = answers.get("Is the camera moving in an arc?", None)
        motion_complexity = answers.get("Is there any camera movement other than shaking?", None)
        if arcing != 'No' or motion_complexity == 'Yes with major, complex motion' or steadiness in ['Unsteady (e.g., Somewhat shaky handheld shot)', 'Very Unsteady (e.g., Shaky shot)']:
            for attribute in attributes:
                answers[attribute] = "Unclear" if answers[attribute] == "No" else answers[attribute]
            item['answers'] = answers
            new_item.append(item)
        else:
            new_item.append(item)
    with open(motion_attributes_json, 'w') as f:
        json.dump(new_item, f, indent=2)

# ──────────────────────────────
# 4.  Main loader
# ──────────────────────────────
def load_ndjson_all_labels(
    ndjson_path: str | Path,
    *,
    base_project_name: str = None,
    question_group_title: str = None,
    max_videos_per_project: int = 15,
    target_path: str = None,
    selected_label: bool = False,          # ← NEW
) -> List[Dict[str, Any]]:
    """
    Parse a Labelbox NDJSON and return **one record per annotator label**.

    • Checklists are expanded to yes/no singles  
    • Any missing questions in the group are filled with defaults/blank  
    • Only questions belonging to <question_group_title> are kept  
    """
    ndjson_path = Path(ndjson_path)
    if not ndjson_path.is_file():
        raise FileNotFoundError(ndjson_path)

    results: List[Dict[str, Any]] = []
    uid_to_project: Dict[str, str] = {}
    batch_no, batch_size = 1, 0

    with SessionLocal() as session:
        group = QuestionGroupService.get_group_by_name(question_group_title, session)
        df = QuestionGroupService.get_group_questions(group.id, session)

        # prepare meta info for every question in the group
        q_meta: Dict[str, Dict[str, Any]] = {}
        for _, row in df.iterrows():
            q_meta[row["Text"]] = {
                "type": row["Type"],
                "default": row["Default"],
                "first_option": (
                    row["Options"].split(",")[0].strip() if row["Options"] else ""
                ),
            }

        # walk through NDJSON
        with ndjson_path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                try:
                    blob = json.loads(line)
                    video_uid = blob["data_row"]["external_id"]

                    # project batching
                    if video_uid not in uid_to_project:
                        if batch_size >= max_videos_per_project:
                            batch_no += 1
                            batch_size = 0
                        uid_to_project[video_uid] = f"{base_project_name} {batch_no}"
                        batch_size += 1
                    project_name = uid_to_project[video_uid]

                    project_obj = next(iter(blob[PROJECT_KEY].values()))
                    labels = project_obj["labels"]

                    if selected_label:
                        sel_id = project_obj.get("project_details", {}).get("selected_label_id")
                        if sel_id:
                            labels = [lbl for lbl in labels if lbl["id"] == sel_id] or labels[:1]
                        else:
                            labels = labels[:1]
                    for label in labels:
                        user_email = label["label_details"]["created_by"]
                        
                        history = project_obj.get("project_details", {}).get("workflow_history", [])
                        reviewer_email = None
                        for h in reversed(history):
                            if h.get("action", "").lower() == "approve":
                                reviewer_email = h.get("created_by")
                                break
                        
                        if selected_label:
                            user_email = reviewer_email or user_email

                        answers = _answers_from_classifications(
                            label["annotations"]["classifications"], q_meta
                        )
                        if video_uid == '1265.1.22.mp4' and question_group_title == 'Camera Angle':
                            print(answers)
                        results.append(
                            {
                                "question_group_title": question_group_title,
                                "project_name": project_name,
                                "user_email": user_email,
                                "video_uid": video_uid,
                                "answers": answers,
                                "is_ground_truth": True if selected_label else False
                            }
                        )
                except Exception as exc:
                    print(f"[WARN] line {line_no}: {exc}")
    
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    if question_group_title == "Camera Movement Attributes":
        modify_motion_attributes(target_path)
    return results

# ──────────────────────────────
# CLI test helper
# ──────────────────────────────
if __name__ == "__main__":
    import glob
    import re
    
    trusted_approvers = [
        "ipi@andrew.cmu.edu",
        "eanoh4111@gmail.com", 
        "rater.4@grandecenter.org",
        "gela.xu@outlook.com",
        "wangluxuan59@gmail.com",
        "rater.64@grandecenter.org",
        "yangeve4967@gmail.com",
        "zixidai185@gmail.com",
        "huangyuhan1130@gmail.com",
        "qsydfzly@outlook.com",
        "sunnyg@andrew.cmu.edu",
        "sarahchenart@gmail.com",
        "aalucythejellyfish@hotmail.com",
        "xieyi0756@gmail.com",
        "1340464709w@gmail.com",
        "wanqi.xing@alumni.uts.edu.au",
        "zouyoumei1131@gmail.com",
        "supervisor@grandecenter.org",
        "zhiqiulin98@gmail.com",
        "ttiffanyyllingg@gmail.com",
        "zhiqiul@andrew.cmu.edu"
    ]
    
    
    # _answers_from_classifications(
    
    ndjson_path = "./videos4labelpizza/Shotcomp485/inreview/485shotcomp.ndjson"
    question_group_titles = ["Video Quality & Effects", "Camera POV & Shot Basics", "Camera Height", "Camera Angle", "Camera Focus"]
    fname = "ShotcompSuppl_Inreview"
    for question_group_title in question_group_titles:
        target_folder = './CameraShotcomp/annotations'
        name_mapping = {
            "Video Quality & Effects": "_Video_Quality",
            "Camera POV & Shot Basics": "_Camera_POV",
            "Camera Height": "_Camera_Height",
            "Camera Angle": "_Camera_Angle",
            "Camera Focus": "_Camera_Focus",
        }
        path_name = fname + name_mapping.get(question_group_title)
        os.makedirs(target_folder, exist_ok=True)
        target_path = os.path.join(target_folder, f'{path_name}.json')
        max_videos_per_project=30
        out = load_ndjson_all_labels(
            ndjson_path = ndjson_path,
            question_group_title=question_group_title,
            base_project_name="Shotcomp",
            max_videos_per_project=max_videos_per_project,
            target_path = target_path,
            selected_label=True,
        )
    
    # ndjson_path = "./videos4labelpizza/CameraMovement/inreview/CameraMotion.ndjson"
    # question_group_titles = ["Camera Movement Attributes", "Camera Motion Effects", "Camera Tracking Shot"]
    # fname = "CameraMotionInreview"
    # for question_group_title in question_group_titles:
    #     target_folder = './CameraMotion/annotations'
    #     name_mapping = {
    #         "Camera Movement Attributes": "_Motion_Attributes",
    #         "Camera Motion Effects": "_Motion_Effects",
    #         "Camera Tracking Shot": "_Tracking_Shot"
    #     }
    #     path_name = fname + name_mapping.get(question_group_title)
    #     os.makedirs(target_folder, exist_ok=True)
    #     target_path = os.path.join(target_folder, f'{path_name}.json')
    #     max_videos_per_project=30
    #     out = load_ndjson_all_labels(
    #         ndjson_path = ndjson_path,
    #         question_group_title=question_group_title,
    #         base_project_name="CameraMotion",
    #         max_videos_per_project=max_videos_per_project,
    #         target_path = target_path,
    #         selected_label=False,
    #     )
    
    
    
    # ndjson_paths = glob.glob("./labelbox_annotations/lighting/*.ndjson")
    # for ndjson_path in ndjson_paths:
    #     fname = os.path.basename(ndjson_path)
    #     m = re.match(r'^(Lightingtest_\d+(_\d+)?).*\.ndjson$', fname)
    #     if m:
    #         test = m.group(1)
    #     # test = 'Camera Movement'
    #         if "color" in fname:
    #             question_group_titles = ["Color Grading"]
    #         elif "light_effect" in fname:
    #             question_group_titles = ["Atmospheric Effects", "Cinematic Motion", "Dynamic Effects", "Lens Flares", "Reflection", "Shadow and Gobos", "Special Lighting", "Volumetric Light"]
    #         elif "subject_light" in fname:
    #             question_group_titles = ["Light Direction", "Subject Contrast", "Subject Light Effect"]
    #         elif "light_setup" in fname:
    #             question_group_titles = ["Light Setup"]
    #         elif "Movement" in fname:
    #             question_group_titles = ["Camera Movement Attributes", "Camera Motion Effects", "Camera Tracking Shot"]
    #         for question_group_title in question_group_titles:
    #             if question_group_title == "Color Grading":
    #                 fname = test + "_color_grading"
    #             elif question_group_title == "Atmospheric Effects":
    #                 fname = test + "_atmospheric_effects"
    #             elif question_group_title == "Cinematic Motion":
    #                 fname = test + "_cinematic_motion"
    #             elif question_group_title == "Dynamic Effects":
    #                 fname = test + "_dynamic_effects"
    #             elif question_group_title == "Lens Flares":
    #                 fname = test + "_lens_flares"
    #             elif question_group_title == "Reflection":
    #                 fname = test + "_reflection"
    #             elif question_group_title == "Shadow and Gobos":
    #                 fname = test + "_shadow_and_gobos"
    #             elif question_group_title == "Special Lighting":
    #                 fname = test + "_special_lighting"
    #             elif question_group_title == "Volumetric Light":
    #                 fname = test + "_volumetric_light"
    #             elif question_group_title == "Light Direction":
    #                 fname = test + "_light_direction"
    #             elif question_group_title == "Subject Contrast":
    #                 fname = test + "_subject_contrast"
    #             elif question_group_title == "Subject Light Effect":
    #                 fname = test + "_subject_light_effect"
    #             elif question_group_title == "Light Setup":
    #                 fname = test + "_light_setup"
    #             elif question_group_title == "Camera Movement Attributes":
    #                 fname = test.replace(' ', '_') + "_motion_attributes"
    #             elif question_group_title == "Camera Motion Effects":
    #                 fname = test.replace(' ', '_') + "_motion_effects"
    #             elif question_group_title == "Camera Tracking Shot":
    #                 fname = test.replace(' ', '_') + "_tracking_shot"
    #             target_folder = './CameraLighting/annotations'
    #             project_name_map = {
    #                 # Cinematic Effects schema
    #                 'Cinematic Motion': 'Cinematic Effects', 
    #                 'Lens Flares': 'Cinematic Effects', 
    #                 'Reflection': 'Cinematic Effects', 
    #                 'Atmospheric Effects': 'Cinematic Effects',
    #                 'Shot Transition': 'Cinematic Effects',
                    
    #                 # Special Effects schema
    #                 "Special Lighting": "Special Effects", 
    #                 "Volumetric Light": "Special Effects", 
    #                 "Shadow and Gobos": "Special Effects", 
    #                 "Dynamic Effects": "Special Effects",
                    
    #                 # Color Grading schema
    #                 'Color Grading': 'Color Grading',
                    
    #                 # Light Setup schema
    #                 'Light Setup': 'Light Setup',
                    
    #                 # Subject Light schema
    #                 'Subject Contrast': 'Subject Light',
    #                 'Light Direction': 'Subject Light',
    #                 'Subject Light Effect': 'Subject Light',
                    
    #                 # Camera Movement
    #                 "Camera Movement Attributes": "Camera Movement", 
    #                 "Camera Motion Effects": "Camera Movement", 
    #                 "Camera Tracking Shot": "Camera Movement"
    #             }
    #             base_prj_name = project_name_map.get(question_group_title, question_group_title)
    #             os.makedirs(target_folder, exist_ok=True)
    #             target_path = os.path.join(target_folder, f'{fname}.json')
    #             max_videos_per_project=30
    #             out = load_ndjson_all_labels(
    #                 ndjson_path = ndjson_path,
    #                 question_group_title=question_group_title,
    #                 base_project_name=base_prj_name + ' ' + test,
    #                 max_videos_per_project=max_videos_per_project,
    #                 target_path = target_path,
    #                 selected_label=False,
    #             )
                
                
                
                
            # elif "motion" in fname:
            #     question_group_title = "Camera Movement Attributes"
    # ndjson_path = "./labelbox_annotations/Video_Segment_Classification_Camera_Movement_cm2iljzsz00rg07117sq10jiw.ndjson"
    # question_group_title=""
    # base_project_name="motion_batch_1"
    # max_videos_per_project=15
    # target_path = './test_json/all_videos_motion_attri_annotations.json'
    # out = load_ndjson_all_labels(
    #     ndjson_path = ndjson_path,
    #     question_group_title=question_group_title,
    #     base_project_name=base_project_name,
    #     max_videos_per_project=max_videos_per_project,
    #     target_path = target_path,
    #     selected_label=False,
    # )
    # print("✔ Done –", len(out), "records")