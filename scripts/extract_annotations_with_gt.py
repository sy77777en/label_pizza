import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
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
        "sunlight":  "Is sunlight the major light source?",
        "moonlight_starlight": "Is moonlight / starlight the major light source?",
        "firelight": "Is firelight the major light source?",
        "artificial_lighting_practical_visible": "Is a practical / visible artificial light the major source?",
        "non_visible_light_sources": "Is a non-visible light source the major source?",
        "n_a_abstract": "Is the lighting abstract / N/A?",
        "complex_others": "Is the major light source complex / other type?",
    },
    "select_light_directions": {
        "back_light": "Is there back light on the subject?",
        "front_light": "Is there front light on the subject?",
        "top_light": "Is there top light on the subject?",
        "bottom_light": "Is there bottom light on the subject?",
        "right_side_light": "Is there right-side light on the subject?",
        "left_side_light": "Is there left-side light on the subject?",
        "ambient_light": "Is lighting direction ambient / no dominant side?",
    },
    "special_lighting_on_subject_s_is": {
        "rembrandt_lighting": "Is Rembrandt lighting used on the subject?",
        "silhouette_lighting_subject_not_always_required": "Is the subject lit as a silhouette?",
        "rim_light_subject_not_always_required": "Is rim lighting present on the subject?",
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
    "What is the camera steadiness?": {
        "static_fixed_camera": "Static (Fixed Camera)",
        "very_smooth_drone_shot": "Very Smooth / No Shaking (e.g., Drone shot with no shaking at all)",
        "smooth_steadicam_stabilized_handheld_shot": "Smooth / Minimal Shaking (e.g., Steadicam shot or stabilized handheld shot)",
        "unsteady_somewhat_shaky_handheld_shot": "Unsteady (e.g., Somewhat shaky handheld shot)",
        "very_unsteady_extreme_shaky_found_footage_style": "Very Unsteady (e.g., Extreme shaky shot, found footage style)"
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
    "The color tones in this video are?": {
        "n_a_black_white": "N/A (black-white)",
        "warm": "Changing and Contrasting",
        "cool": "Changing",
        "changing": "Contrast",
        "contrasting": "Warm",
        "changing_and_contrasting": "Cool",
        "neither_warm_nor_cool": "Neither Warm nor Cool"
    },
    "How colorful is this video?": {
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
    "Is the scene indoors or outdoors?": {
        "interior": "Interior",
        "exterior": "Exterior",
        "synthetic_unrealistic": "Synthetic / Unrealistic",
        "complex_others": "Complex (others)"
    },
    "What is the light quality across the entire scene?": {
        "soft_light": "Unclear",
        "hard_light": "Changing (temporal)",
        "changing_temporal": "Hard Light",
        "unclear": "Soft Light"
    },
    "What is the sunlight level?": {
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
    "Light contrast on subject(s) is?": {
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
    "Are there any regular lens flares?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any anamorphic lens flares?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any mist/diffusion effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any bokeh effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any water-reflection effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any glassy-surface reflection effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any mirror-reflection effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any aerial-perspective effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any rainbow effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any aurora effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any heat-waves or heat-haze effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any lightning effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any volumetric beam light?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any volumetric spot light?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any god-rays?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any volumetric light through medium?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any other volumetric light?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any venetian-blinds lighting?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any subject-shape lighting?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any window-frame lighting?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any foliage lighting?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any gobo-lighting (others)?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any color-shifting (smooth) effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any color-shifting (sudden) effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any flashing effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any moving-light effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any pulsing or flickering effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any vignette effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any wave-light / water-caustics effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any city-light effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any street-light effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any revealing-shot dynamic?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any transformation or morphing dynamic?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any levitation or floating dynamic?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any explosion dynamic?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any shattering or breaking effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any diffusion dynamic?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any splashing or waves dynamic?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Does the size of the subject change during tracking?": {
        "no": "No",
        "subject_gets_larger": "Subject gets larger",
        "subject_gets_smaller": "Subject gets smaller"
    },
    "Are there any professional / portrait lighting effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    },
    "Are there any colored or neon lighting effect?": {
        "yes": "Yes",
        "no": "No",
        "unsure": "Unsure"
    },
    "Are there any head-light or flash-light effect?": {
        "no": "No",
        "yes": "Yes",
        "unsure": "Unsure"
    }
}

# key: value --> labelbox question text: labelpizza question text
NAME_MAPPING: dict[str, str] = {
    
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
    "the_color_tones_in_this_video_are":           "The color tones in this video are?",
    "how_colorful_is_this_video":                  "How colorful is this video?",
    "brightness_and_exposure":                     "Brightness and Exposure?",
    "describe_color_compositions_and_dynamics":    "Describe the color grading",

    # Scene & Lighting setup
    "is_the_scene_indoors_or_outdoors":            "Is the scene indoors or outdoors?",
    "select_the_sunlight_level":                   "What is the sunlight level?",
    "what_is_the_light_quality_across_the_entire_scene": "What is the light quality across the entire scene?",
    "scene_and_lighting_setup_description":        "Scene and Lighting Setup (Description)",
    
    # Subject lighting
    "dominant_light_direction_on_subject_s_is":    "Dominant Light Direction on Subject(s) is?",
    "subject_lighting_description":                "Subject Lighting (Description)",
    "does_subject_lighting_apply":                 "Does subject lighting apply?",
    "light_contrast_on_subject_s_is":              "Light contrast on subject(s) is?",
    
    # Light-effects checklist
    "are_there_any_regular_lens_flares":           "Are there any regular lens flares?",
    "are_there_any_anamorphic_lens_flares":        "Are there any anamorphic lens flares?",
    "are_there_any_mist_diffusion_effect":         "Are there any mist/diffusion effect?",
    "are_there_any_bokeh_effect":                  "Are there any bokeh effect?",
    "are_there_any_water_reflection_effect":       "Are there any water-reflection effect?",
    "are_there_any_glassy_surface_reflection_effect": "Are there any glassy-surface reflection effect?",
    "are_there_any_mirror_reflection_effect":      "Are there any mirror-reflection effect?",
    "are_there_any_aeiral_perspective_effect":     "Are there any aerial-perspective effect?",
    "are_there_any_rainbow_effect":                "Are there any rainbow effect?",
    "are_there_any_aurora_effect":                 "Are there any aurora effect?",
    "are_there_any_heat_waves_or_heat_haze_effect": "Are there any heat-waves or heat-haze effect?",
    "are_there_any_lightning_effect":              "Are there any lightning effect?",
    "are_there_any_wave_light_or_water_caustics_effect": "Are there any wave-light / water-caustics effect?",
    "are_there_any_volumetric_beam_light":         "Are there any volumetric beam light?",
    "are_there_any_volumetric_spot_light":         "Are there any volumetric spot light?",
    "are_ther_any_god_rays":                       "Are there any god-rays?",
    "are_there_any_volumetric_light_through_medium": "Are there any volumetric light through medium?",
    "are_there_any_volumetric_light_others":       "Are there any other volumetric light?",
    "are_there_any_venetian_blinds_lighting":      "Are there any venetian-blinds lighting?",
    "are_there_any_subject_shape_lighting":        "Are there any subject-shape lighting?",
    "are_there_any_window_frame_lighting":         "Are there any window-frame lighting?",
    "are_there_any_foliage_lighting":              "Are there any foliage lighting?",
    "are_there_any_gobo_lighting_others":          "Are there any gobo-lighting (others)?",
    "are_there_any_color_shifting_smooth_effect":  "Are there any color-shifting (smooth) effect?",
    "are_there_any_color_shifting_sudden_effect":  "Are there any color-shifting (sudden) effect?",
    "are_there_any_flashing_effect":               "Are there any flashing effect?",
    "are_there_any_moving_light_effect":           "Are there any moving-light effect?",
    "are_there_any_pulsing_or_flickering_effect":  "Are there any pulsing or flickering effect?",
    "are_there_any_professional_or_portrait_lighting_effect": "Are there any professional / portrait lighting effect?",
    "are_there_any_colored_or_neon_lighting_effect": "Are there any colored or neon lighting effect?",
    "are_there_any_headlight_or_flashlight_effect": "Are there any head-light or flash-light effect?",
    "are_there_any_vignette_effect":               "Are there any vignette effect?",
    "are_there_any_city_light_effect":             "Are there any city-light effect?",
    "are_there_any_street_light_effect":           "Are there any street-light effect?",

    # Descriptive free-text for special lighting
    "special_lighting_effects_description":        "Describe special lighting effects",
    "volumetric_lighting_description":             "Describe volumetric lighting effects",
    "shadow_patterns_gobo_lighting_description":   "Describe shadow-pattern / gobo lighting",
    "dynamic_lighting_effects_description":        "Describe dynamic lighting effects",

    # Dynamic / motion-graphic effects
    "are_there_any_revealing_shot_dynamic":        "Are there any revealing-shot dynamic?",
    "are_there_any_transformation_or_morphing_dynamic": "Are there any transformation or morphing dynamic?",
    "are_there_any_levitation_or_floating_dynamic": "Are there any levitation or floating dynamic?",
    "are_there_any_explosion_dynamic":             "Are there any explosion dynamic?",
    "are_there_any_shattering_or_breaking_effect": "Are there any shattering or breaking effect?",
    "are_there_any_diffusion_dynamic":             "Are there any diffusion dynamic?",
    "are_there_any_splashing_or_waves_dynamic":    "Are there any splashing or waves dynamic?"
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
                    tmp_key = _rename_key("does_the_size_of_the_subject_change")
                    answers[tmp_key] = cls["radio_answer"]["value"]
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
                    print(_expand_checklist("select_light_directions", selected))
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
        if arcing != 'No' or motion_complexity == 'Yes with major, complex motion' or steadiness in ['Unsteady (e.g., Somewhat shaky handheld shot)', 'Very Unsteady (e.g., Extreme shaky shot, found footage style)']:
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
                        uid_to_project[video_uid] = f"{base_project_name}-{batch_no}"
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
                        answers = _answers_from_classifications(
                            label["annotations"]["classifications"], q_meta
                        )

                        results.append(
                            {
                                "question_group_title": question_group_title,
                                "project_name": project_name,
                                "user_email": user_email,
                                "video_uid": video_uid,
                                "answers": answers,
                            }
                        )
                except Exception as exc:
                    print(f"[WARN] line {line_no}: {exc}")
    
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    if question_group_title == "Camera Movement Attributes_syc_test":
        modify_motion_attributes(target_path)
    return results

# ──────────────────────────────
# CLI test helper
# ──────────────────────────────
if __name__ == "__main__":
    ndjson_path = "./test_ndjson/Video_Segment_Classification_Camera_Movement_cm2iljzsz00rg07117sq10jiw.ndjson"
    question_group_title="Camera Movement Attributes_syc_test"
    base_project_name="motion_batch_1"
    max_videos_per_project=15
    target_path = './test_json/all_videos_motion_attri_annotations.json'
    out = load_ndjson_all_labels(
        ndjson_path = ndjson_path,
        question_group_title=question_group_title,
        base_project_name=base_project_name,
        max_videos_per_project=max_videos_per_project,
        target_path = target_path,
        selected_label=False,
    )
    print("✔ Done –", len(out), "records")