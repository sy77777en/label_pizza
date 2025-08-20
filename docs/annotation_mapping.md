# Camera Motion and Shot Composition Mapping

This guide explains how to transform Label Pizza annotation data from human-readable format to structured format for video annotation and pre-caption generation.

## Quick Start

> Assuming you have already stored all the ground truths data in the folder `./workspace`, otherwise, you should use `export_ground_truths` from `export_utils` to export the data.

```python
from your_module import transform_to_caption

with open('./workspace/ground_truths/camera_movement.json', 'r') as f:
    raw_annotations = json.load(f)
    
# Alternatively, you can combine all question-answer pairs into a single file before performing the transformation.
    
# Complete workflow: load → transform → save
transform_to_caption(
    label_pizza_annotations=raw_annotations,
    question_type='camera_motion',  # or 'shot_composition'
    output_path='video_annotation.json'
)
```

## Data Format Requirements

### Input Format (Label Pizza Export)

```python
# Single annotation record structure
{
    "video_uid": "0XjaCd9gj98.0.0.mp4",
    "project_name": "Camera Movement 0", 
    "question_group_title": "Camera Movement",  # or "Shot Composition"
    "user_name": "Annotator Name",
    "is_ground_truth": true,
    "answers": {
        "Is there any camera movement other than shaking?": "No",
        "How fast is the camera movement? (e.g., crash zoom, whip pan)?": "Regular",
        "What is the camera steadiness?": "Static (Fixed Camera)",
        "Is the camera moving forward or backward?": "No",
        # ... more question-answer pairs
    }
}
```

### Output Format (Structured)

```python
# Processed annotation record
### question_type='camera_motion'
[
    {
        "video_name": "0XjaCd9gj98.0.0.mp4",
        "user_name": "Annotator Name", 
        "cam_motion": {
            "camera_movement": "no",
            "steadiness": "smooth",
            "camera_motion_speed": "regular",
            # ... standardized field mappings
        }
    },
...
]

### question_type='shot_composition'
[
    {
        "video_name": "2WL4mIV48FA.0.3.mp4",
        "user_name": "Yuhan Huang",
        "cam_setup": {
            "shot_transition": false,
            "shot_type": "complex",
            "complex_shot_type": "description",
            "shot_size_description_type": "others",
            "shot_size_start": "unknown",
            ...
    }
...
]

```

## Core Functions

| Function               | Purpose                               | Input                       | Output                       |
| ---------------------- | ------------------------------------- | --------------------------- | ---------------------------- |
| `map_camera_motion`    | Transform camera movement annotations | `answers` dict              | Dict with standardized codes |
| `map_shot_composition` | Transform shot framing annotations    | `answers` dict              | Dict with standardized codes |
| `transform_to_caption` | Batch process annotation files        | List of Label Pizza records | Processed JSON file          |

## Input-Output field mapping

```python
# Example transformation
input = {"What is the camera steadiness?": "Smooth / Minimal Shaking"}
output = {"steadiness": "smooth"}
```

### Camera Movement

- "Is there any camera movement other than shaking?" → `camera_movement`
- "How fast is the camera movement?" → `camera_motion_speed`
- "What is the camera steadiness?" → `steadiness`
- "Is the camera moving forward or backward?" → `camera_forward_backward`
- "Is the camera zooming?" → `camera_zoom`
- "Is there a dolly-zoom effect in this video?" → `dolly_zoom`
- "Is the camera moving (trucking) to the left or right?" → `camera_left_right`
- "Is the camera panning?" → `camera_pan`
- "Is the camera moving up or down?" → `camera_up_down`
- "Is the camera tilting?" → `camera_tilt`
- "Is the camera moving in an arc?" → `camera_arc` / `camera_crane`
- "Is the camera rolling?" → `camera_roll`
- "If the camera motion is too complex, how would you describe it?" → `complex_motion_description`

### Camera Setup

- "Initial overall height:" → `overall_height_start`
- "Ending overall height:" → `overall_height_end`
- "Describe complex overall height:" → `overall_height_description`
- "Initial camera angle:" → `camera_angle_start`
- "Ending camera angle:" → `camera_angle_end`
- "Describe complex angle:" → `camera_angle_description`
- "Dutch angle (>15) present?" → `dutch_angle`
- "Focus type:" → `camera_focus`
- "Focus depth (start):" → `focus_plane_start`
- "Focus depth (end):" → `focus_plane_end`
- "Reason for focus change:" → `focus_change_reason`
- "Describe complex focus:" → `camera_focus_description`

### Camera Effects

- "Is there a frame-freezing effect in this video?" → `frame_freezing`
- "Is there a motion blur effect in this video?" → `motion_blur`
- "Is there a cinemagraph effect in this video?" → `cinemagraph`

### Shot Transition

- "Are there any shot transitions?" → `shot_transition`

### Tracking Shot

- "Does the camera track the moving subject(s)?" → `is_tracking`
- "Does the size of the subject change during tracking?" → `subject_size_change`
- "Is the camera side-tracking?" → `tracking_shot_types` (adds 'side')
- "Is the camera tail-tracking?" → `tracking_shot_types` (adds 'tail')
- "Is the camera lead-tracking?" → `tracking_shot_types` (adds 'lead')
- "Is the camera aerial-tracking?" → `tracking_shot_types` (adds 'aerial')
- "Is the camera arc-tracking?" → `tracking_shot_types` (adds 'arc')
- "Is the camera pan-tracking?" → `tracking_shot_types` (adds 'pan')
- "Is the camera tilt-tracking?" → `tracking_shot_types` (adds 'tilt')

### Video Setup

- "Lens distortion:" → `lens_distortion`
- "Text/watermarks present?" → `has_overlays`
- "Video speed:" → `video_speed`
- "Camera POV:" → `camera_pov`

### Shot Type

- "Shot type:" → `shot_type`
- "Complex shot:" → `complex_shot_type`
- "Reason for text description:" → `shot_size_description_type`
- "Initial shot size:" → `shot_size_start`
- "Ending shot size:" → `shot_size_end`
- "Describe complex shot size:" → `shot_size_description`
- "Initial relative height:" → `subject_height_start`
- "Ending relative height:" → `subject_height_end`
- "Describe complex relative height:" → `subject_height_description`