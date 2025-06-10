import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Any
from datetime import datetime
from sqlalchemy.orm import Session
from contextlib import contextmanager

# Import database
from db import SessionLocal
from services import (
    AuthService, AnnotatorService, GroundTruthService, 
    QuestionService, QuestionGroupService, SchemaService
)

###############################################################################
# CONSTANTS AND SHARED STYLES
###############################################################################

# Common color schemes for consistency
COLORS = {
    'primary': '#1f77b4',
    'success': '#28a745',
    'warning': '#ffc107', 
    'danger': '#dc3545',
    'info': '#3498db',
    'secondary': '#6c757d'
}

def get_card_style(color, opacity=0.15):
    """Generate consistent card styling"""
    return f"""
    background: linear-gradient(135deg, {color}{opacity}, {color}08);
    border: 2px solid {color};
    border-radius: 12px;
    padding: 16px;
    margin: 12px 0;
    box-shadow: 0 2px 8px {color}20;
    """

###############################################################################
# HELPER FUNCTIONS - ALL USING SERVICE LAYER
###############################################################################

def get_schema_question_groups(schema_id: int, session: Session) -> List[Dict]:
    """Get question groups in a schema"""
    try:
        return SchemaService.get_schema_question_groups_list(schema_id=schema_id, session=session)
    except ValueError as e:
        st.error(f"Error loading schema question groups: {str(e)}")
        return []

###############################################################################
# DATABASE UTILITIES
###############################################################################

@contextmanager
def get_db_session():
    """Get database session with proper error handling"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def check_database_health() -> dict:
    """Check database connection health"""
    try:
        with get_db_session() as session:
            try:
                users_df = AuthService.get_all_users(session=session)
                return {
                    "healthy": True,
                    "database": "Connected",
                    "status": "OK",
                    "user_count": len(users_df)
                }
            except:
                session.execute(text("SELECT 1"))
                return {"healthy": True, "database": "Connected", "status": "OK"}
    except Exception as e:
        return {"healthy": False, "error": str(e), "database": "Disconnected"}

def handle_database_errors(func):
    """Decorator to handle database connection errors gracefully"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")
            st.error("This might be a temporary issue. Please try:")
            st.info("1. Refresh the page")
            st.info("2. Check your internet connection") 
            st.info("3. Contact support if the problem persists")
            
            with st.expander("🔧 Technical Details"):
                try:
                    health = check_database_health()
                    st.json(health)
                except:
                    st.write("Unable to get database health information")
            return None
    return wrapper

def calculate_overall_accuracy(accuracy_data: Dict[int, Dict[int, Dict[str, int]]]) -> Dict[int, Optional[float]]:
    """Calculate overall accuracy for each user from accuracy data"""
    overall_accuracy = {}
    
    for user_id, question_data in accuracy_data.items():
        total_questions = sum(stats["total"] for stats in question_data.values() if stats["total"] > 0)
        total_correct = sum(stats["correct"] for stats in question_data.values() if stats["total"] > 0)
        
        overall_accuracy[user_id] = (total_correct / total_questions) * 100 if total_questions > 0 else None
    
    return overall_accuracy

def calculate_per_question_accuracy(accuracy_data: Dict[int, Dict[int, Dict[str, int]]]) -> Dict[int, Dict[int, Optional[float]]]:
    """Calculate per-question accuracy for each user from accuracy data"""
    per_question_accuracy = {}
    
    for user_id, question_data in accuracy_data.items():
        per_question_accuracy[user_id] = {
            question_id: (stats["correct"] / stats["total"]) * 100 if stats["total"] > 0 else None
            for question_id, stats in question_data.items()
        }
    
    return per_question_accuracy


###############################################################################
# UTILITY FUNCTIONS
###############################################################################


def _display_enhanced_helper_text_answers(video_id: int, project_id: int, question_id: int, question_text: str, text_key: str, gt_value: str, role: str, answer_reviews: Optional[Dict], session: Session, selected_annotators: List[str] = None):
    """Display helper text showing other annotator answers"""
    
    try:
        annotator_user_ids = []
        text_answers = []
        
        # Get annotator answers if annotators are selected
        if selected_annotators:
            annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            
            text_answers = GroundTruthService.get_question_text_answers(
                video_id=video_id, project_id=project_id, question_id=question_id, 
                annotator_user_ids=annotator_user_ids, session=session
            )
        
        all_answers = []
        
        # Add Ground Truth for meta-reviewer OR if we have existing GT (search portal)
        if role == "meta_reviewer" and gt_value:
            all_answers.append({
                "name": "Ground Truth",
                "full_text": gt_value,
                "has_answer": bool(gt_value.strip()),
                "is_gt": True,
                "display_name": "Ground Truth"
            })
        # For search portal - check if we have ground truth even without annotators
        elif not selected_annotators or len(selected_annotators) == 0:
            try:
                gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                if not gt_df.empty:
                    question_gt = gt_df[gt_df["Question ID"] == question_id]
                    if not question_gt.empty:
                        gt_answer = question_gt.iloc[0]["Answer Value"]
                        if gt_answer and str(gt_answer).strip():
                            all_answers.append({
                                "name": "Ground Truth",
                                "full_text": str(gt_answer),
                                "has_answer": True,
                                "is_gt": True,
                                "display_name": "Ground Truth"
                            })
            except Exception as e:
                print(f"Error getting ground truth: {e}")
                pass
        
        # Add annotator answers
        if text_answers:
            unique_answers = []
            seen_answers = set()
            
            for answer in text_answers:
                answer_key = f"{answer['initials']}:{answer['answer_value']}"
                if answer_key not in seen_answers:
                    seen_answers.add(answer_key)
                    unique_answers.append(answer)
            
            for answer_info in unique_answers:
                annotator_name = answer_info['name']
                answer_value = answer_info['answer_value']
                initials = answer_info['initials']
                
                all_answers.append({
                    "name": annotator_name,
                    "full_text": answer_value,
                    "has_answer": bool(answer_value.strip()),
                    "is_gt": False,
                    "display_name": f"{annotator_name} ({initials})",
                    "initials": initials
                })
        
        if all_answers:
            # Smart tab naming
            if len(all_answers) > 6:
                tab_names = []
                for answer in all_answers:
                    if answer["is_gt"]:
                        tab_names.append("🏆 GT")
                    else:
                        tab_names.append(answer["initials"])
            else:
                tab_names = []
                for answer in all_answers:
                    if answer["is_gt"]:
                        tab_names.append("Ground Truth")
                    else:
                        name = answer["name"]
                        tab_names.append(name[:17] + "..." if len(name) > 20 else name)
            
            tabs = st.tabs(tab_names)
            
            for tab, answer in zip(tabs, all_answers):
                with tab:
                    _display_single_answer_elegant(
                        answer, text_key, question_text, answer_reviews, 
                        video_id, project_id, question_id, session
                    )
                            
    except Exception as e:
        st.caption(f"⚠️ Could not load answer information: {str(e)}")


def _display_unified_status(video_id: int, project_id: int, question_id: int, session: Session, show_annotators: bool = False, selected_annotators: List[str] = None):
    """Display ground truth status and optionally annotator status with confidence scores for models"""
    
    status_parts = []
    
    # Get annotator status first if requested
    if show_annotators and selected_annotators:
        try:
            annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            
            # Get answers for this question/video
            answers_df = AnnotatorService.get_question_answers(
                question_id=question_id, project_id=project_id, session=session
            )
            
            # Get user info for role checking
            users_df = AuthService.get_all_users(session=session)
            user_lookup = {row["ID"]: {"role": row["Role"], "name": row["User ID"]} for _, row in users_df.iterrows()}
            
            # Find which annotators have answered this specific question/video
            annotators_with_answers = set()
            annotators_missing = set()
            
            if not answers_df.empty:
                video_answers = answers_df[answers_df["Video ID"] == video_id]
                answered_user_ids = set(video_answers["User ID"].tolist())
                
                for user_id in annotator_user_ids:
                    user_info = user_lookup.get(user_id, {})
                    user_name = user_info.get("name", f"User {user_id}")
                    user_role = user_info.get("role", "human")
                    
                    if user_id in answered_user_ids:
                        # Check if this is a model user and get confidence score
                        if user_role == "model":
                            try:
                                user_answer_row = video_answers[video_answers["User ID"] == user_id]
                                if not user_answer_row.empty:
                                    confidence = user_answer_row.iloc[0]["Confidence Score"]
                                    if confidence is not None:
                                        user_name += f" (conf: {confidence:.2f})"
                                    else:
                                        user_name += " (🤖)"
                                else:
                                    user_name += " (🤖)"
                            except Exception as e:
                                print(f"Error getting confidence for user {user_id}: {e}")
                                user_name += " (🤖)"
                        
                        annotators_with_answers.add(user_name)
                    else:
                        if user_role == "model":
                            user_name += " (🤖)"
                        annotators_missing.add(user_name)
            else:
                # No answers at all - all are missing
                for user_id in annotator_user_ids:
                    user_info = user_lookup.get(user_id, {})
                    user_name = user_info.get("name", f"User {user_id}")
                    user_role = user_info.get("role", "human")
                    if user_role == "model":
                        user_name += " (🤖)"
                    annotators_missing.add(user_name)
            
            # Add annotator status parts with FULL NAMES and confidence scores
            if annotators_with_answers:
                names_list = list(annotators_with_answers)
                if len(names_list) <= 3:
                    status_parts.append(f"📊 Answered: {', '.join(names_list)}")
                else:
                    shown_names = ', '.join(names_list[:2])
                    remaining = len(names_list) - 2
                    status_parts.append(f"📊 Answered: {shown_names} +{remaining} more")
            
            if annotators_missing:
                missing_list = list(annotators_missing)
                if len(missing_list) <= 2:
                    status_parts.append(f"⚠️ Missing: {', '.join(missing_list)}")
                else:
                    shown_missing = ', '.join(missing_list[:1])
                    remaining = len(missing_list) - 1
                    status_parts.append(f"⚠️ Missing: {shown_missing} +{remaining} more")
                
        except Exception as e:
            print(f"Error getting annotator status: {e}")
            status_parts.append("⚠️ Could not load annotator status")
    
    # Ground truth status (existing code remains the same)
    try:
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        
        if not gt_df.empty:
            question_id_int = int(question_id)
            question_gt = gt_df[gt_df["Question ID"] == question_id_int]
            
            if not question_gt.empty:
                gt_row = question_gt.iloc[0]
                
                try:
                    reviewer_info = AuthService.get_user_info_by_id(user_id=int(gt_row["Reviewer ID"]), session=session)
                    reviewer_name = reviewer_info["user_id_str"]
                    
                    modified_by_admin = gt_row["Modified By Admin"] is not None
                    
                    if modified_by_admin:
                        status_parts.append(f"🏆 GT by: {reviewer_name} (Admin)")
                    else:
                        status_parts.append(f"🏆 GT by: {reviewer_name}")
                except Exception:
                    status_parts.append("🏆 GT exists")
            else:
                status_parts.append("📭 No GT")
        else:
            status_parts.append("📭 No GT")
            
    except Exception:
        status_parts.append("📭 No GT")
    
    # Display single combined status line
    if status_parts:
        st.caption(" | ".join(status_parts))



def _display_clean_sticky_single_choice_question(
    question: Dict, 
    video_id: int, 
    project_id: int, 
    group_id: int,
    role: str, 
    existing_value: str, 
    is_modified_by_admin: bool, 
    admin_info: Optional[Dict], 
    form_disabled: bool, 
    session: Session, 
    gt_value: str = "", 
    mode: str = "", 
    selected_annotators: List[str] = None, 
    key_prefix: str = "", 
    preloaded_answers: Dict = None
) -> str:
    """Display a single choice question with preloaded answer support - CLEAN VERSION"""
    question_id = question["id"]
    question_text = question["display_text"]
    options = question["options"]
    display_values = question.get("display_values", options)
    
    display_to_value = dict(zip(display_values, options))
    value_to_display = dict(zip(options, display_values))
    
    # Get preloaded answer
    preloaded_value = ""
    if preloaded_answers:
        key = (video_id, group_id, question_text)
        preloaded_value = preloaded_answers.get(key, "")
    
    # Use preloaded value if available, otherwise use existing value
    current_value = preloaded_value if preloaded_value else existing_value
    
    # Calculate default index using current_value (which includes preloaded)
    default_idx = 0
    if current_value and current_value in value_to_display:
        display_val = value_to_display[current_value]
        if display_val in display_values:
            default_idx = display_values.index(display_val)
    
    # Question header
    if role == "reviewer" and is_modified_by_admin:
        st.error(f"🔒 {question_text}")
    elif role == "meta_reviewer" and is_modified_by_admin:
        st.warning(f"🎯 {question_text}")
    else:
        st.success(f"❓ {question_text}")
    
    # Training mode feedback
    if mode == "Training" and form_disabled and gt_value and role == "annotator":
        if existing_value == gt_value:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['success'])}">
                    <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
                        ✅ Excellent! You selected the correct answer.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['danger'])}">
                    <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
                        ❌ Incorrect. The ground truth answer is highlighted below.
                    </span>
                </div>
            """, unsafe_allow_html=True)
    
    # Show unified status for reviewers/meta-reviewers
    if role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        show_annotators = selected_annotators is not None and len(selected_annotators) > 0
        _display_unified_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or []
        )
    
    # Question content with UNIQUE KEYS using key_prefix
    if role == "reviewer" and is_modified_by_admin and admin_info:
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]

        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            options=options, display_values=display_values, session=session,
            selected_annotators=selected_annotators or []
        )
        
        admin_idx = default_idx
        if current_value and current_value in value_to_display:
            admin_display_val = value_to_display[current_value]
            if admin_display_val in display_values:
                admin_idx = display_values.index(admin_display_val)
        
        st.warning(f"🔒 **Overridden by {admin_name}**")
        
        st.radio(
            "Admin's selection:",
            options=enhanced_options,
            index=admin_idx,
            key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_locked",
            disabled=True,
            label_visibility="collapsed",
            horizontal=True
        )
        
        result = current_value
        
    elif role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        # ALWAYS get enhanced options for reviewer/meta-reviewer/reviewer_resubmit roles
        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            options=options, display_values=display_values, session=session,
            selected_annotators=selected_annotators or []
        )
        
        radio_key = f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_stable"
        
        # Use default_idx which now includes preloaded values
        current_idx = default_idx
        if current_value:
            for i, opt in enumerate(options):
                if opt == current_value:
                    current_idx = i
                    break
        
        selected_idx = st.radio(
            "Select your answer:",
            options=range(len(enhanced_options)),
            format_func=lambda x: enhanced_options[x],
            index=current_idx,
            key=radio_key,
            disabled=form_disabled,
            label_visibility="collapsed",
            horizontal=True
        )
        result = options[selected_idx]
        
    else:
        # Regular display for annotators
        if mode == "Training" and form_disabled and gt_value:
            enhanced_display_values = []
            for i, display_val in enumerate(display_values):
                actual_val = options[i] if i < len(options) else display_val
                if actual_val == gt_value:
                    enhanced_display_values.append(f"🏆 {display_val} (Ground Truth)")
                elif actual_val == existing_value and actual_val != gt_value:
                    enhanced_display_values.append(f"❌ {display_val} (Your Answer)")
                else:
                    enhanced_display_values.append(display_val)
            
            selected_display = st.radio(
                "Select your answer:",
                options=enhanced_display_values,
                index=default_idx,
                key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}",
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True
            )
            
            result = display_to_value.get(
                selected_display.replace("🏆 ", "").replace(" (Ground Truth)", "").replace("❌ ", "").replace(" (Your Answer)", ""), 
                existing_value
            )
        else:
            selected_display = st.radio(
                "Select your answer:",
                options=display_values,
                index=default_idx,
                key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}",
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True
            )
            result = display_to_value[selected_display]
    
    return result


def _display_clean_sticky_description_question(
    question: Dict, 
    video_id: int, 
    project_id: int, 
    group_id: int,
    role: str, 
    existing_value: str, 
    is_modified_by_admin: bool, 
    admin_info: Optional[Dict], 
    form_disabled: bool, 
    session: Session, 
    gt_value: str = "", 
    mode: str = "", 
    answer_reviews: Optional[Dict] = None, 
    selected_annotators: List[str] = None, 
    key_prefix: str = "", 
    preloaded_answers: Dict = None
) -> str:
    """Display a description question with preloaded answer support - CLEAN VERSION"""
    
    question_id = question["id"]
    question_text = question["display_text"]
    
    # Get preloaded answer
    preloaded_value = ""
    if preloaded_answers:
        key = (video_id, group_id, question_text)
        preloaded_value = preloaded_answers.get(key, "")
    
    # Use preloaded value if available, otherwise use existing value
    current_value = preloaded_value if preloaded_value else existing_value
    
    # Question header
    if role == "reviewer" and is_modified_by_admin:
        st.error(f"🔒 {question_text}")
    elif role == "meta_reviewer" and is_modified_by_admin:
        st.warning(f"🎯 {question_text}")
    else:
        st.info(f"❓ {question_text}")
    
    # Training mode feedback
    if mode == "Training" and form_disabled and gt_value and role == "annotator":
        if existing_value.strip().lower() == gt_value.strip().lower():
            st.markdown(f"""
                <div style="{get_card_style(COLORS['success'])}">
                    <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
                        ✅ Excellent! Your answer matches the ground truth exactly.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        elif existing_value.strip():
            st.markdown(f"""
                <div style="{get_card_style(COLORS['info'])}">
                    <span style="color: #2980b9; font-weight: 600; font-size: 0.95rem;">
                        📚 Great work! Check the ground truth below to learn from this example.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['danger'])}">
                    <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
                        📝 Please provide an answer next time. See the ground truth below to learn!
                    </span>
                </div>
            """, unsafe_allow_html=True)
    
    # Question content with UNIQUE KEYS using key_prefix
    if role == "reviewer" and is_modified_by_admin and admin_info:
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        st.warning(f"🔒 **Overridden by {admin_name}**")
        
        answer = st.text_area(
            "Admin's answer:",
            value=current_value,
            key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_locked",
            disabled=True,
            height=120,
            label_visibility="collapsed"
        )
        
        result = current_value
        
    elif role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        text_key = f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_stable"
        
        answer = st.text_area(
            "Enter your answer:",
            value=current_value,
            key=text_key,
            disabled=form_disabled,
            height=120,
            label_visibility="collapsed"
        )
        
        # Show unified status
        show_annotators = selected_annotators is not None and len(selected_annotators) > 0
        _display_unified_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or []
        )
        
        # Show enhanced helper text if annotators selected OR if we have ground truth (for search portal)
        if show_annotators or (role in ["meta_reviewer", "reviewer_resubmit"] and existing_value):
            _display_enhanced_helper_text_answers(
                video_id=video_id, project_id=project_id, question_id=question_id, 
                question_text=question_text, text_key=text_key,
                gt_value=existing_value if role in ["meta_reviewer", "reviewer_resubmit"] else "",
                role=role, answer_reviews=answer_reviews, session=session,
                selected_annotators=selected_annotators or []
            )
        
        result = answer
        
    else:
        result = st.text_area(
            "Enter your answer:",
            value=current_value,
            key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}",
            disabled=form_disabled,
            height=120,
            label_visibility="collapsed"
        )
        
        if mode == "Training" and form_disabled and gt_value:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['info'])}">
                    <div style="color: #2980b9; font-weight: 700; font-size: 0.95rem; margin-bottom: 8px;">
                        🏆 Ground Truth Answer (for learning):
                    </div>
                    <div style="color: #34495e; font-size: 0.9rem; line-height: 1.4; background: white; padding: 12px; border-radius: 6px; border-left: 4px solid {COLORS['info']};">
                        {gt_value}
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    return result


# ALSO UPDATE display_manual_auto_submit_controls TO REMOVE DEBUG TOGGLE:

def display_manual_auto_submit_controls(selected_groups: List[Dict], videos: List[Dict], project_id: int, user_id: int, role: str, session: Session, is_training_mode: bool):
    """Display manual auto-submit controls - CLEAN VERSION WITHOUT DEBUG TOGGLE"""
    
    # ... [Keep all the existing code until the action buttons] ...
    
    # Enhanced action buttons - REORDERED (REMOVE DEBUG TOGGLE SECTION)
    st.markdown("#### ⚡ Execute Auto-Submit")
    
    # Summary of current configuration
    total_selected_annotators = len(st.session_state.get(f"auto_submit_annotators_{role}_{project_id}", [])) if role == "reviewer" else 0
    
    # Get total questions
    total_questions = 0
    for group in selected_groups:
        questions = QuestionService.get_questions_by_group_id(group_id=group["ID"], session=session)
        total_questions += len(questions)

    summary_text = f"📊 Configuration: {total_questions} questions"
    if role == "reviewer":
        summary_text += f" • {total_selected_annotators} annotators selected"

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #e8f5e8, #d4f1d4); border: 2px solid #28a745; border-radius: 12px; padding: 16px; margin: 16px 0; text-align: center;">
        <div style="color: #155724; font-weight: 600; font-size: 1rem;">
            {summary_text}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # REORDERED ACTION BUTTONS
    action_col1, action_col2, action_col3 = st.columns(3)

    with action_col1:
        if st.button("⚙️ Preload Options", 
                    key=f"preload_options_{role}_{project_id}",
                    use_container_width=True,
                    disabled=is_training_mode,
                    help="Preload options without auto-submitting"):
            run_preload_options_only(selected_groups, videos, project_id, user_id, role, session)

    with action_col2:
        if st.button("🔍 Preview Auto-Submit", 
                    key=f"preview_{role}_{project_id}",
                    use_container_width=True,
                    disabled=is_training_mode,
                    help="Preview what would be auto-submitted"):
            st.session_state[f"show_preview_{role}_{project_id}"] = True
            st.rerun()

    with action_col3:
        if st.button("🚀 Execute Auto-Submit", 
                    key=f"auto_submit_{role}_{project_id}",
                    use_container_width=True,
                    disabled=is_training_mode,
                    type="primary",
                    help="Run auto-submit with current configuration"):
            run_manual_auto_submit(selected_groups, videos, project_id, user_id, role, session)

    # Check if preview button was clicked and show preview outside of columns for full width
    if st.session_state.get(f"show_preview_{role}_{project_id}", False):
        run_preload_preview(selected_groups, videos, project_id, user_id, role, session)
        st.session_state[f"show_preview_{role}_{project_id}"] = False

def _get_enhanced_options_for_reviewer(video_id: int, project_id: int, question_id: int, options: List[str], display_values: List[str], session: Session, selected_annotators: List[str] = None) -> List[str]:
    """Get enhanced options showing who selected what for reviewers with model confidence scores"""
    
    try:
        # Get annotator selections if annotators are provided
        annotator_user_ids = []
        if selected_annotators:
            annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
        
        # Get user info for role checking
        users_df = AuthService.get_all_users(session=session)
        user_lookup = {row["ID"]: {"role": row["Role"], "name": row["User ID"]} for _, row in users_df.iterrows()}
        
        option_selections = GroundTruthService.get_question_option_selections(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            annotator_user_ids=annotator_user_ids, session=session
        )
        
        # Get answers with confidence scores for models
        answers_df = AnnotatorService.get_question_answers(
            question_id=question_id, project_id=project_id, session=session
        )
        
        confidence_map = {}
        if not answers_df.empty:
            video_answers = answers_df[answers_df["Video ID"] == video_id]
            for _, answer_row in video_answers.iterrows():
                user_id = answer_row["User ID"]
                confidence = answer_row["Confidence Score"]
                if confidence is not None and user_id in user_lookup:
                    confidence_map[user_id] = confidence
        
        # Also get ground truth info for search portal
        gt_selection = None
        try:
            gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
            if not gt_df.empty:
                question_gt = gt_df[gt_df["Question ID"] == question_id]
                if not question_gt.empty:
                    gt_selection = question_gt.iloc[0]["Answer Value"]
        except Exception as e:
            print(f"Error getting ground truth: {e}")
            pass
        
    except Exception as e:
        # Fallback: still check for ground truth even if annotator data fails
        gt_selection = None
        option_selections = {}
        try:
            gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
            if not gt_df.empty:
                question_gt = gt_df[gt_df["Question ID"] == question_id]
                if not question_gt.empty:
                    gt_selection = question_gt.iloc[0]["Answer Value"]
        except:
            pass
        confidence_map = {}
    
    total_annotators = len(annotator_user_ids) if selected_annotators else 0
    enhanced_options = []
    
    for i, display_val in enumerate(display_values):
        actual_val = options[i] if i < len(options) else display_val
        option_text = display_val
        
        selection_info = []
        
        # Add annotator selection info if available with confidence scores for models
        if actual_val in option_selections:
            annotators = []
            for selector in option_selections[actual_val]:
                if selector["type"] != "ground_truth":  # Don't double-count GT here
                    user_id = selector.get("user_id")
                    annotator_text = selector["initials"]
                    
                    # Get user role from lookup
                    user_info = user_lookup.get(user_id, {}) if user_id else {}
                    user_role = user_info.get("role", "human")
                    
                    # Add confidence score for model users
                    if user_role == "model" and user_id in confidence_map:
                        annotator_text += f"({confidence_map[user_id]:.2f})"
                    elif user_role == "model":
                        annotator_text += "(🤖)"
                    
                    annotators.append(annotator_text)
            
            if annotators and total_annotators > 0:
                count = len(annotators)
                percentage = (count / total_annotators) * 100
                percentage_str = f"{int(percentage)}%" if percentage == int(percentage) else f"{percentage:.1f}%"
                selection_info.append(f"{percentage_str}: {', '.join(annotators)}")
            elif annotators:
                selection_info.append(f"{', '.join(annotators)}")
        
        # Add ground truth indicator if this option is the GT
        if gt_selection and str(actual_val) == str(gt_selection):
            selection_info.append("🏆 GT")
        
        if selection_info:
            option_text += f" — {' | '.join(selection_info)}"
        
        enhanced_options.append(option_text)
    
    return enhanced_options


def _display_single_answer_elegant(answer, text_key, question_text, answer_reviews, video_id, project_id, question_id, session):
    """Display a single answer with elegant left-right layout"""
    desc_col, controls_col = st.columns([2.6, 1.4])
    
    with desc_col:
        if answer['has_answer']:
            st.text_area(
                f"{answer['name']}'s Answer:",
                value=answer['full_text'],
                height=200,
                disabled=True,
                key=f"display_{video_id}_{question_id}_{answer['name'].replace(' ', '_')}",
                label_visibility="collapsed"
            )
        else:
            st.info(f"{answer['name']}: No answer provided")
    
    with controls_col:
        st.markdown(f"**{answer['name']}**")
        
        if not answer['is_gt'] and answer_reviews is not None:
            if question_text not in answer_reviews:
                answer_reviews[question_text] = {}
            
            display_name = answer['display_name']
            current_review_data = answer_reviews[question_text].get(display_name, {
                "status": "pending", "reviewer_id": None, "reviewer_name": None
            })
            
            current_status = current_review_data.get("status", "pending") if isinstance(current_review_data, dict) else current_review_data
            existing_reviewer_name = current_review_data.get("reviewer_name") if isinstance(current_review_data, dict) else None
            
            status_emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}[current_status]
            
            if current_status != "pending" and existing_reviewer_name:
                st.caption(f"**Status:** {status_emoji} {current_status.title()}")
                st.caption(f"**Reviewed by:** {existing_reviewer_name}")
            else:
                st.caption(f"**Status:** {status_emoji} {current_status.title()}")
            
            review_key = f"review_{text_key}_{answer['name'].replace(' ', '_')}_{video_id}"
            selected_status = st.segmented_control(
                "Review",
                options=["pending", "approved", "rejected"],
                format_func=lambda x: {"pending": "⏳", "approved": "✅", "rejected": "❌"}[x],
                default=current_status,
                key=review_key,
                label_visibility="collapsed"
            )
            
            answer_reviews[question_text][display_name] = selected_status
            
            if selected_status == current_status:
                if current_status == "pending":
                    st.caption("💭 Don't forget to submit your review.")
                elif existing_reviewer_name:
                    st.caption(f"📝 Click submit to override {existing_reviewer_name}'s review.")
                else:
                    st.caption("📝 Click submit to override the previous review.")
            else:
                if existing_reviewer_name:
                    st.caption(f"📝 Click submit to override {existing_reviewer_name}'s review.")
                else:
                    st.caption("📝 Click submit to save your review.")


def _submit_answer_reviews(answer_reviews: Dict, video_id: int, project_id: int, user_id: int, session: Session):
    """Submit answer reviews for annotators using proper service API"""
    for question_text, reviews in answer_reviews.items():
        for annotator_display, review_data in reviews.items():
            review_status = review_data.get("status", "pending") if isinstance(review_data, dict) else review_data
            
            if review_status in ["approved", "rejected", "pending"]:
                try:
                    annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                        display_names=[annotator_display], project_id=project_id, session=session
                    )
                    
                    if annotator_user_ids:
                        annotator_user_id = int(annotator_user_ids[0])
                        question = QuestionService.get_question_by_text(text=question_text, session=session)
                        answers_df = AnnotatorService.get_answers(video_id=video_id, project_id=project_id, session=session)
                        
                        if not answers_df.empty:
                            answer_row = answers_df[
                                (answers_df["Question ID"] == int(question["id"])) & 
                                (answers_df["User ID"] == int(annotator_user_id))
                            ]
                            
                            if not answer_row.empty:
                                answer_id = int(answer_row.iloc[0]["Answer ID"])
                                GroundTruthService.submit_answer_review(
                                    answer_id=answer_id, reviewer_id=user_id, 
                                    status=review_status, session=session
                                )
                except Exception as e:
                    print(f"Error submitting review for {annotator_display}: {e}")
                    continue


def _load_existing_answer_reviews(video_id: int, project_id: int, question_id: int, session: Session) -> Dict[str, Dict]:
    """Load existing answer reviews for a description question from the database"""
    reviews = {}
    
    try:
        selected_annotators = st.session_state.get("selected_annotators", [])
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        for user_id in annotator_user_ids:
            answers_df = AnnotatorService.get_answers(video_id=video_id, project_id=project_id, session=session)
            
            if not answers_df.empty:
                answer_row = answers_df[
                    (answers_df["Question ID"] == int(question_id)) & 
                    (answers_df["User ID"] == int(user_id))
                ]
                
                if not answer_row.empty:
                    answer_id = int(answer_row.iloc[0]["Answer ID"])
                    existing_review = GroundTruthService.get_answer_review(answer_id=answer_id, session=session)
                    
                    user_info = AuthService.get_user_info_by_id(user_id=int(user_id), session=session)
                    name_parts = user_info["user_id_str"].split()
                    initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper() if len(name_parts) >= 2 else user_info["user_id_str"][:2].upper()
                    display_name = f"{user_info['user_id_str']} ({initials})"
                    
                    if existing_review:
                        reviews[display_name] = {
                            "status": existing_review["status"],
                            "reviewer_id": existing_review.get("reviewer_id"),
                            "reviewer_name": None
                        }
                        
                        if existing_review.get("reviewer_id"):
                            try:
                                reviewer_info = AuthService.get_user_info_by_id(
                                    user_id=int(existing_review["reviewer_id"]), session=session
                                )
                                reviews[display_name]["reviewer_name"] = reviewer_info["user_id_str"]
                            except ValueError:
                                reviews[display_name]["reviewer_name"] = f"User {existing_review['reviewer_id']} (Error loading user info)"
                    else:
                        reviews[display_name] = {"status": "pending", "reviewer_id": None, "reviewer_name": None}
    except Exception as e:
        print(f"Error loading answer reviews: {e}")
    
    return reviews
