"""
Label Pizza - Modern Streamlit App
==================================
Updated with Meta-Reviewer Portal, copy/paste functionality, approve/reject buttons, improved dashboard,
NEW ACCURACY FEATURES for training mode projects, ENHANCED ANNOTATOR SELECTION, and ADVANCED SORTING/FILTERING.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Tuple, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text, select
from contextlib import contextmanager
import json
import re
import streamlit.components.v1 as components

# Import modules (adjust paths as needed)
from db import SessionLocal, engine
from models import Base
from services import (
    VideoService, ProjectService, SchemaService, QuestionService, 
    AuthService, QuestionGroupService, AnnotatorService, 
    GroundTruthService, ProjectGroupService
)

# Initialize database
Base.metadata.create_all(engine)

# Seed admin
def _seed_admin():
    with SessionLocal() as session:
        AuthService.seed_admin(session)
_seed_admin()

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
# VIDEO PLAYER WITH HEIGHT RETURN
###############################################################################

def custom_video_player(video_url, aspect_ratio="16:9", autoplay=True, loop=True):
    """Custom video player with responsive design"""
    ratio_parts = aspect_ratio.split(":")
    aspect_ratio_decimal = float(ratio_parts[0]) / float(ratio_parts[1])
    padding_bottom = (1 / aspect_ratio_decimal) * 100
    
    video_attributes = 'preload="metadata"'
    if autoplay:
        video_attributes += ' autoplay muted'
    if loop:
        video_attributes += ' loop'
    
    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            html, body {{ height: 100%; font-family: Arial, sans-serif; overflow: hidden; }}
            
            .video-container {{
                width: 100%; height: 100%; display: flex; flex-direction: column;
                background: #fff; overflow: hidden;
            }}
            
            .video-wrapper {{
                position: relative; width: 100%; flex: 1; background: #000;
                border-radius: 8px 8px 0 0; overflow: hidden; min-height: 200px;
            }}
            
            .video-wrapper::before {{
                content: ''; display: block; padding-bottom: {padding_bottom}%;
            }}
            
            video {{
                position: absolute; top: 0; left: 0; width: 100%; height: 100%;
                object-fit: contain;
            }}
            
            video::-webkit-media-controls, video::-moz-media-controls {{
                display: none !important;
            }}
            
            .controls-container {{
                width: 100%; background: #f8f9fa; border: 1px solid #e9ecef;
                border-top: none; border-radius: 0 0 8px 8px; padding: 8px 12px;
                flex-shrink: 0; overflow: hidden; min-height: 65px; max-height: 80px;
            }}
            
            .progress-container {{
                width: 100%; height: 6px; background: #ddd; border-radius: 3px;
                margin-bottom: 8px; cursor: pointer; position: relative;
                user-select: none; overflow: hidden;
            }}
            
            .progress-bar {{
                height: 100%; background: linear-gradient(90deg, #ff4444, #ff6666);
                border-radius: 3px; width: 0%; pointer-events: none; transition: none;
            }}
            
            .progress-handle {{
                position: absolute; top: -5px; width: 16px; height: 16px;
                background: #ff4444; border: 2px solid white; border-radius: 50%;
                cursor: grab; transform: translateX(-50%); opacity: 0;
                transition: opacity 0.2s ease, transform 0.1s ease;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }}
            
            .progress-handle:active {{ cursor: grabbing; transform: translateX(-50%) scale(1.1); }}
            .progress-container:hover .progress-handle {{ opacity: 1; }}
            
            .controls {{
                display: flex; align-items: center; gap: 6px; width: 100%;
                overflow: hidden; min-height: 32px;
            }}
            
            .control-btn {{
                background: none; border: none; font-size: 14px; cursor: pointer;
                padding: 4px 6px; border-radius: 4px; transition: background 0.2s ease;
                display: flex; align-items: center; justify-content: center;
                min-width: 28px; height: 28px; flex-shrink: 0;
            }}
            
            .control-btn:hover {{ background: #e9ecef; }}
            
            .time-display {{
                font-size: 11px; color: #666; margin-left: auto; white-space: nowrap;
                font-family: 'Courier New', monospace; flex-shrink: 0;
                overflow: hidden; text-overflow: ellipsis; max-width: 120px;
            }}
            
            .volume-control {{ display: flex; align-items: center; gap: 4px; flex-shrink: 0; }}
            
            .volume-slider {{
                width: 50px; height: 3px; background: #ddd; outline: none;
                border-radius: 2px; -webkit-appearance: none; flex-shrink: 0;
            }}
            
            .volume-slider::-webkit-slider-thumb {{
                -webkit-appearance: none; width: 12px; height: 12px;
                background: #ff4444; border-radius: 50%; cursor: pointer;
            }}
            
            .volume-slider::-moz-range-thumb {{
                width: 12px; height: 12px; background: #ff4444;
                border-radius: 50%; cursor: pointer; border: none;
            }}
            
            @media (max-width: 600px) {{
                .controls {{ gap: 4px; }}
                .control-btn {{ font-size: 12px; min-width: 24px; height: 24px; padding: 2px 4px; }}
                .time-display {{ font-size: 10px; max-width: 80px; }}
                .volume-slider {{ width: 40px; }}
                .controls-container {{ padding: 6px 8px; min-height: 60px; }}
                .progress-container {{ height: 5px; margin-bottom: 6px; }}
            }}
        </style>
    </head>
    <body>
        <div class="video-container">
            <div class="video-wrapper">
                <video id="customVideo" {video_attributes}>
                    <source src="{video_url}" type="video/mp4">
                    Your browser does not support the video tag.
                </video>
            </div>
            
            <div class="controls-container">
                <div class="progress-container" id="progressContainer">
                    <div class="progress-bar" id="progressBar"></div>
                    <div class="progress-handle" id="progressHandle"></div>
                </div>
                
                <div class="controls">
                    <button class="control-btn" id="playPauseBtn" title="Play/Pause">{"⏸️" if autoplay else "▶️"}</button>
                    <button class="control-btn" id="muteBtn" title="Mute/Unmute">🔊</button>
                    <div class="volume-control">
                        <input type="range" class="volume-slider" id="volumeSlider" min="0" max="100" value="100" title="Volume">
                    </div>
                    <div class="time-display" id="timeDisplay">0:00 / 0:00</div>
                    <button class="control-btn" id="fullscreenBtn" title="Fullscreen">⛶</button>
                </div>
            </div>
        </div>

        <script>
            const video = document.getElementById('customVideo');
            const playPauseBtn = document.getElementById('playPauseBtn');
            const muteBtn = document.getElementById('muteBtn');
            const volumeSlider = document.getElementById('volumeSlider');
            const progressContainer = document.getElementById('progressContainer');
            const progressBar = document.getElementById('progressBar');
            const progressHandle = document.getElementById('progressHandle');
            const timeDisplay = document.getElementById('timeDisplay');
            const fullscreenBtn = document.getElementById('fullscreenBtn');

            let isDragging = false;
            let wasPlaying = false;
            const isAutoplay = {str(autoplay).lower()};
            
            if (isAutoplay) {{
                video.addEventListener('loadeddata', () => {{
                    if (video.paused) {{
                        video.play().catch(e => console.log('Autoplay prevented:', e));
                    }}
                    playPauseBtn.textContent = video.paused ? '▶️' : '⏸️';
                }});
            }}

            playPauseBtn.addEventListener('click', () => {{
                if (video.paused) {{
                    video.play();
                    playPauseBtn.textContent = '⏸️';
                }} else {{
                    video.pause();
                    playPauseBtn.textContent = '▶️';
                }}
            }});

            muteBtn.addEventListener('click', () => {{
                video.muted = !video.muted;
                muteBtn.textContent = video.muted ? '🔇' : '🔊';
            }});

            volumeSlider.addEventListener('input', () => {{
                video.volume = volumeSlider.value / 100;
            }});

            function updateProgress() {{
                if (!isDragging && video.duration) {{
                    const progress = (video.currentTime / video.duration) * 100;
                    progressBar.style.width = progress + '%';
                    progressHandle.style.left = progress + '%';
                    
                    const currentTime = formatTime(video.currentTime);
                    const duration = formatTime(video.duration);
                    timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                }}
                requestAnimationFrame(updateProgress);
            }}
            
            updateProgress();

            function getProgressFromMouse(e) {{
                const rect = progressContainer.getBoundingClientRect();
                return Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
            }}

            progressContainer.addEventListener('mousedown', (e) => {{
                isDragging = true;
                wasPlaying = !video.paused;
                if (wasPlaying) video.pause();
                
                const percent = getProgressFromMouse(e);
                const newTime = percent * video.duration;
                
                progressBar.style.width = (percent * 100) + '%';
                progressHandle.style.left = (percent * 100) + '%';
                video.currentTime = newTime;
                
                const currentTime = formatTime(newTime);
                const duration = formatTime(video.duration);
                timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                
                e.preventDefault();
            }});

            document.addEventListener('mousemove', (e) => {{
                if (isDragging) {{
                    const percent = getProgressFromMouse(e);
                    const newTime = percent * video.duration;
                    
                    progressBar.style.width = (percent * 100) + '%';
                    progressHandle.style.left = (percent * 100) + '%';
                    video.currentTime = newTime;
                    
                    const currentTime = formatTime(newTime);
                    const duration = formatTime(video.duration);
                    timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                }}
            }});

            document.addEventListener('mouseup', () => {{
                if (isDragging) {{
                    isDragging = false;
                    if (wasPlaying) video.play();
                }}
            }});

            progressContainer.addEventListener('click', (e) => {{
                if (!isDragging) {{
                    const percent = getProgressFromMouse(e);
                    video.currentTime = percent * video.duration;
                }}
            }});

            fullscreenBtn.addEventListener('click', () => {{
                if (document.fullscreenElement) {{
                    document.exitFullscreen();
                }} else {{
                    document.querySelector('.video-wrapper').requestFullscreen();
                }}
            }});

            function formatTime(time) {{
                if (isNaN(time)) return '0:00';
                const minutes = Math.floor(time / 60);
                const seconds = Math.floor(time % 60);
                return `${{minutes}}:${{seconds.toString().padStart(2, '0')}}`;
            }}

            video.addEventListener('ended', () => {{ playPauseBtn.textContent = '▶️'; }});
            video.addEventListener('play', () => {{ playPauseBtn.textContent = '⏸️'; }});
            video.addEventListener('pause', () => {{ playPauseBtn.textContent = '▶️'; }});

            video.addEventListener('loadedmetadata', () => {{
                const duration = formatTime(video.duration);
                timeDisplay.textContent = `0:00 / ${{duration}}`;
            }});
        </script>
    </body>
    </html>
    """
    
    estimated_width = 420
    video_height = estimated_width / aspect_ratio_decimal
    controls_height = 90
    total_height = max(500, min(700, int(video_height + controls_height)))
    
    components.html(html_code, height=total_height, scrolling=False)
    return total_height

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
# ACCURACY DISPLAY FUNCTIONS
###############################################################################

def get_accuracy_color(accuracy: float) -> str:
    """Get color based on accuracy percentage"""
    if accuracy >= 90: return COLORS['success']
    elif accuracy >= 80: return COLORS['warning']
    elif accuracy >= 70: return "#fd7e14"  # Orange
    else: return COLORS['danger']

def format_accuracy_badge(accuracy: Optional[float], total_questions: int = 0) -> str:
    """Format accuracy as a colored badge"""
    if accuracy is None:
        return "📊 No data"
    
    color = get_accuracy_color(accuracy)
    questions_text = f" ({total_questions} answers)" if total_questions > 0 else ""
    
    return f'<span style="background: {color}; color: white; padding: 4px 8px; border-radius: 12px; font-weight: bold; font-size: 0.85rem;">📊 {accuracy:.1f}%{questions_text}</span>'

@st.dialog("📊 Your Personal Accuracy Report", width="large")
def show_personal_annotator_accuracy(user_id: int, project_id: int, session: Session):
    """Show detailed personal accuracy report for a single annotator"""
    try:
        accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
        if user_id not in accuracy_data:
            st.warning("No accuracy data available for your account.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        user_info = AuthService.get_user_info_by_id(user_id=int(user_id), session=session)
        questions = ProjectService.get_project_questions(project_id=int(project_id), session=session)
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        user_name = user_info["user_id_str"]
        accuracy = overall_accuracy.get(user_id)
        
        if accuracy is None:
            st.warning("No accuracy data available.")
            return
        
        st.markdown(f"### 🎯 {user_name}'s Performance Report")
        
        color = get_accuracy_color(accuracy)
        total_correct = sum(stats["correct"] for stats in accuracy_data[user_id].values())
        total_answers = sum(stats["total"] for stats in accuracy_data[user_id].values())
        
        st.markdown(f"""
        <div style="{get_card_style(color)}text-align: center;">
            <h2 style="color: {color}; margin: 0;">🏆 Overall Accuracy: {accuracy:.1f}%</h2>
            <p style="margin: 10px 0 0 0; font-size: 1.1rem;">
                You answered {total_correct} out of {total_answers} questions correctly
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Performance level feedback
        if accuracy >= 90:
            st.success("🌟 **Excellent Performance!** You're doing great!")
        elif accuracy >= 80:
            st.info("👍 **Good Performance!** Keep up the good work!")
        elif accuracy >= 70:
            st.warning("📈 **Room for Improvement** - You're getting there!")
        else:
            st.error("📚 **Keep Learning** - Practice makes perfect!")
        
        st.markdown("---")
        st.markdown("### 📋 Question-by-Question Breakdown")
        
        question_details = []
        for question_id, question_text in question_lookup.items():
            user_accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
            stats = accuracy_data[user_id].get(question_id, {"total": 0, "correct": 0})
            
            if stats["total"] > 0:
                status = ("✅ Correct" if user_accuracy == 100 else 
                         "❌ Incorrect" if user_accuracy == 0 else 
                         f"⭐ {user_accuracy:.1f}%")
                
                question_details.append({
                    "Question": question_text[:80] + ("..." if len(question_text) > 80 else ""),
                    "Your Performance": status,
                    "Score": f"{stats['correct']}/{stats['total']}"
                })
        
        if question_details:
            df = pd.DataFrame(question_details)
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download your accuracy report",
                data=csv,
                file_name=f"my_accuracy_report_project_{project_id}.csv",
                mime="text/csv"
            )
        else:
            st.info("No detailed question data available.")
    
    except Exception as e:
        st.error(f"Error loading your accuracy data: {str(e)}")

@st.dialog("📊 Your Reviewer Accuracy Report", width="large")  
def show_personal_reviewer_accuracy(user_id: int, project_id: int, session: Session):
    """Show detailed personal accuracy report for a single reviewer"""
    try:
        accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
        if user_id not in accuracy_data:
            st.warning("No reviewer accuracy data available for your account.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        user_info = AuthService.get_user_info_by_id(user_id=int(user_id), session=session)
        questions = ProjectService.get_project_questions(project_id=int(project_id), session=session)
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        user_name = user_info["user_id_str"]
        accuracy = overall_accuracy.get(user_id)
        
        if accuracy is None:
            st.warning("No accuracy data available.")
            return
        
        st.markdown(f"### 🎯 {user_name}'s Reviewer Performance Report")
        st.caption("Accuracy is measured by how many of your ground truth answers were NOT overridden by admin")
        
        color = get_accuracy_color(accuracy)
        total_correct = sum(stats["correct"] for stats in accuracy_data[user_id].values())
        total_reviews = sum(stats["total"] for stats in accuracy_data[user_id].values())
        
        st.markdown(f"""
        <div style="{get_card_style(color)}text-align: center;">
            <h2 style="color: {color}; margin: 0;">🏆 Reviewer Accuracy: {accuracy:.1f}%</h2>
            <p style="margin: 10px 0 0 0; font-size: 1.1rem;">
                {total_correct} out of {total_reviews} reviews were not overridden
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Performance level feedback
        if accuracy >= 90:
            st.success("🌟 **Excellent Reviewing!** Your ground truth selections are very reliable!")
        elif accuracy >= 80:
            st.info("👍 **Good Reviewing!** Most of your decisions align with final ground truth!")
        elif accuracy >= 70:
            st.warning("📈 **Room for Improvement** - Some of your reviews were adjusted by admin.")
        else:
            st.error("📚 **Review Carefully** - Consider the guidelines more carefully.")
        
        st.markdown("---")
        st.markdown("### 📋 Question-by-Question Review Performance")
        
        question_details = []
        for question_id, question_text in question_lookup.items():
            user_accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
            stats = accuracy_data[user_id].get(question_id, {"total": 0, "correct": 0})
            
            if stats["total"] > 0:
                status = ("✅ Not Overridden" if user_accuracy == 100 else 
                         "❌ Overridden by Admin" if user_accuracy == 0 else 
                         f"⭐ {user_accuracy:.1f}%")
                
                question_details.append({
                    "Question": question_text[:80] + ("..." if len(question_text) > 80 else ""),
                    "Admin Override Status": status,
                    "Reviews": f"{stats['correct']}/{stats['total']} kept"
                })
        
        if question_details:
            df = pd.DataFrame(question_details)
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download your reviewer report",
                data=csv,
                file_name=f"my_reviewer_report_project_{project_id}.csv",
                mime="text/csv"
            )
        else:
            st.info("No detailed review data available.")
    
    except Exception as e:
        st.error(f"Error loading your reviewer accuracy data: {str(e)}")

def display_user_accuracy_simple(user_id: int, project_id: int, role: str, session: Session) -> bool:
    """Display simple accuracy for a single user if project is in training mode"""
    if not check_project_has_full_ground_truth(project_id=project_id, session=session):
        return False
    
    try:
        if role == "annotator":
            overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
            if overall_progress < 100:
                return False
            
            accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
            if user_id not in accuracy_data:
                return False
            
            overall_accuracy_dict = calculate_overall_accuracy(accuracy_data)
            accuracy = overall_accuracy_dict.get(user_id)
            
            if accuracy is not None:
                total_answered = sum(stats["total"] for stats in accuracy_data[user_id].values())
                accuracy_badge = format_accuracy_badge(accuracy, total_answered)
                
                acc_col1, acc_col2 = st.columns([2, 1])
                with acc_col1:
                    st.markdown(f"**Your Training Accuracy:** {accuracy_badge}", unsafe_allow_html=True)
                with acc_col2:
                    if st.button("📋 View Details", key=f"personal_acc_{user_id}_{project_id}", 
                                help="View detailed accuracy breakdown"):
                        show_personal_annotator_accuracy(user_id=user_id, project_id=project_id, session=session)
                return True
        
        elif role == "reviewer":
            accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
            if user_id not in accuracy_data:
                return False
            
            overall_accuracy_dict = calculate_overall_accuracy(accuracy_data)
            accuracy = overall_accuracy_dict.get(user_id)
            
            if accuracy is not None:
                total_reviewed = sum(stats["total"] for stats in accuracy_data[user_id].values())
                accuracy_badge = format_accuracy_badge(accuracy, total_reviewed)
                
                acc_col1, acc_col2 = st.columns([2, 1])
                with acc_col1:
                    st.markdown(f"**Your Reviewer Accuracy:** {accuracy_badge}", unsafe_allow_html=True)
                with acc_col2:
                    if st.button("📋 View Details", key=f"personal_rev_acc_{user_id}_{project_id}", 
                                help="View detailed reviewer accuracy breakdown"):
                        show_personal_reviewer_accuracy(user_id=user_id, project_id=project_id, session=session)
                return True
        
        elif role == "meta_reviewer":
            return False
    
    except Exception:
        pass
    
    return False

@st.dialog("📊 Detailed Accuracy Analytics", width="large")
def show_annotator_accuracy_detailed(project_id: int, session: Session):
    """Show detailed annotator accuracy analytics in a modal dialog"""
    try:
        accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
        if not accuracy_data:
            st.warning("No annotator accuracy data available for this project.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        users_df = AuthService.get_all_users(session=session)
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        
        user_lookup = {row["ID"]: row["User ID"] for _, row in users_df.iterrows()}
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        overview_tab, detailed_tab, comparison_tab = st.tabs(["📈 Overview", "🔍 Per Question", "⚖️ Compare"])
        
        with overview_tab:
            st.markdown("### 📊 Overall Accuracy Rankings")
            
            overview_data = []
            for user_id, accuracy in overall_accuracy.items():
                if accuracy is not None:
                    user_name = user_lookup.get(user_id, f"User {user_id}")
                    total_answered = sum(stats["total"] for stats in accuracy_data[user_id].values())
                    overview_data.append({
                        "Annotator": user_name,
                        "Overall Accuracy": accuracy,
                        "Total Answers": total_answered,
                        "User ID": user_id
                    })
            
            if overview_data:
                overview_data.sort(key=lambda x: x["Overall Accuracy"], reverse=True)
                
                for i, data in enumerate(overview_data):
                    rank = i + 1
                    accuracy = data["Overall Accuracy"]
                    color = get_accuracy_color(accuracy)
                    
                    rank_emoji = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                    
                    st.markdown(f"""
                    <div style="{get_card_style(color)}display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>{rank_emoji} {data['Annotator']}</strong>
                            <br><small>{data['Total Answers']} total answers</small>
                        </div>
                        <div style="background: {color}; color: white; padding: 8px 16px; border-radius: 20px; font-weight: bold; font-size: 1.1rem;">
                            {accuracy:.1f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("### 📈 Summary Statistics")
                accuracies = [data["Overall Accuracy"] for data in overview_data]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Accuracy", f"{sum(accuracies)/len(accuracies):.1f}%")
                    st.metric("Highest Accuracy", f"{max(accuracies):.1f}%")
                with col2:
                    st.metric("Lowest Accuracy", f"{min(accuracies):.1f}%")
                    st.metric("Total Annotators", len(overview_data))
            else:
                st.info("No accuracy data available to display.")
        
        with detailed_tab:
            st.markdown("### 🔍 Per-Question Accuracy Analysis")
            
            user_options = {user_lookup.get(uid, f"User {uid}"): uid for uid in accuracy_data.keys()}
            if user_options:
                selected_users = st.multiselect(
                    "Select annotators to analyze:",
                    list(user_options.keys()),
                    default=list(user_options.keys())[:5] if len(user_options) > 5 else list(user_options.keys())
                )
                
                if selected_users:
                    selected_user_ids = [user_options[name] for name in selected_users]
                    
                    detailed_data = []
                    for question_id, question_text in question_lookup.items():
                        row = {"Question": question_text[:60] + ("..." if len(question_text) > 60 else "")}
                        
                        for user_name in selected_users:
                            user_id = user_options[user_name]
                            accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
                            row[f"{user_name}"] = f"{accuracy:.1f}%" if accuracy is not None else "No data"
                        
                        detailed_data.append(row)
                    
                    if detailed_data:
                        df = pd.DataFrame(detailed_data)
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download detailed accuracy data",
                            data=csv,
                            file_name=f"annotator_accuracy_project_{project_id}.csv",
                            mime="text/csv"
                        )
            else:
                st.info("No annotators found with accuracy data.")
        
        with comparison_tab:
            st.markdown("### ⚖️ Compare Selected Annotators")
            
            user_options = {user_lookup.get(uid, f"User {uid}"): uid for uid in accuracy_data.keys()}
            if user_options:
                compare_users = st.multiselect(
                    "Select annotators to compare (max 5):",
                    list(user_options.keys()),
                    max_selections=5
                )
                
                if len(compare_users) >= 2:
                    compare_user_ids = [user_options[name] for name in compare_users]
                    
                    selected_accuracies = [overall_accuracy.get(uid) for uid in compare_user_ids if overall_accuracy.get(uid) is not None]
                    
                    if selected_accuracies:
                        avg_accuracy = sum(selected_accuracies) / len(selected_accuracies)
                        st.metric("Average Accuracy of Selected Annotators", f"{avg_accuracy:.1f}%")
                        
                        st.markdown("#### Individual Performance:")
                        for user_name in compare_users:
                            user_id = user_options[user_name]
                            accuracy = overall_accuracy.get(user_id)
                            if accuracy is not None:
                                diff_from_avg = accuracy - avg_accuracy
                                color = get_accuracy_color(accuracy)
                                
                                diff_icon = "📈" if diff_from_avg > 0 else "📉" if diff_from_avg < 0 else "➡️"
                                diff_text = f"({diff_from_avg:+.1f}% vs group avg)"
                                
                                st.markdown(f"""
                                <div style="background: linear-gradient(135deg, {color}15, {color}05); border-left: 4px solid {color}; padding: 10px 15px; margin: 5px 0; border-radius: 5px;">
                                    <strong>{user_name}</strong>: 
                                    <span style="color: {color}; font-weight: bold;">{accuracy:.1f}%</span>
                                    <small style="color: #666; margin-left: 10px;">{diff_icon} {diff_text}</small>
                                </div>
                                """, unsafe_allow_html=True)
                else:
                    st.info("Select at least 2 annotators to compare.")
            else:
                st.info("No annotators available for comparison.")
    
    except Exception as e:
        st.error(f"Error loading accuracy data: {str(e)}")

@st.dialog("📊 Reviewer Accuracy Analytics", width="large")
def show_reviewer_accuracy_detailed(project_id: int, session: Session):
    """Show detailed reviewer accuracy analytics in a modal dialog"""
    try:
        accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
        if not accuracy_data:
            st.warning("No reviewer accuracy data available for this project.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        users_df = AuthService.get_all_users(session=session)
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        
        user_lookup = {row["ID"]: row["User ID"] for _, row in users_df.iterrows()}
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        overview_tab, detailed_tab = st.tabs(["📈 Overview", "🔍 Per Question"])
        
        with overview_tab:
            st.markdown("### 📊 Reviewer Accuracy (Questions Not Overridden by Admin)")
            st.caption("Accuracy is measured by how many of a reviewer's ground truth answers were NOT overridden by admin")
            
            overview_data = []
            for user_id, accuracy in overall_accuracy.items():
                if accuracy is not None:
                    user_name = user_lookup.get(user_id, f"User {user_id}")
                    total_reviewed = sum(stats["total"] for stats in accuracy_data[user_id].values())
                    correct_count = sum(stats["correct"] for stats in accuracy_data[user_id].values())
                    overview_data.append({
                        "Reviewer": user_name,
                        "Accuracy": accuracy,
                        "Correct Answers": correct_count,
                        "Total Reviews": total_reviewed,
                        "User ID": user_id
                    })
            
            if overview_data:
                overview_data.sort(key=lambda x: x["Accuracy"], reverse=True)
                
                for i, data in enumerate(overview_data):
                    rank = i + 1
                    accuracy = data["Accuracy"]
                    color = get_accuracy_color(accuracy)
                    
                    rank_emoji = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                    
                    st.markdown(f"""
                    <div style="{get_card_style(color)}display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>{rank_emoji} {data['Reviewer']}</strong>
                            <br><small>{data['Correct Answers']}/{data['Total Reviews']} not overridden</small>
                        </div>
                        <div style="background: {color}; color: white; padding: 8px 16px; border-radius: 20px; font-weight: bold; font-size: 1.1rem;">
                            {accuracy:.1f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("### 📈 Summary Statistics")
                accuracies = [data["Accuracy"] for data in overview_data]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Accuracy", f"{sum(accuracies)/len(accuracies):.1f}%")
                    st.metric("Highest Accuracy", f"{max(accuracies):.1f}%")
                with col2:
                    st.metric("Lowest Accuracy", f"{min(accuracies):.1f}%")
                    st.metric("Total Reviewers", len(overview_data))
            else:
                st.info("No reviewer accuracy data available to display.")
        
        with detailed_tab:
            st.markdown("### 🔍 Per-Question Reviewer Analysis")
            
            user_options = {user_lookup.get(uid, f"User {uid}"): uid for uid in accuracy_data.keys()}
            if user_options:
                selected_users = st.multiselect(
                    "Select reviewers to analyze:",
                    list(user_options.keys()),
                    default=list(user_options.keys())
                )
                
                if selected_users:
                    detailed_data = []
                    for question_id, question_text in question_lookup.items():
                        row = {"Question": question_text[:60] + ("..." if len(question_text) > 60 else "")}
                        
                        for user_name in selected_users:
                            user_id = user_options[user_name]
                            accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
                            row[f"{user_name}"] = f"{accuracy:.1f}%" if accuracy is not None else "No data"
                        
                        detailed_data.append(row)
                    
                    if detailed_data:
                        df = pd.DataFrame(detailed_data)
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download detailed reviewer accuracy data",
                            data=csv,
                            file_name=f"reviewer_accuracy_project_{project_id}.csv",
                            mime="text/csv"
                        )
            else:
                st.info("No reviewers found with accuracy data.")
    
    except Exception as e:
        st.error(f"Error loading reviewer accuracy data: {str(e)}")

def display_accuracy_button_for_project(project_id: int, role: str, session: Session):
    """Display an elegant accuracy analytics button for training mode projects"""
    if not check_project_has_full_ground_truth(project_id=project_id, session=session):
        return False
    
    try:
        if role in ["reviewer", "meta_reviewer"]:
            accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
            if accuracy_data:
                annotator_count = len(accuracy_data)
                if st.button(f"📊 Annotator Analytics ({annotator_count})", 
                           key=f"accuracy_btn_{project_id}_{role}",
                           help=f"View detailed accuracy analytics for {annotator_count} annotators",
                           use_container_width=True):
                    show_annotator_accuracy_detailed(project_id=project_id, session=session)
                return True
            
            reviewer_accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
            if reviewer_accuracy_data:
                reviewer_count = len(reviewer_accuracy_data)
                if st.button(f"📊 Reviewer Analytics ({reviewer_count})", 
                           key=f"reviewer_accuracy_btn_{project_id}_{role}",
                           help=f"View detailed accuracy analytics for {reviewer_count} reviewers",
                           use_container_width=True):
                    show_reviewer_accuracy_detailed(project_id=project_id, session=session)
                return True
    except Exception:
        pass
    
    return False

###############################################################################
# AUTHENTICATION & ROUTING
###############################################################################

def login_page():
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1 style="color: #1f77b4; margin-bottom: 0.5rem;">🍕 Label Pizza</h1>
                <p style="color: #6c757d; font-size: 1.1rem; margin: 0;">Welcome back! Please sign in to your account</p>
            </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            st.markdown("### Sign In")
            
            email = st.text_input("Email Address", placeholder="Enter your email address", help="Use your registered email address")
            password = st.text_input("Password", type="password", placeholder="Enter your password", help="Enter your account password")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            submitted = st.form_submit_button("🚀 Sign In", use_container_width=True, type="primary")
            
            if submitted:
                authenticate_user(email, password)
        
        st.markdown("<br><br>", unsafe_allow_html=True)

def get_user_display_name(user_name: str, user_email: str) -> str:
    """Create display name for multiselect like 'Zhiqiu Lin (ZL)'"""
    name_parts = user_name.split()
    initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper() if len(name_parts) >= 2 else user_name[:2].upper()
    return f"{user_name} ({initials})"

def authenticate_user(email: str, password: str):
    if not email or not password:
        st.error("Please enter both email and password")
        return
        
    try:
        with get_db_session() as session:
            user = (AuthService.authenticate(email=email, pwd=password, role="admin", session=session) or
                   AuthService.authenticate(email=email, pwd=password, role="human", session=session))
            
            if user:
                if 'email' not in user or not user['email']:
                    user['email'] = email
                    
                st.session_state.user = user
                st.session_state.user_projects = get_user_projects(user_id=user["id"], session=session)
                st.session_state.available_portals = get_available_portals(user=user, user_projects=st.session_state.user_projects)
                st.session_state.current_view = "dashboard"
                st.session_state.selected_project_id = None
                st.success(f"Welcome {user['name']}!")
                st.rerun()
            else:
                st.error("Invalid email or password")
    except Exception as e:
        st.error(f"Login failed: {str(e)}")

def get_user_projects(user_id: int, session: Session) -> Dict:
    """Get projects assigned to user by role - using ONLY service layer"""
    try:
        user_projects = AuthService.get_user_projects_by_role(user_id=user_id, session=session)
        
        if user_projects.get("admin"):
            all_projects_df = ProjectService.get_all_projects(session=session)
            all_projects_list = [
                {"id": project_row["ID"], "name": project_row["Name"], "description": "", "created_at": None}
                for _, project_row in all_projects_df.iterrows()
            ]
            
            return {
                "annotator": all_projects_list.copy(),
                "reviewer": all_projects_list.copy(),
                "admin": all_projects_list.copy(),
                "meta_reviewer": all_projects_list.copy()
            }
        
        return user_projects
        
    except ValueError as e:
        st.error(f"Error getting user projects: {str(e)}")
        return {"annotator": [], "reviewer": [], "admin": []}

def get_available_portals(user: Dict, user_projects: Dict) -> List[str]:
    """Determine which portals the user can access"""
    if user["role"] == "admin":
        return ["annotator", "reviewer", "meta_reviewer", "search", "admin"]
    
    available_portals = []
    if user_projects.get("annotator"):
        available_portals.append("annotator")
    if user_projects.get("reviewer"):
        available_portals.append("reviewer")
    
    return available_portals

###############################################################################
# SEARCH PORTAL - COMPLETELY REDESIGNED FOR ADMIN WITH CLEAN UI/UX
###############################################################################

@handle_database_errors
def search_portal():
    """Advanced Search Portal for Admins - Clean, Organized, Functional"""
    st.title("🔍 Advanced Search Portal")
    st.markdown("**Comprehensive search and editing capabilities across all projects**")
    
    # Clean tab design with clear separation
    main_tabs = st.tabs([
        "🎬 Video Answer Search", 
        "📊 Video Criteria Search",
        "📋 Bulk Operations"
    ])
    
    with main_tabs[0]:
        video_answer_search_portal()
    
    with main_tabs[1]:
        video_criteria_search_portal()
    
    with main_tabs[2]:
        bulk_operations_portal()

###############################################################################
# VIDEO ANSWER SEARCH PORTAL
###############################################################################

@st.fragment
def video_answer_search_portal():
    """Search and edit all answers for a specific video with improved UI"""
    
    st.markdown("## 🎬 Video Answer Search & Editor")
    st.markdown("*Find and edit all answers for any video across all project groups*")
    
    with get_db_session() as session:
        # Improved Step 1: Video Selection Section
        video_info = display_improved_video_selection_section(session)
        
        if not video_info:
            return
        
        st.markdown("---")
        
        # Improved Step 2: Project Group Filter Section  
        project_group_filter = display_improved_project_group_filter_section(session)
        
        st.markdown("---")
        
        # Improved Step 3: Main Video + Answers Layout
        display_improved_video_answers_editor(video_info, project_group_filter, session)

def display_improved_video_selection_section(session: Session) -> Optional[Dict[str, Any]]:
    """Improved video selection interface with better styling"""
    
    st.markdown("### 📹 Step 1: Select Video")
    
    # Get videos with better organization
    videos_df = VideoService.get_all_videos(session=session)
    if videos_df.empty:
        st.warning("🚫 No videos available in the system")
        return None
    
    # Improved filter controls with consistent heights - ALL using st.selectbox for uniform styling
    col1, col2, col3 = st.columns([3, 2, 2])
    
    with col1:
        search_term = st.text_input(
            "🔍 Search videos by UID", 
            placeholder="Type video UID to filter...",
            key="admin_video_search",
            help="Search through video UIDs to find the one you want"
        )
    
    with col2:
        # Use selectbox to match height with text input
        include_archived = st.selectbox(
            "📦 Archive Filter",
            ["Active videos only", "Include archived videos"],
            key="include_archived_videos_select",
            help="Choose whether to show archived videos"
        ) == "Include archived videos"
    
    with col3:
        total_videos = len(videos_df)
        active_videos = len(videos_df[~videos_df["Archived"]])
        
        # Use selectbox for consistent height - display as info
        if include_archived:
            info_display = st.selectbox(
                "📊 Video Count",
                [f"Total: {total_videos} ({active_videos} active + {total_videos - active_videos} archived)"],
                key="video_count_display",
                disabled=True,
                help="Currently showing all videos including archived ones"
            )
        else:
            info_display = st.selectbox(
                "📊 Video Count", 
                [f"Active: {active_videos}" + (f" ({total_videos - active_videos} archived hidden)" if total_videos > active_videos else " (no archived)")],
                key="video_count_display",
                disabled=True,
                help="Currently showing only active videos"
            )
    
    # Apply filters
    filtered_videos = videos_df
    if not include_archived:
        filtered_videos = videos_df[~videos_df["Archived"]]
    
    if search_term:
        filtered_videos = filtered_videos[
            filtered_videos["Video UID"].str.contains(search_term, case=False, na=False)
        ]
    
    if filtered_videos.empty:
        st.warning("🔍 No videos match your search criteria")
        return None
    
    # Improved video selection
    st.markdown("**📋 Select Video:**")
    
    video_options = {}
    for _, row in filtered_videos.iterrows():
        created_date = row["Created At"].strftime("%m/%d/%Y") if pd.notna(row["Created At"]) else "Unknown"
        status_emoji = "📦" if row["Archived"] else "📹"
        display_name = f"{status_emoji} {row['Video UID']} • {created_date}"
        video_options[display_name] = row["Video UID"]
    
    # Better selectbox presentation
    if len(video_options) > 20:
        st.info(f"📊 Showing {len(video_options)} videos (use search to narrow results)")
    
    selected_video_display = st.selectbox(
        "Choose from available videos:",
        [""] + list(video_options.keys()),
        key="selected_video_admin",
        help=f"Select from {len(video_options)} available videos",
        label_visibility="collapsed"
    )
    
    if not selected_video_display:
        st.info("👆 Please select a video from the dropdown above to continue")
        return None
    
    # Get video info
    selected_video_uid = video_options[selected_video_display]
    video_info = get_video_info_by_uid(selected_video_uid, session)
    
    if not video_info:
        st.error("❌ Error loading video information")
        return None
    
    # Improved selected video display
    st.markdown(f"""
    <div style="{get_card_style('#28a745')}text-align: center;">
        <div style="color: #155724; font-weight: 600; font-size: 1rem;">
            ✅ Selected: <strong>{video_info['uid']}</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    return video_info

def display_improved_project_group_filter_section(session: Session) -> List[int]:
    """Improved project group filtering interface"""
    
    st.markdown("### 🗂️ Step 2: Configure Project Group Filters")
    
    try:
        project_groups = ProjectGroupService.list_project_groups(session=session)
        
        if not project_groups:
            st.warning("🚫 No project groups found")
            return []
        
        # Initialize with all groups selected
        if "admin_selected_groups" not in st.session_state:
            st.session_state.admin_selected_groups = [g.id for g in project_groups]
        
        # Improved quick actions
        action_col1, action_col2, action_col3 = st.columns([1, 1, 2])
        
        with action_col1:
            if st.button("✅ Select All", key="admin_select_all_groups", use_container_width=True):
                st.session_state.admin_selected_groups = [g.id for g in project_groups]
                st.rerun(scope="fragment")
        
        with action_col2:
            if st.button("❌ Clear All", key="admin_clear_all_groups", use_container_width=True):
                st.session_state.admin_selected_groups = []
                st.rerun(scope="fragment")
        
        with action_col3:
            selected_count = len(st.session_state.admin_selected_groups)
            total_count = len(project_groups)
            
            # Use metric for consistent display
            st.metric(
                label="📊 Selected Groups", 
                value=f"{selected_count}/{total_count}",
                delta="All selected" if selected_count == total_count else f"{total_count - selected_count} remaining"
            )
        
        st.markdown("**📋 Select Project Groups:**")
        
        # Improved group selection grid
        num_cols = min(2, len(project_groups))  # Use 2 columns for better readability
        cols = st.columns(num_cols)
        
        updated_selections = []
        
        for i, group in enumerate(project_groups):
            with cols[i % num_cols]:
                # Get project count
                try:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=group.id, session=session)
                    project_count = len(group_info["projects"])
                except:
                    project_count = 0
                
                is_selected = group.id in st.session_state.admin_selected_groups
                
                # Improved checkbox design
                checkbox_value = st.checkbox(
                    f"**{group.name}**",
                    value=is_selected,
                    key=f"admin_group_cb_{group.id}",
                    help=f"Description: {group.description or 'No description'}"
                )
                
                # Better project group info card
                card_color = "#e7f3ff" if is_selected else "#f8f9fa"
                border_color = "#2196f3" if is_selected else "#e9ecef"
                
                st.markdown(f"""
                <div style="background: {card_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 12px; margin: 8px 0;">
                    <div style="color: #1976d2; font-weight: 600; font-size: 0.9rem; margin-bottom: 6px;">
                        📁 {project_count} project{'s' if project_count != 1 else ''}
                    </div>
                    <div style="color: #424242; font-size: 0.85rem; line-height: 1.4;">
                        {group.description if group.description else '<em>No description provided</em>'}
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                if checkbox_value:
                    updated_selections.append(group.id)
        
        # Update selections if changed
        if set(updated_selections) != set(st.session_state.admin_selected_groups):
            st.session_state.admin_selected_groups = updated_selections
            st.rerun(scope="fragment")
        
        return st.session_state.admin_selected_groups
        
    except Exception as e:
        st.error(f"❌ Error loading project groups: {str(e)}")
        return []

def display_improved_video_answers_editor(video_info: Dict[str, Any], selected_group_ids: List[int], session: Session):
    """Improved video + answers editor layout"""
    
    st.markdown("### 🎯 Step 3: Video Player & Ground Truth Editor")
    
    if not selected_group_ids:
        st.warning("⚠️ Please select at least one project group in Step 2 to see results")
        return
    
    # Get all ground truth for this video
    gt_data = get_video_ground_truth_across_groups(video_info["id"], selected_group_ids, session)
    
    # Improved video player section
    display_improved_video_player_section(video_info)
    
    st.markdown("---")
    
    # Improved project groups display
    display_improved_project_groups_section(gt_data, video_info, session)

def display_improved_video_player_section(video_info: Dict[str, Any]):
    """Improved video player section"""
    
    st.markdown("#### 📹 Video Player")
    
    # Improved video info card
    created_at = video_info["created_at"].strftime('%Y-%m-%d %H:%M') if video_info["created_at"] else 'Unknown'
    
    st.markdown(f"""
    <div style="{get_card_style('#2196f3')}">
        <div style="color: #1565c0; font-weight: 700; font-size: 1.1rem; margin-bottom: 8px;">
            📹 {video_info["uid"]}
        </div>
        <div style="color: #1976d2; font-size: 0.85rem; margin-bottom: 6px;">
            <strong>URL:</strong> <code style="background: rgba(255,255,255,0.8); padding: 2px 6px; border-radius: 4px; font-size: 0.8rem;">{video_info["url"]}</code>
        </div>
        <div style="color: #1976d2; font-size: 0.85rem;">
            <strong>Created:</strong> {created_at}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Video player
    custom_video_player(video_info["url"], autoplay=False, loop=True)

def display_improved_project_groups_section(gt_data: Dict, video_info: Dict[str, Any], session: Session):
    """Improved project groups display with better organization"""
    
    if not gt_data:
        st.info("📭 No projects found with questions in the selected project groups")
        return
    
    # Enhanced summary stats
    total_projects = sum(len(group_data["projects"]) for group_data in gt_data.values())
    total_groups = len(gt_data)
    
    # Count total question groups across all projects
    total_qg = 0
    for group_data in gt_data.values():
        for project_data in group_data["projects"].values():
            total_qg += len(project_data.get("question_groups", {}))
    
    summary_col1, summary_col2, summary_col3 = st.columns(3)
    
    with summary_col1:
        st.metric("📁 Project Groups", total_groups)
    
    with summary_col2:
        st.metric("📂 Projects", total_projects)
    
    with summary_col3:
        st.metric("❓ Question Groups", total_qg)
    
    # Display each project group with improved styling
    for group_id, group_data in gt_data.items():
        group_name = group_data["group_name"]
        projects = group_data["projects"]
        project_count = len(projects)
        
        # Improved project group header
        st.markdown(f"""
        <div style="{get_card_style('#3498db')}text-align: center; margin: 20px 0;">
            <div style="color: #2980b9; font-weight: 700; font-size: 1.2rem; margin-bottom: 4px;">
                📁 {group_name}
            </div>
            <div style="color: #2980b9; font-size: 0.9rem;">
                {project_count} project{'s' if project_count != 1 else ''} with questions
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Display projects with improved styling
        display_improved_projects_as_expanders(group_data, video_info, session)

def display_improved_projects_as_expanders(group_data: Dict, video_info: Dict[str, Any], session: Session):
    """Display projects with improved expander styling"""
    
    projects = group_data["projects"]
    
    if not projects:
        st.info("📭 No projects with questions in this group")
        return
    
    current_user = st.session_state.user
    user_id = current_user["id"]
    
    # Display each project with enhanced info
    for project_id, project_data in projects.items():
        project_name = project_data["project_name"]
        project_description = project_data.get("project_description", "")
        
        # Count question groups and get completion status
        question_groups = project_data.get("question_groups", {})
        qg_count = len(question_groups)
        
        # Calculate completion status WITH QUESTION GROUP NAMES
        completed_qg = 0
        completion_details = []
        
        for qg_id, qg_data in question_groups.items():
            qg_title = qg_data.get("title", f"Question Group {qg_id}")
            has_gt = any(q_data.get("ground_truth") is not None for q_data in qg_data.get("questions", {}).values())
            
            if has_gt:
                completed_qg += 1
                completion_details.append(f"✅ {qg_title}")
            else:
                completion_details.append(f"⏳ {qg_title}")
        
        # Enhanced project label with detailed status like other portals
        completion_icon = "✅" if completed_qg == qg_count and qg_count > 0 else "⏳" if completed_qg > 0 else "📝"
        
        if qg_count > 0:
            # Show individual question group status like other portals
            detailed_progress = " | ".join(completion_details)
            project_label = f"{completion_icon} {project_name} • {detailed_progress}"
        else:
            project_label = f"{completion_icon} {project_name} • No question groups"
        
        with st.expander(project_label, expanded=(completed_qg == 0)):  # Expand incomplete projects by default
            if project_description:
                st.markdown(f"*{project_description}*")
            
            # Show project stats
            if qg_count > 1:
                progress_percent = (completed_qg / qg_count * 100) if qg_count > 0 else 0
                st.progress(progress_percent / 100)
                st.caption(f"📊 Overall Progress: {completed_qg}/{qg_count} question groups completed ({progress_percent:.0f}%)")
            
            display_project_question_groups_with_tabs(
                project_data, project_id, video_info, user_id, session
            )

def display_project_question_groups_with_tabs(project_data: Dict, project_id: int, video_info: Dict[str, Any], user_id: int, session: Session):
    """Display question groups using tabs exactly like other portals"""
    
    question_groups = project_data.get("question_groups", {})
    
    if not question_groups:
        st.info("📭 No question groups found in this project")
        return
    
    # Convert to list format like other portals
    qg_list = []
    for qg_id, qg_data in question_groups.items():
        qg_list.append({
            "ID": qg_id,
            "Title": qg_data["title"],
            "Description": qg_data.get("description", "")
        })
    
    # Use tabs for question groups EXACTLY like other portals
    if len(qg_list) > 1:
        qg_tab_names = [group['Title'] for group in qg_list]
        qg_tabs = st.tabs(qg_tab_names)
        
        for qg_tab, group in zip(qg_tabs, qg_list):
            with qg_tab:
                display_single_question_group_for_search(
                    video_info, project_id, user_id, group["ID"], 
                    question_groups[group["ID"]], session
                )
    else:
        # Single question group
        group = qg_list[0]
        st.markdown(f"### ❓ {group['Title']}")
        if group.get('Description'):
            st.caption(group['Description'])
        
        display_single_question_group_for_search(
            video_info, project_id, user_id, group["ID"], 
            question_groups[group["ID"]], session
        )

def display_single_question_group_for_search(video_info: Dict, project_id: int, user_id: int, qg_id: int, qg_data: Dict, session: Session):
    """Display single question group with proper portal-consistent messaging"""
    
    # Determine the correct role and button text
    try:
        gt_status = determine_ground_truth_status(video_info["id"], project_id, qg_id, user_id, session)
    except Exception as e:
        st.error(f"Error determining ground truth status: {str(e)}")
        # Create fallback form
        with st.form(f"error_gt_status_{video_info['id']}_{qg_id}_{project_id}"):
            st.error("Could not determine ground truth status")
            st.form_submit_button("Unable to Load", disabled=True)
        return
    
    # Show mode messages exactly like other portals
    if gt_status["role"] == "meta_reviewer":
        st.warning("🎯 **Meta-Reviewer Mode** - Override ground truth answers as needed.")
    else:  # reviewer or reviewer_resubmit
        st.info("🔍 **Review Mode** - Help create the ground truth dataset!")
    
    # Get questions from service
    try:
        service_questions = QuestionService.get_questions_by_group_id(group_id=qg_id, session=session)
        
        if not service_questions:
            st.info("No questions found in this group")
            # Create empty form with submit button to prevent error
            with st.form(f"empty_form_{video_info['id']}_{qg_id}_{project_id}"):
                st.info("No questions available")
                st.form_submit_button("No Actions Available", disabled=True)
            return
        
        # Get existing answers to populate form
        existing_answers = {}
        try:
            existing_answers = GroundTruthService.get_ground_truth_for_question_group(
                video_id=video_info["id"], project_id=project_id, question_group_id=qg_id, session=session
            )
        except Exception as e:
            st.warning(f"Could not load existing answers: {str(e)}")
            existing_answers = {}
        
        # Create form with unique key INCLUDING project_id to prevent duplicates
        form_key = f"search_form_{video_info['id']}_{qg_id}_{project_id}_{gt_status['role']}_{user_id}"
        
        # Ensure form is always created
        try:
            with st.form(form_key):
                answers = {}
                
                # Content height (same as other portals)
                content_height = 500
                
                # Wrap question display in try/catch
                try:
                    with st.container(height=content_height, border=False):
                        for i, question in enumerate(service_questions):
                            question_id = question["id"]
                            question_text = question["text"]
                            existing_value = existing_answers.get(question_text, "")
                            
                            if i > 0:
                                st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                            
                            # Check admin modification status
                            is_modified_by_admin = False
                            admin_info = None
                            if gt_status["role"] == "meta_reviewer":
                                try:
                                    is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                        video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                                    )
                                    if is_modified_by_admin:
                                        admin_info = GroundTruthService.get_admin_modification_details(
                                            video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                                        )
                                except Exception:
                                    is_modified_by_admin = False
                                    admin_info = None
                            
                            # Use the EXACT same question display functions as other portals
                            display_role = "meta_reviewer" if gt_status["role"] == "meta_reviewer" else "reviewer"
                            
                            # Pass None for selected_annotators in search portal (we don't show annotator info)
                            try:
                                if question["type"] == "single":
                                    answers[question_text] = _display_clean_sticky_single_choice_question(
                                        question, video_info["id"], project_id, display_role, existing_value,
                                        is_modified_by_admin, admin_info, False, session,
                                        "", "", None  # No gt_value, mode, or selected_annotators for search
                                    )
                                else:
                                    answers[question_text] = _display_clean_sticky_description_question(
                                        question, video_info["id"], project_id, display_role, existing_value,
                                        is_modified_by_admin, admin_info, False, session,
                                        "", "", None, None  # No gt_value, mode, answer_reviews, or selected_annotators for search
                                    )
                            except Exception as e:
                                st.error(f"Error displaying question {question_id}: {str(e)}")
                                # Provide fallback answer
                                answers[question_text] = existing_value
                except Exception as e:
                    st.error(f"Error displaying questions: {str(e)}")
                    # Still provide empty answers
                    answers = {}
                
                st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
                
                # ALWAYS include submit button
                submitted = st.form_submit_button(gt_status["button_text"], use_container_width=True, type="primary")
                
                if submitted:
                    try:
                        if gt_status["role"] == "meta_reviewer":
                            GroundTruthService.override_ground_truth_to_question_group(
                                video_id=video_info["id"], project_id=project_id, 
                                question_group_id=qg_id, admin_id=user_id, 
                                answers=answers, session=session
                            )
                            st.success("✅ Ground truth overridden successfully!")
                        else:  # reviewer or reviewer_resubmit
                            GroundTruthService.submit_ground_truth_to_question_group(
                                video_id=video_info["id"], project_id=project_id, 
                                reviewer_id=user_id, question_group_id=qg_id, 
                                answers=answers, session=session
                            )
                            success_msg = "✅ Ground truth re-submitted successfully!" if gt_status["role"] == "reviewer_resubmit" else "✅ Ground truth submitted successfully!"
                            st.success(success_msg)
                        
                        st.rerun(scope="fragment")
                        
                    except Exception as e:
                        st.error(f"❌ Error saving ground truth: {str(e)}")
        
        except Exception as e:
            st.error(f"Error creating form: {str(e)}")
            # Create emergency fallback form
            with st.form(f"emergency_form_{video_info['id']}_{qg_id}_{project_id}_{user_id}"):
                st.error("Could not create proper form")
                st.form_submit_button("Form Creation Failed", disabled=True)
                    
    except Exception as e:
        st.error(f"❌ Error loading question group: {str(e)}")
        # Create empty form with submit button to prevent error
        with st.form(f"error_form_{video_info['id']}_{qg_id}_{project_id}_{user_id}"):
            st.error("Failed to load questions")
            st.form_submit_button("Unable to Load", disabled=True)

def display_single_question_group_like_other_portals(video_info: Dict, project_id: int, user_id: int, qg_id: int, qg_data: Dict, session: Session, group_id: int):
    """Display single question group exactly like other portals with proper GT logic"""
    
    # Determine the correct role and button text based on GT status
    gt_status = determine_ground_truth_status(int(video_info["id"]), int(project_id), int(qg_id), int(user_id), session)
    
    # Show appropriate mode message (same as other portals)
    if gt_status["role"] == "meta_reviewer":
        st.warning(f"🎯 **Admin Override Mode** - {gt_status['message']}")
    elif gt_status["role"] == "reviewer_resubmit":
        st.info(f"🔄 **Re-submit Mode** - {gt_status['message']}")
    else:
        st.info(f"🔍 **Ground Truth Creation Mode** - {gt_status['message']}")
    
    # Get questions from service
    try:
        service_questions = QuestionService.get_questions_by_group_id(group_id=qg_id, session=session)
        
        if not service_questions:
            st.info("No questions found in this group")
            return
        
        # Get existing answers to populate form
        existing_answers = {}
        try:
            existing_answers = GroundTruthService.get_ground_truth_for_question_group(
                video_id=video_info["id"], project_id=project_id, question_group_id=qg_id, session=session
            )
        except:
            pass
        
        # Create form with unique key
        form_key = f"search_form_{video_info['id']}_{qg_id}_{project_id}_{group_id}_{gt_status['role']}"
        
        with st.form(form_key):
            answers = {}
            
            # Content height (same as other portals)
            content_height = 500
            
            with st.container(height=content_height, border=False):
                for i, question in enumerate(service_questions):
                    question_id = question["id"]
                    question_text = question["text"]
                    existing_value = existing_answers.get(question_text, "")
                    
                    if i > 0:
                        st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                    
                    # Check admin modification status
                    is_modified_by_admin = False
                    admin_info = None
                    if gt_status["role"] == "meta_reviewer":
                        is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                            video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                        )
                        if is_modified_by_admin:
                            admin_info = GroundTruthService.get_admin_modification_details(
                                video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                            )
                    
                    # Use the EXACT same question display functions as other portals
                    display_role = "meta_reviewer" if gt_status["role"] == "meta_reviewer" else "reviewer"
                    
                    if question["type"] == "single":
                        answers[question_text] = _display_clean_sticky_single_choice_question(
                            question, video_info["id"], project_id, display_role, existing_value,
                            is_modified_by_admin, admin_info, False, session
                        )
                    else:
                        answers[question_text] = _display_clean_sticky_description_question(
                            question, video_info["id"], project_id, display_role, existing_value,
                            is_modified_by_admin, admin_info, False, session, "", ""
                        )
            
            # Margin exactly like other portals
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            # Submit button with exact same styling as other portals
            submitted = st.form_submit_button(gt_status["button_text"], use_container_width=True, type="primary")
            
            if submitted:
                try:
                    if gt_status["role"] == "meta_reviewer":
                        GroundTruthService.override_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            question_group_id=qg_id, admin_id=user_id, 
                            answers=answers, session=session
                        )
                        st.success("✅ Ground truth overridden successfully!")
                    else:  # reviewer or reviewer_resubmit
                        GroundTruthService.submit_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            reviewer_id=user_id, question_group_id=qg_id, 
                            answers=answers, session=session
                        )
                        success_msg = "✅ Ground truth re-submitted successfully!" if gt_status["role"] == "reviewer_resubmit" else "✅ Ground truth set successfully!"
                        st.success(success_msg)
                    
                    st.rerun(scope="fragment")
                    
                except Exception as e:
                    st.error(f"❌ Error saving ground truth: {str(e)}")
                    
    except Exception as e:
        st.error(f"❌ Error loading question group: {str(e)}")

def display_admin_project_group_gt_editor_with_fixed_height(group_data: Dict, video_info: Dict[str, Any], session: Session, group_id: int):
    """Display project group GT editor with fixed height container and unique form keys"""
    
    projects = group_data["projects"]
    
    if not projects:
        st.info("📭 No projects with questions in this group")
        return
    
    # Get current user for admin operations
    current_user = st.session_state.user
    user_id = current_user["id"]
    
    # Fixed height container for this project group (same as other portals)
    container_height = 600
    
    with st.container(height=container_height, border=True):
        # Use tabs for projects within this group (but now inside fixed container)
        if len(projects) > 1:
            project_names = [data["project_name"] for data in projects.values()]
            project_tabs = st.tabs([f"📂 {name}" for name in project_names])
            
            for tab, (project_id, project_data) in zip(project_tabs, projects.items()):
                with tab:
                    display_admin_project_with_proper_logic(
                        project_data, project_id, video_info, user_id, session, group_id
                    )
        else:
            # Single project
            project_id, project_data = next(iter(projects.items()))
            st.markdown(f"**📂 Project:** {project_data['project_name']}")
            if project_data.get("project_description"):
                st.caption(project_data['project_description'])
            
            display_admin_project_with_proper_logic(
                project_data, project_id, video_info, user_id, session, group_id
            )


def display_admin_project_with_proper_logic(project_data: Dict, project_id: int, video_info: Dict[str, Any], user_id: int, session: Session, group_id: int):
    """Display project with proper ground truth logic matching your requirements"""
    
    question_groups = project_data.get("question_groups", {})
    
    if not question_groups:
        st.info("📭 No question groups found in this project")
        return
    
    # Display each question group
    for qg_id, qg_data in question_groups.items():
        qg_title = qg_data["title"]
        qg_description = qg_data.get("description", "")
        
        st.markdown(f"### ❓ {qg_title}")
        if qg_description:
            st.caption(qg_description)
        
        # Determine the correct role and button text based on GT status
        gt_status = determine_ground_truth_status(int(video_info["id"]), int(project_id), int(qg_id), int(user_id), session)
        
        # Show appropriate mode message
        if gt_status["role"] == "meta_reviewer":
            st.warning(f"🎯 **Admin Override Mode** - {gt_status['message']}")
        elif gt_status["role"] == "reviewer_resubmit":
            st.info(f"🔄 **Re-submit Mode** - {gt_status['message']}")
        else:
            st.info(f"🔍 **Ground Truth Creation Mode** - {gt_status['message']}")
        
        # Get questions from service
        try:
            service_questions = QuestionService.get_questions_by_group_id(group_id=qg_id, session=session)
            
            if not service_questions:
                st.info("No questions found in this group")
                continue
            
            # Get existing answers to populate form
            existing_answers = {}
            try:
                existing_answers = GroundTruthService.get_ground_truth_for_question_group(
                    video_id=video_info["id"], project_id=project_id, question_group_id=qg_id, session=session
                )
            except:
                pass
            
            # Create form with unique key including group_id to avoid conflicts
            form_key = f"search_form_{video_info['id']}_{qg_id}_{project_id}_{group_id}_{gt_status['role']}"
            
            with st.form(form_key):
                answers = {}
                
                # Content area (smaller since we're in a container already)
                content_height = 400
                
                with st.container(height=content_height, border=False):
                    for i, question in enumerate(service_questions):
                        question_id = question["id"]
                        question_text = question["text"]
                        existing_value = existing_answers.get(question_text, "")
                        
                        if i > 0:
                            st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                        
                        # Check admin modification status
                        is_modified_by_admin = False
                        admin_info = None
                        if gt_status["role"] == "meta_reviewer":
                            is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                            )
                            if is_modified_by_admin:
                                admin_info = GroundTruthService.get_admin_modification_details(
                                    video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                                )
                        
                        # Use the exact same question display functions as other portals
                        display_role = "meta_reviewer" if gt_status["role"] == "meta_reviewer" else "reviewer"
                        
                        if question["type"] == "single":
                            answers[question_text] = _display_clean_sticky_single_choice_question(
                                question, video_info["id"], project_id, display_role, existing_value,
                                is_modified_by_admin, admin_info, False, session
                            )
                        else:
                            answers[question_text] = _display_clean_sticky_description_question(
                                question, video_info["id"], project_id, display_role, existing_value,
                                is_modified_by_admin, admin_info, False, session, "", ""
                            )
                
                st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
                
                # Submit button with correct text based on status
                submitted = st.form_submit_button(gt_status["button_text"], use_container_width=True, type="primary")
                
                if submitted:
                    try:
                        if gt_status["role"] == "meta_reviewer":
                            GroundTruthService.override_ground_truth_to_question_group(
                                video_id=video_info["id"], project_id=project_id, 
                                question_group_id=qg_id, admin_id=user_id, 
                                answers=answers, session=session
                            )
                            st.success("✅ Ground truth overridden successfully!")
                        else:  # reviewer or reviewer_resubmit
                            GroundTruthService.submit_ground_truth_to_question_group(
                                video_id=video_info["id"], project_id=project_id, 
                                reviewer_id=user_id, question_group_id=qg_id, 
                                answers=answers, session=session
                            )
                            success_msg = "✅ Ground truth re-submitted successfully!" if gt_status["role"] == "reviewer_resubmit" else "✅ Ground truth set successfully!"
                            st.success(success_msg)
                        
                        st.rerun(scope="fragment")
                        
                    except Exception as e:
                        st.error(f"❌ Error saving ground truth: {str(e)}")
                        
        except Exception as e:
            st.error(f"❌ Error loading question group: {str(e)}")
        
        # Add separator between question groups
        if len(question_groups) > 1:
            st.markdown("---")

def determine_ground_truth_status(video_id: int, project_id: int, question_group_id: int, user_id: int, session: Session) -> Dict[str, str]:
    """Determine the correct role and button text - simplified"""
    
    try:
        # Check if ground truth exists for this question group
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
        
        if gt_df.empty or not questions:
            return {
                "role": "reviewer",
                "button_text": "Submit Ground Truth",
                "message": "Create Ground Truth for this question group"
            }
        
        # Check if any questions in this group have ground truth
        question_ids = [q["id"] for q in questions]
        group_gt = gt_df[gt_df["Question ID"].isin(question_ids)]
        
        if group_gt.empty:
            return {
                "role": "reviewer", 
                "button_text": "Submit Ground Truth",
                "message": "Create Ground Truth for this question group"
            }
        
        # Check if any questions were modified by admin
        has_admin_override = any(
            GroundTruthService.check_question_modified_by_admin(
                video_id=int(video_id), project_id=int(project_id), question_id=int(qid), session=session
            )
            for qid in question_ids
        )
        
        if has_admin_override:
            return {
                "role": "meta_reviewer",
                "button_text": "🎯 Override Ground Truth",
                "message": "Override Ground Truth for this question group"
            }
        
        # Check if current user is the original reviewer
        original_reviewer_ids = set(group_gt["Reviewer ID"].unique())
        if user_id in original_reviewer_ids:
            return {
                "role": "reviewer_resubmit",
                "button_text": "✅ Re-submit Ground Truth",
                "message": "Re-submit Ground Truth for this question group"
            }
        
        # Ground truth exists but user is not original reviewer
        return {
            "role": "meta_reviewer",
            "button_text": "🎯 Override Ground Truth",
            "message": "Override Ground Truth for this question group"
        }
        
    except Exception as e:
        return {
            "role": "reviewer",
            "button_text": "Submit Ground Truth",
            "message": "Create Ground Truth for this question group"
        }

def _display_unified_status(video_id: int, project_id: int, question_id: int, session: Session, show_annotators: bool = False, selected_annotators: List[str] = None):
    """Display ground truth status and optionally annotator status in single line"""
    
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
            
            # Find which annotators have answered this specific question/video
            annotators_with_answers = set()
            if not answers_df.empty:
                video_answers = answers_df[answers_df["Video ID"] == video_id]
                for _, answer_row in video_answers.iterrows():
                    user_id = answer_row["User ID"]
                    if user_id in annotator_user_ids:
                        users_df = AuthService.get_all_users(session=session)
                        user_info = users_df[users_df["ID"] == user_id]
                        if not user_info.empty:
                            annotators_with_answers.add(user_info.iloc[0]["User ID"])
            
            # Get initials for present and missing annotators
            present_initials = []
            missing_initials = []
            
            for display_name in selected_annotators:
                if " (" in display_name and display_name.endswith(")"):
                    name = display_name.split(" (")[0]
                    initials = display_name.split(" (")[1][:-1]
                else:
                    name = display_name
                    initials = display_name[:2].upper()
                
                if name in annotators_with_answers:
                    present_initials.append(initials)
                else:
                    missing_initials.append(initials)
            
            # Add annotator status parts
            if present_initials:
                status_parts.append(f"📊 Showing: {', '.join(present_initials)}")
            if missing_initials:
                status_parts.append(f"⚠️ Missing: {', '.join(missing_initials)}")
                
        except Exception:
            status_parts.append("⚠️ Could not load annotator status")
    
    try:
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        
        if not gt_df.empty:
            # Make sure question_id is the right type
            question_id_int = int(question_id)
            question_gt = gt_df[gt_df["Question ID"] == question_id_int]
            
            if not question_gt.empty:
                gt_row = question_gt.iloc[0]
                
                try:
                    reviewer_info = AuthService.get_user_info_by_id(user_id=int(gt_row["Reviewer ID"]), session=session)
                    
                    name_parts = reviewer_info["user_id_str"].split()
                    initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper() if len(name_parts) >= 2 else reviewer_info["user_id_str"][:2].upper()
                    
                    modified_by_admin = gt_row["Modified By Admin"] is not None
                    
                    if modified_by_admin:
                        status_parts.append(f"🏆 GT by: {initials} (Admin)")
                    else:
                        status_parts.append(f"🏆 GT by: {initials}")
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

def display_video_answers_editor(video_info: Dict[str, Any], selected_group_ids: List[int], session: Session):
    """Single column layout for search portal - cleaner design"""
    
    st.markdown("### 🎯 Step 3: Video Player & Ground Truth Editor")
    
    if not selected_group_ids:
        st.warning("⚠️ Please select at least one project group in Step 2 to see results")
        return
    
    # Get all ground truth for this video
    gt_data = get_video_ground_truth_across_groups(video_info["id"], selected_group_ids, session)
    
    # Single column layout - video at top, then project groups
    display_video_player_section(video_info)
    
    st.markdown("---")
    
    display_project_groups_as_expanders(gt_data, video_info, session)

def display_project_groups_as_expanders(gt_data: Dict, video_info: Dict[str, Any], session: Session):
    """Display project groups with projects as expanders (no nested expanders)"""
    
    if not gt_data:
        st.info("📭 No projects found with questions in the selected project groups")
        return
    
    # Summary stats
    total_projects = sum(len(group_data["projects"]) for group_data in gt_data.values())
    total_groups = len(gt_data)
    
    st.markdown(f"""
    <div style="{get_card_style(COLORS['info'])}text-align: center;">
        <div style="color: #2980b9; font-weight: 600; font-size: 1rem;">
            📊 Found <strong>{total_projects} projects</strong> across <strong>{total_groups} project groups</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display each project group with headers (no expanders for groups)
    for group_id, group_data in gt_data.items():
        group_name = group_data["group_name"]
        projects = group_data["projects"]
        project_count = len(projects)
        
        # Project group header (not an expander)
        st.markdown(f"""
        <div style="{get_card_style('#3498db')}text-align: center; margin: 20px 0;">
            <div style="color: #2980b9; font-weight: 700; font-size: 1.2rem;">
                📁 {group_name} ({project_count} projects)
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Each project as an expander (no nesting)
        display_projects_as_expanders_only(group_data, video_info, session)

def display_projects_as_expanders_only(group_data: Dict, video_info: Dict[str, Any], session: Session):
    """Display each project as its own expander (no nested expanders)"""
    
    projects = group_data["projects"]
    
    if not projects:
        st.info("📭 No projects with questions in this group")
        return
    
    # Get current user for admin operations
    current_user = st.session_state.user
    user_id = current_user["id"]
    
    # Each project as its own expander
    for project_id, project_data in projects.items():
        project_name = project_data["project_name"]
        project_description = project_data.get("project_description", "")
        
        # Count question groups for display
        question_groups = project_data.get("question_groups", {})
        qg_count = len(question_groups)
        
        project_label = f"📂 {project_name}"
        if qg_count > 0:
            project_label += f" ({qg_count} question groups)"
        
        with st.expander(project_label, expanded=False):
            if project_description:
                st.caption(project_description)
            
            display_project_question_groups_with_tabs(
                project_data, project_id, video_info, user_id, session
            )

def display_projects_as_expanders(group_data: Dict, video_info: Dict[str, Any], session: Session):
    """Display each project as its own expander"""
    
    projects = group_data["projects"]
    
    if not projects:
        st.info("📭 No projects with questions in this group")
        return
    
    # Get current user for admin operations
    current_user = st.session_state.user
    user_id = current_user["id"]
    
    # Each project as its own expander
    for project_id, project_data in projects.items():
        project_name = project_data["project_name"]
        project_description = project_data.get("project_description", "")
        
        # Count question groups for display
        question_groups = project_data.get("question_groups", {})
        qg_count = len(question_groups)
        
        project_label = f"📂 {project_name}"
        if qg_count > 0:
            project_label += f" ({qg_count} question groups)"
        
        with st.expander(project_label, expanded=False):
            if project_description:
                st.caption(project_description)
            
            display_project_question_groups_with_tabs(
                project_data, project_id, video_info, user_id, session
            )

def get_video_ground_truth_across_groups(video_id: int, selected_group_ids: List[int], session: Session) -> Dict:
    """Get only ground truth for a video across selected project groups"""
    
    results = {}
    
    for group_id in selected_group_ids:
        try:
            # Get group info
            group_info = ProjectGroupService.get_project_group_by_id(group_id=group_id, session=session)
            group_name = group_info["group"].name
            projects = group_info["projects"]
            
            group_results = {"group_name": group_name, "projects": {}}
            
            for project in projects:
                # Check if video is in this project
                project_videos = VideoService.get_project_videos(project_id=project.id, session=session)
                video_in_project = any(v["id"] == video_id for v in project_videos)
                
                if not video_in_project:
                    continue
                
                # Get project ground truth only
                project_results = get_project_ground_truth_for_video(video_id, project.id, session)
                
                if project_results["has_data"]:
                    group_results["projects"][project.id] = {
                        "project_name": project.name,
                        "project_description": project.description,
                        **project_results
                    }
            
            if group_results["projects"]:
                results[group_id] = group_results
                
        except Exception as e:
            st.error(f"Error processing group {group_id}: {str(e)}")
            continue
    
    return results

def get_project_ground_truth_for_video(video_id: int, project_id: int, session: Session) -> Dict:
    """Get only ground truth for a video in a specific project"""
    
    try:
        # Get project info
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        
        # Get schema question groups
        question_groups = SchemaService.get_schema_question_groups_list(
            schema_id=project.schema_id, session=session
        )
        
        project_results = {
            "has_data": False,
            "question_groups": {}
        }
        
        for group in question_groups:
            group_id = group["ID"]
            group_title = group["Title"]
            
            # Get questions in this group
            questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
            
            group_data = {
                "title": group_title,
                "description": group["Description"],
                "questions": {}
            }
            
            has_group_data = False
            
            for question in questions:
                question_id = question["id"]
                question_text = question["text"]
                question_type = question["type"]
                
                question_data = {
                    "text": question_text,
                    "type": question_type,
                    "ground_truth": None
                }
                
                # Get ground truth only
                try:
                    gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                    
                    if not gt_df.empty:
                        gt_row = gt_df[gt_df["Question ID"] == question_id]
                        
                        if not gt_row.empty:
                            gt_data = gt_row.iloc[0]
                            reviewer_info = AuthService.get_user_info_by_id(
                                user_id=int(gt_data["Reviewer ID"]), session=session
                            )
                            
                            question_data["ground_truth"] = {
                                "answer_value": gt_data["Answer Value"],
                                "original_value": gt_data["Original Value"],
                                "reviewer_name": reviewer_info["user_id_str"],
                                "modified_by_admin": gt_data["Modified By Admin"] is not None,
                                "created_at": gt_data["Created At"],
                                "modified_at": gt_data["Modified At"]
                            }
                            has_group_data = True
                except Exception:
                    pass
                
                # Always include question even if no GT yet (for editing)
                group_data["questions"][question_id] = question_data
                if not has_group_data and len(questions) > 0:
                    has_group_data = True  # Include groups that have questions even without GT
            
            if has_group_data:
                project_results["question_groups"][group_id] = group_data
                project_results["has_data"] = True
        
        return project_results
        
    except Exception as e:
        st.error(f"Error getting project ground truth: {str(e)}")
        return {"has_data": False, "question_groups": {}}

def display_video_player_section(video_info: Dict[str, Any]):
    """Clean video player section matching other portals exactly"""
    
    st.markdown("#### 📹 Video Player")
    
    # Video info card (keep same styling as other portals)
    created_at = video_info["created_at"].strftime('%Y-%m-%d %H:%M') if video_info["created_at"] else 'Unknown'
    
    st.markdown(f"""
    <div style="{get_card_style('#2196f3')}">
        <div style="color: #1565c0; font-weight: 700; font-size: 1.1rem; margin-bottom: 12px;">
            📹 {video_info["uid"]}
        </div>
        <div style="color: #1976d2; font-size: 0.9rem; margin-bottom: 8px;">
            <strong>URL:</strong> <span style="font-family: monospace; background: rgba(255,255,255,0.8); padding: 2px 6px; border-radius: 4px;">{video_info["url"]}</span>
        </div>
        <div style="color: #1976d2; font-size: 0.9rem;">
            <strong>Created:</strong> {created_at}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Video player (same as other portals)
    custom_video_player(video_info["url"], autoplay=True, loop=True)

def display_answers_editor_section(answers_data: Dict, video_info: Dict[str, Any], session: Session):
    """Clean answers editor using the same layout patterns as other portals"""
    
    st.markdown("#### 📝 Answers & Ground Truth Editor")
    
    # Summary stats
    total_projects = sum(len(group_data["projects"]) for group_data in answers_data.values())
    total_groups = len(answers_data)
    
    st.markdown(f"""
    <div style="{get_card_style(COLORS['success'])}text-align: center;">
        <div style="color: #1e8449; font-weight: 600;">
            📊 Found data in <strong>{total_projects} projects</strong> across <strong>{total_groups} project groups</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Use the same approach as other portals - tabs for top level organization
    if len(answers_data) > 1:
        group_names = [group_data["group_name"] for group_data in answers_data.values()]
        group_tabs = st.tabs([f"📁 {name}" for name in group_names])
        
        for tab, (group_id, group_data) in zip(group_tabs, answers_data.items()):
            with tab:
                display_admin_project_group_editor(group_data, video_info, session)
    else:
        # Single group - show directly
        group_data = next(iter(answers_data.values()))
        st.markdown(f"### 📁 {group_data['group_name']}")
        display_admin_project_group_editor(group_data, video_info, session)

def display_admin_project_group_editor(group_data: Dict, video_info: Dict[str, Any], session: Session):
    """Display project group using the same patterns as other portals"""
    
    projects = group_data["projects"]
    
    if not projects:
        st.info("📭 No projects with data in this group")
        return
    
    # Use the same project selection approach as other portals
    if len(projects) > 1:
        project_names = [data["project_name"] for data in projects.values()]
        project_tabs = st.tabs([f"📂 {name}" for name in project_names])
        
        for tab, (project_id, project_data) in zip(project_tabs, projects.items()):
            with tab:
                display_admin_project_editor(project_data, project_id, video_info, session)
    else:
        # Single project
        project_id, project_data = next(iter(projects.items()))
        st.markdown(f"**📂 Project:** {project_data['project_name']}")
        if project_data.get("project_description"):
            st.caption(project_data['project_description'])
        
        display_admin_project_editor(project_data, project_id, video_info, session)

def display_admin_project_editor(project_data: Dict, project_id: int, video_info: Dict[str, Any], session: Session):
    """Display project editor using the same layout as other portals"""
    
    question_groups = project_data.get("question_groups", {})
    
    if not question_groups:
        st.info("📭 No question groups with data in this project")
        return
    
    # Get current user for admin operations
    current_user = st.session_state.user
    user_id = current_user["id"]
    
    # Convert our data structure to match what the other portals expect
    # Create a list of question groups similar to how other portals organize them
    qg_list = []
    for qg_id, qg_data in question_groups.items():
        qg_list.append({
            "ID": qg_id,
            "Title": qg_data["title"],
            "Description": qg_data.get("description", "")
        })
    
    # Use tabs for question groups like other portals
    if len(qg_list) > 1:
        qg_tab_names = [group['Title'] for group in qg_list]
        qg_tabs = st.tabs(qg_tab_names)
        
        for tab, group in zip(qg_tabs, qg_list):
            with tab:
                # Determine admin role and display the question group
                display_admin_question_group_editor(
                    video_info, project_id, user_id, group["ID"], 
                    question_groups[group["ID"]], session
                )
    else:
        # Single question group
        group = qg_list[0]
        st.markdown(f"### ❓ {group['Title']}")
        if group.get('Description'):
            st.caption(group['Description'])
        
        display_admin_question_group_editor(
            video_info, project_id, user_id, group["ID"], 
            question_groups[group["ID"]], session
        )

def display_admin_question_group_editor(video_info: Dict, project_id: int, user_id: int, group_id: int, qg_data: Dict, session: Session):
    """Display question group editor using the same container and form patterns as other portals"""
    
    questions_dict = qg_data.get("questions", {})
    
    if not questions_dict:
        st.info("📭 No questions with data in this group")
        return
    
    # Check if this question group has ground truth to determine admin role
    has_ground_truth = any(
        q_data.get("ground_truth") is not None 
        for q_data in questions_dict.values()
    )
    
    admin_role = "meta_reviewer" if has_ground_truth else "reviewer"
    mode = "Admin Override" if has_ground_truth else "Ground Truth Creation"
    
    # Show admin role info like other portals show mode info
    if admin_role == "meta_reviewer":
        st.warning(f"🎯 **Admin Override Mode** - You can override existing ground truth")
    else:
        st.info(f"🔍 **Ground Truth Creation Mode** - Create ground truth for this question group")
    
    # Get questions from service to match other portal patterns
    try:
        service_questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        
        # Filter to only questions that have data
        questions_with_data = [q for q in service_questions if q["id"] in questions_dict.keys()]
        
        if not questions_with_data:
            st.info("No questions found in this group")
            return
        
        # Get existing ground truth to populate form
        existing_answers = {}
        try:
            if has_ground_truth:
                existing_answers = GroundTruthService.get_ground_truth_for_question_group(
                    video_id=video_info["id"], project_id=project_id, question_group_id=group_id, session=session
                )
        except:
            pass
        
        # Use the same container height as other portals
        content_height = 500
        
        # Create form exactly like other portals
        form_key = f"admin_form_{video_info['id']}_{group_id}_search"
        with st.form(form_key):
            answers = {}
            
            with st.container(height=content_height, border=False):
                for i, question in enumerate(questions_with_data):
                    question_id = question["id"]
                    question_text = question["text"]
                    existing_value = existing_answers.get(question_text, "")
                    
                    # Show existing data from our search results
                    q_data = questions_dict.get(question_id, {})
                    
                    if i > 0:
                        st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                    
                    # Display question using the same patterns as other portals
                    if question["type"] == "single":
                        answers[question_text] = display_admin_single_choice_question(
                            question, video_info["id"], project_id, existing_value, 
                            q_data, admin_role, session
                        )
                    else:
                        answers[question_text] = display_admin_description_question(
                            question, video_info["id"], project_id, existing_value, 
                            q_data, admin_role, session
                        )
            
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            # Submit button like other portals
            button_text = "🎯 Override Ground Truth" if admin_role == "meta_reviewer" else "✅ Set Ground Truth"
            submitted = st.form_submit_button(button_text, use_container_width=True, type="primary")
            
            if submitted:
                try:
                    if admin_role == "meta_reviewer":
                        GroundTruthService.override_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            question_group_id=group_id, admin_id=user_id, 
                            answers=answers, session=session
                        )
                        st.success("✅ Ground truth overridden successfully!")
                    else:
                        GroundTruthService.submit_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            reviewer_id=user_id, question_group_id=group_id, 
                            answers=answers, session=session
                        )
                        st.success("✅ Ground truth set successfully!")
                    
                    st.rerun(scope="fragment")
                    
                except Exception as e:
                    st.error(f"❌ Error saving ground truth: {str(e)}")
                    
    except Exception as e:
        st.error(f"❌ Error loading question group: {str(e)}")



###############################################################################
# VIDEO CRITERIA SEARCH PORTAL
###############################################################################

@st.fragment
def video_criteria_search_portal():
    """Search videos by criteria with clean results display"""
    
    st.markdown("## 📊 Video Criteria Search")
    st.markdown("*Find videos based on ground truth criteria or completion status*")
    
    with get_db_session() as session:
        search_type_tabs = st.tabs(["🎯 Ground Truth Criteria", "📈 Completion Status"])
        
        with search_type_tabs[0]:
            ground_truth_criteria_search(session)
        
        with search_type_tabs[1]:
            completion_status_search(session)

def ground_truth_criteria_search(session: Session):
    """Search videos by ground truth criteria"""
    
    st.markdown("### 🎯 Search by Ground Truth Answers")
    
    # Project selection
    projects_df = ProjectService.get_all_projects(session=session)
    if projects_df.empty:
        st.warning("🚫 No projects available")
        return
    
    st.markdown("**Step 1: Select Projects to Search**")
    selected_projects = st.multiselect(
        "Projects",
        projects_df["ID"].tolist(),
        format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
        key="criteria_search_projects",
        help="Choose which projects to include in the search"
    )
    
    if not selected_projects:
        st.info("👆 Please select one or more projects to continue")
        return
    
    # Criteria builder
    st.markdown("**Step 2: Build Search Criteria**")
    
    if "search_criteria_admin" not in st.session_state:
        st.session_state.search_criteria_admin = []
    
    # Add criteria interface
    with st.expander("➕ Add New Criteria", expanded=True):
        add_col1, add_col2, add_col3, add_col4 = st.columns([2, 2, 2, 1])
        
        with add_col1:
            project_for_criteria = st.selectbox(
                "Project",
                selected_projects,
                format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
                key="criteria_project_select"
            )
        
        with add_col2:
            if project_for_criteria:
                questions = ProjectService.get_project_questions(project_id=project_for_criteria, session=session)
                single_questions = [q for q in questions if q["type"] == "single"]
                
                if single_questions:
                    selected_question = st.selectbox(
                        "Question",
                        [None] + single_questions,
                        format_func=lambda x: x["text"] if x else "Select question...",
                        key="criteria_question_select"
                    )
                else:
                    selected_question = None
                    st.warning("No single-choice questions available")
            else:
                selected_question = None
        
        with add_col3:
            if selected_question:
                question_obj = QuestionService.get_question_by_id(question_id=selected_question["id"], session=session)
                if question_obj.options:
                    selected_answer = st.selectbox(
                        "Required Answer",
                        question_obj.options,
                        key="criteria_answer_select"
                    )
                else:
                    selected_answer = None
                    st.warning("No options available")
            else:
                selected_answer = None
        
        with add_col4:
            if project_for_criteria and selected_question and selected_answer:
                if st.button("➕ Add", key="add_criteria_btn", use_container_width=True):
                    project_name = projects_df[projects_df["ID"]==project_for_criteria]["Name"].iloc[0]
                    
                    criterion = {
                        "project_id": project_for_criteria,
                        "project_name": project_name,
                        "question_id": selected_question["id"],
                        "question_text": selected_question["text"],
                        "required_answer": selected_answer
                    }
                    
                    if criterion not in st.session_state.search_criteria_admin:
                        st.session_state.search_criteria_admin.append(criterion)
                        st.rerun(scope="fragment")
    
    # Display current criteria
    if st.session_state.search_criteria_admin:
        st.markdown("**Current Search Criteria:**")
        
        for i, criterion in enumerate(st.session_state.search_criteria_admin):
            crit_col1, crit_col2 = st.columns([6, 1])
            
            with crit_col1:
                st.markdown(f"""
                <div style="background: #e3f2fd; border: 1px solid #2196f3; border-radius: 8px; padding: 12px; margin: 4px 0;">
                    <div style="color: #1976d2; font-weight: 600;">{criterion['project_name']}</div>
                    <div style="color: #424242; margin-top: 4px;">
                        <strong>Question:</strong> {criterion['question_text']}<br>
                        <strong>Required Answer:</strong> {criterion['required_answer']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            with crit_col2:
                if st.button("🗑️", key=f"remove_crit_{i}", help="Remove criteria"):
                    st.session_state.search_criteria_admin.pop(i)
                    st.rerun(scope="fragment")
        
        # Search execution
        st.markdown("**Step 3: Execute Search**")
        
        exec_col1, exec_col2, exec_col3 = st.columns([2, 2, 2])
        
        with exec_col1:
            match_logic = st.radio(
                "Match Logic",
                ["Match ALL criteria", "Match ANY criteria"],
                key="criteria_match_logic",
                help="ALL = video must match every criteria, ANY = video matches at least one"
            )
        
        with exec_col2:
            if st.button("🔍 Search Videos", key="execute_criteria_search", type="primary", use_container_width=True):
                match_all = (match_logic == "Match ALL criteria")
                results = execute_ground_truth_search(st.session_state.search_criteria_admin, match_all, session)
                st.session_state.criteria_search_results = results
                st.rerun(scope="fragment")
        
        with exec_col3:
            if st.button("🧹 Clear Criteria", key="clear_criteria_admin", use_container_width=True):
                st.session_state.search_criteria_admin = []
                if "criteria_search_results" in st.session_state:
                    del st.session_state.criteria_search_results
                st.rerun(scope="fragment")
        
        # Display results
        if "criteria_search_results" in st.session_state:
            display_video_search_results_grid(st.session_state.criteria_search_results, session)

def completion_status_search(session: Session):
    """Search videos by completion status"""
    
    st.markdown("### 📈 Search by Completion Status")
    
    # Search interface
    search_col1, search_col2, search_col3 = st.columns(3)
    
    with search_col1:
        projects_df = ProjectService.get_all_projects(session=session)
        if projects_df.empty:
            st.warning("No projects available")
            return
        
        selected_projects_status = st.multiselect(
            "Select Projects",
            projects_df["ID"].tolist(),
            format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
            key="status_search_projects"
        )
    
    with search_col2:
        completion_filter = st.selectbox(
            "Completion Status",
            ["All videos", "Complete ground truth", "Missing ground truth", "Partial ground truth"],
            key="status_completion_filter"
        )
    
    with search_col3:
        if selected_projects_status:
            if st.button("🔍 Search", key="execute_status_search", type="primary", use_container_width=True):
                results = execute_project_based_search(selected_projects_status, completion_filter, session)
                display_project_status_results(results, session)

def display_video_search_results_grid(results: List[Dict], session: Session):
    """Display video search results in organized grid"""
    
    if not results:
        st.warning("🔍 No videos match your search criteria")
        return
    
    st.markdown(f"### 🎬 Search Results ({len(results)} videos found)")
    
    # Pagination
    videos_per_page = 20
    total_pages = (len(results) - 1) // videos_per_page + 1 if results else 1
    
    if total_pages > 1:
        page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
        with page_col2:
            current_page = st.selectbox(
                "Page",
                range(1, total_pages + 1),
                key="criteria_results_page",
                help=f"Showing {videos_per_page} videos per page"
            ) - 1
    else:
        current_page = 0
    
    start_idx = current_page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(results))
    page_results = results[start_idx:end_idx]
    
    st.markdown(f"**Showing {start_idx + 1}-{end_idx} of {len(results)} videos**")
    
    # Results grid
    cols_per_row = 4
    
    for i in range(0, len(page_results), cols_per_row):
        cols = st.columns(cols_per_row)
        row_results = page_results[i:i + cols_per_row]
        
        for j, result in enumerate(row_results):
            with cols[j]:
                display_video_result_card_admin(result, session)

def display_video_result_card_admin(result: Dict, session: Session):
    """Display video result card with admin editing capability"""
    
    video_info = result["video_info"]
    matches = result.get("matches", [])
    criteria = result.get("criteria", [])
    
    # Calculate match percentage
    if criteria:
        match_count = sum(matches) if matches else 0
        total_criteria = len(criteria)
        match_percentage = match_count / total_criteria if total_criteria > 0 else 0
    else:
        match_percentage = 1.0
        match_count = 0
        total_criteria = 0
    
    # Card styling based on match percentage
    if match_percentage == 1.0:
        card_color = "#4caf50"
    elif match_percentage >= 0.5:
        card_color = "#ff9800"
    else:
        card_color = "#2196f3"
    
    created_date = video_info["created_at"].strftime('%m/%d/%Y') if video_info["created_at"] else 'Unknown'
    
    # Video card
    st.markdown(f"""
    <div style="background: {card_color}15; border: 2px solid {card_color}; border-radius: 12px; padding: 16px; margin: 8px 0; text-align: center; min-height: 140px;">
        <div style="color: {card_color}; font-weight: 700; font-size: 1rem; margin-bottom: 8px;">
            📹 {video_info["uid"]}
        </div>
        {f'<div style="color: #424242; font-size: 0.85rem; margin-bottom: 6px;"><strong>{match_count}/{total_criteria}</strong> criteria matched</div>' if criteria else ''}
        <div style="color: #666; font-size: 0.8rem; margin-bottom: 8px;">
            Created: {created_date}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Action buttons
    btn_col1, btn_col2 = st.columns(2)
    
    with btn_col1:
        if st.button("👁️ View", key=f"view_result_{video_info['id']}", use_container_width=True):
            show_video_details_admin_modal(video_info, matches, criteria, session)
    
    with btn_col2:
        if st.button("✏️ Edit", key=f"edit_result_{video_info['id']}", use_container_width=True):
            # Set this video for editing and switch to video answer search
            st.session_state.admin_quick_edit_video = video_info["uid"]
            st.rerun()

def display_project_status_results(results: List[Dict], session: Session):
    """Display project completion status results"""
    
    if not results:
        st.warning("🔍 No videos match your criteria")
        return
    
    st.markdown(f"### 📊 Status Search Results ({len(results)} videos)")
    
    # Status summary
    status_counts = {}
    for result in results:
        status = result["completion_status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Display summary
    summary_cols = st.columns(4)
    status_styles = {
        "complete": ("✅", "Complete", "#4caf50"),
        "partial": ("⚠️", "Partial", "#ff9800"),
        "missing": ("❌", "Missing", "#f44336"),
        "no_questions": ("❓", "No Questions", "#9e9e9e")
    }
    
    col_idx = 0
    for status, count in status_counts.items():
        if status in status_styles and col_idx < len(summary_cols):
            emoji, label, color = status_styles[status]
            with summary_cols[col_idx]:
                st.metric(f"{emoji} {label}", count)
            col_idx += 1
    
    # Results table
    st.markdown("**Detailed Results:**")
    
    table_data = []
    for result in results:
        status_info = status_styles.get(result["completion_status"], ("❓", "Unknown", "#000"))
        completion_pct = round(result['completed_questions'] / result['total_questions'] * 100, 1) if result['total_questions'] > 0 else 0
        
        table_data.append({
            "Video UID": result["video_uid"],
            "Project": result["project_name"],
            "Status": f"{status_info[0]} {status_info[1]}",
            "Progress": f"{result['completed_questions']}/{result['total_questions']} ({completion_pct}%)"
        })
    
    # Display table with pagination
    df = pd.DataFrame(table_data)
    st.dataframe(df, use_container_width=True)
    
    # Download option
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Results",
        data=csv,
        file_name="completion_status_search_results.csv",
        mime="text/csv"
    )

@st.dialog("📹 Video Details", width="large")
def show_video_details_admin_modal(video_info: Dict[str, Any], matches: List[bool], criteria: List[Dict], session: Session):
    """Show video details in admin modal with editing options"""
    
    st.markdown(f"### 📹 {video_info['uid']} - Detailed View")
    
    # Video info
    info_col1, info_col2 = st.columns(2)
    
    with info_col1:
        st.markdown(f"**URL:** {video_info['url']}")
        created_at = video_info["created_at"].strftime('%Y-%m-%d %H:%M') if video_info["created_at"] else 'Unknown'
        st.markdown(f"**Created:** {created_at}")
    
    with info_col2:
        if criteria:
            match_count = sum(matches)
            total_criteria = len(criteria)
            st.metric("Criteria Matches", f"{match_count}/{total_criteria}")
    
    # Video preview
    st.markdown("#### 🎬 Video Preview")
    custom_video_player(video_info["url"], autoplay=False)
    
    # Criteria results
    if criteria:
        st.markdown("#### 📋 Criteria Match Details")
        
        for i, (criterion, match) in enumerate(zip(criteria, matches)):
            match_color = "#4caf50" if match else "#f44336"
            match_icon = "✅" if match else "❌"
            
            st.markdown(f"""
            <div style="background: {match_color}15; border-left: 4px solid {match_color}; padding: 12px; margin: 8px 0; border-radius: 6px;">
                <div style="color: {match_color}; font-weight: 600; font-size: 1rem;">
                    {match_icon} {criterion['project_name']}
                </div>
                <div style="color: #424242; margin-top: 6px;">
                    <strong>Question:</strong> {criterion['question_text']}<br>
                    <strong>Required Answer:</strong> {criterion['required_answer']}<br>
                    <strong>Status:</strong> {'Match found' if match else 'No match or missing ground truth'}
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # Quick edit button
    if st.button("✏️ Open in Answer Editor", type="primary", use_container_width=True):
        st.session_state.admin_quick_edit_video = video_info["uid"]
        st.rerun()

###############################################################################
# BULK OPERATIONS PORTAL
###############################################################################

@st.fragment
def bulk_operations_portal():
    """Bulk operations for admin efficiency"""
    
    st.markdown("## 📋 Bulk Operations")
    st.markdown("*Perform batch operations across multiple videos and projects*")
    
    st.info("🚧 **Coming Soon:** Bulk ground truth operations, batch video management, and mass project assignments.")
    
    # Placeholder for future bulk operations
    with st.expander("🔮 Planned Features"):
        st.markdown("""
        - **Bulk Ground Truth Import/Export**
        - **Mass Video Assignment to Projects**
        - **Batch Project Creation**
        - **Bulk User Assignment Management**
        - **Ground Truth Validation Reports**
        - **Performance Analytics Dashboard**
        """)

###############################################################################
# HELPER FUNCTIONS (REUSED AND OPTIMIZED)
###############################################################################

def get_video_info_by_uid(video_uid: str, session: Session) -> Optional[Dict[str, Any]]:
    """Get video information as a dictionary by UID"""
    try:
        video = VideoService.get_video_by_uid(video_uid=video_uid, session=session)
        if not video:
            return None
        
        return {
            "id": video.id,
            "uid": video.video_uid,
            "url": video.url,
            "metadata": video.video_metadata or {},
            "created_at": video.created_at,
            "is_archived": video.is_archived
        }
    except Exception:
        return None

def get_video_answers_across_groups(video_id: int, selected_group_ids: List[int], session: Session) -> Dict:
    """Get all answers and ground truth for a video across selected project groups"""
    
    results = {}
    
    for group_id in selected_group_ids:
        try:
            # Get group info
            group_info = ProjectGroupService.get_project_group_by_id(group_id=group_id, session=session)
            group_name = group_info["group"].name
            projects = group_info["projects"]
            
            group_results = {"group_name": group_name, "projects": {}}
            
            for project in projects:
                # Check if video is in this project
                project_videos = VideoService.get_project_videos(project_id=project.id, session=session)
                video_in_project = any(v["id"] == video_id for v in project_videos)
                
                if not video_in_project:
                    continue
                
                # Get project answers and ground truth
                project_results = get_project_answers_for_video(video_id, project.id, session)
                
                if project_results["has_data"]:
                    group_results["projects"][project.id] = {
                        "project_name": project.name,
                        "project_description": project.description,
                        **project_results
                    }
            
            if group_results["projects"]:
                results[group_id] = group_results
                
        except Exception as e:
            st.error(f"Error processing group {group_id}: {str(e)}")
            continue
    
    return results

def get_project_answers_for_video(video_id: int, project_id: int, session: Session) -> Dict:
    """Get all answers and ground truth for a video in a specific project"""
    
    try:
        # Get project info
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        
        # Get schema question groups
        question_groups = SchemaService.get_schema_question_groups_list(
            schema_id=project.schema_id, session=session
        )
        
        project_results = {
            "has_data": False,
            "question_groups": {}
        }
        
        for group in question_groups:
            group_id = group["ID"]
            group_title = group["Title"]
            
            # Get questions in this group
            questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
            
            group_data = {
                "title": group_title,
                "description": group["Description"],
                "questions": {}
            }
            
            has_group_data = False
            
            for question in questions:
                question_id = question["id"]
                question_text = question["text"]
                question_type = question["type"]
                
                question_data = {
                    "text": question_text,
                    "type": question_type,
                    "annotator_answers": [],
                    "ground_truth": None
                }
                
                # Get annotator answers
                try:
                    answers_df = AnnotatorService.get_answers(video_id=video_id, project_id=project_id, session=session)
                    
                    if not answers_df.empty:
                        question_answers = answers_df[answers_df["Question ID"] == question_id]
                        
                        for _, answer_row in question_answers.iterrows():
                            user_info = AuthService.get_user_info_by_id(
                                user_id=int(answer_row["User ID"]), session=session
                            )
                            
                            question_data["annotator_answers"].append({
                                "user_name": user_info["user_id_str"],
                                "user_id": answer_row["User ID"],
                                "answer_value": answer_row["Answer Value"],
                                "confidence": answer_row["Confidence Score"],
                                "created_at": answer_row["Created At"],
                                "notes": answer_row["Notes"]
                            })
                except Exception:
                    pass
                
                # Get ground truth
                try:
                    gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                    
                    if not gt_df.empty:
                        gt_row = gt_df[gt_df["Question ID"] == question_id]
                        
                        if not gt_row.empty:
                            gt_data = gt_row.iloc[0]
                            reviewer_info = AuthService.get_user_info_by_id(
                                user_id=int(gt_data["Reviewer ID"]), session=session
                            )
                            
                            question_data["ground_truth"] = {
                                "answer_value": gt_data["Answer Value"],
                                "original_value": gt_data["Original Value"],
                                "reviewer_name": reviewer_info["user_id_str"],
                                "modified_by_admin": gt_data["Modified By Admin"] is not None,
                                "created_at": gt_data["Created At"],
                                "modified_at": gt_data["Modified At"]
                            }
                except Exception:
                    pass
                
                # Check if this question has any data
                if question_data["annotator_answers"] or question_data["ground_truth"]:
                    group_data["questions"][question_id] = question_data
                    has_group_data = True
            
            if has_group_data:
                project_results["question_groups"][group_id] = group_data
                project_results["has_data"] = True
        
        return project_results
        
    except Exception as e:
        st.error(f"Error getting project answers: {str(e)}")
        return {"has_data": False, "question_groups": {}}

def execute_ground_truth_search(criteria: List[Dict], match_all: bool, session: Session) -> List[Dict]:
    """Execute ground truth search with given criteria"""
    
    if not criteria:
        return []
    
    # Get all videos that could potentially match
    all_videos = VideoService.get_all_videos(session=session)
    all_videos = all_videos[~all_videos["Archived"]]  # Only non-archived videos
    
    matching_videos = []
    
    for _, video_row in all_videos.iterrows():
        video_uid = video_row["Video UID"]
        video_info = get_video_info_by_uid(video_uid=video_uid, session=session)
        
        if not video_info:
            continue
        
        # Check criteria
        matches = []
        
        for criterion in criteria:
            project_id = criterion["project_id"]
            question_id = criterion["question_id"]
            required_answer = criterion["required_answer"]
            
            # Check if video is in this project
            project_videos = VideoService.get_project_videos(project_id=project_id, session=session)
            video_in_project = any(v["id"] == video_info["id"] for v in project_videos)
            
            if not video_in_project:
                matches.append(False)
                continue
            
            # Check ground truth
            try:
                gt_df = GroundTruthService.get_ground_truth(video_id=video_info["id"], project_id=project_id, session=session)
                
                if gt_df.empty:
                    matches.append(False)
                    continue
                
                question_gt = gt_df[gt_df["Question ID"] == question_id]
                
                if question_gt.empty:
                    matches.append(False)
                    continue
                
                actual_answer = question_gt.iloc[0]["Answer Value"]
                matches.append(str(actual_answer) == str(required_answer))
                
            except:
                matches.append(False)
        
        # Apply match logic
        if match_all and all(matches):
            # Video matches all criteria
            matching_videos.append({
                "video_info": video_info,
                "matches": matches,
                "criteria": criteria
            })
        elif not match_all and any(matches):
            # Video matches at least one criterion
            matching_videos.append({
                "video_info": video_info,
                "matches": matches,
                "criteria": criteria
            })
    
    return matching_videos

def execute_project_based_search(project_ids: List[int], completion_filter: str, session: Session) -> List[Dict]:
    """Execute project-based search"""
    
    results = []
    
    for project_id in project_ids:
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        project_videos = VideoService.get_project_videos(project_id=project_id, session=session)
        
        for video_info in project_videos:
            video_id = video_info["id"]
            video_uid = video_info["uid"]
            
            # Get completion status
            try:
                gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                questions = ProjectService.get_project_questions(project_id=project_id, session=session)
                
                total_questions = len(questions)
                completed_questions = len(gt_df) if not gt_df.empty else 0
                
                if total_questions == 0:
                    completion_status = "no_questions"
                elif completed_questions == 0:
                    completion_status = "missing"
                elif completed_questions == total_questions:
                    completion_status = "complete"
                else:
                    completion_status = "partial"
                
            except:
                completion_status = "error"
            
            # Apply filter
            include_video = False
            
            if completion_filter == "All videos":
                include_video = True
            elif completion_filter == "Complete ground truth" and completion_status == "complete":
                include_video = True
            elif completion_filter == "Missing ground truth" and completion_status == "missing":
                include_video = True
            elif completion_filter == "Partial ground truth" and completion_status == "partial":
                include_video = True
            
            if include_video:
                results.append({
                    "video_id": video_id,
                    "video_uid": video_uid,
                    "video_url": video_info["url"],
                    "project_id": project_id,
                    "project_name": project.name,
                    "completion_status": completion_status,
                    "completed_questions": completed_questions if completion_status != "error" else 0,
                    "total_questions": total_questions if completion_status != "error" else 0
                })
    
    return results

###############################################################################
# SHARED UTILITY FUNCTIONS
###############################################################################

def get_all_project_annotators(project_id: int, session: Session) -> Dict[str, Dict]:
    """Get all annotators who have answered questions in this project"""
    try:
        return ProjectService.get_project_annotators(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error getting project annotators: {str(e)}")
        return {}

def display_user_simple(user_name: str, user_email: str, is_ground_truth: bool = False):
    """Simple user display using native Streamlit components"""
    name_parts = user_name.split()
    initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper() if len(name_parts) >= 2 else user_name[:2].upper()
    
    if is_ground_truth:
        st.success(f"🏆 **{user_name}** ({initials}) - {user_email}")
    else:
        st.info(f"👤 **{user_name}** ({initials}) - {user_email}")

def check_project_has_full_ground_truth(project_id: int, session: Session) -> bool:
    """Check if project has complete ground truth for ALL questions and videos"""
    try:
        return ProjectService.check_project_has_full_ground_truth(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error checking ground truth status: {str(e)}")
        return False

def get_project_groups_with_projects(user_id: int, role: str, session: Session) -> Dict:
    """Get project groups with their projects for a user"""
    try:
        return ProjectGroupService.get_grouped_projects_for_user(user_id=user_id, role=role, session=session)
    except ValueError as e:
        st.error(f"Error getting grouped projects: {str(e)}")
        return {}

def get_user_assignment_dates(user_id: int, session: Session) -> Dict[int, Dict[str, str]]:
    """Get assignment dates for all projects for a specific user"""
    try:
        assignments_df = AuthService.get_project_assignments(session=session)
        user_assignments = {}
        
        for _, assignment in assignments_df.iterrows():
            if assignment["User ID"] == user_id:
                project_id = assignment["Project ID"]
                role = assignment["Role"]
                
                if project_id not in user_assignments:
                    user_assignments[project_id] = {}
                
                assigned_at = assignment.get("Assigned At")
                if assigned_at:
                    try:
                        date_str = assigned_at.strftime("%Y-%m-%d") if hasattr(assigned_at, 'strftime') else str(assigned_at)[:10]
                        user_assignments[project_id][role] = date_str
                    except:
                        user_assignments[project_id][role] = "Unknown"
                else:
                    user_assignments[project_id][role] = "Not set"
        
        return user_assignments
    except Exception as e:
        st.error(f"Error getting assignment dates: {str(e)}")
        return {}

def display_project_dashboard(user_id: int, role: str, session: Session) -> Optional[int]:
    """Display project group dashboard with enhanced clarity and pagination"""
    st.markdown("## 📂 Project Dashboard")
    
    backend_role = "admin" if role == "meta_reviewer" else role
    grouped_projects = get_project_groups_with_projects(user_id=user_id, role=backend_role, session=session)
    
    if not grouped_projects:
        st.warning(f"No projects assigned to you as {role}.")
        return None
    
    assignment_dates = get_user_assignment_dates(user_id=user_id, session=session)
    
    st.markdown("### 🔍 Search & Filter")
    col1, col2, col3 = st.columns(3)
    with col1:
        search_term = st.text_input("🔍 Search projects", placeholder="Enter project name...")
    with col2:
        sort_by = st.selectbox("Sort by", ["Completion Rate", "Name", "Assignment Date"])
    with col3:
        sort_order = st.selectbox("Order", ["Ascending", "Descending"])
    
    st.markdown("---")
    
    selected_project_id = None
    
    for group_index, (group_name, projects) in enumerate(grouped_projects.items()):
        if not projects:
            continue
        
        filtered_projects = [p for p in projects if not search_term or search_term.lower() in p["name"].lower()]
        if not filtered_projects:
            continue
        
        # Calculate completion rates and assignment dates for sorting
        for project in filtered_projects:
            try:
                if role == "annotator":
                    project["completion_rate"] = calculate_user_overall_progress(user_id=user_id, project_id=project["id"], session=session)
                else:
                    project_progress = ProjectService.progress(project_id=project["id"], session=session)
                    project["completion_rate"] = project_progress['completion_percentage']
                
                project_assignments = assignment_dates.get(project["id"], {})
                project_assignment_date = project_assignments.get(backend_role, "Not set")
                project["assignment_date"] = project_assignment_date
                
                if project_assignment_date and project_assignment_date not in ["Not set", "Unknown"]:
                    try:
                        project["assignment_datetime"] = datetime.strptime(project_assignment_date, "%Y-%m-%d")
                    except:
                        project["assignment_datetime"] = datetime.min
                else:
                    project["assignment_datetime"] = datetime.min
                    
            except:
                project["completion_rate"] = 0.0
                project["assignment_date"] = "Unknown"
                project["assignment_datetime"] = datetime.min
        
        # Sort projects
        if sort_by == "Completion Rate":
            filtered_projects.sort(key=lambda x: x["completion_rate"], reverse=(sort_order == "Descending"))
        elif sort_by == "Name":
            filtered_projects.sort(key=lambda x: x["name"], reverse=(sort_order == "Descending"))
        elif sort_by == "Assignment Date":
            filtered_projects.sort(key=lambda x: x["assignment_datetime"], reverse=(sort_order == "Descending"))
        else:
            filtered_projects.sort(key=lambda x: x["completion_rate"])
        
        total_projects = len(filtered_projects)
        projects_per_page = 6
        total_pages = (total_projects - 1) // projects_per_page + 1 if total_projects > 0 else 1
        
        page_key = f"group_page_{group_name}_{user_id}_{role}"
        if page_key not in st.session_state:
            st.session_state[page_key] = 0
        
        current_page = st.session_state[page_key]
        
        group_color = ["#3498db", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6", "#1abc9c"][group_index % 6]
        display_group_name = group_name
        truncated_group_name = group_name[:67] + "..." if len(group_name) > 70 else group_name
        
        st.markdown(f"""
        <div style="{get_card_style(group_color)}position: relative;">
            <div style="position: absolute; top: -8px; left: 20px; background: {group_color}; color: white; padding: 4px 12px; border-radius: 10px; font-size: 0.8rem; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                PROJECT GROUP
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 8px;">
                <div>
                    <h2 style="margin: 0; color: {group_color}; font-size: 1.8rem;" title="{display_group_name}">📁 {truncated_group_name}</h2>
                    <p style="margin: 8px 0 0 0; color: #34495e; font-size: 1.1rem; font-weight: 500;">
                        {total_projects} projects in this group {f"• Page {current_page + 1} of {total_pages}" if total_pages > 1 else ""}
                    </p>
                </div>
                <div style="text-align: right;">
                    <span style="background: {group_color}; color: white; padding: 10px 18px; border-radius: 20px; font-weight: bold; font-size: 1.1rem; box-shadow: 0 3px 6px rgba(0,0,0,0.2);">{total_projects} Projects</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Pagination controls
        if total_pages > 1:
            page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
            
            with page_col1:
                if st.button("◀ Previous", disabled=(current_page == 0), key=f"prev_{page_key}", use_container_width=True):
                    st.session_state[page_key] = max(0, current_page - 1)
                    st.rerun()
            
            with page_col2:
                page_options = [f"Page {i+1}" for i in range(total_pages)]
                selected_page_name = st.selectbox(
                    f"Page for {group_name}",
                    page_options,
                    index=current_page,
                    key=f"page_select_{page_key}",
                    label_visibility="collapsed"
                )
                new_page = page_options.index(selected_page_name)
                if new_page != current_page:
                    st.session_state[page_key] = new_page
                    st.rerun()
            
            with page_col3:
                if st.button("Next ▶", disabled=(current_page == total_pages - 1), key=f"next_{page_key}", use_container_width=True):
                    st.session_state[page_key] = min(total_pages - 1, current_page + 1)
                    st.rerun()
        
        # Display projects
        start_idx = current_page * projects_per_page
        end_idx = min(start_idx + projects_per_page, total_projects)
        page_projects = filtered_projects[start_idx:end_idx]
        
        if page_projects:
            cols = st.columns(3)
            
            for i, project in enumerate(page_projects):
                with cols[i % 3]:
                    has_full_gt = check_project_has_full_ground_truth(project_id=project["id"], session=session)
                    mode = "🎓 Training" if has_full_gt else "📝 Annotation"
                    
                    completion_rate = project.get("completion_rate", 0.0)
                    assignment_date = project.get("assignment_date", "Unknown")
                    progress_text = f"{completion_rate:.1f}% Complete"
                    
                    project_name = project["name"]
                    truncated_tag_group_name = group_name[:57] + "..." if len(group_name) > 60 else group_name
                    
                    with st.container():
                        st.markdown(f"""
                        <div style="border: 2px solid {group_color}; border-radius: 12px; padding: 18px; margin: 8px 0; background: linear-gradient(135deg, white, {group_color}05); box-shadow: 0 4px 8px rgba(0,0,0,0.1); min-height: 200px; position: relative;" title="Group: {display_group_name}">
                            <div style="position: absolute; top: -6px; right: 10px; background: {group_color}; color: white; padding: 2px 6px; border-radius: 6px; font-size: 0.7rem; font-weight: bold;" title="{display_group_name}">
                                {truncated_tag_group_name}
                            </div>
                            <h4 style="margin: 10px 0 8px 0; color: #1f77b4; font-size: 1.1rem; line-height: 1.3; word-wrap: break-word;" title="{project_name}">{project_name}</h4>
                            <p style="margin: 8px 0; color: #666; font-size: 0.9rem; min-height: 50px;">
                                {project["description"] or 'No description'}
                            </p>
                            <div style="margin: 12px 0;">
                                <p style="margin: 4px 0;"><strong>Mode:</strong> {mode}</p>
                                <p style="margin: 4px 0;"><strong>Progress:</strong> {progress_text}</p>
                                <p style="margin: 4px 0; color: #666; font-size: 0.85rem;">
                                    <strong>Assigned:</strong> {assignment_date}
                                </p>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("Open Project", 
                                   key=f"select_project_{project['id']}", 
                                   use_container_width=True,
                                   help=f"Open '{project_name}' from {display_group_name} group"):
                            selected_project_id = project["id"]
                            st.session_state.selected_project_id = project["id"]
                            st.session_state.current_view = "project"
                            st.rerun()
        else:
            st.info(f"No projects match your search in {group_name}")
        
        if group_index < len(grouped_projects) - 1:
            st.markdown("""<div style="height: 2px; background: linear-gradient(90deg, transparent, #ddd, transparent); margin: 30px 0;"></div>""", unsafe_allow_html=True)
        
        if selected_project_id:
            break
    
    return None

def display_smart_annotator_selection(annotators: Dict[str, Dict], project_id: int):
    """Modern, compact annotator selection with single-line display"""
    if not annotators:
        st.warning("No annotators have submitted answers for this project yet.")
        return []
    
    if "selected_annotators" not in st.session_state:
        annotator_options = list(annotators.keys())
        st.session_state.selected_annotators = annotator_options[:3] if len(annotator_options) > 3 else annotator_options
    
    annotator_options = list(annotators.keys())
    
    with st.container():
        st.markdown("#### 🎯 Quick Actions")
        
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("✅ Select All", key=f"select_all_{project_id}", help="Select all annotators", use_container_width=True):
                st.session_state.selected_annotators = annotator_options.copy()
                st.rerun()
        
        with btn_col2:
            if st.button("❌ Clear All", key=f"clear_all_{project_id}", help="Deselect all annotators", use_container_width=True):
                st.session_state.selected_annotators = []
                st.rerun()
        
        # Status display
        selected_count = len(st.session_state.selected_annotators)
        total_count = len(annotator_options)
        
        status_color = COLORS['success'] if selected_count > 0 else COLORS['secondary']
        status_text = f"📊 {selected_count} of {total_count} annotators selected" if selected_count > 0 else f"📊 No annotators selected ({total_count} available)"
        delta_text = f"{selected_count} active" if selected_count > 0 else "None selected"
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {status_color}15, {status_color}08); border: 1px solid {status_color}40; border-radius: 8px; padding: 8px 16px; margin: 12px 0; text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,0.1);">
            <div style="color: {status_color}; font-weight: 600; font-size: 0.9rem;">{status_text}</div>
            <div style="color: {status_color}cc; font-size: 0.75rem; margin-top: 2px;">{delta_text}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with st.container():
        st.markdown("#### 👥 Choose Annotators")
        st.caption("Select annotators whose responses you want to see during review")
        
        num_annotators = len(annotator_options)
        if num_annotators <= 3:
            num_cols = num_annotators
        elif num_annotators <= 8:
            num_cols = 4
        elif num_annotators <= 16:
            num_cols = 4
        else:
            num_cols = 5
        
        updated_selection = []
        
        for row_start in range(0, num_annotators, num_cols):
            cols = st.columns(num_cols)
            row_annotators = annotator_options[row_start:row_start + num_cols]
            
            for i, annotator_display in enumerate(row_annotators):
                with cols[i]:
                    if " (" in annotator_display and annotator_display.endswith(")"):
                        full_name = annotator_display.split(" (")[0]
                        initials = annotator_display.split(" (")[1][:-1]
                    else:
                        full_name = annotator_display
                        initials = annotator_display[:2].upper()
                    
                    annotator_info = annotators.get(annotator_display, {})
                    email = annotator_info.get('email', '')
                    user_id = annotator_info.get('id', '')
                    
                    # Keep full display name for checkbox
                    display_name = annotator_display
                    
                    tooltip = f"Email: {email}\nID: {user_id}" if email and email != f"user_{user_id}@example.com" else f"User ID: {user_id}"
                    
                    checkbox_key = f"annotator_cb_{project_id}_{row_start + i}"
                    is_selected = st.checkbox(
                        display_name,
                        value=annotator_display in st.session_state.selected_annotators,
                        key=checkbox_key,
                        help=tooltip
                    )
                    
                    if is_selected:
                        updated_selection.append(annotator_display)
        
        if set(updated_selection) != set(st.session_state.selected_annotators):
            st.session_state.selected_annotators = updated_selection
            st.rerun()
    
    # Selection summary
    if st.session_state.selected_annotators:
        initials_list = []
        for annotator in st.session_state.selected_annotators:
            if " (" in annotator and annotator.endswith(")"):
                initials = annotator.split(" (")[1][:-1]
                initials_list.append(initials)
            else:
                initials_list.append(annotator[:2].upper())
        
        if len(initials_list) <= 8:
            initials_text = " • ".join(initials_list)
        else:
            shown = initials_list[:6]
            remaining = len(initials_list) - 6
            initials_text = f"{' • '.join(shown)} + {remaining} more"
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #e8f5e8, #d4f1d4); border: 2px solid #28a745; border-radius: 12px; padding: 12px 16px; margin: 16px 0; text-align: center; box-shadow: 0 2px 8px rgba(40, 167, 69, 0.2);">
            <div style="color: #155724; font-weight: 600; font-size: 0.95rem;">
                ✅ Currently Selected: {initials_text}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #fff3cd, #ffeaa7); border: 2px solid #ffc107; border-radius: 12px; padding: 12px 16px; margin: 16px 0; text-align: center; box-shadow: 0 2px 8px rgba(255, 193, 7, 0.2);">
            <div style="color: #856404; font-weight: 600; font-size: 0.95rem;">
                ⚠️ No annotators selected - results will only show ground truth
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    return st.session_state.selected_annotators

###############################################################################
# NEW FUNCTIONS FOR SORTING AND FILTERING
###############################################################################

def get_model_confidence_scores_enhanced(project_id: int, model_user_ids: List[int], question_ids: List[int], session: Session) -> Dict[int, float]:
    """Get confidence scores for specific model users on specific questions"""
    try:
        if not model_user_ids or not question_ids:
            return {}
        
        confidence_scores = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for video in videos:
            video_id = video["id"]
            total_confidence = 0.0
            answer_count = 0
            
            for model_user_id in model_user_ids:
                for question_id in question_ids:
                    try:
                        # Use AnnotatorService to get answers
                        answers_df = AnnotatorService.get_question_answers(
                            question_id=question_id, project_id=project_id, session=session
                        )
                        
                        if not answers_df.empty:
                            # Filter for this model user and video
                            user_video_answers = answers_df[
                                (answers_df["User ID"] == model_user_id) & 
                                (answers_df["Video ID"] == video_id)
                            ]
                            
                            if not user_video_answers.empty:
                                confidence = user_video_answers.iloc[0]["Confidence Score"]
                                if confidence is not None:
                                    total_confidence += confidence
                                    answer_count += 1
                    except Exception:
                        continue
            
            if answer_count > 0:
                confidence_scores[video_id] = total_confidence / answer_count
            else:
                confidence_scores[video_id] = 0.0
        
        return confidence_scores
    except Exception as e:
        st.error(f"Error getting model confidence scores: {str(e)}")
        return {}

def get_annotator_consensus_rates_enhanced(project_id: int, selected_annotators: List[str], question_ids: List[int], session: Session) -> Dict[int, float]:
    """Calculate consensus rates with proper handling of incomplete annotations"""
    try:
        if not selected_annotators or not question_ids or len(selected_annotators) < 2:
            return {}
        
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        if len(annotator_user_ids) < 2:
            return {}
        
        consensus_rates = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for video in videos:
            video_id = video["id"]
            total_questions_with_multiple_answers = 0
            consensus_count = 0
            
            for question_id in question_ids:
                try:
                    answers_df = AnnotatorService.get_question_answers(
                        question_id=question_id, project_id=project_id, session=session
                    )
                    
                    if not answers_df.empty:
                        video_answers = answers_df[
                            (answers_df["Video ID"] == video_id) & 
                            (answers_df["User ID"].isin(annotator_user_ids))
                        ]
                        
                        # Only consider questions where at least 2 annotators answered
                        if len(video_answers) >= 2:
                            total_questions_with_multiple_answers += 1
                            answer_values = video_answers["Answer Value"].tolist()
                            
                            from collections import Counter
                            answer_counts = Counter(answer_values)
                            most_common_count = answer_counts.most_common(1)[0][1]
                            consensus_rate = most_common_count / len(answer_values)
                            
                            if consensus_rate >= 0.5:  # Majority agreement
                                consensus_count += 1
                except Exception:
                    continue
            
            if total_questions_with_multiple_answers > 0:
                consensus_rates[video_id] = consensus_count / total_questions_with_multiple_answers
            else:
                consensus_rates[video_id] = 0.0
        
        return consensus_rates
    except Exception as e:
        st.error(f"Error calculating consensus rates: {str(e)}")
        return {}

def get_video_accuracy_rates(project_id: int, selected_annotators: List[str], question_ids: List[int], session: Session) -> Dict[int, float]:
    """Calculate accuracy rates with proper handling of incomplete annotations"""
    try:
        if not selected_annotators or not question_ids:
            return {}
        
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        if not annotator_user_ids:
            return {}
        
        accuracy_rates = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for video in videos:
            video_id = video["id"]
            total_comparisons = 0
            correct_comparisons = 0
            
            try:
                gt_df = GroundTruthService.get_ground_truth(
                    video_id=video_id, project_id=project_id, session=session
                )
                
                if gt_df.empty:
                    accuracy_rates[video_id] = 0.0
                    continue
                
                for question_id in question_ids:
                    question_gt = gt_df[gt_df["Question ID"] == question_id]
                    if question_gt.empty:
                        continue
                    
                    gt_answer = question_gt.iloc[0]["Answer Value"]
                    
                    answers_df = AnnotatorService.get_question_answers(
                        question_id=question_id, project_id=project_id, session=session
                    )
                    
                    if not answers_df.empty:
                        video_answers = answers_df[
                            (answers_df["Video ID"] == video_id) & 
                            (answers_df["User ID"].isin(annotator_user_ids))
                        ]
                        
                        # Count all annotator answers for this question/video
                        for _, answer_row in video_answers.iterrows():
                            total_comparisons += 1
                            if answer_row["Answer Value"] == gt_answer:
                                correct_comparisons += 1
                
                if total_comparisons > 0:
                    accuracy_rates[video_id] = correct_comparisons / total_comparisons
                else:
                    accuracy_rates[video_id] = 0.0
                    
            except Exception:
                accuracy_rates[video_id] = 0.0
        
        return accuracy_rates
    except Exception as e:
        st.error(f"Error calculating accuracy rates: {str(e)}")
        return {}
    
def get_video_completion_rates_enhanced(project_id: int, question_ids: List[int], session: Session) -> Dict[int, float]:
    """Get completion rates for each video based on specific questions"""
    try:
        if not question_ids:
            return {}
            
        videos = get_project_videos(project_id=project_id, session=session)
        completion_rates = {}
        
        for video in videos:
            video_id = video["id"]
            completed_questions = 0
            
            for question_id in question_ids:
                try:
                    # Check if this question has ground truth for this video
                    gt_df = GroundTruthService.get_ground_truth(
                        video_id=video_id, project_id=project_id, session=session
                    )
                    if not gt_df.empty:
                        question_answers = gt_df[gt_df["Question ID"] == question_id]
                        if not question_answers.empty:
                            completed_questions += 1
                except:
                    continue
            
            completion_rates[video_id] = (completed_questions / len(question_ids)) * 100 if question_ids else 0.0
        
        return completion_rates
    except Exception as e:
        st.error(f"Error calculating video completion rates: {str(e)}")
        return {}

def get_ground_truth_option_filters_enhanced(project_id: int, session: Session) -> Dict[int, List[str]]:
    """Get available ground truth options for filtering"""
    try:
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        single_choice_questions = [q for q in questions if q["type"] == "single"]
        
        if not single_choice_questions:
            return {}
        
        gt_options = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for question in single_choice_questions:
            question_id = question["id"]
            unique_answers = set()
            
            # Check each video for ground truth answers
            for video in videos:
                try:
                    gt_df = GroundTruthService.get_ground_truth(
                        video_id=video["id"], project_id=project_id, session=session
                    )
                    if not gt_df.empty:
                        question_gt = gt_df[gt_df["Question ID"] == question_id]
                        if not question_gt.empty:
                            unique_answers.add(question_gt.iloc[0]["Answer Value"])
                except:
                    continue
            
            if unique_answers:
                gt_options[question_id] = list(unique_answers)
        
        return gt_options
    except Exception as e:
        st.error(f"Error getting ground truth options: {str(e)}")
        return {}

def apply_video_sorting_and_filtering_enhanced(videos: List[Dict], sort_by: str, sort_order: str, 
                                             filter_by_gt: Dict[int, str], project_id: int, 
                                             selected_annotators: List[str], session: Session,
                                             sort_config: Dict[str, Any] = None) -> List[Dict]:
    """Apply sorting and filtering with proper ascending/descending logic"""
    try:
        sort_config = sort_config or {}
        
        # For Default sorting, we still need to respect ascending/descending order
        # Sort by video ID as a stable sort key
        if sort_by == "Default":
            reverse = (sort_order == "Descending")
            videos.sort(key=lambda x: x.get("id", 0), reverse=reverse)
        else:
            # Only apply advanced sorting if it was explicitly applied by user
            sort_applied = st.session_state.get(f"sort_applied_{project_id}", False)
            
            if not sort_applied:
                # If sort hasn't been applied yet, use default order
                reverse = (sort_order == "Descending")
                videos.sort(key=lambda x: x.get("id", 0), reverse=reverse)
            else:
                # Get sorting data based on configuration
                if sort_by == "Model Confidence":
                    model_user_ids = sort_config.get("model_user_ids", [])
                    question_ids = sort_config.get("question_ids", [])
                    if model_user_ids and question_ids:
                        confidence_scores = get_model_confidence_scores_enhanced(
                            project_id=project_id, model_user_ids=model_user_ids, 
                            question_ids=question_ids, session=session
                        )
                    else:
                        confidence_scores = {}
                elif sort_by == "Annotator Consensus":
                    question_ids = sort_config.get("question_ids", [])
                    if question_ids and len(selected_annotators) >= 2:
                        consensus_rates = get_annotator_consensus_rates_enhanced(
                            project_id=project_id, selected_annotators=selected_annotators, 
                            question_ids=question_ids, session=session
                        )
                    else:
                        consensus_rates = {}
                elif sort_by == "Completion Rate":
                    question_ids = sort_config.get("question_ids", [])
                    if question_ids:
                        completion_rates = get_video_completion_rates_enhanced(
                            project_id=project_id, question_ids=question_ids, session=session
                        )
                    else:
                        completion_rates = {}
                elif sort_by == "Accuracy Rate":
                    question_ids = sort_config.get("question_ids", [])
                    if question_ids and selected_annotators:
                        accuracy_rates = get_video_accuracy_rates(
                            project_id=project_id, selected_annotators=selected_annotators, 
                            question_ids=question_ids, session=session
                        )
                    else:
                        accuracy_rates = {}
        
        # Apply ground truth filtering
        filtered_videos = []
        for video in videos:
            video_id = video["id"]
            include_video = True
            
            if filter_by_gt:
                try:
                    gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                    if gt_df.empty:
                        include_video = False
                    else:
                        for question_id, required_answer in filter_by_gt.items():
                            question_gt = gt_df[gt_df["Question ID"] == question_id]
                            if question_gt.empty or question_gt.iloc[0]["Answer Value"] != required_answer:
                                include_video = False
                                break
                except:
                    include_video = False
            
            if include_video:
                # Add sorting score to video (only for non-default sorts)
                if sort_by != "Default" and st.session_state.get(f"sort_applied_{project_id}", False):
                    if sort_by == "Model Confidence":
                        video["sort_score"] = confidence_scores.get(video_id, 0)
                    elif sort_by == "Annotator Consensus":
                        video["sort_score"] = consensus_rates.get(video_id, 0)
                    elif sort_by == "Completion Rate":
                        video["sort_score"] = completion_rates.get(video_id, 0)
                    elif sort_by == "Accuracy Rate":
                        video["sort_score"] = accuracy_rates.get(video_id, 0)
                
                filtered_videos.append(video)
        
        # Sort videos by score if not default
        if sort_by != "Default" and st.session_state.get(f"sort_applied_{project_id}", False):
            reverse = (sort_order == "Descending")
            filtered_videos.sort(key=lambda x: x.get("sort_score", 0), reverse=reverse)
        
        return filtered_videos
    except Exception as e:
        st.error(f"Error applying sorting and filtering: {str(e)}")
        return videos

def display_project_progress(user_id: int, project_id: int, role: str, session: Session):
    """Display project progress in a refreshable fragment"""
    if role == "annotator":
        overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
        st.progress(overall_progress / 100)
        st.markdown(f"**Your Overall Progress:** {overall_progress:.1f}%")
        display_user_accuracy_simple(user_id=user_id, project_id=project_id, role=role, session=session)
        
    elif role == "meta_reviewer":
        try:
            project_progress = ProjectService.progress(project_id=project_id, session=session)
            st.progress(project_progress['completion_percentage'] / 100)
            st.markdown(f"**Ground Truth Progress:** {project_progress['completion_percentage']:.1f}%")
            display_user_accuracy_simple(user_id=user_id, project_id=project_id, role=role, session=session)
        except ValueError as e:
            st.error(f"Error loading project progress: {str(e)}")
    else:
        try:
            project_progress = ProjectService.progress(project_id=project_id, session=session)
            st.progress(project_progress['completion_percentage'] / 100)
            st.markdown(f"**Ground Truth Progress:** {project_progress['completion_percentage']:.1f}%")
            display_user_accuracy_simple(user_id=user_id, project_id=project_id, role=role, session=session)
        except ValueError as e:
            st.error(f"Error loading project progress: {str(e)}")

def display_enhanced_sort_tab(project_id: int, session: Session):
    """Enhanced sort tab with improved UI/UX and proper validation"""
    st.markdown("#### 🔄 Video Sorting Options")
    
    # Revert to original style to match other tabs
    st.markdown(f"""
    <div style="{get_card_style(COLORS['primary'])}text-align: center;">
        <div style="color: #1f77b4; font-weight: 500; font-size: 0.95rem;">
            📊 Sort videos by different criteria to optimize your review workflow
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Main configuration in a clean layout
    config_col1, config_col2 = st.columns([2, 1])
    
    with config_col1:
        sort_options = ["Default", "Model Confidence", "Annotator Consensus", "Completion Rate", "Accuracy Rate"]
        sort_by = st.selectbox(
            "Sort method",
            sort_options,
            key=f"video_sort_by_{project_id}",
            help="Choose sorting criteria"
        )
    
    with config_col2:
        sort_order = st.selectbox(
            "Order",
            ["Ascending", "Descending"],  # Default to Ascending first
            key=f"video_sort_order_{project_id}",
            help="Sort direction"
        )
    
    # Configuration and validation
    sort_config = {}
    config_valid = True
    config_messages = []
    
    if sort_by != "Default":
        st.markdown("**Configuration:**")
        
        if sort_by == "Model Confidence":
            # Model users selection
            try:
                users_df = AuthService.get_all_users(session=session)
                assignments_df = AuthService.get_project_assignments(session=session)
                project_assignments = assignments_df[assignments_df["Project ID"] == project_id]
                
                model_users = []
                for _, assignment in project_assignments.iterrows():
                    if assignment["Role"] == "model":
                        user_id = assignment["User ID"]
                        user_info = users_df[users_df["ID"] == user_id]
                        if not user_info.empty:
                            model_users.append({
                                "id": user_id,
                                "name": user_info.iloc[0]["User ID"]
                            })
                
                if not model_users:
                    config_messages.append(("error", "No model users found in this project."))
                    config_valid = False
                else:
                    selected_models = st.multiselect(
                        "Model users:",
                        [f"{user['name']} (ID: {user['id']})" for user in model_users],
                        key=f"model_users_{project_id}"
                    )
                    sort_config["model_user_ids"] = [
                        user["id"] for user in model_users 
                        if f"{user['name']} (ID: {user['id']})" in selected_models
                    ]
                    
                    if not sort_config["model_user_ids"]:
                        config_messages.append(("warning", "Select at least one model user."))
                        config_valid = False
            except Exception as e:
                config_messages.append(("error", f"Error loading model users: {str(e)}"))
                config_valid = False
            
            # Questions selection for model confidence
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"confidence_questions_{project_id}",
                    help="Select questions with confidence scores"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
        
        elif sort_by == "Annotator Consensus":
            # Check annotator selection first
            selected_annotators = st.session_state.get("selected_annotators", [])
            if len(selected_annotators) < 2:
                if len(selected_annotators) == 1:
                    config_messages.append(("error", "Consensus requires at least 2 annotators. Only 1 annotator selected."))
                else:
                    config_messages.append(("error", "No annotators selected. Go to Annotators tab first."))
                config_valid = False
            
            # Questions selection
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"consensus_questions_{project_id}",
                    help="Select questions for consensus calculation"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
        
        elif sort_by == "Completion Rate":
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"completion_questions_{project_id}",
                    help="Select questions for completion tracking"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
        
        elif sort_by == "Accuracy Rate":
            # Check annotator selection
            selected_annotators = st.session_state.get("selected_annotators", [])
            if not selected_annotators:
                config_messages.append(("error", "No annotators selected. Go to Annotators tab first."))
                config_valid = False
            
            # Questions selection
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"accuracy_questions_{project_id}",
                    help="Select questions for accuracy comparison"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
    
    # Show configuration messages compactly
    if config_messages:
        for msg_type, msg in config_messages:
            if msg_type == "error":
                st.error(msg)
            elif msg_type == "warning":
                st.warning(msg)
            else:
                st.info(msg)
    
    # Action buttons in a compact row
    action_col1, action_col2, action_col3 = st.columns([1, 1, 1])
    
    with action_col1:
        if st.button("🔄 Apply", 
                    key=f"apply_sort_{project_id}", 
                    disabled=not config_valid,
                    use_container_width=True,
                    type="primary"):
            st.session_state[f"sort_config_{project_id}"] = sort_config
            st.session_state[f"sort_applied_{project_id}"] = True
            st.success("✅ Applied!")
            st.rerun()
    
    with action_col2:
        if st.button("🔄 Reset", 
                    key=f"reset_sort_{project_id}",
                    use_container_width=True):
            st.session_state[f"video_sort_by_{project_id}"] = "Default"
            st.session_state[f"sort_config_{project_id}"] = {}
            st.session_state[f"sort_applied_{project_id}"] = False
            st.success("✅ Reset!")
            st.rerun()
    
    with action_col3:
        # Status indicator
        current_sort = st.session_state.get(f"video_sort_by_{project_id}", "Default")
        sort_applied = st.session_state.get(f"sort_applied_{project_id}", False)
        
        if current_sort != "Default" and sort_applied:
            st.success("✅ Active")
        elif current_sort != "Default":
            st.warning("⏳ Ready")
        else:
            st.info("📋 Default")
    
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid {COLORS['primary']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
        💡 <strong>Tip:</strong> Configure your sorting options above, then click "Apply Sorting" to sort the videos accordingly.
    </div>
    """, unsafe_allow_html=True)

def display_enhanced_filter_tab(project_id: int, session: Session):
    """Enhanced filter tab with proper ground truth detection and full question text"""
    st.markdown("#### 🔍 Video Filtering Options")
    
    st.markdown(f"""
    <div style="{get_card_style(COLORS['warning'])}text-align: center;">
        <div style="color: #856404; font-weight: 500; font-size: 0.95rem;">
            🎯 Filter videos by specific ground truth answers to focus your review
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Get available ground truth options using enhanced function
    gt_options = get_ground_truth_option_filters_enhanced(project_id=project_id, session=session)
    
    if gt_options:
        st.markdown("**Filter by Ground Truth Answers:**")
        
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        selected_filters = {}
        
        for question_id, available_answers in gt_options.items():
            question_text = question_lookup.get(question_id, f"Question {question_id}")
            
            # Don't truncate - show full question text
            display_question = question_text
            
            filter_key = f"video_filter_q_{question_id}_{project_id}"
            selected_answer = st.selectbox(
                f"**{display_question}**",
                ["Any"] + sorted(available_answers),
                key=filter_key,
                help=f"Filter videos where this question has the selected ground truth answer"
            )
            
            if selected_answer != "Any":
                selected_filters[question_id] = selected_answer
        
        if selected_filters:
            filter_summary = []
            for q_id, answer in selected_filters.items():
                q_text = question_lookup.get(q_id, f"Q{q_id}")
                # Show more of the question text in summary
                display_text = q_text[:80] + "..." if len(q_text) > 80 else q_text
                filter_summary.append(f"{display_text} = {answer}")
            
            st.success(f"🔍 **Active Filters:** {' | '.join(filter_summary)}")
        else:
            st.info("ℹ️ **No filters active** - showing all videos")
        
        # Store filters in session state
        st.session_state[f"video_filters_{project_id}"] = selected_filters
    else:
        st.info("No ground truth data available for filtering yet. Complete ground truth annotation to enable filtering.")
        st.session_state[f"video_filters_{project_id}"] = {}
    
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #fff3cd, #ffeaa7); border-left: 4px solid {COLORS['warning']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
        💡 <strong>Tip:</strong> Filters only work on questions that have ground truth answers. Complete annotation first to see more filter options.
    </div>
    """, unsafe_allow_html=True)

def display_project_view(user_id: int, role: str, session: Session):
    """Display the selected project with modern, compact layout and enhanced sorting/filtering"""
    project_id = st.session_state.selected_project_id
    
    if st.button("← Back to Dashboard", key="back_to_dashboard"):
        st.session_state.current_view = "dashboard"
        st.session_state.selected_project_id = None
        if "selected_annotators" in st.session_state:
            del st.session_state.selected_annotators
        # Clear any sorting/filtering state
        for key in list(st.session_state.keys()):
            if key.startswith(f"video_sort_") or key.startswith(f"video_filter_") or key.startswith(f"question_order_"):
                del st.session_state[key]
        st.rerun()
    
    try:
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error loading project: {str(e)}")
        return
    
    mode = "Training" if check_project_has_full_ground_truth(project_id=project_id, session=session) else "Annotation"
    
    st.markdown(f"## 📁 {project.name}")
    
    # Mode display
    if role == "annotator":
        if mode == "Training":
            st.success("🎓 **Training Mode** - Try your best! You'll get immediate feedback after each submission.")
        else:
            st.info("📝 **Annotation Mode** - Try your best to answer the questions accurately.")
    elif role == "meta_reviewer":
        st.warning("🎯 **Meta-Reviewer Mode** - Override ground truth answers as needed. No completion tracking.")
    else:
        st.info("🔍 **Review Mode** - Help create the ground truth dataset!")
    
    display_project_progress(user_id=user_id, project_id=project_id, role=role, session=session)
    
    videos = get_project_videos(project_id=project_id, session=session)
    
    if not videos:
        st.error("No videos found in this project.")
        return
    
    # Role-specific control panels
    if role in ["reviewer", "meta_reviewer"]:
        st.markdown("---")
        
        if mode == "Training":
            analytics_tab, annotator_tab, sort_tab, filter_tab, order_tab, layout_tab = st.tabs([
                "📊 Analytics", "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout"
            ])
            
            with analytics_tab:
                st.markdown("#### 🎯 Performance Insights")
                
                st.markdown(f"""
                <div style="{get_card_style(COLORS['info'])}text-align: center;">
                    <div style="color: #2980b9; font-weight: 500; font-size: 0.95rem;">
                        📈 Access detailed accuracy analytics for all participants in this training project
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                display_accuracy_button_for_project(project_id=project_id, role=role, session=session)
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid {COLORS['info']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
                    💡 <strong>Tip:</strong> Use analytics to identify patterns in annotator performance and areas for improvement.
                </div>
                """, unsafe_allow_html=True)
        else:
            annotator_tab, sort_tab, filter_tab, order_tab, layout_tab = st.tabs([
                "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout"
            ])
        
        with annotator_tab:
            st.markdown("#### 👥 Annotator Management")
            
            st.markdown(f"""
            <div style="{get_card_style('#9c27b0')}text-align: center;">
                <div style="color: #7b1fa2; font-weight: 500; font-size: 0.95rem;">
                    🎯 Select which annotators' responses to display during your review process
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                annotators = get_all_project_annotators(project_id=project_id, session=session)
                display_smart_annotator_selection(annotators=annotators, project_id=project_id)
            except Exception as e:
                st.error(f"Error loading annotators: {str(e)}")
                st.session_state.selected_annotators = []
            
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid #9c27b0; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
                💡 <strong>Tip:</strong> Select annotators whose responses you want to see alongside your review interface.
            </div>
            """, unsafe_allow_html=True)
        
        with sort_tab:
            display_enhanced_sort_tab(project_id=project_id, session=session)

        with filter_tab:
            display_enhanced_filter_tab(project_id=project_id, session=session)
        
        with order_tab:
            st.markdown("#### 📋 Question Group Display Order")
            
            st.markdown(f"""
            <div style="{get_card_style('#e74c3c')}text-align: center;">
                <div style="color: #c0392b; font-weight: 500; font-size: 0.95rem;">
                    🔄 Customize the order of question groups for this session
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Get question groups for this project
            question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
            
            if question_groups:
                order_key = f"question_order_{project_id}_{role}"
                if order_key not in st.session_state:
                    st.session_state[order_key] = [group["ID"] for group in question_groups]
                
                working_order = st.session_state[order_key]
                group_lookup = {group["ID"]: group for group in question_groups}
                
                st.info("💡 Use the ⬆️ and ⬇️ buttons to reorder question groups. This only affects your current session.")
                
                for i, group_id in enumerate(working_order):
                    if group_id in group_lookup:
                        group = group_lookup[group_id]
                        group_title = group["Title"]
                        
                        order_col1, order_col2, order_col3 = st.columns([0.1, 0.8, 0.1])
                        
                        with order_col1:
                            if st.button("⬆️", key=f"group_up_{project_id}_{group_id}_{i}", 
                                        disabled=(i == 0), help="Move up"):
                                st.session_state[order_key][i], st.session_state[order_key][i-1] = \
                                    st.session_state[order_key][i-1], st.session_state[order_key][i]
                                st.rerun()
                        
                        with order_col2:
                            st.write(f"**{i+1}.** {group_title}")
                            st.caption(f"Group ID: {group_id}")
                        
                        with order_col3:
                            if st.button("⬇️", key=f"group_down_{project_id}_{group_id}_{i}", 
                                        disabled=(i == len(working_order) - 1), help="Move down"):
                                st.session_state[order_key][i], st.session_state[order_key][i+1] = \
                                    st.session_state[order_key][i+1], st.session_state[order_key][i]
                                st.rerun()
                
                order_action_col1, order_action_col2 = st.columns(2)
                with order_action_col1:
                    if st.button("🔄 Reset to Default", key=f"reset_group_order_{project_id}"):
                        st.session_state[order_key] = [group["ID"] for group in question_groups]
                        st.rerun()
                
                with order_action_col2:
                    original_order = [group["ID"] for group in question_groups]
                    if working_order != original_order:
                        st.warning("⚠️ Order changed from default")
                    else:
                        st.success("✅ Default order")
            else:
                st.info("No question groups found for this project.")
            
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #ffebee, #ffcdd2); border-left: 4px solid #e74c3c; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
                💡 <strong>Tip:</strong> Reorder groups to match your preferred workflow. Changes only apply to your current session.
            </div>
            """, unsafe_allow_html=True)
        
        with layout_tab:
            st.markdown("#### 🎛️ Video Layout Settings")
            
            st.markdown(f"""
            <div style="{get_card_style(COLORS['warning'])}text-align: center;">
                <div style="color: #856404; font-weight: 500; font-size: 0.95rem;">
                    🎛️ Customize Your Video Display - Adjust how videos and questions are laid out
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            _display_video_layout_controls(videos, role)
            st.info("💡 **Tip:** Adjust layout to optimize your workflow.")
    
    else:  # Annotator role
        st.markdown("---")
        
        layout_tab, = st.tabs(["🎛️ Layout Settings"])
        
        with layout_tab:
            st.markdown("#### 🎛️ Video Layout Settings")
            
            st.markdown(f"""
            <div style="{get_card_style(COLORS['warning'])}text-align: center;">
                <div style="color: #856404; font-weight: 500; font-size: 0.95rem;">
                    🎛️ Customize Your Video Display - Adjust how videos and questions are laid out
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            _display_video_layout_controls(videos, role)
            st.info("💡 **Tip:** Adjust layout to optimize your annotation workflow.")
    
    # Apply sorting and filtering to videos
    if role in ["reviewer", "meta_reviewer"]:
        sort_by = st.session_state.get(f"video_sort_by_{project_id}", "Default")
        sort_order = st.session_state.get(f"video_sort_order_{project_id}", "Descending")
        filter_by_gt = st.session_state.get(f"video_filters_{project_id}", {})
        selected_annotators = st.session_state.get("selected_annotators", [])
        sort_config = st.session_state.get(f"sort_config_{project_id}", {})
        
        videos = apply_video_sorting_and_filtering_enhanced(
            videos=videos, sort_by=sort_by, sort_order=sort_order,
            filter_by_gt=filter_by_gt, project_id=project_id,
            selected_annotators=selected_annotators, session=session,
            sort_config=sort_config
        )
    
    # Get layout settings
    video_pairs_per_row = st.session_state.get(f"{role}_pairs_per_row", 1)
    videos_per_page = st.session_state.get(f"{role}_per_page", min(4, len(videos)))
    
    st.markdown("---")
    
    # Show sorting/filtering summary
    if role in ["reviewer", "meta_reviewer"]:
        sort_by = st.session_state.get(f"video_sort_by_{project_id}", "Default")
        sort_applied = st.session_state.get(f"sort_applied_{project_id}", False)
        filter_by_gt = st.session_state.get(f"video_filters_{project_id}", {})
        
        summary_parts = []
        if sort_by != "Default" and sort_applied:
            sort_order = st.session_state.get(f"video_sort_order_{project_id}", "Ascending")
            summary_parts.append(f"🔄 {sort_by} ({sort_order})")
        elif sort_by != "Default" and not sort_applied:
            summary_parts.append(f"⚙️ {sort_by} configured")
        elif sort_by == "Default":
            sort_order = st.session_state.get(f"video_sort_order_{project_id}", "Ascending")
            summary_parts.append(f"📋 Default order ({sort_order})")
        
        if filter_by_gt:
            summary_parts.append(f"🔍 {len(filter_by_gt)} filter(s)")
        
        if summary_parts:
            st.info(" • ".join(summary_parts))
    
    # Calculate pagination
    total_pages = (len(videos) - 1) // videos_per_page + 1 if videos else 1
    
    page_key = f"{role}_current_page_{project_id}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0
    
    current_page = st.session_state[page_key]
    
    start_idx = current_page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(videos))
    page_videos = videos[start_idx:end_idx]
    
    st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(videos)}**")
    
    # Display videos
    for i in range(0, len(page_videos), video_pairs_per_row):
        row_videos = page_videos[i:i + video_pairs_per_row]
        
        if video_pairs_per_row == 1:
            display_video_answer_pair(row_videos[0], project_id, user_id, role, mode, session)
        else:
            pair_cols = st.columns(video_pairs_per_row)
            for j, video in enumerate(row_videos):
                with pair_cols[j]:
                    display_video_answer_pair(video, project_id, user_id, role, mode, session)
        
        st.markdown("---")
    
    # Pagination controls
    if total_pages > 1:
        def get_pagination_options(current, total):
            if total <= 7:
                return list(range(total))
            
            options = [0]
            start = max(1, current - 1)
            end = min(total - 1, current + 2)
            
            if start > 1:
                options.append("...")
            
            for i in range(start, end):
                if i not in options:
                    options.append(i)
            
            if end < total - 1:
                options.append("...")
            
            if total - 1 not in options:
                options.append(total - 1)
            
            return options
        
        nav_col1, nav_col2, nav_col3 = st.columns([1, 5, 1])
        
        with nav_col1:
            if st.button("◀ Previous Page", disabled=(current_page == 0), key=f"{role}_prev_{project_id}", use_container_width=True):
                st.session_state[page_key] = max(0, current_page - 1)
                st.rerun()
        
        with nav_col2:
            _, center_col, _ = st.columns([1, 2, 1])
            
            with center_col:
                pagination_options = get_pagination_options(current_page, total_pages)
                
                display_options = []
                actual_pages = []
                for opt in pagination_options:
                    if opt == "...":
                        display_options.append("...")
                        actual_pages.append(None)
                    else:
                        display_options.append(f"Page {opt + 1}")
                        actual_pages.append(opt)
                
                try:
                    current_display_index = actual_pages.index(current_page)
                except ValueError:
                    current_display_index = 0
                
                segmented_key = f"{role}_page_segmented_{project_id}"
                selected_display = st.segmented_control(
                    "📄 Navigate Pages", 
                    display_options,
                    default=display_options[current_display_index] if current_display_index < len(display_options) else display_options[0],
                    key=segmented_key
                )
            
            if selected_display and selected_display != "...":
                try:
                    selected_index = display_options.index(selected_display)
                    new_page = actual_pages[selected_index]
                    if new_page is not None and new_page != current_page:
                        st.session_state[page_key] = new_page
                        st.rerun()
                except (ValueError, IndexError):
                    pass
        
        with nav_col3:
            if st.button("Next Page ▶", disabled=(current_page == total_pages - 1), key=f"{role}_next_{project_id}", use_container_width=True):
                st.session_state[page_key] = min(total_pages - 1, current_page + 1)
                st.rerun()
        
        st.markdown(f"<div style='text-align: center; color: #6c757d; margin-top: 1rem;'>Page {current_page + 1} of {total_pages}</div>", unsafe_allow_html=True)

def _display_video_layout_controls(videos: List[Dict], role: str):
    """Display video layout controls"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🔄 Video Pairs Per Row**")
        st.slider(
            "Choose layout", 
            1, 2, 
            st.session_state.get(f"{role}_pairs_per_row", 1), 
            key=f"{role}_pairs_per_row",
            help="Choose how many video-answer pairs to display side by side"
        )
    
    with col2:
        st.markdown("**📄 Videos Per Page**")
        
        min_videos_per_page = st.session_state.get(f"{role}_pairs_per_row", 1)
        max_videos_per_page = max(min(20, len(videos)), min_videos_per_page + 1)
        default_videos_per_page = min(min(4, len(videos)), max_videos_per_page)
        
        if len(videos) == 1:
            st.write("**1** (only video in project)")
        elif max_videos_per_page > min_videos_per_page:
            st.slider(
                "Pagination setting", 
                min_videos_per_page, 
                max_videos_per_page, 
                st.session_state.get(f"{role}_per_page", default_videos_per_page),
                key=f"{role}_per_page",
                help="Set how many videos to show on each page"
            )
        else:
            st.write(f"**{len(videos)}** (showing all videos)")
    
    st.markdown("**🎬 Video Playback Settings**")
    col3, col4 = st.columns(2)
    
    with col3:
        st.checkbox(
            "🚀 Auto-play videos on load",
            value=st.session_state.get(f"{role}_autoplay", True),
            key=f"{role}_autoplay",
            help="Automatically start playing videos when they load"
        )
    
    with col4:
        st.checkbox(
            "🔄 Loop videos",
            value=st.session_state.get(f"{role}_loop", True),
            key=f"{role}_loop",
            help="Automatically restart videos when they finish"
        )

@st.fragment
def display_video_answer_pair(video: Dict, project_id: int, user_id: int, role: str, mode: str, session: Session):
    """Display a single video-answer pair in side-by-side layout with tabs"""
    try:
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
        
        # Apply custom question group order if set
        order_key = f"question_order_{project_id}_{role}"
        if order_key in st.session_state:
            custom_order = st.session_state[order_key]
            group_lookup = {group["ID"]: group for group in question_groups}
            question_groups = [group_lookup[group_id] for group_id in custom_order if group_id in group_lookup]
        
        if not question_groups:
            st.info("No question groups found for this project.")
            return
        
        # Check completion status
        completion_status = {}
        for group in question_groups:
            completion_status[group["ID"]] = check_question_group_completion(
                video_id=video["id"], project_id=project_id, user_id=user_id, 
                question_group_id=group["ID"], role=role, session=session
            )
        
        completed_count = sum(completion_status.values())
        total_count = len(question_groups)
        
        completion_details = [
            f"✅ {group['Title']}" if completion_status[group["ID"]] else group['Title']
            for group in question_groups
        ]
        
        # ORIGINAL Progress display format (REVERTED)
        st.markdown(f"""
        <div style="{get_card_style(COLORS['info'])}text-align: center;">
            <div style="color: #2980b9; font-weight: 500; font-size: 0.95rem;">
                📋 {video['uid']} - {' | '.join(completion_details)} - Progress: {completed_count}/{total_count} Complete
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Two columns layout
        video_col, answer_col = st.columns([1, 1])
        
        with video_col:
            autoplay = st.session_state.get(f"{role}_autoplay", True)
            loop = st.session_state.get(f"{role}_loop", True)
            video_height = custom_video_player(video["url"], autoplay=autoplay, loop=loop)
        
        with answer_col:
            tab_names = [group['Title'] for group in question_groups]
            tabs = st.tabs(tab_names)
            
            for tab, group in zip(tabs, question_groups):
                with tab:
                    display_question_group_in_fixed_container(
                        video=video, project_id=project_id, user_id=user_id, 
                        group_id=group["ID"], role=role, mode=mode, 
                        session=session, container_height=video_height
                    )
                    
    except ValueError as e:
        st.error(f"Error loading project data: {str(e)}")

def check_question_group_completion(video_id: int, project_id: int, user_id: int, question_group_id: int, role: str, session: Session) -> bool:
    """Check if a question group is complete for the user/role"""
    try:
        if role == "annotator":
            return AnnotatorService.check_user_has_submitted_answers(
                video_id=video_id, project_id=project_id, user_id=user_id, 
                question_group_id=question_group_id, session=session
            )
        elif role == "meta_reviewer":
            return (GroundTruthService.check_all_questions_modified_by_admin(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ) or check_all_questions_have_ground_truth(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ))
        else:  # reviewer
            return (GroundTruthService.check_all_questions_modified_by_admin(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ) or check_all_questions_have_ground_truth(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ))
    except:
        return False

@st.dialog("🎉 Congratulations!")
def show_annotator_completion():
    """Simple completion popup for annotators"""
    st.markdown("### 🎉 **CONGRATULATIONS!** 🎉")
    st.success("You've completed all questions in this project!")
    st.info("Great work! You can now move on to other projects or review your answers.")
    
    st.snow()
    st.balloons()
    
    if st.button("Close", use_container_width=True):
        st.rerun()

@st.dialog("🎉 Outstanding Work!")
def show_reviewer_completion():
    """Simple completion popup for reviewers"""
    st.markdown("### 🎉 **OUTSTANDING WORK!** 🎉")
    st.success("This project's ground truth dataset is now complete!")
    st.info("Please notify the admin that you have completed this project. Excellent job!")
    
    st.snow()
    st.balloons()
    
    if st.button("Close", use_container_width=True):
        st.rerun()

def check_all_questions_have_ground_truth(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
    """Check if all questions in a group have ground truth answers for this specific video"""
    try:
        questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
        if not questions:
            return False
        
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        if gt_df.empty:
            return False
        
        gt_question_ids = set(gt_df["Question ID"].tolist())
        return all(question["id"] in gt_question_ids for question in questions)
    except:
        return False

def check_ground_truth_exists_for_group(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
    """Check if ANY ground truth exists for questions in this group"""
    try:
        questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
        if not questions:
            return False
        
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        if gt_df.empty:
            return False
        
        gt_question_ids = set(gt_df["Question ID"].tolist())
        return any(question["id"] in gt_question_ids for question in questions)
    except:
        return False

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

def display_question_group_in_fixed_container(video: Dict, project_id: int, user_id: int, group_id: int, role: str, mode: str, session: Session, container_height: int):
    """Display question group content with fixed review loading and submission"""
    try:
        questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        
        if not questions:
            st.info("No questions in this group.")
            # Create empty form to prevent missing submit button error
            with st.form(f"empty_form_{video['id']}_{group_id}_{role}"):
                st.info("No questions available in this group.")
                st.form_submit_button("No Actions Available", disabled=True)
            return
        
        # Get selected annotators for reviewer/meta-reviewer roles
        selected_annotators = None
        if role in ["reviewer", "meta_reviewer"]:
            selected_annotators = st.session_state.get("selected_annotators", [])
        
        # Check admin modifications
        has_any_admin_modified_questions = False
        if role == "reviewer":
            for question in questions:
                if GroundTruthService.check_question_modified_by_admin(
                    video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                ):
                    has_any_admin_modified_questions = True
                    break
        
        display_data = _get_question_display_data(
            video["id"], project_id, user_id, group_id, role, mode, session,
            has_any_admin_modified_questions
        )
        
        if display_data["error"]:
            st.info(display_data["error"])
            # Create empty form to prevent missing submit button error
            with st.form(f"error_form_{video['id']}_{group_id}_{role}"):
                st.error(display_data["error"])
                st.form_submit_button("Unable to Load", disabled=True)
            return
        
        questions = display_data["questions"]
        all_questions_modified_by_admin = display_data["all_questions_modified_by_admin"]
        existing_answers = display_data["existing_answers"]
        form_disabled = display_data["form_disabled"]
        
        # Get ground truth for training mode
        gt_answers = {}
        if mode == "Training" and role == "annotator":
            try:
                gt_df = GroundTruthService.get_ground_truth(video_id=video["id"], project_id=project_id, session=session)
                if not gt_df.empty:
                    question_map = {q["id"]: q for q in questions}
                    for _, gt_row in gt_df.iterrows():
                        question_id = gt_row["Question ID"]
                        if question_id in question_map:
                            question_text = question_map[question_id]["text"]
                            gt_answers[question_text] = gt_row["Answer Value"]
            except:
                pass
        
        # Check if we have editable questions
        has_any_editable_questions = False
        for question in questions:
            if role == "reviewer":
                is_admin_modified = GroundTruthService.check_question_modified_by_admin(
                    video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                )
                if not is_admin_modified:
                    has_any_editable_questions = True
                    break
            elif role == "meta_reviewer":
                has_any_editable_questions = True
                break
            else:
                has_any_editable_questions = True
                break
        
        is_group_complete = check_question_group_completion(
            video_id=video["id"], project_id=project_id, user_id=user_id, 
            question_group_id=group_id, role=role, session=session
        )
        
        ground_truth_exists = False
        if role == "meta_reviewer":
            ground_truth_exists = check_ground_truth_exists_for_group(
                video_id=video["id"], project_id=project_id, question_group_id=group_id, session=session
            )
        
        button_text, button_disabled = _get_submit_button_config(
            role, form_disabled, all_questions_modified_by_admin, 
            has_any_editable_questions, is_group_complete, mode, ground_truth_exists,
            has_any_admin_modified_questions
        )
        
        # Initialize answer review states
        answer_reviews = {}
        if role in ["reviewer", "meta_reviewer"]:
            for question in questions:
                if question["type"] == "description":
                    question_text = question["text"]
                    existing_review_data = _load_existing_answer_reviews(
                        video_id=video["id"], project_id=project_id, 
                        question_id=question["id"], session=session
                    )
                    answer_reviews[question_text] = existing_review_data
        
        # Create form - ensure it always has questions and a submit button
        form_key = f"form_{video['id']}_{group_id}_{role}"
        with st.form(form_key):
            answers = {}
            
            content_height = max(350, container_height - 150)
            
            # Ensure we always display content even if questions fail to load
            try:
                with st.container(height=content_height, border=False):
                    for i, question in enumerate(questions):
                        question_id = question["id"]
                        question_text = question["text"]
                        existing_value = existing_answers.get(question_text, "")
                        gt_value = gt_answers.get(question_text, "")
                        
                        is_modified_by_admin = False
                        admin_info = None
                        if role in ["reviewer", "meta_reviewer"]:
                            is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                video_id=video["id"], project_id=project_id, question_id=question_id, session=session
                            )
                            if is_modified_by_admin:
                                admin_info = GroundTruthService.get_admin_modification_details(
                                    video_id=video["id"], project_id=project_id, question_id=question_id, session=session
                                )
                        
                        if i > 0:
                            st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                        
                        # Pass selected_annotators parameter
                        if question["type"] == "single":
                            answers[question_text] = _display_clean_sticky_single_choice_question(
                                question, video["id"], project_id, role, existing_value,
                                is_modified_by_admin, admin_info, form_disabled, session,
                                gt_value, mode, selected_annotators
                            )
                        else:
                            answers[question_text] = _display_clean_sticky_description_question(
                                question, video["id"], project_id, role, existing_value,
                                is_modified_by_admin, admin_info, form_disabled, session,
                                gt_value, mode, answer_reviews, selected_annotators
                            )
            except Exception as e:
                st.error(f"Error displaying questions: {str(e)}")
                # Still provide empty answers dict for form submission
                answers = {}
            
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            # ALWAYS include a submit button
            submitted = st.form_submit_button(button_text, use_container_width=True, disabled=button_disabled)
            
            # Handle form submission (rest remains the same...)
            if submitted and not button_disabled:
                try:
                    if role == "annotator":
                        AnnotatorService.submit_answer_to_question_group(
                            video_id=video["id"], project_id=project_id, user_id=user_id,
                            question_group_id=group_id, answers=answers, session=session
                        )
                        
                        try:
                            overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
                            if overall_progress >= 100:
                                show_annotator_completion()
                                return
                        except:
                            pass
                        
                        if mode == "Training":
                            show_training_feedback(video_id=video["id"], project_id=project_id, group_id=group_id, user_answers=answers, session=session)
                        else:
                            st.success("✅ Answers submitted!")
                    
                    elif role == "meta_reviewer":
                        try:
                            GroundTruthService.override_ground_truth_to_question_group(
                                video_id=video["id"], project_id=project_id, question_group_id=group_id,
                                admin_id=user_id, answers=answers, session=session
                            )
                            
                            if answer_reviews:
                                _submit_answer_reviews(answer_reviews, video["id"], project_id, user_id, session)
                            
                            st.success("✅ Ground truth overridden!")
                            
                        except ValueError as e:
                            st.error(f"Error overriding ground truth: {str(e)}")
                    
                    else:  # reviewer
                        if has_any_admin_modified_questions:
                            st.warning("Cannot submit: Some questions have been overridden by admin.")
                            return
                        
                        editable_answers = {
                            question["text"]: answers[question["text"]]
                            for question in questions
                            if not GroundTruthService.check_question_modified_by_admin(
                                video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                            )
                        }
                        
                        if editable_answers:
                            GroundTruthService.submit_ground_truth_to_question_group(
                                video_id=video["id"], project_id=project_id, reviewer_id=user_id,
                                question_group_id=group_id, answers=editable_answers, session=session
                            )
                            
                            if answer_reviews:
                                _submit_answer_reviews(answer_reviews, video["id"], project_id, user_id, session)
                            
                            try:
                                project_progress = ProjectService.progress(project_id=project_id, session=session)
                                if project_progress['completion_percentage'] >= 100:
                                    show_reviewer_completion()
                                    return
                            except:
                                pass
                            
                            st.success("✅ Ground truth submitted!")
                        else:
                            st.warning("No editable questions to submit.")
                    
                    st.rerun(scope="fragment")
                    
                except ValueError as e:
                    st.error(f"Error: {str(e)}")
                    
    except ValueError as e:
        st.error(f"Error loading question group: {str(e)}")
        # Create fallback form to prevent missing submit button error
        with st.form(f"fallback_form_{video['id']}_{group_id}_{role}"):
            st.error("Failed to load question group properly")
            st.form_submit_button("Unable to Load Questions", disabled=True)

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
                                (answers_df["Question ID"] == int(question.id)) & 
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

def _get_question_display_data(video_id: int, project_id: int, user_id: int, group_id: int, role: str, mode: str, session: Session, has_any_admin_modified_questions: bool) -> Dict:
    """Get all the data needed to display a question group"""
    questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
    
    if not questions:
        return {"questions": [], "error": "No questions in this group."}
    
    all_questions_modified_by_admin = GroundTruthService.check_all_questions_modified_by_admin(
        video_id=video_id, project_id=project_id, question_group_id=group_id, session=session
    )
    
    if role == "annotator":
        existing_answers = AnnotatorService.get_user_answers_for_question_group(
            video_id=video_id, project_id=project_id, user_id=user_id, question_group_id=group_id, session=session
        )
    else:
        existing_answers = GroundTruthService.get_ground_truth_for_question_group(
            video_id=video_id, project_id=project_id, question_group_id=group_id, session=session
        )
    
    form_disabled = False
    if role == "annotator":
        form_disabled = (mode == "Training" and 
                       AnnotatorService.check_user_has_submitted_answers(
                           video_id=video_id, project_id=project_id, user_id=user_id, question_group_id=group_id, session=session
                       ))
    elif role == "reviewer":
        form_disabled = has_any_admin_modified_questions
    
    return {
        "questions": questions,
        "all_questions_modified_by_admin": all_questions_modified_by_admin,
        "existing_answers": existing_answers,
        "form_disabled": form_disabled,
        "error": None
    }

def _display_clean_sticky_single_choice_question(question: Dict, video_id: int, project_id: int, role: str, existing_value: str, is_modified_by_admin: bool, admin_info: Optional[Dict], form_disabled: bool, session: Session, gt_value: str = "", mode: str = "", selected_annotators: List[str] = None) -> str:
    """Display a single choice question with unified status display"""
    question_id = question["id"]
    question_text = question["display_text"]
    options = question["options"]
    display_values = question.get("display_values", options)
    
    display_to_value = dict(zip(display_values, options))
    value_to_display = dict(zip(options, display_values))
    
    default_idx = 0
    if existing_value and existing_value in value_to_display:
        display_val = value_to_display[existing_value]
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
    if role in ["reviewer", "meta_reviewer"]:
        show_annotators = selected_annotators is not None and len(selected_annotators) > 0
        _display_unified_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or []
        )
    
    # Question content with FIXED UNIQUE KEYS
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
            key=f"q_{video_id}_{project_id}_{question_id}_{role}_locked",  # ✅ Added project_id
            disabled=True,
            label_visibility="collapsed",
            horizontal=True
        )
        
        result = current_value
        
    elif role in ["reviewer", "meta_reviewer"]:
        # ALWAYS get enhanced options for reviewer/meta-reviewer roles (including search portal)
        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            options=options, display_values=display_values, session=session,
            selected_annotators=selected_annotators or []
        )
        
        radio_key = f"q_{video_id}_{project_id}_{question_id}_{role}_stable"  # ✅ Added project_id
        
        current_idx = default_idx
        if existing_value:
            for i, opt in enumerate(options):
                if opt == existing_value:
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
                key=f"q_{video_id}_{project_id}_{question_id}_{role}",  # ✅ Added project_id
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
                key=f"q_{video_id}_{project_id}_{question_id}_{role}",  # ✅ Added project_id
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True
            )
            result = display_to_value[selected_display]
    
    return result

def _display_clean_sticky_description_question(question: Dict, video_id: int, project_id: int, role: str, existing_value: str, is_modified_by_admin: bool, admin_info: Optional[Dict], form_disabled: bool, session: Session, gt_value: str = "", mode: str = "", answer_reviews: Optional[Dict] = None, selected_annotators: List[str] = None) -> str:
    """Display a description question with unified status display"""
    
    question_id = question["id"]
    question_text = question["display_text"]
    
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
    
    # Question content with FIXED UNIQUE KEYS
    if role == "reviewer" and is_modified_by_admin and admin_info:
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        st.warning(f"🔒 **Overridden by {admin_name}**")
        
        answer = st.text_area(
            "Admin's answer:",
            value=current_value,
            key=f"q_{video_id}_{project_id}_{question_id}_{role}_locked",  # ✅ Added project_id
            disabled=True,
            height=120,
            label_visibility="collapsed"
        )
        
        result = current_value
        
    elif role in ["reviewer", "meta_reviewer"]:
        text_key = f"q_{video_id}_{project_id}_{question_id}_{role}_stable"  # ✅ Added project_id
        
        answer = st.text_area(
            "Enter your answer:",
            value=existing_value,
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
        if show_annotators or (role == "meta_reviewer" and existing_value):
            _display_enhanced_helper_text_answers(
                video_id=video_id, project_id=project_id, question_id=question_id, 
                question_text=question_text, text_key=text_key,
                gt_value=existing_value if role == "meta_reviewer" else "",
                role=role, answer_reviews=answer_reviews, session=session,
                selected_annotators=selected_annotators or []
            )
        
        result = answer
        
    else:
        result = st.text_area(
            "Enter your answer:",
            value=existing_value,
            key=f"q_{video_id}_{project_id}_{question_id}_{role}",  # ✅ Added project_id
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

def _get_enhanced_options_for_reviewer(video_id: int, project_id: int, question_id: int, options: List[str], display_values: List[str], session: Session, selected_annotators: List[str] = None) -> List[str]:
    """Get enhanced options showing who selected what for reviewers"""
    
    try:
        # Get annotator selections if annotators are provided
        annotator_user_ids = []
        if selected_annotators:
            annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
        
        option_selections = GroundTruthService.get_question_option_selections(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            annotator_user_ids=annotator_user_ids, session=session
        )
        
        # Also get ground truth info for search portal
        gt_selection = None
        try:
            gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
            if not gt_df.empty:
                question_gt = gt_df[gt_df["Question ID"] == question_id]
                if not question_gt.empty:
                    gt_selection = question_gt.iloc[0]["Answer Value"]
        except:
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
    
    total_annotators = len(annotator_user_ids) if selected_annotators else 0
    enhanced_options = []
    
    for i, display_val in enumerate(display_values):
        actual_val = options[i] if i < len(options) else display_val
        option_text = display_val
        
        selection_info = []
        
        # Add annotator selection info if available
        if actual_val in option_selections:
            annotators = []
            for selector in option_selections[actual_val]:
                if selector["type"] != "ground_truth":  # Don't double-count GT here
                    annotators.append(selector["initials"])
            
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
            except:
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

def _get_submit_button_config(role: str, form_disabled: bool, all_questions_modified_by_admin: bool, has_any_editable_questions: bool, is_group_complete: bool, mode: str, ground_truth_exists: bool = False, has_any_admin_modified_questions: bool = False) -> Tuple[str, bool]:
    """Get the submit button text and disabled state with improved logic"""
    if role == "annotator":
        if form_disabled:
            return "🔒 Already Submitted", True
        elif is_group_complete and mode != "Training":
            return "✅ Re-submit Answers", False
        elif is_group_complete:
            return "✅ Completed", True
        else:
            return "Submit Answers", False
    elif role == "meta_reviewer":
        if not ground_truth_exists:
            return "🚫 No Ground Truth Yet", True
        else:
            return "🎯 Override Ground Truth", False
    else:  # reviewer
        if has_any_admin_modified_questions:
            return "🔒 Overridden by Admin", True
        elif not has_any_editable_questions:
            return "🔒 No Editable Questions", True
        elif is_group_complete:
            return "✅ Re-submit Ground Truth", False
        else:
            return "Submit Ground Truth", False

###############################################################################
# ANNOTATOR PORTAL
###############################################################################

@handle_database_errors
def annotator_portal():
    st.title("👥 Annotator Portal")
    user = st.session_state.user
    
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            display_project_dashboard(user_id=user["id"], role="annotator", session=session)
        elif current_view == "project":
            display_project_view(user_id=user["id"], role="annotator", session=session)

###############################################################################
# REVIEWER PORTAL
###############################################################################

@handle_database_errors
def reviewer_portal():
    st.title("🔍 Reviewer Portal")
    user = st.session_state.user
    
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            display_project_dashboard(user_id=user["id"], role="reviewer", session=session)
        elif current_view == "project":
            display_project_view(user_id=user["id"], role="reviewer", session=session)

###############################################################################
# META-REVIEWER PORTAL
###############################################################################

@handle_database_errors
def meta_reviewer_portal():
    """Meta-Reviewer Portal for global admins"""
    st.title("🎯 Meta-Reviewer Portal")
    user = st.session_state.user
    
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            display_project_dashboard(user_id=user["id"], role="meta_reviewer", session=session)
        elif current_view == "project":
            display_project_view(user_id=user["id"], role="meta_reviewer", session=session)

###############################################################################
# ADMIN PORTAL - OPTIMIZED WITH FRAGMENTS
###############################################################################

@handle_database_errors
def admin_portal():
    st.title("⚙️ Admin Portal")
    
    tabs = st.tabs([
        "📹 Videos", "📁 Projects", "📋 Schemas", 
        "❓ Questions", "👥 Users", "🔗 Assignments", "📊 Project Groups"
    ])
    
    with tabs[0]:
        admin_videos()
    with tabs[1]:
        admin_projects()
    with tabs[2]:
        admin_schemas()
    with tabs[3]:
        admin_questions()
    with tabs[4]:
        admin_users()
    with tabs[5]:
        admin_assignments()
    with tabs[6]:
        admin_project_groups()

@st.fragment
def admin_videos():
    st.subheader("📹 Video Management")
    
    with get_db_session() as session:
        videos_df = VideoService.get_videos_with_project_status(session=session)
        
        if not videos_df.empty:
            st.dataframe(videos_df, use_container_width=True)
        else:
            st.info("No videos in the database yet.")
        
        video_tabs = st.tabs(["➕ Add Video", "✏️ Edit Video"])
        
        with video_tabs[0]:
            url = st.text_input("Video URL", key="admin_video_url")
            metadata_json = st.text_area("Metadata (JSON, optional)", "{}", key="admin_video_metadata")
            
            if st.button("Add Video", key="admin_add_video_btn"):
                if url:
                    try:
                        import json
                        metadata = json.loads(metadata_json) if metadata_json.strip() else {}
                        VideoService.add_video(url=url, session=session, metadata=metadata)
                        st.success("Video added!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        with video_tabs[1]:
            if not videos_df.empty:
                # Get all videos for selection
                all_videos_df = VideoService.get_all_videos(session=session)
                if not all_videos_df.empty:
                    video_options = {f"{row['Video UID']} - {row['URL'][:50]}...": row['Video UID'] for _, row in all_videos_df.iterrows()}
                    selected_video_display = st.selectbox(
                        "Select Video to Edit",
                        list(video_options.keys()),
                        key="admin_edit_video_select"
                    )
                    
                    if selected_video_display:
                        selected_video_uid = video_options[selected_video_display]
                        
                        try:
                            # Get current video details
                            current_video = VideoService.get_video_by_uid(video_uid=selected_video_uid, session=session)
                            
                            if current_video:
                                st.markdown(f"**Editing Video:** {selected_video_uid}")
                                
                                col1, col2 = st.columns([2, 1])
                                
                                with col1:
                                    new_url = st.text_input(
                                        "Video URL",
                                        value=current_video.url,
                                        key="admin_edit_video_url",
                                        help="Update the video URL"
                                    )
                                
                                with col2:
                                    st.markdown("**Current Video UID:**")
                                    st.code(selected_video_uid)
                                    st.caption("Video UID cannot be changed")
                                
                                # Metadata editing
                                st.markdown("**Video Metadata:**")
                                current_metadata = current_video.video_metadata or {}
                                
                                metadata_tab1, metadata_tab2 = st.tabs(["🎛️ Form Editor", "📝 JSON Editor"])
                                
                                with metadata_tab1:
                                    st.markdown("**Edit metadata fields:**")
                                    
                                    # Dynamic metadata form
                                    updated_metadata = {}
                                    
                                    # Show existing metadata fields
                                    for key, value in current_metadata.items():
                                        col_key, col_val, col_del = st.columns([1, 2, 0.3])
                                        with col_key:
                                            new_key = st.text_input(f"Key", value=key, key=f"meta_key_{key}")
                                        with col_val:
                                            if isinstance(value, (str, int, float)):
                                                new_value = st.text_input(f"Value", value=str(value), key=f"meta_val_{key}")
                                                try:
                                                    # Try to convert back to original type
                                                    if isinstance(value, int):
                                                        new_value = int(new_value)
                                                    elif isinstance(value, float):
                                                        new_value = float(new_value)
                                                except ValueError:
                                                    pass  # Keep as string
                                            else:
                                                new_value = st.text_area(f"Value (JSON)", value=str(value), key=f"meta_val_{key}")
                                                try:
                                                    import json
                                                    new_value = json.loads(new_value)
                                                except:
                                                    new_value = str(value)  # Fallback to string
                                        with col_del:
                                            if st.button("🗑️", key=f"del_{key}", help="Delete this field"):
                                                continue  # Skip adding to updated_metadata
                                        
                                        if new_key:  # Only add if key is not empty
                                            updated_metadata[new_key] = new_value
                                    
                                    # Add new metadata field
                                    st.markdown("**Add new field:**")
                                    col_new_key, col_new_val = st.columns(2)
                                    with col_new_key:
                                        new_field_key = st.text_input("New field key", key="admin_new_meta_key")
                                    with col_new_val:
                                        new_field_value = st.text_input("New field value", key="admin_new_meta_value")
                                    
                                    if new_field_key and new_field_value:
                                        # Try to parse as number if possible
                                        try:
                                            if '.' in new_field_value:
                                                updated_metadata[new_field_key] = float(new_field_value)
                                            else:
                                                updated_metadata[new_field_key] = int(new_field_value)
                                        except ValueError:
                                            updated_metadata[new_field_key] = new_field_value
                                
                                with metadata_tab2:
                                    import json
                                    metadata_json = st.text_area(
                                        "Metadata JSON",
                                        value=json.dumps(current_metadata, indent=2) if current_metadata else "{}",
                                        height=200,
                                        key="admin_edit_video_metadata_json",
                                        help="Edit metadata as JSON. This will override form editor changes."
                                    )
                                    
                                    try:
                                        json_metadata = json.loads(metadata_json)
                                        st.success("✅ Valid JSON")
                                        updated_metadata = json_metadata
                                    except json.JSONDecodeError as e:
                                        st.error(f"❌ Invalid JSON: {str(e)}")
                                        updated_metadata = current_metadata
                                
                                # Update button
                                update_col, preview_col = st.columns(2)
                                
                                with update_col:
                                    if st.button("💾 Update Video", key="admin_update_video_btn", use_container_width=True):
                                        try:
                                            # Use proper service methods
                                            VideoService.update_video(
                                                video_uid=selected_video_uid, 
                                                new_url=new_url, 
                                                new_metadata=updated_metadata, 
                                                session=session
                                            )
                                            
                                            st.success(f"✅ Video '{selected_video_uid}' updated successfully!")
                                            st.rerun(scope="fragment")
                                        except Exception as e:
                                            st.error(f"❌ Error updating video: {str(e)}")
                                
                                with preview_col:
                                    with st.expander("👁️ Preview Changes"):
                                        st.markdown("**New URL:**")
                                        st.code(new_url)
                                        st.markdown("**New Metadata:**")
                                        st.json(updated_metadata)
                            else:
                                st.error(f"Video with UID '{selected_video_uid}' not found")
                        except Exception as e:
                            st.error(f"Error loading video details: {str(e)}")
                else:
                    st.info("No videos available to edit")
            else:
                st.info("No videos available to edit")

@st.fragment
def admin_questions():
    st.subheader("❓ Question & Group Management")
    
    with get_db_session() as session:
        q_tab1, q_tab2 = st.tabs(["📁 Question Groups", "❓ Individual Questions"])
        
        with q_tab1:
            groups_df = QuestionGroupService.get_all_groups(session=session)
            st.dataframe(groups_df, use_container_width=True)
            
            group_management_tabs = st.tabs(["➕ Create Group", "✏️ Edit Group"])
            
            with group_management_tabs[0]:
                st.markdown("### 🆕 Create New Question Group")
                
                basic_col1, basic_col2 = st.columns(2)
                with basic_col1:
                    title = st.text_input("Group Title", key="admin_group_title", placeholder="Enter group title...")
                with basic_col2:
                    is_reusable = st.checkbox("Reusable across schemas", key="admin_group_reusable", 
                                            help="Allow this group to be used in multiple schemas")
                
                description = st.text_area("Description", key="admin_group_description", 
                                         placeholder="Describe the purpose of this question group...")
                
                # Verification function selection
                st.markdown("**🔧 Verification Function (Optional):**")
                try:
                    available_functions = QuestionGroupService.get_available_verification_functions()
                    if available_functions:
                        verification_function = st.selectbox(
                            "Select verification function",
                            ["None"] + available_functions,
                            key="admin_group_verification",
                            help="Optional function to validate answers"
                        )
                        
                        if verification_function != "None":
                            try:
                                func_info = QuestionGroupService.get_verification_function_info(verification_function)
                                st.info(f"**Function:** `{func_info['name']}{func_info['signature']}`")
                                if func_info['docstring']:
                                    st.markdown(f"**Documentation:** {func_info['docstring']}")
                            except Exception as e:
                                st.error(f"Error loading function info: {str(e)}")
                        
                        verification_function = verification_function if verification_function != "None" else None
                    else:
                        st.info("No verification functions found in verify.py")
                        verification_function = None
                except Exception as e:
                    st.error(f"Error loading verification functions: {str(e)}")
                    verification_function = None
                
                st.markdown("**📋 Select Questions:**")
                questions_df = QuestionService.get_all_questions(session=session)
                if not questions_df.empty:
                    available_questions = questions_df[~questions_df["Archived"]]
                    selected_questions = st.multiselect(
                        "Questions",
                        available_questions["ID"].tolist(),
                        format_func=lambda x: available_questions[available_questions["ID"]==x]["Text"].iloc[0],
                        key="admin_group_questions",
                        help="Select questions to include in this group"
                    )
                else:
                    selected_questions = []
                    st.warning("No questions available.")
                
                if st.button("🚀 Create Question Group", key="admin_create_group_btn", type="primary", use_container_width=True):
                    if title and selected_questions:
                        try:
                            QuestionGroupService.create_group(
                                title=title, description=description, is_reusable=is_reusable, 
                                question_ids=selected_questions, verification_function=verification_function, 
                                session=session
                            )
                            st.success("✅ Question group created successfully!")
                            st.rerun(scope="fragment")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
            
            with group_management_tabs[1]:
                st.markdown("### ✏️ Edit Existing Question Group")
                
                if not groups_df.empty:
                    available_groups = groups_df[~groups_df["Archived"]]
                    if not available_groups.empty:
                        group_options = {f"{row['Name']} (ID: {row['ID']})": row['ID'] for _, row in available_groups.iterrows()}
                        selected_group_name = st.selectbox(
                            "Select Group to Edit",
                            list(group_options.keys()),
                            key="admin_edit_group_select",
                            help="Choose a question group to modify"
                        )
                        
                        if selected_group_name:
                            selected_group_id = group_options[selected_group_name]
                            
                            try:
                                group_details = QuestionGroupService.get_group_details_with_verification(
                                    group_id=selected_group_id, session=session
                                )
                                current_verification = group_details.get("verification_function")
                                
                                edit_basic_col1, edit_basic_col2 = st.columns(2)
                                with edit_basic_col1:
                                    new_title = st.text_input(
                                        "Group Title",
                                        value=group_details["title"],
                                        key="admin_edit_group_title"
                                    )
                                with edit_basic_col2:
                                    new_is_reusable = st.checkbox(
                                        "Reusable across schemas",
                                        value=group_details["is_reusable"],
                                        key="admin_edit_group_reusable"
                                    )
                                
                                new_description = st.text_area(
                                    "Description",
                                    value=group_details["description"] or "",
                                    key="admin_edit_group_description"
                                )
                                
                                # Verification function editing
                                st.markdown("**🔧 Verification Function:**")
                                try:
                                    available_functions = QuestionGroupService.get_available_verification_functions()
                                    verification_options = ["None"] + available_functions
                                    current_index = 0
                                    if current_verification and current_verification in available_functions:
                                        current_index = verification_options.index(current_verification)
                                    
                                    new_verification_function = st.selectbox(
                                        "Select verification function",
                                        verification_options,
                                        index=current_index,
                                        key="admin_edit_group_verification",
                                        help="Optional function to validate answers"
                                    )
                                    
                                    func_col1, func_col2 = st.columns(2)
                                    
                                    with func_col1:
                                        st.markdown("**Current Function:**")
                                        if current_verification:
                                            try:
                                                current_func_info = QuestionGroupService.get_verification_function_info(current_verification)
                                                st.code(f"{current_func_info['name']}{current_func_info['signature']}")
                                                if current_func_info['docstring']:
                                                    st.caption(f"**Doc:** {current_func_info['docstring']}")
                                            except Exception as e:
                                                st.error(f"Error loading current function: {str(e)}")
                                        else:
                                            st.info("No verification function set")
                                    
                                    with func_col2:
                                        st.markdown("**New Function:**")
                                        if new_verification_function != "None":
                                            try:
                                                new_func_info = QuestionGroupService.get_verification_function_info(new_verification_function)
                                                st.code(f"{new_func_info['name']}{new_func_info['signature']}")
                                                if new_func_info['docstring']:
                                                    st.caption(f"**Doc:** {new_func_info['docstring']}")
                                            except Exception as e:
                                                st.error(f"Error loading function info: {str(e)}")
                                        else:
                                            st.info("No verification function will be set")
                                    
                                    new_verification_function = new_verification_function if new_verification_function != "None" else None
                                    
                                except Exception as e:
                                    st.error(f"Error loading verification functions: {str(e)}")
                                    new_verification_function = current_verification
                                
                                # Question order management
                                st.markdown("**📋 Question Order Management:**")
                                current_order = QuestionGroupService.get_question_order(group_id=selected_group_id, session=session)
                                
                                if current_order:
                                    questions_df = QuestionService.get_all_questions(session=session)
                                    
                                    order_key = f"edit_group_order_{selected_group_id}"
                                    if order_key not in st.session_state:
                                        st.session_state[order_key] = current_order.copy()
                                    
                                    working_order = st.session_state[order_key]
                                    
                                    st.info("💡 Use the ⬆️ and ⬇️ buttons to reorder questions. Changes will be applied when you click 'Update Group'.")
                                    
                                    if len(working_order) > 5:
                                        search_term = st.text_input(
                                            "🔍 Search questions (to quickly find questions in large groups)",
                                            key=f"search_questions_{selected_group_id}",
                                            placeholder="Type part of a question..."
                                        )
                                    else:
                                        search_term = ""
                                    
                                    for i, q_id in enumerate(working_order):
                                        question_row = questions_df[questions_df["ID"] == q_id]
                                        if not question_row.empty:
                                            q_text = question_row.iloc[0]["Text"]
                                            
                                            if search_term and search_term.lower() not in q_text.lower():
                                                continue
                                            
                                            order_col1, order_col2, order_col3 = st.columns([0.1, 0.8, 0.1])
                                            
                                            with order_col1:
                                                if st.button("⬆️", key=f"up_{selected_group_id}_{q_id}_{i}", 
                                                            disabled=(i == 0), help="Move up"):
                                                    st.session_state[order_key][i], st.session_state[order_key][i-1] = \
                                                        st.session_state[order_key][i-1], st.session_state[order_key][i]
                                                    st.rerun()
                                            
                                            with order_col2:
                                                display_text = q_text[:80] + ('...' if len(q_text) > 80 else '')
                                                if search_term and search_term.lower() in q_text.lower():
                                                    st.write(f"**{i+1}.** {display_text} 🔍")
                                                else:
                                                    st.write(f"**{i+1}.** {display_text}")
                                                st.caption(f"ID: {q_id}")
                                            
                                            with order_col3:
                                                if st.button("⬇️", key=f"down_{selected_group_id}_{q_id}_{i}", 
                                                            disabled=(i == len(working_order) - 1), help="Move down"):
                                                    st.session_state[order_key][i], st.session_state[order_key][i+1] = \
                                                        st.session_state[order_key][i+1], st.session_state[order_key][i]
                                                    st.rerun()
                                    
                                    order_action_col1, order_action_col2 = st.columns(2)
                                    with order_action_col1:
                                        if st.button("🔄 Reset Order", key=f"reset_order_{selected_group_id}"):
                                            st.session_state[order_key] = current_order.copy()
                                            st.rerun()
                                    
                                    with order_action_col2:
                                        if working_order != current_order:
                                            st.warning("⚠️ Order changed - click 'Update Group' to save")
                                        else:
                                            st.success("✅ Order matches saved state")
                                    
                                    new_order = working_order
                                else:
                                    new_order = current_order
                                    st.info("No questions in this group.")
                                
                                if st.button("💾 Update Question Group", key="admin_update_group_btn", type="primary", use_container_width=True):
                                    try:
                                        QuestionGroupService.edit_group(
                                            group_id=selected_group_id, new_title=new_title,
                                            new_description=new_description, is_reusable=new_is_reusable,
                                            verification_function=new_verification_function, session=session
                                        )
                                        
                                        if new_order != current_order:
                                            QuestionGroupService.update_question_order(
                                                group_id=selected_group_id, question_ids=new_order, session=session
                                            )
                                        
                                        order_key = f"edit_group_order_{selected_group_id}"
                                        if order_key in st.session_state:
                                            del st.session_state[order_key]
                                        
                                        st.success("✅ Question group updated successfully!")
                                        st.rerun(scope="fragment")
                                    except Exception as e:
                                        st.error(f"❌ Error updating group: {str(e)}")
                                        
                            except Exception as e:
                                st.error(f"Error loading group details: {str(e)}")
                    else:
                        st.info("No non-archived question groups available to edit.")
                else:
                    st.info("No question groups available to edit.")
        
        with q_tab2:
            questions_df = QuestionService.get_all_questions(session=session)
            st.dataframe(questions_df, use_container_width=True)
            
            question_management_tabs = st.tabs(["➕ Create Question", "✏️ Edit Question"])
            
            with question_management_tabs[0]:
                st.markdown("### 🆕 Create New Question")
                
                basic_info_col1, basic_info_col2 = st.columns(2)
                with basic_info_col1:
                    text = st.text_input("Question Text", key="admin_question_text", 
                                       placeholder="Enter the question text...")
                with basic_info_col2:
                    q_type = st.selectbox("Question Type", ["single", "description"], key="admin_question_type",
                                        help="Single: Multiple choice | Description: Text input")
                
                use_text_as_display = st.checkbox("Use question text as display text", value=True, 
                                                key="admin_question_use_text_as_display",
                                                help="Uncheck to provide custom display text")
                if not use_text_as_display:
                    display_text = st.text_input("Question to display to user", key="admin_question_display_text", 
                                                value=text, placeholder="Text shown to users...")
                else:
                    display_text = None
                
                options = []
                option_weights = []
                default = None
                
                if q_type == "single":
                    st.markdown("**🎯 Options and Weights:**")
                    st.info("💡 Default weight is 1.0 for each option. Customize weights to influence scoring.")
                    
                    num_options = st.number_input("Number of options", 1, 10, 2, key="admin_question_num_options")
                    
                    for i in range(num_options):
                        opt_col1, opt_col2 = st.columns([3, 1])
                        with opt_col1:
                            option = st.text_input(f"Option {i+1}", key=f"admin_question_opt_{i}",
                                                 placeholder=f"Enter option {i+1}...")
                        with opt_col2:
                            weight = st.number_input(f"Weight {i+1}", min_value=0.0, value=1.0, step=0.1, 
                                                   key=f"admin_question_weight_{i}", 
                                                   help="Weight for scoring (default: 1.0)")
                        
                        if option:
                            options.append(option)
                            option_weights.append(weight)
                    
                    if options:
                        default = st.selectbox("Default option", [""] + options, key="admin_question_default",
                                             help="Option selected by default")
                        if default == "":
                            default = None
                
                if st.button("🚀 Create Question", key="admin_create_question_btn", type="primary", use_container_width=True):
                    if text:
                        try:
                            QuestionService.add_question(
                                text=text, qtype=q_type, options=options if q_type == "single" else None,
                                default=default if q_type == "single" else None, session=session,
                                display_text=display_text, option_weights=option_weights if q_type == "single" else None
                            )
                            st.success("✅ Question created successfully!")
                            st.rerun(scope="fragment")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
            
            with question_management_tabs[1]:
                st.markdown("### ✏️ Edit Existing Question")
                
                if not questions_df.empty:
                    available_questions = questions_df[~questions_df["Archived"]]
                    if not available_questions.empty:
                        question_options = {f"{row['Text'][:50]}... (ID: {row['ID']})": row['ID'] for _, row in available_questions.iterrows()}
                        selected_question_name = st.selectbox(
                            "Select Question to Edit",
                            list(question_options.keys()),
                            key="admin_edit_question_select",
                            help="Choose a question to modify"
                        )
                        
                        if selected_question_name:
                            selected_question_id = question_options[selected_question_name]
                            
                            try:
                                current_question = QuestionService.get_question_by_id(
                                    question_id=selected_question_id, session=session
                                )
                                
                                st.text_input(
                                    "Question Text (immutable)",
                                    value=current_question.text,
                                    key="admin_edit_question_text",
                                    disabled=True,
                                    help="Question text cannot be changed to preserve data integrity"
                                )
                                
                                new_display_text = st.text_input(
                                    "Question to display to user",
                                    value=current_question.display_text,
                                    key="admin_edit_question_display_text"
                                )
                                
                                st.markdown(f"**Question Type:** `{current_question.type}`")
                                
                                new_options = None
                                new_default = None
                                new_display_values = None
                                new_option_weights = None
                                
                                if current_question.type == "single":
                                    st.markdown("**🎯 Options, Weights & Order Management:**")
                                    
                                    current_options = current_question.options or []
                                    current_display_values = current_question.display_values or current_options
                                    current_option_weights = current_question.option_weights or [1.0] * len(current_options)
                                    current_default = current_question.default_option or ""
                                    
                                    # Current options display
                                    st.markdown("**Current Options:**")
                                    for i, (opt, disp, weight) in enumerate(zip(current_options, current_display_values, current_option_weights)):
                                        default_indicator = " 🌟 (DEFAULT)" if opt == current_default else ""
                                        st.markdown(f"`{i+1}.` **Value:** `{opt}` | **Display:** `{disp}` | **Weight:** `{weight}`{default_indicator}")
                                    
                                    # Option order management
                                    st.markdown("**📋 Option Order Management:**")
                                    option_order_key = f"edit_question_option_order_{selected_question_id}"
                                    if option_order_key not in st.session_state:
                                        st.session_state[option_order_key] = list(range(len(current_options)))
                                    
                                    working_option_order = st.session_state[option_order_key]
                                    
                                    if len(current_options) > 1:
                                        st.info("💡 Use the ⬆️ and ⬇️ buttons to reorder options. This will affect the display order for users.")
                                        
                                        for i, option_idx in enumerate(working_option_order):
                                            if option_idx < len(current_options):
                                                opt = current_options[option_idx]
                                                disp = current_display_values[option_idx]
                                                weight = current_option_weights[option_idx]
                                                
                                                order_opt_col1, order_opt_col2, order_opt_col3 = st.columns([0.1, 0.8, 0.1])
                                                
                                                with order_opt_col1:
                                                    if st.button("⬆️", key=f"opt_up_{selected_question_id}_{option_idx}_{i}", 
                                                                disabled=(i == 0), help="Move up"):
                                                        st.session_state[option_order_key][i], st.session_state[option_order_key][i-1] = \
                                                            st.session_state[option_order_key][i-1], st.session_state[option_order_key][i]
                                                        st.rerun()
                                                
                                                with order_opt_col2:
                                                    default_indicator = " 🌟" if opt == current_default else ""
                                                    st.write(f"**{i+1}.** {disp} (Weight: {weight}){default_indicator}")
                                                    st.caption(f"Value: {opt}")
                                                
                                                with order_opt_col3:
                                                    if st.button("⬇️", key=f"opt_down_{selected_question_id}_{option_idx}_{i}", 
                                                                disabled=(i == len(working_option_order) - 1), help="Move down"):
                                                        st.session_state[option_order_key][i], st.session_state[option_order_key][i+1] = \
                                                            st.session_state[option_order_key][i+1], st.session_state[option_order_key][i]
                                                        st.rerun()
                                        
                                        opt_order_col1, opt_order_col2 = st.columns(2)
                                        with opt_order_col1:
                                            if st.button("🔄 Reset Option Order", key=f"reset_option_order_{selected_question_id}"):
                                                st.session_state[option_order_key] = list(range(len(current_options)))
                                                st.rerun()
                                        
                                        with opt_order_col2:
                                            original_order = list(range(len(current_options)))
                                            if working_option_order != original_order:
                                                st.warning("⚠️ Option order changed")
                                            else:
                                                st.success("✅ Original order")
                                    
                                    # Edit options and add new ones
                                    st.markdown("**✏️ Edit Options and Weights:**")
                                    st.info("📝 Note: You can only add new options, not remove existing ones (to preserve data integrity).")
                                    
                                    num_options = st.number_input(
                                        "Total number of options", 
                                        min_value=len(current_options), 
                                        max_value=10, 
                                        value=len(current_options),
                                        key="admin_edit_question_num_options"
                                    )
                                    
                                    new_options = []
                                    new_display_values = []
                                    new_option_weights = []
                                    
                                    # Apply the working order to existing options
                                    reordered_options = [current_options[i] for i in working_option_order]
                                    reordered_display_values = [current_display_values[i] for i in working_option_order]
                                    reordered_weights = [current_option_weights[i] for i in working_option_order]
                                    
                                    for i in range(num_options):
                                        edit_opt_col1, edit_opt_col2, edit_opt_col3 = st.columns([2, 2, 1])
                                        
                                        with edit_opt_col1:
                                            if i < len(reordered_options):
                                                st.text_input(
                                                    f"Option {i+1} Value",
                                                    value=reordered_options[i],
                                                    disabled=True,
                                                    key=f"admin_edit_question_opt_val_{i}",
                                                    help="Cannot change existing option values"
                                                )
                                                new_options.append(reordered_options[i])
                                            else:
                                                new_opt = st.text_input(
                                                    f"Option {i+1} Value (NEW)",
                                                    key=f"admin_edit_question_opt_val_{i}",
                                                    placeholder="Enter new option value..."
                                                )
                                                if new_opt:
                                                    new_options.append(new_opt)
                                        
                                        with edit_opt_col2:
                                            if i < len(reordered_display_values):
                                                new_disp = st.text_input(
                                                    f"Option {i+1} Display",
                                                    value=reordered_display_values[i],
                                                    key=f"admin_edit_question_opt_disp_{i}"
                                                )
                                                new_display_values.append(new_disp if new_disp else reordered_display_values[i])
                                            else:
                                                new_disp = st.text_input(
                                                    f"Option {i+1} Display (NEW)",
                                                    value=new_options[i] if i < len(new_options) else "",
                                                    key=f"admin_edit_question_opt_disp_{i}",
                                                    placeholder="Display text for new option..."
                                                )
                                                if new_disp:
                                                    new_display_values.append(new_disp)
                                                elif i < len(new_options):
                                                    new_display_values.append(new_options[i])
                                        
                                        with edit_opt_col3:
                                            if i < len(reordered_weights):
                                                new_weight = st.number_input(
                                                    f"Weight {i+1}",
                                                    min_value=0.0,
                                                    value=reordered_weights[i],
                                                    step=0.1,
                                                    key=f"admin_edit_question_opt_weight_{i}",
                                                    help="Weight for scoring"
                                                )
                                                new_option_weights.append(new_weight)
                                            else:
                                                new_weight = st.number_input(
                                                    f"Weight {i+1} (NEW)",
                                                    min_value=0.0,
                                                    value=1.0,
                                                    step=0.1,
                                                    key=f"admin_edit_question_opt_weight_{i}",
                                                    help="Weight for scoring (default: 1.0)"
                                                )
                                                new_option_weights.append(new_weight)
                                    
                                    if new_options:
                                        new_default = st.selectbox(
                                            "Default option",
                                            [""] + new_options,
                                            index=new_options.index(current_default) + 1 if current_default in new_options else 0,
                                            key="admin_edit_question_default"
                                        )
                                        if new_default == "":
                                            new_default = None
                                
                                if st.button("💾 Update Question", key="admin_update_question_btn", type="primary", use_container_width=True):
                                    try:
                                        # Apply option reordering if changed
                                        final_options = new_options
                                        final_display_values = new_display_values
                                        final_option_weights = new_option_weights
                                        
                                        # Clear option order state after update
                                        option_order_key = f"edit_question_option_order_{selected_question_id}"
                                        if option_order_key in st.session_state:
                                            del st.session_state[option_order_key]
                                        
                                        QuestionService.edit_question(
                                            question_id=selected_question_id, new_display_text=new_display_text,
                                            new_opts=final_options, new_default=new_default,
                                            new_display_values=final_display_values, new_option_weights=final_option_weights,
                                            session=session
                                        )
                                        st.success("✅ Question updated successfully!")
                                        st.rerun(scope="fragment")
                                    except Exception as e:
                                        st.error(f"❌ Error updating question: {str(e)}")
                                        
                            except Exception as e:
                                st.error(f"Error loading question details: {str(e)}")
                    else:
                        st.info("No non-archived questions available to edit.")
                else:
                    st.info("No questions available to edit.")

@st.fragment 
def display_assignment_management(session: Session):
    """Optimized assignment management with fragments and user weight support"""
    st.markdown("### 🎯 Assign Users to Projects")
    
    # Initialize session state
    if "selected_project_ids" not in st.session_state:
        st.session_state.selected_project_ids = []
    if "selected_user_ids" not in st.session_state:
        st.session_state.selected_user_ids = []
    if "assignment_role" not in st.session_state:
        st.session_state.assignment_role = "annotator"
    if "assignment_user_weight" not in st.session_state:
        st.session_state.assignment_user_weight = 1.0
    
    # Project selection
    st.markdown("**Step 1: Select Projects**")
    
    try:
        projects_df = ProjectService.get_all_projects(session=session)
        if projects_df.empty:
            st.warning("No projects available.")
            return
    except Exception as e:
        st.error(f"Error loading projects: {str(e)}")
        return
    
    project_search = st.text_input("🔍 Search projects", placeholder="Project name...", key="proj_search_mgmt")
    
    filtered_projects = [
        project_row for _, project_row in projects_df.iterrows()
        if not project_search or project_search.lower() in project_row["Name"].lower()
    ]
    
    if not filtered_projects:
        st.warning("No projects match the search criteria.")
        return
    
    st.info(f"Found {len(filtered_projects)} projects")
    
    select_col1, select_col2 = st.columns(2)
    with select_col1:
        if st.button("Select All Visible Projects", key="select_all_projects_mgmt"):
            st.session_state.selected_project_ids = [int(p["ID"]) for p in filtered_projects]
            st.rerun(scope="fragment")
    
    with select_col2:
        if st.button("Clear Project Selection", key="clear_projects_mgmt"):
            st.session_state.selected_project_ids = []
            st.rerun(scope="fragment")
    
    # Project selection grid
    project_cols = st.columns(4)
    current_selections = []
    
    for i, project_row in enumerate(filtered_projects):
        with project_cols[i % 4]:
            project_id = int(project_row["ID"])
            project_name = project_row["Name"]
            
            is_selected = project_id in st.session_state.selected_project_ids
            
            checkbox_value = st.checkbox(
                project_name,
                value=is_selected,
                key=f"proj_cb_mgmt_{project_id}",
                help=f"Project ID: {project_id}"
            )
            
            if checkbox_value:
                current_selections.append(project_id)
    
    if set(current_selections) != set(st.session_state.selected_project_ids):
        st.session_state.selected_project_ids = current_selections
        st.rerun(scope="fragment")
    
    if not st.session_state.selected_project_ids:
        st.info("Please select projects above to continue.")
        return
    
    st.success(f"✅ Selected {len(st.session_state.selected_project_ids)} projects")
    
    # User selection
    st.markdown("**Step 2: Select Users**")
    
    try:
        users_df = AuthService.get_all_users(session=session)
        if users_df.empty:
            st.warning("No users available.")
            return
    except Exception as e:
        st.error(f"Error loading users: {str(e)}")
        return
    
    user_filter_col1, user_filter_col2 = st.columns(2)
    with user_filter_col1:
        user_search = st.text_input("Search users", placeholder="Name or email...", key="user_search_mgmt")
    with user_filter_col2:
        user_role_filter = st.selectbox("Filter by user role", ["All", "admin", "human", "model"], key="user_role_filter_mgmt")
    
    # Filter users
    filtered_users = []
    for _, user_row in users_df.iterrows():
        if user_role_filter != "All" and user_row["Role"] != user_role_filter:
            continue
        
        if user_search:
            if (user_search.lower() not in user_row["User ID"].lower() and 
                user_search.lower() not in user_row["Email"].lower()):
                continue
        
        filtered_users.append(user_row)
    
    if not filtered_users:
        st.warning("No users match the search criteria.")
        return
    
    st.info(f"Found {len(filtered_users)} users")
    
    # User pagination
    users_per_page = 12
    total_pages = (len(filtered_users) - 1) // users_per_page + 1 if len(filtered_users) > 0 else 1
    
    if total_pages > 1:
        page = st.selectbox(f"Page (showing {users_per_page} users per page)", 
                           range(1, total_pages + 1), key="user_page_mgmt") - 1
    else:
        page = 0
    
    start_idx = page * users_per_page
    end_idx = min(start_idx + users_per_page, len(filtered_users))
    page_users = filtered_users[start_idx:end_idx]
    
    user_select_col1, user_select_col2 = st.columns(2)
    with user_select_col1:
        if st.button("Select All on Page", key="select_all_users_mgmt"):
            page_user_ids = [int(u["ID"]) for u in page_users]
            st.session_state.selected_user_ids = list(set(st.session_state.selected_user_ids + page_user_ids))
            st.rerun(scope="fragment")
    
    with user_select_col2:
        if st.button("Clear User Selection", key="clear_users_mgmt"):
            st.session_state.selected_user_ids = []
            st.rerun(scope="fragment")
    
    # User selection grid
    user_cols = st.columns(4)
    current_user_selections = list(st.session_state.selected_user_ids)
    
    for i, user_row in enumerate(page_users):
        with user_cols[i % 4]:
            user_id = int(user_row["ID"])
            user_name = user_row["User ID"]
            user_email = user_row["Email"]
            user_role = user_row["Role"]
            
            is_selected = user_id in st.session_state.selected_user_ids
            
            checkbox_value = st.checkbox(
                user_name,
                value=is_selected,
                key=f"user_cb_mgmt_{user_id}",
                help=f"Email: {user_email}\nRole: {user_role}\nID: {user_id}"
            )
            
            if checkbox_value and user_id not in current_user_selections:
                current_user_selections.append(user_id)
            elif not checkbox_value and user_id in current_user_selections:
                current_user_selections.remove(user_id)
    
    if set(current_user_selections) != set(st.session_state.selected_user_ids):
        st.session_state.selected_user_ids = current_user_selections
        st.rerun(scope="fragment")
    
    if not st.session_state.selected_user_ids:
        st.info("Please select users above to continue.")
        return
    
    st.success(f"✅ Selected {len(st.session_state.selected_user_ids)} users")
    
    # Assignment actions
    st.markdown("**Step 3: Assignment Role & Settings**")
    
    settings_col1, settings_col2 = st.columns(2)
    
    with settings_col1:
        role = st.selectbox("Assignment Role", ["annotator", "reviewer", "admin", "model"], 
                           index=["annotator", "reviewer", "admin", "model"].index(st.session_state.assignment_role),
                           key="assign_role_mgmt")
    
    with settings_col2:
        user_weight = st.number_input(
            "User Weight", 
            min_value=0.0, 
            value=st.session_state.assignment_user_weight, 
            step=0.1,
            key="assign_user_weight_mgmt",
            help="Weight for user's answers in scoring (default: 1.0)"
        )
    
    if role != st.session_state.assignment_role:
        st.session_state.assignment_role = role
    
    if user_weight != st.session_state.assignment_user_weight:
        st.session_state.assignment_user_weight = user_weight
    
    st.info(f"Ready to assign {len(st.session_state.selected_user_ids)} users as **{role}** with weight **{user_weight}** to {len(st.session_state.selected_project_ids)} projects")
    
    action_col1, action_col2 = st.columns(2)
    
    with action_col1:
        if st.button("✅ Execute Assignments", key="execute_assignments", use_container_width=True):
            project_ids = st.session_state.selected_project_ids
            user_ids = st.session_state.selected_user_ids
            total_operations = len(user_ids) * len(project_ids)
            success_count = 0
            error_count = 0
            
            progress_bar = st.progress(0)
            status_container = st.empty()
            
            operation_counter = 0
            
            for project_id in project_ids:
                for user_id in user_ids:
                    try:
                        ProjectService.add_user_to_project(
                            project_id=project_id, user_id=user_id, role=role, 
                            session=session, user_weight=user_weight
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 3:
                            st.error(f"Failed to assign user {user_id} to project {project_id}: {str(e)}")
                    
                    operation_counter += 1
                    progress = operation_counter / total_operations
                    progress_bar.progress(progress)
                    status_container.text(f"Processing: {operation_counter}/{total_operations}")
            
            if success_count > 0:
                st.success(f"✅ Successfully completed {success_count} assignments with weight {user_weight}!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} assignments failed")
            
            if success_count > 0:
                st.session_state.selected_project_ids = []
                st.session_state.selected_user_ids = []
                st.rerun(scope="fragment")
    
    with action_col2:
        if st.button("🗑️ Remove Assignments", key="execute_removals", use_container_width=True):
            project_ids = st.session_state.selected_project_ids
            user_ids = st.session_state.selected_user_ids
            total_operations = len(user_ids) * len(project_ids)
            success_count = 0
            error_count = 0
            
            progress_bar = st.progress(0)
            status_container = st.empty()
            
            operation_counter = 0
            
            for project_id in project_ids:
                for user_id in user_ids:
                    try:
                        AuthService.archive_user_from_project(user_id=user_id, project_id=project_id, session=session)
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 3:
                            st.error(f"Failed to remove user {user_id} from project {project_id}: {str(e)}")
                    
                    operation_counter += 1
                    progress = operation_counter / total_operations
                    progress_bar.progress(progress)
                    status_container.text(f"Processing: {operation_counter}/{total_operations}")
            
            if success_count > 0:
                st.success(f"🗑️ Successfully removed {success_count} assignments!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} removals failed")
            
            if success_count > 0:
                st.session_state.selected_project_ids = []
                st.session_state.selected_user_ids = []
                st.rerun(scope="fragment")

@st.fragment
def admin_projects():
    st.subheader("📁 Project Management")
    
    with get_db_session() as session:
        projects_df = ProjectService.get_all_projects(session=session)
        
        if not projects_df.empty:
            enhanced_projects = []
            for _, project in projects_df.iterrows():
                try:
                    progress = ProjectService.progress(project_id=project["ID"], session=session)
                    has_full_gt = check_project_has_full_ground_truth(project_id=project["ID"], session=session)
                    mode = "🎓 Training" if has_full_gt else "📝 Annotation"
                    
                    enhanced_projects.append({
                        "ID": project["ID"],
                        "Name": project["Name"],
                        "Videos": project["Videos"],
                        "Schema ID": project["Schema ID"],
                        "Mode": mode,
                        "GT Progress": f"{progress['completion_percentage']:.1f}%",
                        "GT Answers": f"{progress['ground_truth_answers']}/{progress['total_videos'] * progress['total_questions']}"
                    })
                except:
                    enhanced_projects.append({
                        "ID": project["ID"],
                        "Name": project["Name"],
                        "Videos": project["Videos"],
                        "Schema ID": project["Schema ID"],
                        "Mode": "Error",
                        "GT Progress": "Error",
                        "GT Answers": "Error"
                    })
            
            enhanced_df = pd.DataFrame(enhanced_projects)
            st.dataframe(enhanced_df, use_container_width=True)
        else:
            st.info("No projects available.")
        
        with st.expander("➕ Create Project"):
            name = st.text_input("Project Name", key="admin_project_name")
            description = st.text_area("Description", key="admin_project_description")
            
            schemas_df = SchemaService.get_all_schemas(session=session)
            if schemas_df.empty:
                st.warning("No schemas available.")
                return
                
            schema_name = st.selectbox("Schema", schemas_df["Name"], key="admin_project_schema")
            
            videos_df = VideoService.get_all_videos(session=session)
            if videos_df.empty:
                st.warning("No videos available.")
                return
                
            selected_videos = st.multiselect("Videos", videos_df["Video UID"], key="admin_project_videos")
            
            if st.button("Create Project", key="admin_create_project_btn"):
                if name and schema_name:
                    try:
                        schema_id = SchemaService.get_schema_id_by_name(name=schema_name, session=session)
                        video_ids = ProjectService.get_video_ids_by_uids(video_uids=selected_videos, session=session)
                        ProjectService.create_project(name=name, schema_id=schema_id, video_ids=video_ids, session=session)
                        st.success("Project created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

@st.fragment
def admin_schemas():
    st.subheader("📋 Schema Management")
    
    with get_db_session() as session:
        schemas_df = SchemaService.get_all_schemas(session=session)
        st.dataframe(schemas_df, use_container_width=True)
        
        with st.expander("➕ Create Schema"):
            schema_name = st.text_input("Schema Name", key="admin_schema_name")
            
            groups_df = QuestionGroupService.get_all_groups(session=session)
            if groups_df.empty:
                st.warning("No question groups available.")
                return
                
            available_groups = groups_df[~groups_df["Archived"]]
            selected_groups = st.multiselect(
                "Question Groups", 
                available_groups["ID"].tolist(),
                format_func=lambda x: available_groups[available_groups["ID"]==x]["Name"].iloc[0],
                key="admin_schema_groups"
            )
            
            if st.button("Create Schema", key="admin_create_schema_btn"):
                if schema_name and selected_groups:
                    try:
                        SchemaService.create_schema(name=schema_name, question_group_ids=selected_groups, session=session)
                        st.success("Schema created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

@st.fragment
def admin_users():
    st.subheader("👥 User Management")
    
    with get_db_session() as session:
        users_df = AuthService.get_all_users(session=session)
        st.dataframe(users_df, use_container_width=True)
        
        create_tab, edit_tab = st.tabs(["➕ Create User", "✏️ Edit User"])
        
        with create_tab:
            user_id = st.text_input("User ID", key="admin_user_id")
            email = st.text_input("Email", key="admin_user_email")
            password = st.text_input("Password", type="password", key="admin_user_password")
            user_type = st.selectbox("User Type", ["human", "model", "admin"], key="admin_user_type")
            
            if st.button("Create User", key="admin_create_user_btn"):
                if user_id and email and password:
                    try:
                        AuthService.create_user(
                            user_id=user_id, email=email, password_hash=password, 
                            user_type=user_type, session=session
                        )
                        st.success(f"✅ User '{user_id}' created successfully!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning("Please fill in all required fields (User ID, Email, Password)")
        
        with edit_tab:
            if not users_df.empty:
                user_options = {f"{row['User ID']} ({row['Email']})": row['ID'] for _, row in users_df.iterrows()}
                selected_user_display = st.selectbox(
                    "Select User to Edit",
                    list(user_options.keys()),
                    key="admin_edit_user_select"
                )
                
                if selected_user_display:
                    selected_user_id = user_options[selected_user_display]
                    current_user = users_df[users_df['ID'] == selected_user_id].iloc[0]
                    
                    st.markdown(f"**Editing User:** {current_user['User ID']}")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        new_user_id = st.text_input(
                            "User ID",
                            value=current_user['User ID'],
                            key="admin_edit_user_id"
                        )
                        new_email = st.text_input(
                            "Email",
                            value=current_user['Email'],
                            key="admin_edit_user_email"
                        )
                    
                    with col2:
                        new_role = st.selectbox(
                            "User Type",
                            ["human", "model", "admin"],
                            index=["human", "model", "admin"].index(current_user['Role']),
                            key="admin_edit_user_role"
                        )
                        new_password = st.text_input(
                            "New Password (leave empty to keep current)",
                            type="password",
                            key="admin_edit_user_password"
                        )
                    
                    update_col, archive_col = st.columns(2)
                    
                    with update_col:
                        if st.button("💾 Update User", key="admin_update_user_btn", use_container_width=True):
                            if new_user_id and new_email:
                                try:
                                    changes_made = []
                                    
                                    if new_user_id != current_user['User ID']:
                                        AuthService.update_user_id(
                                            user_id=selected_user_id, new_user_id=new_user_id, session=session
                                        )
                                        changes_made.append("User ID")
                                    
                                    if new_email != current_user['Email']:
                                        AuthService.update_user_email(
                                            user_id=selected_user_id, new_email=new_email, session=session
                                        )
                                        changes_made.append("Email")
                                    
                                    if new_password:
                                        AuthService.update_user_password(
                                            user_id=selected_user_id, new_password=new_password, session=session
                                        )
                                        changes_made.append("Password")
                                    
                                    if new_role != current_user['Role']:
                                        AuthService.update_user_role(
                                            user_id=selected_user_id, new_role=new_role, session=session
                                        )
                                        changes_made.append("Role")
                                    
                                    if changes_made:
                                        st.success(f"✅ User '{new_user_id}' updated successfully! Changed: {', '.join(changes_made)}")
                                    else:
                                        st.info("No changes were made")
                                    st.rerun(scope="fragment")
                                except Exception as e:
                                    st.error(f"Error updating user: {str(e)}")
                            else:
                                st.warning("User ID and Email are required")
                    
                    with archive_col:
                        archive_status = "Archived" if current_user['Archived'] else "Active"
                        action_text = "Unarchive" if current_user['Archived'] else "Archive"
                        
                        if st.button(f"🗄️ {action_text} User", key="admin_archive_user_btn", use_container_width=True):
                            if st.session_state.get(f"confirm_archive_{selected_user_id}", False):
                                try:
                                    AuthService.toggle_user_archived(user_id=selected_user_id, session=session)
                                    new_status = "Unarchived" if current_user['Archived'] else "Archived"
                                    st.success(f"✅ User '{current_user['User ID']}' {new_status.lower()} successfully!")
                                    if f"confirm_archive_{selected_user_id}" in st.session_state:
                                        del st.session_state[f"confirm_archive_{selected_user_id}"]
                                    st.rerun(scope="fragment")
                                except Exception as e:
                                    st.error(f"Error toggling user archive status: {str(e)}")
                            else:
                                st.session_state[f"confirm_archive_{selected_user_id}"] = True
                                st.warning(f"⚠️ Click {action_text} again to confirm")
                                st.rerun(scope="fragment")
                        
                        st.caption(f"**Current Status:** {archive_status}")
            else:
                st.info("No users available to edit")

def perform_bulk_assignments(project_ids, user_ids, role, session, action_type):
    """Perform bulk assignments or removals with progress tracking"""
    total_operations = len(user_ids) * len(project_ids)
    success_count = 0
    error_count = 0
    
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    operation_counter = 0
    
    for project_id in project_ids:
        for user_id in user_ids:
            try:
                if action_type == "assign":
                    ProjectService.add_user_to_project(
                        project_id=project_id, user_id=user_id, role=role, session=session
                    )
                else:
                    AuthService.archive_user_from_project(
                        user_id=user_id, project_id=project_id, session=session
                    )
                success_count += 1
            except Exception as e:
                error_count += 1
                if error_count <= 3:
                    st.error(f"Failed: User {user_id} {'to' if action_type == 'assign' else 'from'} Project {project_id}")
            
            operation_counter += 1
            progress = operation_counter / total_operations
            progress_bar.progress(progress)
            status_container.text(f"Processing: {operation_counter}/{total_operations}")
    
    action_word = "assignments" if action_type == "assign" else "removals"
    if success_count > 0:
        st.success(f"✅ Successfully completed {success_count} {action_word}!")
    if error_count > 0:
        st.warning(f"⚠️ {error_count} {action_word} failed")
    
    if success_count > 0:
        st.session_state.selected_project_ids = []
        st.session_state.selected_user_ids = []
        st.rerun(scope="fragment")

@st.fragment 
def admin_assignments():
    st.subheader("🔗 Project Assignments")
    
    def get_user_role_emoji(role):
        """Get appropriate emoji for user role"""
        role_emojis = {"human": "👤", "admin": "👑", "model": "🤖"}
        return role_emojis.get(role, "❓")
    
    with get_db_session() as session:
        assignments_df = AuthService.get_project_assignments(session=session)
        
        if not assignments_df.empty:
            with st.expander("➕ **Manage Project Assignments**", expanded=False):
                display_assignment_management(session)
            
            st.markdown("---")
            
            # Get data for enhanced display
            users_df = AuthService.get_all_users(session=session)
            projects_df = ProjectService.get_all_projects(session=session)
            
            # Get project groups
            try:
                project_groups = ProjectGroupService.list_project_groups(session=session)
                project_group_lookup = {}
                for group in project_groups:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=group.id, session=session)
                    for project in group_info["projects"]:
                        project_group_lookup[project.id] = group.name
            except:
                project_group_lookup = {}
            
            # Create lookups
            user_lookup = {row["ID"]: {"name": row["User ID"], "email": row["Email"], "role": row["Role"]} for _, row in users_df.iterrows()}
            project_lookup = {row["ID"]: row["Name"] for _, row in projects_df.iterrows()}
            
            # Process assignments
            user_assignments = {}
            
            for _, assignment in assignments_df.iterrows():
                user_id = assignment["User ID"]
                project_id = assignment["Project ID"]
                user_info = user_lookup.get(user_id, {"name": f"Unknown User {user_id}", "email": "Unknown", "role": "Unknown"})
                project_name = project_lookup.get(project_id, f"Unknown Project {project_id}")
                project_group = project_group_lookup.get(project_id, "Ungrouped")
                
                if user_id not in user_assignments:
                    user_assignments[user_id] = {
                        "name": user_info["name"],
                        "email": user_info["email"],
                        "user_role": user_info["role"],
                        "projects": {},
                        "is_archived": True
                    }
                
                project_key = project_id
                if project_key not in user_assignments[user_id]["projects"]:
                    user_assignments[user_id]["projects"][project_key] = {
                        "name": project_name,
                        "group": project_group,
                        "role_assignments": {}
                    }
                
                role = assignment["Role"]
                if role not in user_assignments[user_id]["projects"][project_key]["role_assignments"]:
                    user_assignments[user_id]["projects"][project_key]["role_assignments"][role] = {
                        "assigned_date": None,
                        "completed_date": None,
                        "archived": assignment.get("Archived", False)
                    }
                
                role_data = user_assignments[user_id]["projects"][project_key]["role_assignments"][role]
                
                # Set dates
                if assignment.get("Assigned At"):
                    try:
                        assigned_date = assignment["Assigned At"].strftime("%Y-%m-%d") if hasattr(assignment["Assigned At"], 'strftime') else str(assignment["Assigned At"])[:10]
                        role_data["assigned_date"] = assigned_date
                    except:
                        role_data["assigned_date"] = "Unknown"
                
                if assignment.get("Completed At"):
                    try:
                        completed_date = assignment["Completed At"].strftime("%Y-%m-%d") if hasattr(assignment["Completed At"], 'strftime') else str(assignment["Completed At"])[:10]
                        role_data["completed_date"] = completed_date
                    except:
                        role_data["completed_date"] = "Unknown"
                
                if not assignment.get("Archived", False):
                    user_assignments[user_id]["is_archived"] = False
            
            # Search and filter controls
            st.markdown("### 🔍 Search & Filter")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                search_term = st.text_input("🔍 Search users", placeholder="Name or email...")
            
            with col2:
                status_filter = st.selectbox("Status", ["All", "Active", "Archived"])
            
            with col3:
                role_filter = st.selectbox("User Role", ["All", "admin", "human", "model"])
            
            with col4:
                project_role_filter = st.selectbox("Assignment Role", ["All", "annotator", "reviewer", "admin", "model"])
            
            # Apply filters
            filtered_assignments = {}
            for user_id, user_data in user_assignments.items():
                # Search filter
                if search_term:
                    if (search_term.lower() not in user_data["name"].lower() and 
                        search_term.lower() not in user_data["email"].lower()):
                        continue
                
                # Status filter
                if status_filter == "Active" and user_data["is_archived"]:
                    continue
                elif status_filter == "Archived" and not user_data["is_archived"]:
                    continue
                
                # User role filter
                if role_filter != "All" and user_data["user_role"] != role_filter:
                    continue
                
                # Project role filter
                if project_role_filter != "All":
                    has_role = any(
                        project_role_filter in project_info["role_assignments"]
                        for project_info in user_data["projects"].values()
                    )
                    if not has_role:
                        continue
                
                filtered_assignments[user_id] = user_data
            
            total_users = len(user_assignments)
            filtered_count = len(filtered_assignments)
            st.info(f"📊 Showing **{filtered_count}** of **{total_users}** total users")
            
            # Main assignments display
            if filtered_assignments:
                st.markdown("### 📋 Assignment Overview")
                
                display_data = []
                for user_id, user_data in filtered_assignments.items():
                    # Group projects by project group
                    grouped_projects = {}
                    for project_id, project_info in user_data["projects"].items():
                        project_group = project_info["group"]
                        project_name = project_info["name"]
                        
                        if project_group not in grouped_projects:
                            grouped_projects[project_group] = []
                        
                        role_parts = []
                        
                        for role, role_data in project_info["role_assignments"].items():
                            is_completed = (role != "admin" and 
                                          role_data["completed_date"] and 
                                          role_data["completed_date"] != "Unknown")
                            
                            completion_emoji = "✅ " if is_completed else ""
                            
                            # Add accuracy information for training mode projects
                            accuracy_info = ""
                            if is_completed:
                                try:
                                    if check_project_has_full_ground_truth(project_id=project_id, session=session):
                                        if role == "annotator":
                                            accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
                                            if user_id in accuracy_data:
                                                overall_accuracy = calculate_overall_accuracy(accuracy_data)
                                                accuracy = overall_accuracy.get(user_id)
                                                if accuracy is not None:
                                                    color = get_accuracy_color(accuracy)
                                                    accuracy_info = f" <span style='color: {color}; font-weight: bold;'>[{accuracy:.1f}%]</span>"
                                        elif role == "reviewer":
                                            accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
                                            if user_id in accuracy_data:
                                                overall_accuracy = calculate_overall_accuracy(accuracy_data)
                                                accuracy = overall_accuracy.get(user_id)
                                                if accuracy is not None:
                                                    color = get_accuracy_color(accuracy)
                                                    accuracy_info = f" <span style='color: {color}; font-weight: bold;'>[{accuracy:.1f}%]</span>"
                                except Exception:
                                    pass
                            
                            # Build date info
                            if role == "admin":
                                date_part = f"({role_data['assigned_date'] or 'not set'})"
                            else:
                                assigned = role_data["assigned_date"] or "not set"
                                completed = role_data["completed_date"] if role_data["completed_date"] and role_data["completed_date"] != "Unknown" else None
                                
                                date_part = f"({assigned} → {completed})" if completed else f"({assigned})"
                            
                            archived_indicator = " 🗄️" if role_data["archived"] else ""
                            
                            role_parts.append(f"{completion_emoji}{role.title()}{date_part}{accuracy_info}{archived_indicator}")
                        
                        roles_text = ", ".join(role_parts)
                        project_display = f"  • **{project_name}**: {roles_text}"
                        
                        grouped_projects[project_group].append({
                            "display": project_display,
                            "name": project_name,
                            "roles": list(project_info["role_assignments"].keys())
                        })
                    
                    # Format grouped projects
                    projects_data = []
                    projects_display = ""
                    
                    for group_name in sorted(grouped_projects.keys()):
                        if projects_display:
                            projects_display += "\n---\n\n"
                        
                        if group_name != "Ungrouped":
                            projects_display += f"## 📁 {group_name}\n\n"
                        else:
                            projects_display += f"## 📄 Individual Projects\n\n"
                        
                        for project_info in grouped_projects[group_name]:
                            projects_display += f"{project_info['display']}\n\n"
                            projects_data.append({
                                "group": group_name,
                                "name": project_info["name"],
                                "roles": project_info["roles"],
                                "display": project_info["display"]
                            })
                    
                    display_data.append({
                        "User Name": user_data["name"],
                        "Email": user_data["email"],
                        "User Role": user_data["user_role"].upper(),
                        "Status": "🗄️ Archived" if user_data["is_archived"] else "✅ Active",
                        "Project Assignments": projects_display.strip(),
                        "Projects Data": projects_data,
                        "Total Projects": len(user_data["projects"])
                    })
                
                display_data.sort(key=lambda x: x["User Name"])
                
                # Display in card format
                for i, user_data in enumerate(display_data):
                    status_color = COLORS['danger'] if "Archived" in user_data["Status"] else COLORS['success']
                    
                    sample_projects = user_data["Projects Data"][:3] if user_data["Projects Data"] else []
                    sample_text = ""
                    if sample_projects:
                        sample_names = [p["name"] for p in sample_projects]
                        if len(user_data["Projects Data"]) > 3:
                            sample_text = f"Recent: {', '.join(sample_names[:2])}... (+{len(user_data['Projects Data'])-2} more)"
                        else:
                            sample_text = f"Projects: {', '.join(sample_names)}"
                    
                    with st.container():
                        st.markdown(f"""
                        <div style="border: 2px solid {status_color}; border-radius: 10px; padding: 15px; margin: 10px 0; background: linear-gradient(135deg, #ffffff, #f8f9fa); box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div style="flex: 1;">
                                    <h4 style="margin: 0; color: #1f77b4;">👤 {user_data['User Name']}</h4>
                                    <p style="margin: 5px 0; color: #6c757d;">📧 {user_data['Email']}</p>
                                    <p style="margin: 5px 0; color: #6c757d; font-size: 0.9rem; font-style: italic;">{sample_text}</p>
                                </div>
                                <div style="text-align: right;">
                                    <span style="background: {status_color}; color: white; padding: 5px 10px; border-radius: 15px; font-weight: bold; font-size: 0.9rem; margin-left: 10px;">{user_data["Status"]}</span>
                                    <br><br>
                                    <span style="color: #495057; font-weight: bold;">
                                        {get_user_role_emoji(user_data['User Role'].lower())} {user_data['User Role']}
                                    </span>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        total_projects = user_data["Total Projects"]
                        
                        with st.expander(f"📁 View assignments ({total_projects} projects)", expanded=False):
                            if user_data["Projects Data"]:
                                search_col1, search_col2 = st.columns(2)
                                
                                with search_col1:
                                    user_search = st.text_input(
                                        f"🔍 Search projects for {user_data['User Name']}", 
                                        placeholder="Project name...",
                                        key=f"user_search_{i}"
                                    )
                                
                                with search_col2:
                                    role_options = ["All"] + list(set([role for project in user_data["Projects Data"] for role in project["roles"]]))
                                    user_role_filter = st.selectbox(
                                        "Filter by role",
                                        role_options,
                                        key=f"user_role_filter_{i}"
                                    )
                                
                                filtered_projects = user_data["Projects Data"]
                                
                                if user_search:
                                    filtered_projects = [p for p in filtered_projects if user_search.lower() in p["name"].lower()]
                                
                                if user_role_filter != "All":
                                    filtered_projects = [p for p in filtered_projects if user_role_filter in p["roles"]]
                                
                                if len(filtered_projects) != total_projects:
                                    st.info(f"Showing {len(filtered_projects)} of {total_projects} projects")
                                
                                # Pagination
                                projects_per_page = 10
                                total_project_pages = (len(filtered_projects) - 1) // projects_per_page + 1 if filtered_projects else 1
                                
                                if total_project_pages > 1:
                                    project_page = st.selectbox(
                                        f"Page", 
                                        range(1, total_project_pages + 1),
                                        key=f"project_page_{i}"
                                    ) - 1
                                else:
                                    project_page = 0
                                
                                start_idx = project_page * projects_per_page
                                end_idx = min(start_idx + projects_per_page, len(filtered_projects))
                                page_projects = filtered_projects[start_idx:end_idx]
                                
                                if page_projects:
                                    display_groups = {}
                                    for project in page_projects:
                                        group = project["group"]
                                        if group not in display_groups:
                                            display_groups[group] = []
                                        display_groups[group].append(project["display"])
                                    
                                    for group_name in sorted(display_groups.keys()):
                                        if group_name != "Ungrouped":
                                            st.markdown(f"### 📁 {group_name}")
                                        else:
                                            st.markdown(f"### 📄 Individual Projects")
                                        
                                        for project_display in display_groups[group_name]:
                                            st.markdown(project_display, unsafe_allow_html=True)
                                        
                                        if len(display_groups) > 1:
                                            st.markdown("---")
                                else:
                                    st.info("No projects match the current filters.")
                            else:
                                st.info("No project assignments")
            else:
                st.warning("No users match the current filters.")
        else:
            st.info("No project assignments found in the database.")
        
        with st.expander("🗄️ Raw Assignment Data (Database View)", expanded=False):
            if not assignments_df.empty:
                st.markdown("**Direct database table view:**")
                st.dataframe(assignments_df, use_container_width=True)
            else:
                st.info("No raw assignment data available.")

@st.fragment 
def admin_project_groups():
    st.subheader("📊 Project Group Management")
    
    with get_db_session() as session:
        try:
            groups = ProjectGroupService.list_project_groups(session=session)
            if groups:
                group_data = []
                for group in groups:
                    try:
                        group_info = ProjectGroupService.get_project_group_by_id(group_id=group.id, session=session)
                        project_count = len(group_info["projects"])
                        group_data.append({
                            "ID": group.id,
                            "Name": group.name,
                            "Description": group.description,
                            "Project Count": project_count,
                            "Created": group.created_at
                        })
                    except:
                        group_data.append({
                            "ID": group.id,
                            "Name": group.name,
                            "Description": group.description,
                            "Project Count": "Error",
                            "Created": group.created_at
                        })
                
                st.dataframe(pd.DataFrame(group_data), use_container_width=True)
            else:
                st.info("No project groups exist yet.")
        except Exception as e:
            st.error(f"Error loading project groups: {str(e)}")
        
        with st.expander("➕ Create Project Group"):
            group_name = st.text_input("Group Name", key="admin_pgroup_name")
            group_description = st.text_area("Description", key="admin_pgroup_description")
            
            projects_df = ProjectService.get_all_projects(session=session)
            if not projects_df.empty:
                selected_projects = st.multiselect(
                    "Projects (optional)",
                    projects_df["ID"].tolist(),
                    format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
                    key="admin_pgroup_projects"
                )
            else:
                selected_projects = []
                st.warning("No projects available to add to group.")
            
            if st.button("Create Project Group", key="admin_create_pgroup_btn"):
                if group_name:
                    try:
                        ProjectGroupService.create_project_group(
                            name=group_name, description=group_description, 
                            project_ids=selected_projects, session=session
                        )
                        st.success("Project group created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        with st.expander("✏️ Edit Project Group"):
            try:
                groups = ProjectGroupService.list_project_groups(session=session)
                if groups:
                    group_options = {f"{g.name} (ID: {g.id})": g.id for g in groups}
                    selected_group_name = st.selectbox(
                        "Select Group to Edit", 
                        list(group_options.keys()),
                        key="admin_edit_group_select"
                    )
                    
                    if selected_group_name:
                        selected_group_id = group_options[selected_group_name]
                        
                        group_info = ProjectGroupService.get_project_group_by_id(
                            group_id=selected_group_id, session=session
                        )
                        current_group = group_info["group"]
                        current_projects = group_info["projects"]
                        
                        new_name = st.text_input(
                            "Group Name", 
                            value=current_group.name,
                            key="admin_edit_group_name"
                        )
                        new_description = st.text_area(
                            "Description", 
                            value=current_group.description or "",
                            key="admin_edit_group_description"
                        )
                        
                        st.markdown("**Project Management:**")
                        
                        if current_projects:
                            st.markdown("**Current Projects:**")
                            current_project_ids = [p.id for p in current_projects]
                            for project in current_projects:
                                st.write(f"- {project.name} (ID: {project.id})")
                        else:
                            st.info("No projects currently in this group")
                            current_project_ids = []
                        
                        all_projects_df = ProjectService.get_all_projects(session=session)
                        
                        if not all_projects_df.empty:
                            available_to_add = all_projects_df[~all_projects_df["ID"].isin(current_project_ids)]
                            if not available_to_add.empty:
                                add_projects = st.multiselect(
                                    "Add Projects",
                                    available_to_add["ID"].tolist(),
                                    format_func=lambda x: available_to_add[available_to_add["ID"]==x]["Name"].iloc[0],
                                    key="admin_edit_group_add_projects"
                                )
                            else:
                                add_projects = []
                                st.info("All projects are already in this group")
                            
                            if current_project_ids:
                                remove_projects = st.multiselect(
                                    "Remove Projects",
                                    current_project_ids,
                                    format_func=lambda x: all_projects_df[all_projects_df["ID"]==x]["Name"].iloc[0],
                                    key="admin_edit_group_remove_projects"
                                )
                            else:
                                remove_projects = []
                        else:
                            add_projects = []
                            remove_projects = []
                            st.warning("No projects available in the system")
                        
                        if st.button("Update Project Group", key="admin_update_pgroup_btn"):
                            try:
                                ProjectGroupService.edit_project_group(
                                    group_id=selected_group_id,
                                    name=new_name if new_name != current_group.name else None,
                                    description=new_description if new_description != (current_group.description or "") else None,
                                    add_project_ids=add_projects if add_projects else None,
                                    remove_project_ids=remove_projects if remove_projects else None,
                                    session=session
                                )
                                st.success("Project group updated!")
                                st.rerun(scope="fragment")
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                else:
                    st.info("No project groups available to edit")
            except Exception as e:
                st.error(f"Error loading groups for editing: {str(e)}")

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

def get_project_videos(project_id: int, session: Session) -> List[Dict]:
    """Get videos in a project"""
    try:
        return VideoService.get_project_videos(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error getting project videos: {str(e)}")
        return []

def calculate_user_overall_progress(user_id: int, project_id: int, session: Session) -> float:
    """Calculate user's overall progress"""
    try:
        return AnnotatorService.calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error calculating user progress: {str(e)}")
        return 0.0

def show_training_feedback(video_id: int, project_id: int, group_id: int, user_answers: Dict[str, str], session: Session):
    """Show training feedback comparing user answers to ground truth"""
    try:
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        question_map = {q["id"]: q for q in questions}
        
        st.subheader("📊 Training Feedback")
        
        for _, gt_row in gt_df.iterrows():
            question_id = gt_row["Question ID"]
            if question_id not in question_map:
                continue
                
            question = question_map[question_id]
            question_text = question["text"]
            gt_answer = gt_row["Answer Value"]
            user_answer = user_answers.get(question_text, "")
            
            is_correct = user_answer == gt_answer
            
            if is_correct:
                st.success(f"✅ **{question_text}**: Correct!")
            else:
                st.error(f"❌ **{question_text}**: Incorrect")
                st.write(f"Your answer: {user_answer}")
                st.write(f"Correct answer: {gt_answer}")
                
    except ValueError as e:
        st.error(f"Error showing training feedback: {str(e)}")

def show_database_health():
    """Show database health information for troubleshooting"""
    try:
        health = check_database_health()
        
        if health.get("healthy"):
            st.success("🟢 Database connection healthy")
        else:
            st.error("🔴 Database connection issues")
            if "error" in health:
                st.error(f"Error: {health['error']}")
        
        if health.get("database"):
            st.write(f"**Database:** {health['database']}")
        if health.get("status"):
            st.write(f"**Status:** {health['status']}")
        
        with st.expander("Full Details"):
            st.json(health)
            
    except Exception as e:
        st.error(f"Unable to check database health: {e}")

###############################################################################
# MAIN APP
###############################################################################

def main():
    st.set_page_config(
        page_title="Label Pizza",
        page_icon="🍕",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Enhanced Custom CSS - Modern design with elegant sidebar
    st.markdown("""
        <style>
        /* Global improvements */
        .stProgress > div > div > div > div {
            background-color: #4CAF50;
        }
        
        /* Clean, modern tabs styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 12px;
            padding: 4px;
            border: 1px solid #dee2e6;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        
        .stTabs [data-baseweb="tab"] {
            height: 40px;
            white-space: pre-wrap;
            background-color: transparent;
            border-radius: 8px;
            color: #495057;
            font-weight: 600;
            border: none;
            padding: 8px 16px;
            transition: all 0.2s ease;
            font-size: 0.9rem;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background-color: rgba(31, 119, 180, 0.1);
            transform: translateY(-1px);
        }
        
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #1f77b4, #4a90e2) !important;
            color: white !important;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.3);
            transform: translateY(-1px);
        }
        
        /* Remove form borders */
        .stForm {
            border: none !important;
            padding: 0 !important;
            margin: 0 !important;
            background: transparent !important;
        }
        
        /* Sleek radio button styling */
        .stRadio > div {
            gap: 0.5rem;
            background: transparent !important;
            padding: 0 !important;
            border: none !important;
            border-radius: 0 !important;
            margin: 0 !important;
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: wrap !important;
            align-items: flex-start !important;
        }
        
        .stRadio > div > label {
            margin-bottom: 0.5rem;
            font-size: 0.85rem;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 6px 12px;
            border-radius: 6px;
            border: 1px solid #e9ecef;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            flex: 0 0 auto;
            min-width: fit-content;
            max-width: 280px;
            font-weight: 500;
            line-height: 1.2;
        }
        
        .stRadio > div > label:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.15);
        }
        
        .stRadio > div > label[data-checked="true"] {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-color: #1f77b4;
            font-weight: 600;
            color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.2);
        }
        
        /* Enhanced text areas */
        .stTextArea > div > div > textarea {
            border-radius: 8px;
            border: 1px solid #e9ecef;
            font-size: 0.9rem;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 12px;
            transition: all 0.2s ease;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            line-height: 1.4;
        }
        
        .stTextArea > div > div > textarea:focus {
            border-color: #1f77b4;
            box-shadow: 0 0 0 2px rgba(31, 119, 180, 0.1), 0 2px 8px rgba(31, 119, 180, 0.15);
            background: #ffffff;
        }
        
        /* Enhanced button styling */
        .stButton > button {
            border-radius: 8px;
            border: none;
            transition: all 0.2s ease;
            font-weight: 600;
            background: linear-gradient(135deg, #1f77b4, #4a90e2);
            color: white;
            padding: 10px 20px;
            font-size: 0.9rem;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.2);
            position: relative;
            z-index: 100;
            letter-spacing: 0.3px;
        }
        
        .stButton > button:hover {
            box-shadow: 0 4px 12px rgba(31, 119, 180, 0.3);
            transform: translateY(-2px);
            background: linear-gradient(135deg, #1a6ca8, #4088d4);
        }
        
        .stButton > button:disabled {
            background: linear-gradient(135deg, #6c757d, #adb5bd);
            transform: none;
            box-shadow: 0 1px 3px rgba(0,0,0,0.15);
            opacity: 0.6;
        }
        
        /* Segmented control */
        .stSegmentedControl {
            margin: 12px 0;
            padding: 3px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 8px;
            border: 1px solid #dee2e6;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        .stSegmentedControl > div {
            gap: 2px;
        }
        
        .stSegmentedControl button {
            border-radius: 6px !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
            border: none !important;
            font-size: 0.85rem !important;
            padding: 6px 12px !important;
        }
        
        .stSegmentedControl button:hover {
            transform: translateY(-1px) !important;
            box-shadow: 0 1px 4px rgba(31, 119, 180, 0.15) !important;
        }
        
        .stSegmentedControl button[aria-selected="true"] {
            background: linear-gradient(135deg, #1f77b4, #4a90e2) !important;
            color: white !important;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.25) !important;
        }
        
        /* Enhanced alert styling */
        .stAlert {
            border-radius: 8px;
            border: none;
            box-shadow: 0 1px 4px rgba(0,0,0,0.08);
            font-weight: 500;
        }
        
        /* Remove container borders */
        .element-container {
            border: none !important;
        }
        
        /* Custom scrollbars */
        ::-webkit-scrollbar {
            width: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: linear-gradient(180deg, #f1f1f1, #e9ecef);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: linear-gradient(180deg, #1f77b4, #4a90e2);
            border-radius: 4px;
            border: 1px solid #f1f1f1;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(180deg, #1a6ca8, #4088d4);
        }
        
        /* Enhanced success styling */
        .stSuccess {
            font-weight: 600;
            text-align: center;
            background: linear-gradient(135deg, #d4edda, #c3e6cb);
            border: 1px solid #28a745;
            border-radius: 8px;
        }
        
        /* Tab content padding */
        .stTabs [data-baseweb="tab-panel"] {
            padding: 16px 0;
        }
        
        /* Helper text styling */
        .stCaption {
            font-size: 0.8rem;
            color: #6c757d;
            font-style: italic;
            margin-top: 8px;
            padding: 6px 10px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 4px;
            border-left: 2px solid #1f77b4;
        }
        
        /* Enhanced checkbox styling with uniform width */
        .stCheckbox > label {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 6px 8px;
            border-radius: 6px;
            border: 1px solid #e9ecef;
            margin-bottom: 4px;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 500;
            font-size: 0.75rem;
            width: 100%;
            min-height: 40px;
            text-align: center;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .stCheckbox > label > div {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            width: 100%;
            text-align: center;
        }
        
        .stCheckbox > label:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.15);
        }
        
        .stCheckbox > label[data-checked="true"] {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-color: #1f77b4;
            font-weight: 600;
            color: #1f77b4;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.2);
        }
        
        /* Responsive improvements */
        @media (max-width: 768px) {
            .stRadio > div {
                gap: 0.4rem;
            }
            
            .stRadio > div > label {
                padding: 5px 10px;
                font-size: 0.8rem;
                max-width: 200px;
            }
            
            .stTabs [data-baseweb="tab"] {
                padding: 6px 12px;
                font-size: 0.8rem;
            }
            
            .stCheckbox > label {
                padding: 6px 10px;
                font-size: 0.8rem;
                min-height: 50px;
            }
        }
        
        /* Enhanced multiselect styling */
        .stMultiSelect > div > div {
            border-radius: 8px;
            border: 1px solid #e9ecef;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            min-height: 42px;
        }
        
        .stMultiSelect > div > div:focus-within {
            border-color: #1f77b4;
            box-shadow: 0 0 0 2px rgba(31, 119, 180, 0.1);
        }
        
        /* Modern sidebar styling */
        .css-1d391kg {
            padding-top: 1rem;
            background: linear-gradient(180deg, #f8f9fa 0%, #ffffff 100%);
        }
        
        .css-1d391kg h3 {
            font-size: 1.2rem;
            margin-bottom: 0.8rem;
            color: #2c3e50;
            font-weight: 700;
            text-align: center;
            padding: 10px;
            background: linear-gradient(135deg, #ecf0f1, #bdc3c7);
            border-radius: 12px;
            border: 1px solid #bdc3c7;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .css-1d391kg .element-container {
            margin-bottom: 0.8rem;
        }
        
        /* Enhanced sidebar radio buttons */
        .css-1d391kg .stRadio > div > label {
            padding: 12px 16px;
            margin-bottom: 8px;
            font-size: 0.95rem;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border: 2px solid #e1e5e9;
            border-radius: 12px;
            transition: all 0.3s ease;
            font-weight: 600;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        
        .css-1d391kg .stRadio > div > label:hover {
            background: linear-gradient(135deg, #e3f2fd, #bbdefb);
            border-color: #2196f3;
            transform: translateX(5px) translateY(-2px);
            box-shadow: 0 4px 15px rgba(33, 150, 243, 0.2);
        }
        
        .css-1d391kg .stRadio > div > label[data-checked="true"] {
            background: linear-gradient(135deg, #1976d2, #42a5f5);
            color: white;
            border-color: #1976d2;
            transform: translateX(5px) translateY(-2px);
            box-shadow: 0 6px 20px rgba(25, 118, 210, 0.4);
        }
        
        /* Enhanced sidebar button styling */
        .css-1d391kg .stButton > button {
            padding: 12px 20px;
            font-size: 0.9rem;
            margin-top: 1rem;
            background: linear-gradient(135deg, #e74c3c, #c0392b);
            border: none;
            border-radius: 12px;
            color: white;
            font-weight: 700;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(231, 76, 60, 0.3);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .css-1d391kg .stButton > button:hover {
            background: linear-gradient(135deg, #c0392b, #a93226);
            transform: translateY(-3px);
            box-shadow: 0 6px 20px rgba(231, 76, 60, 0.4);
        }
        
        .css-1d391kg .stMarkdown {
            margin-bottom: 0.8rem;
        }
        
        /* Enhanced user info cards */
        .css-1d391kg .stSuccess,
        .css-1d391kg .stInfo {
            border-radius: 12px;
            padding: 12px 16px;
            margin: 10px 0;
            border: none;
            box-shadow: 0 3px 12px rgba(0,0,0,0.12);
            font-size: 0.9rem;
            font-weight: 600;
        }
        
        .css-1d391kg .stSuccess {
            background: linear-gradient(135deg, #2ecc71, #27ae60);
            color: white;
        }
        
        .css-1d391kg .stInfo {
            background: linear-gradient(135deg, #3498db, #2980b9);
            color: white;
        }
        
        /* Enhanced metrics styling */
        .stMetric {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            padding: 12px;
            border-radius: 8px;
            border: 1px solid #dee2e6;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        /* Enhanced expander styling */
        .streamlit-expanderHeader {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 8px;
            padding: 8px 12px;
            font-weight: 600;
            border: 1px solid #dee2e6;
            transition: all 0.2s ease;
        }
        
        .streamlit-expanderHeader:hover {
            background: linear-gradient(135deg, #e9ecef, #dee2e6);
            transform: translateY(-1px);
        }
        
        /* Enhanced dataframe styling */
        .stDataFrame {
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }

        /* Search Portal specific styling */
        .search-video-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 16px;
            margin: 16px 0;
        }

        .search-criteria-card {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border: 2px solid #dee2e6;
            border-radius: 12px;
            padding: 12px;
            margin: 8px 0;
            transition: all 0.2s ease;
        }

        .search-criteria-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }

        .search-result-card {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border: 2px solid #e9ecef;
            border-radius: 12px;
            padding: 16px;
            margin: 8px 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: all 0.3s ease;
            cursor: pointer;
        }

        .search-result-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 6px 20px rgba(0,0,0,0.15);
            border-color: #1f77b4;
        }

        .search-expandable-content {
            max-height: 600px;
            overflow-y: auto;
            padding: 8px;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            background: #fafbfc;
        }

        .search-question-answer {
            background: #ffffff;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            padding: 12px;
            margin: 8px 0;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }

        .search-ground-truth {
            background: linear-gradient(135deg, #d4edda, #c3e6cb);
            border-left: 4px solid #28a745;
            padding: 8px 12px;
            margin: 4px 0;
            border-radius: 4px;
        }

        .search-annotator-answer {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-left: 4px solid #1f77b4;
            padding: 8px 12px;
            margin: 4px 0;
            border-radius: 4px;
        }

        .search-status-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
            text-align: center;
        }

        .search-stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 12px;
            margin: 16px 0;
        }

        /* Enhanced expander styling for search */
        .search-portal .streamlit-expanderHeader {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border: 2px solid #e9ecef;
            border-radius: 10px;
            padding: 10px 16px;
            font-weight: 600;
            margin: 8px 0;
            transition: all 0.2s ease;
        }

        .search-portal .streamlit-expanderHeader:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 2px 8px rgba(31, 119, 180, 0.15);
        }

        /* Search input enhancement */
        .search-portal .stTextInput > div > div > input {
            border-radius: 10px;
            border: 2px solid #e9ecef;
            padding: 12px 16px;
            font-size: 0.95rem;
            transition: all 0.2s ease;
        }

        .search-portal .stTextInput > div > div > input:focus {
            border-color: #1f77b4;
            box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.1);
        }

        /* Search multiselect enhancement */
        .search-portal .stMultiSelect > div > div {
            border-radius: 10px;
            border: 2px solid #e9ecef;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            transition: all 0.2s ease;
        }

        .search-portal .stMultiSelect > div > div:focus-within {
            border-color: #1f77b4;
            box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.1);
        }

        /* Video grid responsive design */
        @media (max-width: 768px) {
            .search-video-grid {
                grid-template-columns: 1fr;
            }
            
            .search-stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Initialize session state
    if "current_view" not in st.session_state:
        st.session_state.current_view = "dashboard"
    if "selected_project_id" not in st.session_state:
        st.session_state.selected_project_id = None
    
    # Check authentication
    if "user" not in st.session_state:
        login_page()
        return
    
    user = st.session_state.user
    available_portals = st.session_state.get("available_portals", [])
    
    # Modern, compact sidebar with elegant Label Pizza card
    with st.sidebar:
        st.markdown("""
        <div style="
            width: 100%;
            padding: 20px;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border-radius: 16px;
            margin: 10px 0 20px 0;
            box-shadow: 0 8px 24px rgba(0,0,0,0.15);
            border: 2px solid #e9ecef;
            text-align: center;
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -20px;
                right: -20px;
                width: 60px;
                height: 60px;
                background: linear-gradient(135deg, #ff6b6b, #ffa500);
                border-radius: 50%;
                opacity: 0.1;
            "></div>
            <div style="
                position: absolute;
                bottom: -15px;
                left: -15px;
                width: 40px;
                height: 40px;
                background: linear-gradient(135deg, #ff4757, #ff6b6b);
                border-radius: 50%;
                opacity: 0.1;
            "></div>
            <div style="
                font-size: 2.2rem;
                margin-bottom: 8px;
                filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));
            ">🍕</div>
            <div style="
                font-size: 1.4rem;
                font-weight: 800;
                color: #2c3e50;
                margin-bottom: 4px;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                letter-spacing: 0.5px;
            ">Label Pizza</div>
            <div style="
                font-size: 0.85rem;
                color: #6c757d;
                font-weight: 500;
                font-style: italic;
            ">Video Annotation Made Simple</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### 👋 Welcome!")
        
        user_email = user.get('email', 'No email')
        if not user_email or user_email == 'No email':
            user_email = user.get('Email', user.get('user_email', 'No email'))
        
        display_user_simple(user['name'], user_email, is_ground_truth=(user['role'] == 'admin'))
        
        role_color = COLORS['danger'] if user['role'] == 'admin' else COLORS['info']
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {role_color}, {role_color}dd);
            color: white;
            padding: 6px 12px;
            border-radius: 8px;
            text-align: center;
            font-weight: 600;
            font-size: 0.9rem;
            margin: 8px 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        ">
            Role: {user['role'].title()}
        </div>
        """, unsafe_allow_html=True)
        
        # Portal selection
        if available_portals:
            st.markdown("---")
            st.markdown("**Portal:**")
            
            portal_labels = {
                "annotator": "👥 Annotator",
                "reviewer": "🔍 Reviewer", 
                "meta_reviewer": "🎯 Meta-Reviewer",
                "search": "🔍 Search",
                "admin": "⚙️ Admin"
            }
            
            if "selected_portal" not in st.session_state:
                if user["role"] == "admin":
                    st.session_state.selected_portal = "admin"
                else:
                    st.session_state.selected_portal = available_portals[0]
            
            selected_portal = st.radio(
                "Portal",
                available_portals,
                format_func=lambda x: portal_labels[x],
                index=available_portals.index(st.session_state.selected_portal) if st.session_state.selected_portal in available_portals else 0,
                label_visibility="collapsed"
            )
            
            if selected_portal != st.session_state.get("selected_portal"):
                st.session_state.selected_portal = selected_portal
                st.session_state.current_view = "dashboard"
                st.session_state.selected_project_id = None
                if "selected_annotators" in st.session_state:
                    del st.session_state.selected_annotators
                st.rerun()
        
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Route to selected portal
    if not available_portals:
        st.warning("No portals available. You may not be assigned to any projects or your account may not have the necessary permissions. Please contact an administrator.")
        return
    
    selected_portal = st.session_state.get("selected_portal", available_portals[0])
    
    portal_functions = {
        "admin": admin_portal,
        "meta_reviewer": meta_reviewer_portal,
        "reviewer": reviewer_portal,
        "annotator": annotator_portal,
        "search": search_portal
    }
    
    portal_function = portal_functions.get(selected_portal)
    if portal_function:
        portal_function()
    else:
        st.error(f"Unknown portal: {selected_portal}")

if __name__ == "__main__":
    main()