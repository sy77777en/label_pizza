"""
Label Pizza - Modern Streamlit App
==================================
Updated to work with complete service layer design.
NO direct database access - all data access through service layer.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from contextlib import contextmanager
import re
import streamlit.components.v1 as components

# Import your modules (adjust paths as needed)
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
# VIDEO PLAYER WITH HEIGHT RETURN
###############################################################################

def custom_video_player(video_url, aspect_ratio="16:9"):
    """
    Custom video player with progress bar positioned below the video
    Responsive design that adapts to Streamlit column width
    
    Args:
        video_url: URL or path to the video
        aspect_ratio: Video aspect ratio as string (e.g., "16:9", "4:3", "21:9")
        
    Returns:
        int: The calculated height of the video player component
    """
    
    # Calculate aspect ratio
    ratio_parts = aspect_ratio.split(":")
    aspect_ratio_decimal = float(ratio_parts[0]) / float(ratio_parts[1])
    padding_bottom = (1 / aspect_ratio_decimal) * 100
    
    # HTML/CSS/JS for responsive custom video player
    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }}
            
            html, body {{
                height: 100%;
                margin: 0;
                padding: 0;
                font-family: Arial, sans-serif;
                overflow: hidden;
            }}
            
            .video-container {{
                width: 100%;
                height: 100%;
                display: flex;
                flex-direction: column;
                background: #fff;
                overflow: hidden;
            }}
            
            .video-wrapper {{
                position: relative;
                width: 100%;
                flex: 1;
                background: #000;
                border-radius: 8px 8px 0 0;
                overflow: hidden;
                min-height: 200px;
            }}
            
            .video-wrapper::before {{
                content: '';
                display: block;
                padding-bottom: {padding_bottom}%;
            }}
            
            video {{
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                object-fit: contain;
            }}
            
            /* Hide default video controls */
            video::-webkit-media-controls {{
                display: none !important;
            }}
            
            video::-moz-media-controls {{
                display: none !important;
            }}
            
            /* Custom controls container */
            .controls-container {{
                width: 100%;
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-top: none;
                border-radius: 0 0 8px 8px;
                padding: 8px 12px;
                flex-shrink: 0;
                overflow: hidden;
                min-height: 65px;
                max-height: 80px;
            }}
            
            /* Progress bar container */
            .progress-container {{
                width: 100%;
                height: 6px;
                background: #ddd;
                border-radius: 3px;
                margin-bottom: 8px;
                cursor: pointer;
                position: relative;
                user-select: none;
                overflow: hidden;
            }}
            
            .progress-bar {{
                height: 100%;
                background: linear-gradient(90deg, #ff4444, #ff6666);
                border-radius: 3px;
                width: 0%;
                pointer-events: none;
                transition: none;
            }}
            
            .progress-handle {{
                position: absolute;
                top: -5px;
                width: 16px;
                height: 16px;
                background: #ff4444;
                border: 2px solid white;
                border-radius: 50%;
                cursor: grab;
                transform: translateX(-50%);
                opacity: 0;
                transition: opacity 0.2s ease, transform 0.1s ease;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }}
            
            .progress-handle:active {{
                cursor: grabbing;
                transform: translateX(-50%) scale(1.1);
            }}
            
            .progress-container:hover .progress-handle {{
                opacity: 1;
            }}
            
            .progress-container:active .progress-handle {{
                opacity: 1;
            }}
            
            /* Control buttons */
            .controls {{
                display: flex;
                align-items: center;
                gap: 6px;
                width: 100%;
                overflow: hidden;
                min-height: 32px;
            }}
            
            .control-btn {{
                background: none;
                border: none;
                font-size: 14px;
                cursor: pointer;
                padding: 4px 6px;
                border-radius: 4px;
                transition: background 0.2s ease;
                display: flex;
                align-items: center;
                justify-content: center;
                min-width: 28px;
                height: 28px;
                flex-shrink: 0;
            }}
            
            .control-btn:hover {{
                background: #e9ecef;
            }}
            
            .time-display {{
                font-size: 11px;
                color: #666;
                margin-left: auto;
                white-space: nowrap;
                font-family: 'Courier New', monospace;
                flex-shrink: 0;
                overflow: hidden;
                text-overflow: ellipsis;
                max-width: 120px;
            }}
            
            .volume-control {{
                display: flex;
                align-items: center;
                gap: 4px;
                flex-shrink: 0;
            }}
            
            .volume-slider {{
                width: 50px;
                height: 3px;
                background: #ddd;
                outline: none;
                border-radius: 2px;
                -webkit-appearance: none;
                flex-shrink: 0;
            }}
            
            .volume-slider::-webkit-slider-thumb {{
                -webkit-appearance: none;
                width: 12px;
                height: 12px;
                background: #ff4444;
                border-radius: 50%;
                cursor: pointer;
            }}
            
            .volume-slider::-moz-range-thumb {{
                width: 12px;
                height: 12px;
                background: #ff4444;
                border-radius: 50%;
                cursor: pointer;
                border: none;
            }}
            
            /* Extra responsive handling */
            @media (max-width: 600px) {{
                .controls {{
                    gap: 4px;
                }}
                
                .control-btn {{
                    font-size: 12px;
                    min-width: 24px;
                    height: 24px;
                    padding: 2px 4px;
                }}
                
                .time-display {{
                    font-size: 10px;
                    max-width: 80px;
                }}
                
                .volume-slider {{
                    width: 40px;
                }}
                
                .controls-container {{
                    padding: 6px 8px;
                    min-height: 60px;
                }}
                
                .progress-container {{
                    height: 5px;
                    margin-bottom: 6px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="video-container">
            <div class="video-wrapper">
                <video id="customVideo" preload="metadata">
                    <source src="{video_url}" type="video/mp4">
                    Your browser does not support the video tag.
                </video>
            </div>
            
            <div class="controls-container">
                <!-- Progress bar -->
                <div class="progress-container" id="progressContainer">
                    <div class="progress-bar" id="progressBar"></div>
                    <div class="progress-handle" id="progressHandle"></div>
                </div>
                
                <!-- Control buttons -->
                <div class="controls">
                    <button class="control-btn" id="playPauseBtn" title="Play/Pause">▶️</button>
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

            // Play/Pause functionality
            playPauseBtn.addEventListener('click', () => {{
                if (video.paused) {{
                    video.play();
                    playPauseBtn.textContent = '⏸️';
                }} else {{
                    video.pause();
                    playPauseBtn.textContent = '▶️';
                }}
            }});

            // Mute functionality
            muteBtn.addEventListener('click', () => {{
                video.muted = !video.muted;
                muteBtn.textContent = video.muted ? '🔇' : '🔊';
            }});

            // Volume control
            volumeSlider.addEventListener('input', () => {{
                video.volume = volumeSlider.value / 100;
            }});

            // Smooth progress bar update
            function updateProgress() {{
                if (!isDragging && video.duration) {{
                    const progress = (video.currentTime / video.duration) * 100;
                    progressBar.style.width = progress + '%';
                    progressHandle.style.left = progress + '%';
                    
                    // Update time display
                    const currentTime = formatTime(video.currentTime);
                    const duration = formatTime(video.duration);
                    timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                }}
                requestAnimationFrame(updateProgress);
            }}
            
            // Start smooth updates
            updateProgress();

            // Helper function to get progress percentage from mouse position
            function getProgressFromMouse(e) {{
                const rect = progressContainer.getBoundingClientRect();
                const percent = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
                return percent;
            }}

            // Progress bar mouse down (start dragging)
            progressContainer.addEventListener('mousedown', (e) => {{
                isDragging = true;
                wasPlaying = !video.paused;
                if (wasPlaying) video.pause();
                
                const percent = getProgressFromMouse(e);
                const newTime = percent * video.duration;
                
                progressBar.style.width = (percent * 100) + '%';
                progressHandle.style.left = (percent * 100) + '%';
                video.currentTime = newTime;
                
                // Update time display during drag
                const currentTime = formatTime(newTime);
                const duration = formatTime(video.duration);
                timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                
                e.preventDefault();
            }});

            // Mouse move (dragging)
            document.addEventListener('mousemove', (e) => {{
                if (isDragging) {{
                    const percent = getProgressFromMouse(e);
                    const newTime = percent * video.duration;
                    
                    progressBar.style.width = (percent * 100) + '%';
                    progressHandle.style.left = (percent * 100) + '%';
                    video.currentTime = newTime;
                    
                    // Update time display during drag
                    const currentTime = formatTime(newTime);
                    const duration = formatTime(video.duration);
                    timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                }}
            }});

            // Mouse up (stop dragging)
            document.addEventListener('mouseup', () => {{
                if (isDragging) {{
                    isDragging = false;
                    if (wasPlaying) {{
                        video.play();
                    }}
                }}
            }});

            // Progress bar click (for non-drag clicks)
            progressContainer.addEventListener('click', (e) => {{
                if (!isDragging) {{
                    const percent = getProgressFromMouse(e);
                    video.currentTime = percent * video.duration;
                }}
            }});

            // Fullscreen functionality
            fullscreenBtn.addEventListener('click', () => {{
                if (document.fullscreenElement) {{
                    document.exitFullscreen();
                }} else {{
                    document.querySelector('.video-wrapper').requestFullscreen();
                }}
            }});

            // Format time helper function
            function formatTime(time) {{
                if (isNaN(time)) return '0:00';
                const minutes = Math.floor(time / 60);
                const seconds = Math.floor(time % 60);
                return `${{minutes}}:${{seconds.toString().padStart(2, '0')}}`;
            }}

            // Handle video end
            video.addEventListener('ended', () => {{
                playPauseBtn.textContent = '▶️';
            }});

            // Handle video load
            video.addEventListener('loadedmetadata', () => {{
                const duration = formatTime(video.duration);
                timeDisplay.textContent = `0:00 / ${{duration}}`;
            }});
        </script>
    </body>
    </html>
    """
    
    # Calculate more precise height based on aspect ratio
    # Use reasonable screen width estimates for better height calculation
    estimated_width = 400  # Adjusted for side-by-side layout
    video_height = estimated_width / aspect_ratio_decimal
    controls_height = 90  # More precise controls height
    total_height = int(video_height + controls_height)
    
    # Ensure minimum and maximum bounds
    total_height = max(300, min(600, total_height))
    
    components.html(html_code, height=total_height, scrolling=False)
    
    # Return the calculated height for matching with answer forms
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
            session.execute(text("SELECT 1"))
            return {
                "healthy": True,
                "database": "Connected",
                "status": "OK"
            }
    except Exception as e:
        return {
            "healthy": False,
            "error": str(e),
            "database": "Disconnected"
        }

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

###############################################################################
# AUTHENTICATION & ROUTING
###############################################################################

def login_page():
    st.title("🍕 Label Pizza")
    st.markdown("### Login to your account")
    
    with st.form("login_form"):
        email = st.text_input("Email", placeholder="Enter your email")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        
        submitted = st.form_submit_button("Login", use_container_width=True)
        
        if submitted:
            authenticate_user(email, password)

def get_user_display_name(user_name: str, user_email: str) -> str:
    """Create display name for multiselect like 'Zhiqiu Lin (ZL)'"""
    name_parts = user_name.split()
    if len(name_parts) >= 2:
        initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper()
    else:
        initials = user_name[:2].upper()
    
    return f"{user_name} ({initials})"

def authenticate_user(email: str, password: str):
    if not email or not password:
        st.error("Please enter both email and password")
        return
        
    try:
        with get_db_session() as session:
            user = None
            
            # First try admin authentication
            user = AuthService.authenticate(email, password, "admin", session)
            if not user:
                user = AuthService.authenticate(email, password, "human", session)
            
            if user:
                st.session_state.user = user
                st.session_state.user_projects = get_user_projects(user["id"], session)
                st.session_state.available_portals = get_available_portals(user, st.session_state.user_projects)
                # Reset navigation state
                st.session_state.current_view = "dashboard"
                st.session_state.selected_project_id = None
                st.success(f"Welcome {user['name']}!")
                st.rerun()
            else:
                st.error("Invalid email or password")
    except Exception as e:
        st.error(f"Login failed: {str(e)}")

def get_user_projects(user_id: int, session: Session) -> Dict:
    """Get projects assigned to user by role - using service layer"""
    try:
        return AuthService.get_user_projects_by_role(user_id, session)
    except ValueError as e:
        st.error(f"Error getting user projects: {str(e)}")
        return {"annotator": [], "reviewer": [], "admin": []}

def get_available_portals(user: Dict, user_projects: Dict) -> List[str]:
    """Determine which portals the user can access"""
    available_portals = []
    
    if user["role"] == "admin":
        available_portals = ["annotator", "reviewer", "admin"]
    else:
        if user_projects.get("annotator"):
            available_portals.append("annotator")
        if user_projects.get("reviewer"):
            available_portals.append("reviewer")
    
    return available_portals

###############################################################################
# SHARED UTILITY FUNCTIONS
###############################################################################

def get_all_project_annotators(project_id: int, session: Session) -> Dict[str, Dict]:
    """Get all annotators who have answered questions in this project - using service layer"""
    try:
        return ProjectService.get_project_annotators(project_id, session)
    except ValueError as e:
        st.error(f"Error getting project annotators: {str(e)}")
        return {}

def display_user_simple(user_name: str, user_email: str, is_ground_truth: bool = False):
    """Simple user display using native Streamlit components"""
    # Extract initials from name
    name_parts = user_name.split()
    if len(name_parts) >= 2:
        initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper()
    else:
        initials = user_name[:2].upper()
    
    if is_ground_truth:
        st.success(f"🏆 **{user_name}** ({initials}) - {user_email}")
    else:
        st.info(f"👤 **{user_name}** ({initials}) - {user_email}")

def check_project_has_full_ground_truth(project_id: int, session: Session) -> bool:
    """Check if project has complete ground truth for ALL questions and videos - using service layer"""
    try:
        return ProjectService.check_project_has_full_ground_truth(project_id, session)
    except ValueError as e:
        st.error(f"Error checking ground truth status: {str(e)}")
        return False

def get_project_groups_with_projects(user_id: int, role: str, session: Session) -> Dict:
    """Get project groups with their projects for a user - using service layer"""
    try:
        return ProjectGroupService.get_grouped_projects_for_user(user_id, role, session)
    except ValueError as e:
        st.error(f"Error getting grouped projects: {str(e)}")
        return {}

def display_project_dashboard(user_id: int, role: str, session: Session) -> Optional[int]:
    """Display project group dashboard with scrollable containers"""
    
    st.markdown("## 📂 Project Dashboard")
    
    # Get grouped projects using service layer
    grouped_projects = get_project_groups_with_projects(user_id, role, session)
    
    if not grouped_projects:
        st.warning(f"No projects assigned to you as {role}.")
        return None
    
    # Search and sort controls
    col1, col2, col3 = st.columns(3)
    with col1:
        search_term = st.text_input("🔍 Search projects", placeholder="Enter project name...")
    with col2:
        sort_by = st.selectbox("Sort by", ["Name", "Created Time", "Completion Rate"])
    with col3:
        sort_order = st.selectbox("Order", ["Ascending", "Descending"])
    
    # Display each group with scrollable container
    selected_project_id = None
    
    for group_name, projects in grouped_projects.items():
        if not projects:
            continue
            
        st.markdown(f"### 📁 {group_name}")
        
        # Filter projects by search
        filtered_projects = projects
        if search_term:
            filtered_projects = [p for p in projects if search_term.lower() in p["name"].lower()]
        
        if not filtered_projects:
            st.info(f"No projects match your search in {group_name}")
            continue
        
        # Sort projects
        if sort_by == "Name":
            filtered_projects = sorted(filtered_projects, key=lambda x: x["name"], reverse=(sort_order == "Descending"))
        elif sort_by == "Created Time":
            filtered_projects = sorted(filtered_projects, key=lambda x: x["created_at"], reverse=(sort_order == "Descending"))
        
        # Create scrollable container for projects
        with st.container():
            # Use grid layout for projects
            cols = st.columns(min(3, len(filtered_projects)))
            
            for i, project in enumerate(filtered_projects):
                with cols[i % 3]:
                    # Check if this project has full ground truth
                    has_full_gt = check_project_has_full_ground_truth(project["id"], session)
                    mode = "🎓 Training" if has_full_gt else "📝 Annotation"
                    
                    # Get project progress for this user
                    try:
                        if role == "annotator":
                            personal_progress = calculate_user_overall_progress(user_id, project["id"], session)
                            progress_text = f"{personal_progress:.1f}% Complete"
                        else:
                            project_progress = ProjectService.progress(project["id"], session)
                            progress_text = f"{project_progress['completion_percentage']:.1f}% GT Complete"
                    except:
                        progress_text = "Progress unavailable"
                    
                    # Project card
                    with st.container():
                        st.markdown(f"""
                        <div style="
                            border: 1px solid #ddd;
                            border-radius: 10px;
                            padding: 1rem;
                            margin: 0.5rem 0;
                            background: white;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        ">
                            <h4 style="margin: 0 0 0.5rem 0;">{project["name"]}</h4>
                            <p style="margin: 0.5rem 0; color: #666; font-size: 0.9rem;">{project["description"] or 'No description'}</p>
                            <p style="margin: 0.5rem 0;"><strong>Mode:</strong> {mode}</p>
                            <p style="margin: 0.5rem 0;"><strong>Progress:</strong> {progress_text}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Select button
                        if st.button(f"Select {project['name']}", key=f"select_project_{project['id']}"):
                            selected_project_id = project["id"]
                            st.session_state.selected_project_id = project["id"]
                            st.session_state.current_view = "project"
                            st.rerun()
        
        if selected_project_id:
            break
    
    return None

def display_project_view(user_id: int, role: str, session: Session):
    """Display the selected project with videos in side-by-side layout"""
    project_id = st.session_state.selected_project_id
    
    # Back button
    if st.button("← Back to Dashboard", key="back_to_dashboard"):
        st.session_state.current_view = "dashboard"
        st.session_state.selected_project_id = None
        # Clear annotator selection when leaving project
        if "selected_annotators" in st.session_state:
            del st.session_state.selected_annotators
        st.rerun()
    
    # Get project details using service layer
    try:
        project = ProjectService.get_project_by_id(project_id, session)
    except ValueError as e:
        st.error(f"Error loading project: {str(e)}")
        return
    
    # Determine project mode
    mode = "Training" if check_project_has_full_ground_truth(project_id, session) else "Annotation"
    
    st.markdown(f"## 📁 {project.name}")
    
    # Updated mode display with proper messaging
    if role == "annotator":
        if mode == "Training":
            st.success("🎓 **Training Mode** - Try your best! You'll get immediate feedback after each submission.")
        else:
            st.info("📝 **Annotation Mode** - Try your best to answer the questions accurately.")
    else:  # reviewer
        st.info("🔍 **Review Mode** - Help create the ground truth dataset!")
    
    # Show overall progress
    if role == "annotator":
        overall_progress = calculate_user_overall_progress(user_id, project_id, session)
        st.progress(overall_progress / 100)
        st.markdown(f"**Your Overall Progress:** {overall_progress:.1f}%")
    else:
        try:
            project_progress = ProjectService.progress(project_id, session)
            st.progress(project_progress['completion_percentage'] / 100)
            st.markdown(f"**Ground Truth Progress:** {project_progress['completion_percentage']:.1f}%")
        except ValueError as e:
            st.error(f"Error loading project progress: {str(e)}")
    
    # Global annotator selection for reviewers
    if role == "reviewer":
        st.markdown("### 👥 Select Annotators to Review")
        
        try:
            annotators = get_all_project_annotators(project_id, session)
            
            if annotators:
                annotator_options = list(annotators.keys())
                
                # Initialize session state for selected annotators
                if "selected_annotators" not in st.session_state:
                    st.session_state.selected_annotators = annotator_options[:3] if len(annotator_options) > 3 else annotator_options
                
                selected_annotators = st.multiselect(
                    "Choose annotators to view across all videos:",
                    options=annotator_options,
                    default=st.session_state.selected_annotators,
                    key="global_annotator_selector"
                )
                
                # Update session state
                st.session_state.selected_annotators = selected_annotators
                
                if not selected_annotators:
                    st.warning("Please select at least one annotator to review their work.")
                    # Still allow proceeding, but with empty selection
            else:
                st.info("No annotators have submitted answers for this project yet.")
                # Initialize empty selection so the rest doesn't break
                st.session_state.selected_annotators = []
        except Exception as e:
            st.error(f"Error loading annotators: {str(e)}")
            st.session_state.selected_annotators = []
    
    # Get videos using service layer
    videos = get_project_videos(project_id, session)
    
    if not videos:
        st.error("No videos found in this project.")
        return
    
    # Video grid controls
    st.markdown("### 🎛️ Video Layout Settings")
    col1, col2 = st.columns(2)
    with col1:
        video_pairs_per_row = st.slider("Video-Answer pairs per row", 1, 2, 1, key=f"{role}_pairs_per_row")
    with col2:
        videos_per_page = st.slider("Videos per page", video_pairs_per_row, min(10, len(videos)), min(4, len(videos)), key=f"{role}_per_page")
    
    # Pagination
    total_pages = (len(videos) - 1) // videos_per_page + 1
    if total_pages > 1:
        page = st.selectbox("Page", list(range(1, total_pages + 1)), key=f"{role}_page") - 1
    else:
        page = 0
    
    start_idx = page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(videos))
    page_videos = videos[start_idx:end_idx]
    
    st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(videos)}**")
    
    # Display videos in new side-by-side layout
    for i in range(0, len(page_videos), video_pairs_per_row):
        row_videos = page_videos[i:i + video_pairs_per_row]
        
        # Create columns for each video-answer pair
        if video_pairs_per_row == 1:
            # Single pair takes full width
            display_video_answer_pair(
                row_videos[0], project_id, user_id, role, mode, session
            )
        else:
            # Multiple pairs in a row
            for video in row_videos:
                display_video_answer_pair(
                    video, project_id, user_id, role, mode, session
                )
        
        # Add some spacing between rows
        st.markdown("---")

def display_video_answer_pair(
    video: Dict,
    project_id: int,
    user_id: int,
    role: str,
    mode: str,
    session: Session
):
    """Display a single video-answer pair in side-by-side layout"""
    
    # Create two columns: video (left) and answers (right)
    video_col, answer_col = st.columns([1, 1])
    
    with video_col:
        # Centered video title
        st.markdown(f"<h4 style='text-align: center;'>📹 {video['uid']}</h4>", unsafe_allow_html=True)
        
        # Video player - get the height for matching
        video_height = custom_video_player(video["url"])
    
    with answer_col:
        # Centered answer forms title
        st.markdown(f"<h4 style='text-align: center;'>📋 Questions for {video['uid']}</h4>", unsafe_allow_html=True)
        
        # Container with fixed height matching the video
        with st.container(height=video_height, border=True):
            try:
                # Get project and its question groups using service layer
                project = ProjectService.get_project_by_id(project_id, session)
                question_groups = get_schema_question_groups(project.schema_id, session)
                
                # Display all question groups in the scrollable container
                for group in question_groups:
                    st.markdown(f"**📋 {group['Title']}**")
                    display_question_group_for_video(
                        video, project_id, user_id, group["ID"], role, mode, session
                    )
                    st.markdown("---")  # Separator between groups
                    
            except ValueError as e:
                st.error(f"Error loading project data: {str(e)}")

def _get_question_display_data(
    video_id: int, 
    project_id: int, 
    user_id: int, 
    group_id: int, 
    role: str, 
    mode: str, 
    session: Session
) -> Dict:
    """Get all the data needed to display a question group"""
    
    # Get questions in group using service layer
    questions = QuestionService.get_questions_by_group_id(group_id, session)
    
    if not questions:
        return {"questions": [], "error": "No questions in this group."}
    
    # Check admin modification status using service layer
    all_questions_modified_by_admin = GroundTruthService.check_all_questions_modified_by_admin(
        video_id, project_id, group_id, session
    )
    
    # Get existing answers using service layer
    if role == "annotator":
        existing_answers = AnnotatorService.get_user_answers_for_question_group(
            video_id, project_id, user_id, group_id, session
        )
    else:  # reviewer
        # Get existing ground truth using service layer
        existing_answers = GroundTruthService.get_ground_truth_for_question_group(
            video_id, project_id, group_id, session
        )
    
    # Determine if form should be disabled for annotators
    form_disabled = False
    if role == "annotator":
        # Check if user already submitted answers in training mode using service layer
        form_disabled = (mode == "Training" and 
                       AnnotatorService.check_user_has_submitted_answers(
                           video_id, project_id, user_id, group_id, session
                       ))
    
    return {
        "questions": questions,
        "all_questions_modified_by_admin": all_questions_modified_by_admin,
        "existing_answers": existing_answers,
        "form_disabled": form_disabled,
        "error": None
    }

def _display_single_choice_question(
    question: Dict,
    video_id: int,
    project_id: int,
    role: str,
    existing_value: str,
    is_modified_by_admin: bool,
    admin_info: Optional[Dict],
    form_disabled: bool,
    session: Session
) -> str:
    """Display a single choice question and return the selected answer"""
    
    question_id = question["id"]
    question_text = question["text"]
    options = question["options"]
    display_values = question.get("display_values", options)
    
    # Create mapping from display to actual values
    display_to_value = dict(zip(display_values, options))
    value_to_display = dict(zip(options, display_values))
    
    default_idx = 0
    if existing_value and existing_value in value_to_display:
        display_val = value_to_display[existing_value]
        if display_val in display_values:
            default_idx = display_values.index(display_val)
    
    # Question label - clean without unnecessary prefixes
    if role == "reviewer" and is_modified_by_admin:
        label = f"🔒 Admin Modified: {question_text}"
    else:
        label = question_text
    
    if role == "reviewer" and is_modified_by_admin and admin_info:
        # Show read-only version with admin modification info
        st.markdown(f"**{label}**")
        
        original_value = admin_info["original_value"]
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        # Show original and current values
        original_display = value_to_display.get(original_value, original_value)
        current_display = value_to_display.get(current_value, current_value)
        
        st.warning(f"**Original Answer:** {original_display}")
        st.error(f"**Admin Override by {admin_name}:** {current_display}")
        
        return current_value
        
    elif role == "reviewer":
        # Editable version for reviewers with enhanced options
        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id, project_id, question_id, options, display_values, session
        )
        
        selected_idx = st.radio(
            label,
            options=range(len(enhanced_options)),
            format_func=lambda x: enhanced_options[x],
            index=default_idx,
            key=f"q_{video_id}_{question_id}_{role}",
            disabled=form_disabled
        )
        return options[selected_idx]
        
    else:
        # Regular display for annotators
        selected_display = st.radio(
            label,
            options=display_values,
            index=default_idx,
            key=f"q_{video_id}_{question_id}_{role}",
            disabled=form_disabled
        )
        return display_to_value[selected_display]

def _get_enhanced_options_for_reviewer(
    video_id: int,
    project_id: int, 
    question_id: int,
    options: List[str],
    display_values: List[str],
    session: Session
) -> List[str]:
    """Get enhanced options showing who selected what for reviewers"""
    
    # Get enhanced options showing who selected what using service layer
    selected_annotators = st.session_state.get("selected_annotators", [])
    annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
        selected_annotators, project_id, session
    )
    option_selections = GroundTruthService.get_question_option_selections(
        video_id, project_id, question_id, annotator_user_ids, session
    )
    
    # Create enhanced options with inline selection info
    enhanced_options = []
    for i, display_val in enumerate(display_values):
        actual_val = options[i] if i < len(options) else display_val
        option_text = display_val
        
        if actual_val in option_selections:
            selectors = []
            for selector in option_selections[actual_val]:
                if selector["type"] == "ground_truth":
                    selectors.append("🏆 Ground Truth")
                else:
                    selectors.append(f"{selector['name']} ({selector['initials']})")
            
            if selectors:
                option_text += f" — {', '.join(selectors)}"
        
        enhanced_options.append(option_text)
    
    return enhanced_options

def _display_description_question(
    question: Dict,
    video_id: int,
    project_id: int,
    role: str,
    existing_value: str,
    is_modified_by_admin: bool,
    admin_info: Optional[Dict],
    form_disabled: bool,
    session: Session
) -> str:
    """Display a description question and return the entered text"""
    
    question_id = question["id"]
    question_text = question["text"]
    
    # Question label - clean without unnecessary prefixes
    if role == "reviewer" and is_modified_by_admin:
        label = f"🔒 Admin Modified: {question_text}"
    else:
        label = question_text
    
    if role == "reviewer" and is_modified_by_admin and admin_info:
        # Show read-only version with admin modification info
        st.markdown(f"**{label}**")
        
        original_value = admin_info["original_value"]
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        st.warning(f"**Original Answer:** {original_value}")
        st.error(f"**Admin Override by {admin_name}:** {current_value}")
        
        return current_value
        
    elif role == "reviewer":
        # Editable version for reviewers
        answer = st.text_area(
            label,
            value=existing_value,
            key=f"q_{video_id}_{question_id}_{role}",
            disabled=form_disabled,
            height=100  # Smaller height to fit in scrollable container
        )
        
        # Show existing annotator answers as helper text using service layer
        _display_helper_text_answers(video_id, project_id, question_id, session)
        
        return answer
        
    else:
        # Regular text area for annotators
        return st.text_area(
            label,
            value=existing_value,
            key=f"q_{video_id}_{question_id}_{role}",
            disabled=form_disabled,
            height=100  # Smaller height to fit in scrollable container
        )

def _display_helper_text_answers(video_id: int, project_id: int, question_id: int, session: Session):
    """Display helper text showing other annotator answers for description questions"""
    
    selected_annotators = st.session_state.get("selected_annotators", [])
    annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
        selected_annotators, project_id, session
    )
    text_answers = GroundTruthService.get_question_text_answers(
        video_id, project_id, question_id, annotator_user_ids, session
    )
    
    if text_answers:
        # Remove duplicates by creating a set of unique answer texts
        unique_answers = []
        seen_answers = set()
        
        for answer in text_answers:
            answer_key = f"{answer['name']}:{answer['answer_value']}"
            if answer_key not in seen_answers:
                seen_answers.add(answer_key)
                unique_answers.append(f"**{answer['name']} ({answer['initials']}):** {answer['answer_value']}")
        
        if unique_answers:
            # Display each annotator's answer on a new line
            st.caption("Other answers:")
            for answer_text in unique_answers:
                st.caption(answer_text)

def _get_submit_button_config(
    role: str,
    form_disabled: bool,
    all_questions_modified_by_admin: bool,
    has_any_editable_questions: bool
) -> Tuple[str, bool]:
    """Get the submit button text and disabled state"""
    
    if role == "annotator":
        button_text = "🔒 Already Submitted" if form_disabled else "Submit Answers"
        button_disabled = form_disabled
    else:  # reviewer
        if all_questions_modified_by_admin:
            button_text = "🔒 All Questions Modified by Admin"
            button_disabled = True
        elif not has_any_editable_questions:
            button_text = "🔒 No Editable Questions"
            button_disabled = True
        else:
            button_text = "Submit Ground Truth"
            button_disabled = False
    
    return button_text, button_disabled

def display_question_group_for_video(
    video: Dict,
    project_id: int, 
    user_id: int,
    group_id: int,
    role: str,
    mode: str,
    session: Session
):
    """Display a single question group for a video - modularized and clean"""
    
    try:
        # Get all display data
        display_data = _get_question_display_data(
            video["id"], project_id, user_id, group_id, role, mode, session
        )
        
        if display_data["error"]:
            st.info(display_data["error"])
            return
        
        questions = display_data["questions"]
        all_questions_modified_by_admin = display_data["all_questions_modified_by_admin"]
        existing_answers = display_data["existing_answers"]
        form_disabled = display_data["form_disabled"]
        
        # Create form
        form_key = f"form_{video['id']}_{group_id}_{role}"
        with st.form(form_key):
            answers = {}
            has_any_editable_questions = False
            
            for question in questions:
                question_id = question["id"]
                question_text = question["text"]
                existing_value = existing_answers.get(question_text, "")
                
                # Check if this specific question has been modified by admin
                is_modified_by_admin = False
                admin_info = None
                if role == "reviewer":
                    is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                        video["id"], project_id, question_id, session
                    )
                    if is_modified_by_admin:
                        admin_info = GroundTruthService.get_admin_modification_details(
                            video["id"], project_id, question_id, session
                        )
                    else:
                        has_any_editable_questions = True
                
                # Display question based on type
                if question["type"] == "single":
                    answers[question_text] = _display_single_choice_question(
                        question, video["id"], project_id, role, existing_value,
                        is_modified_by_admin, admin_info, form_disabled, session
                    )
                else:  # description
                    answers[question_text] = _display_description_question(
                        question, video["id"], project_id, role, existing_value,
                        is_modified_by_admin, admin_info, form_disabled, session
                    )
            
            # Submit button
            button_text, button_disabled = _get_submit_button_config(
                role, form_disabled, all_questions_modified_by_admin, has_any_editable_questions
            )
            
            submitted = st.form_submit_button(
                button_text, 
                use_container_width=True,
                disabled=button_disabled
            )
            
            if submitted and not button_disabled:
                try:
                    if role == "annotator":
                        # Submit annotator answers using service layer
                        AnnotatorService.submit_answer_to_question_group(
                            video_id=video["id"],
                            project_id=project_id,
                            user_id=user_id,
                            question_group_id=group_id,
                            answers=answers,
                            session=session
                        )
                        
                        # Only show feedback in training mode
                        if mode == "Training":
                            show_training_feedback(video["id"], project_id, group_id, answers, session)
                        else:
                            st.success("✅ Answers submitted!")
                        
                    else:  # reviewer
                        # Filter out admin-modified questions from answers
                        editable_answers = {}
                        for question in questions:
                            question_text = question["text"]
                            if not GroundTruthService.check_question_modified_by_admin(
                                video["id"], project_id, question["id"], session
                            ):
                                editable_answers[question_text] = answers[question_text]
                        
                        if editable_answers:
                            # Submit ground truth for editable questions only using service layer
                            GroundTruthService.submit_ground_truth_to_question_group(
                                video_id=video["id"],
                                project_id=project_id,
                                reviewer_id=user_id,
                                question_group_id=group_id,
                                answers=editable_answers,
                                session=session
                            )
                            st.success("✅ Ground truth submitted!")
                        else:
                            st.warning("No editable questions to submit.")
                    
                    st.rerun()
                    
                except ValueError as e:
                    st.error(f"Error: {str(e)}")
                    
    except ValueError as e:
        st.error(f"Error loading question group: {str(e)}")

###############################################################################
# ANNOTATOR PORTAL
###############################################################################

@handle_database_errors
def annotator_portal():
    st.title("👥 Annotator Portal")
    user = st.session_state.user
    
    # Check current view state
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            # Display project dashboard
            display_project_dashboard(user["id"], "annotator", session)
        elif current_view == "project":
            # Display project view
            display_project_view(user["id"], "annotator", session)

###############################################################################
# REVIEWER PORTAL
###############################################################################

@handle_database_errors
def reviewer_portal():
    st.title("🔍 Reviewer Portal")
    user = st.session_state.user
    
    # Check current view state
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            # Display project dashboard
            display_project_dashboard(user["id"], "reviewer", session)
        elif current_view == "project":
            # Display project view
            display_project_view(user["id"], "reviewer", session)

###############################################################################
# ADMIN PORTAL  
###############################################################################

@handle_database_errors
def admin_portal():
    st.title("⚙️ Admin Portal")
    
    # Admin tabs
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

def admin_videos():
    st.subheader("📹 Video Management")
    
    with get_db_session() as session:
        videos_df = VideoService.get_videos_with_project_status(session)
        
        if not videos_df.empty:
            st.dataframe(videos_df, use_container_width=True)
        else:
            st.info("No videos in the database yet.")
        
        with st.expander("➕ Add Video"):
            url = st.text_input("Video URL")
            metadata_json = st.text_area("Metadata (JSON, optional)", "{}")
            
            if st.button("Add Video"):
                if url:
                    try:
                        import json
                        metadata = json.loads(metadata_json) if metadata_json.strip() else {}
                        VideoService.add_video(url, session, metadata)
                        st.success("Video added!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

def admin_projects():
    st.subheader("📁 Project Management")
    
    with get_db_session() as session:
        projects_df = ProjectService.get_all_projects(session)
        
        # Enhanced project display
        if not projects_df.empty:
            enhanced_projects = []
            for _, project in projects_df.iterrows():
                try:
                    progress = ProjectService.progress(project["ID"], session)
                    has_full_gt = check_project_has_full_ground_truth(project["ID"], session)
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
            name = st.text_input("Project Name")
            description = st.text_area("Description")
            
            schemas_df = SchemaService.get_all_schemas(session)
            if schemas_df.empty:
                st.warning("No schemas available.")
                return
                
            schema_name = st.selectbox("Schema", schemas_df["Name"])
            
            videos_df = VideoService.get_all_videos(session)
            if videos_df.empty:
                st.warning("No videos available.")
                return
                
            selected_videos = st.multiselect("Videos", videos_df["Video UID"])
            
            if st.button("Create Project"):
                if name and schema_name:
                    try:
                        schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
                        video_ids = ProjectService.get_video_ids_by_uids(selected_videos, session)
                        ProjectService.create_project(name, schema_id, video_ids, session)
                        st.success("Project created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

def admin_schemas():
    st.subheader("📋 Schema Management")
    
    with get_db_session() as session:
        schemas_df = SchemaService.get_all_schemas(session)
        st.dataframe(schemas_df, use_container_width=True)
        
        with st.expander("➕ Create Schema"):
            schema_name = st.text_input("Schema Name")
            
            groups_df = QuestionGroupService.get_all_groups(session)
            if groups_df.empty:
                st.warning("No question groups available.")
                return
                
            available_groups = groups_df[~groups_df["Archived"]]
            selected_groups = st.multiselect(
                "Question Groups", 
                available_groups["ID"].tolist(),
                format_func=lambda x: available_groups[available_groups["ID"]==x]["Name"].iloc[0]
            )
            
            if st.button("Create Schema"):
                if schema_name and selected_groups:
                    try:
                        SchemaService.create_schema(schema_name, selected_groups, session)
                        st.success("Schema created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

def admin_questions():
    st.subheader("❓ Question & Group Management")
    
    with get_db_session() as session:
        q_tab1, q_tab2 = st.tabs(["Question Groups", "Individual Questions"])
        
        with q_tab1:
            groups_df = QuestionGroupService.get_all_groups(session)
            st.dataframe(groups_df, use_container_width=True)
            
            with st.expander("➕ Create Question Group"):
                title = st.text_input("Group Title")
                description = st.text_area("Description")
                is_reusable = st.checkbox("Reusable across schemas")
                
                questions_df = QuestionService.get_all_questions(session)
                if not questions_df.empty:
                    available_questions = questions_df[~questions_df["Archived"]]
                    selected_questions = st.multiselect(
                        "Questions",
                        available_questions["ID"].tolist(),
                        format_func=lambda x: available_questions[available_questions["ID"]==x]["Text"].iloc[0]
                    )
                else:
                    selected_questions = []
                    st.warning("No questions available.")
                
                if st.button("Create Group"):
                    if title and selected_questions:
                        try:
                            QuestionGroupService.create_group(
                                title, description, is_reusable, 
                                selected_questions, None, session
                            )
                            st.success("Question group created!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
        
        with q_tab2:
            questions_df = QuestionService.get_all_questions(session)
            st.dataframe(questions_df, use_container_width=True)
            
            with st.expander("➕ Create Question"):
                text = st.text_input("Question Text")
                q_type = st.selectbox("Type", ["single", "description"])
                
                options = []
                default = None
                
                if q_type == "single":
                    st.write("**Options:**")
                    num_options = st.number_input("Number of options", 1, 10, 2)
                    
                    for i in range(num_options):
                        option = st.text_input(f"Option {i+1}", key=f"opt_{i}")
                        if option:
                            options.append(option)
                    
                    if options:
                        default = st.selectbox("Default option", [""] + options)
                
                if st.button("Create Question"):
                    if text:
                        try:
                            QuestionService.add_question(
                                text, q_type, 
                                options if q_type == "single" else None,
                                default if q_type == "single" else None,
                                session
                            )
                            st.success("Question created!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")

def admin_users():
    st.subheader("👥 User Management")
    
    with get_db_session() as session:
        users_df = AuthService.get_all_users(session)
        st.dataframe(users_df, use_container_width=True)
        
        with st.expander("➕ Create User"):
            user_id = st.text_input("User ID")
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            user_type = st.selectbox("User Type", ["human", "model", "admin"])
            
            if st.button("Create User"):
                if user_id and email and password:
                    try:
                        AuthService.create_user(user_id, email, password, user_type, session)
                        st.success("User created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

def admin_assignments():
    st.subheader("🔗 Project Assignments")
    
    with get_db_session() as session:
        assignments_df = AuthService.get_project_assignments(session)
        st.dataframe(assignments_df, use_container_width=True)
        
        with st.expander("➕ Manage Assignments"):
            projects_df = ProjectService.get_all_projects(session)
            if projects_df.empty:
                st.warning("No projects available.")
                return
                
            project_id = st.selectbox(
                "Project",
                projects_df["ID"].tolist(),
                format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0]
            )
            
            users_df = AuthService.get_all_users(session)
            if users_df.empty:
                st.warning("No users available.")
                return
                
            user_ids = st.multiselect(
                "Users",
                users_df["ID"].tolist(),
                format_func=lambda x: f"{users_df[users_df['ID']==x]['User ID'].iloc[0]} ({users_df[users_df['ID']==x]['Email'].iloc[0]})"
            )
            
            role = st.selectbox("Role", ["annotator", "reviewer", "admin", "model"])
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Assign Users"):
                    if project_id and user_ids:
                        try:
                            for user_id in user_ids:
                                ProjectService.add_user_to_project(user_id, project_id, role, session)
                            st.success(f"Assigned {len(user_ids)} users!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
            
            with col2:
                if st.button("Remove Users"):
                    if project_id and user_ids:
                        try:
                            for user_id in user_ids:
                                AuthService.archive_user_from_project(user_id, project_id, session)
                            st.success(f"Removed {len(user_ids)} users!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")

def admin_project_groups():
    st.subheader("📊 Project Group Management")
    
    with get_db_session() as session:
        # List existing project groups
        try:
            groups = ProjectGroupService.list_project_groups(session)
            if groups:
                group_data = []
                for group in groups:
                    try:
                        group_info = ProjectGroupService.get_project_group_by_id(group.id, session)
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
        
        # Create new project group
        with st.expander("➕ Create Project Group"):
            group_name = st.text_input("Group Name")
            group_description = st.text_area("Description")
            
            projects_df = ProjectService.get_all_projects(session)
            if not projects_df.empty:
                selected_projects = st.multiselect(
                    "Projects (optional)",
                    projects_df["ID"].tolist(),
                    format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0]
                )
            else:
                selected_projects = []
                st.warning("No projects available to add to group.")
            
            if st.button("Create Project Group"):
                if group_name:
                    try:
                        ProjectGroupService.create_project_group(
                            group_name, group_description, selected_projects, session
                        )
                        st.success("Project group created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

###############################################################################
# HELPER FUNCTIONS - ALL USING SERVICE LAYER
###############################################################################

def get_schema_question_groups(schema_id: int, session: Session) -> List[Dict]:
    """Get question groups in a schema - using service layer"""
    try:
        return SchemaService.get_schema_question_groups_list(schema_id, session)
    except ValueError as e:
        st.error(f"Error loading schema question groups: {str(e)}")
        return []

def get_project_videos(project_id: int, session: Session) -> List[Dict]:
    """Get videos in a project - using service layer"""
    try:
        return VideoService.get_project_videos(project_id, session)
    except ValueError as e:
        st.error(f"Error getting project videos: {str(e)}")
        return []

def calculate_user_overall_progress(user_id: int, project_id: int, session: Session) -> float:
    """Calculate user's overall progress - using service layer"""
    try:
        return AnnotatorService.calculate_user_overall_progress(user_id, project_id, session)
    except ValueError as e:
        st.error(f"Error calculating user progress: {str(e)}")
        return 0.0

def show_training_feedback(video_id: int, project_id: int, group_id: int, 
                          user_answers: Dict[str, str], session: Session):
    """Show training feedback comparing user answers to ground truth - using service layer"""
    
    try:
        gt_df = GroundTruthService.get_ground_truth(video_id, project_id, session)
        questions = QuestionService.get_questions_by_group_id(group_id, session)
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
        
        # Show basic status info
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
    
    # Custom CSS - simplified
    st.markdown("""
        <style>
        .stProgress > div > div > div > div {
            background-color: #4CAF50;
        }
        .stForm {
            border: 1px solid #e0e0e0;
            border-radius: 0.5rem;
            padding: 1rem;
            margin-bottom: 1rem;
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
    
    # Sidebar
    with st.sidebar:
        # User info
        st.markdown("### 👋 Welcome!")
        
        # Show user info simply
        display_user_simple(user['name'], user.get('email', 'No email'), is_ground_truth=(user['role'] == 'admin'))
        st.markdown(f"**Role:** {user['role'].title()}")
        
        # Portal selection
        if available_portals:
            st.markdown("---")
            st.markdown("**Select Portal:**")
            
            portal_labels = {
                "annotator": "👥 Annotator Portal",
                "reviewer": "🔍 Reviewer Portal", 
                "admin": "⚙️ Admin Portal"
            }
            
            # Default portal selection
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
                # Reset view when switching portals
                st.session_state.current_view = "dashboard"
                st.session_state.selected_project_id = None
                # Clear annotator selection when switching portals
                if "selected_annotators" in st.session_state:
                    del st.session_state.selected_annotators
                st.rerun()
            
            current_portal_name = portal_labels.get(selected_portal, selected_portal)
            st.success(f"**Active:** {current_portal_name}")
        
        # Logout button
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        
        # Database health indicator for admins
        if user.get("role") == "admin":
            with st.expander("🔧 Database Status"):
                show_database_health()
    
    # Route to selected portal
    if not available_portals:
        st.warning("No portals available. You may not be assigned to any projects or your account may not have the necessary permissions. Please contact an administrator.")
        return
    
    selected_portal = st.session_state.get("selected_portal", available_portals[0])
    
    if selected_portal == "admin":
        admin_portal()
    elif selected_portal == "reviewer":
        reviewer_portal()
    elif selected_portal == "annotator":
        annotator_portal()
    else:
        st.error(f"Unknown portal: {selected_portal}")

if __name__ == "__main__":
    main()