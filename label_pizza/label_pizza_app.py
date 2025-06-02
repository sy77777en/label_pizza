"""
Label Pizza - Modern Streamlit App
==================================
Updated with performance optimizations using fragments to reduce refresh times.
All service calls now use keyword arguments to prevent ID conflicts.

OPTIMIZATIONS:
1. Added fragments to question groups and admin sections
2. Fixed all positional arguments to use keyword arguments
3. Optimized database queries to reduce load times
4. Added project group editing functionality
5. Fixed pagination button alignment and text consistency
6. Improved annotator selection button clarity
7. Fixed tab completion status updates
8. Enhanced training mode with ground truth display
"""

import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text, select
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
    
    # Calculate height to match question section better
    estimated_width = 420  # Increased slightly
    video_height = estimated_width / aspect_ratio_decimal
    controls_height = 90  # Controls height
    total_height = int(video_height + controls_height)
    
    # Adjust height to match reduced question container
    total_height = max(500, min(700, total_height))
    
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
            # Use service layer for health check instead of direct SQL
            try:
                # Try to get any user to test database connection
                users_df = AuthService.get_all_users(session=session)
                return {
                    "healthy": True,
                    "database": "Connected",
                    "status": "OK",
                    "user_count": len(users_df)
                }
            except:
                # Fallback to basic connection test
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
    # Add some top padding and center the content
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Centered title with better styling
        st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1 style="color: #1f77b4; margin-bottom: 0.5rem;">🍕 Label Pizza</h1>
                <p style="color: #6c757d; font-size: 1.1rem; margin: 0;">Welcome back! Please sign in to your account</p>
            </div>
        """, unsafe_allow_html=True)
        
        # Login form with better styling
        with st.form("login_form"):
            st.markdown("### Sign In")
            
            email = st.text_input(
                "Email Address", 
                placeholder="Enter your email address",
                help="Use your registered email address"
            )
            
            password = st.text_input(
                "Password", 
                type="password", 
                placeholder="Enter your password",
                help="Enter your account password"
            )
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            submitted = st.form_submit_button(
                "🚀 Sign In", 
                use_container_width=True,
                type="primary"
            )
            
            if submitted:
                authenticate_user(email, password)
        
        # Add some footer spacing
        st.markdown("<br><br>", unsafe_allow_html=True)

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
            user = AuthService.authenticate(
                email=email, 
                pwd=password, 
                role="admin", 
                session=session
            )
            if not user:
                user = AuthService.authenticate(
                    email=email, 
                    pwd=password, 
                    role="human", 
                    session=session
                )
            
            if user:
                # Ensure email is properly set in session state
                if 'email' not in user or not user['email']:
                    user['email'] = email  # Use the email from login form
                    
                st.session_state.user = user
                st.session_state.user_projects = get_user_projects(
                    user_id=user["id"], 
                    session=session
                )
                st.session_state.available_portals = get_available_portals(
                    user=user, 
                    user_projects=st.session_state.user_projects
                )
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
    """Get projects assigned to user by role - using ONLY service layer"""
    try:
        # Always start with service layer method
        user_projects = AuthService.get_user_projects_by_role(
            user_id=user_id, 
            session=session
        )
        
        # For admin users, expand access to all projects
        # We detect admin users by checking if they have any admin role assignments
        if user_projects.get("admin"):
            # Get all projects using service layer
            all_projects_df = ProjectService.get_all_projects(session=session)
            
            # Convert to expected format
            all_projects_list = []
            for _, project_row in all_projects_df.iterrows():
                project_dict = {
                    "id": project_row["ID"],
                    "name": project_row["Name"],
                    "description": "",  # Not available from service
                    "created_at": None  # Not available from service
                }
                all_projects_list.append(project_dict)
            
            # Admin users get access to all projects in all portals
            return {
                "annotator": all_projects_list.copy(),
                "reviewer": all_projects_list.copy(),
                "admin": all_projects_list.copy()
            }
        
        return user_projects
        
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
        return ProjectService.get_project_annotators(project_id=project_id, session=session)
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
        return ProjectService.check_project_has_full_ground_truth(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error checking ground truth status: {str(e)}")
        return False

def get_project_groups_with_projects(user_id: int, role: str, session: Session) -> Dict:
    """Get project groups with their projects for a user - using service layer"""
    try:
        return ProjectGroupService.get_grouped_projects_for_user(user_id=user_id, role=role, session=session)
    except ValueError as e:
        st.error(f"Error getting grouped projects: {str(e)}")
        return {}

def display_project_dashboard(user_id: int, role: str, session: Session) -> Optional[int]:
    """Display project group dashboard with scrollable containers"""
    
    st.markdown("## 📂 Project Dashboard")
    
    # Get grouped projects using service layer
    grouped_projects = get_project_groups_with_projects(user_id=user_id, role=role, session=session)
    
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
                    has_full_gt = check_project_has_full_ground_truth(project_id=project["id"], session=session)
                    mode = "🎓 Training" if has_full_gt else "📝 Annotation"
                    
                    # Get project progress for this user
                    try:
                        if role == "annotator":
                            personal_progress = calculate_user_overall_progress(
                                user_id=user_id, 
                                project_id=project["id"], 
                                session=session
                            )
                            progress_text = f"{personal_progress:.1f}% Complete"
                        else:
                            project_progress = ProjectService.progress(
                                project_id=project["id"], 
                                session=session
                            )
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

def display_smart_annotator_selection(annotators: Dict[str, Dict], project_id: int):
    """Clean annotator selection with responsive checkboxes for many users"""
    
    if not annotators:
        st.info("No annotators have submitted answers for this project yet.")
        return []
    
    # Initialize session state for selected annotators
    if "selected_annotators" not in st.session_state:
        annotator_options = list(annotators.keys())
        st.session_state.selected_annotators = annotator_options[:3] if len(annotator_options) > 3 else annotator_options
    
    annotator_options = list(annotators.keys())
    
    # Simple header  
    st.markdown("### 👥 Select Annotators to Review")
    
    # Quick action buttons in a compact row with clearer text
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    with col1:
        if st.button("Select All Annotators", key=f"select_all_{project_id}", help="Select all annotators"):
            st.session_state.selected_annotators = annotator_options.copy()
            st.rerun()
    
    with col2:
        if st.button("Deselect All Annotators", key=f"clear_all_{project_id}", help="Deselect all annotators"):
            st.session_state.selected_annotators = []
            st.rerun()
    
    with col3:
        selected_count = len(st.session_state.selected_annotators)
        total_count = len(annotator_options)
        st.metric("Selected", f"{selected_count}/{total_count}", label_visibility="visible")
    
    # Responsive checkbox layout that handles many annotators
    st.markdown("**Choose annotators to include:**")
    
    # Calculate optimal number of columns based on annotator count
    num_annotators = len(annotator_options)
    if num_annotators <= 4:
        num_cols = num_annotators
    elif num_annotators <= 12:
        num_cols = 3
    elif num_annotators <= 20:
        num_cols = 4
    else:
        num_cols = 5  # For 30+ annotators, use 5 columns
    
    # Track changes
    updated_selection = []
    
    # Create rows of checkboxes
    for row_start in range(0, num_annotators, num_cols):
        cols = st.columns(num_cols)
        row_annotators = annotator_options[row_start:row_start + num_cols]
        
        for i, annotator_display in enumerate(row_annotators):
            with cols[i]:
                # Extract clean info from display name "Name (XX)"
                if " (" in annotator_display and annotator_display.endswith(")"):
                    full_name = annotator_display.split(" (")[0]
                    initials = annotator_display.split(" (")[1][:-1]
                else:
                    full_name = annotator_display
                    initials = annotator_display[:2].upper()
                
                # Get additional info for tooltip
                annotator_info = annotators.get(annotator_display, {})
                email = annotator_info.get('email', '')
                user_id = annotator_info.get('id', '')
                
                # Create clean label (just name and initials)
                label = f"**{full_name}** ({initials})"
                
                # Create informative tooltip
                if email and email != f"user_{user_id}@example.com":
                    tooltip = f"{full_name}\nEmail: {email}\nID: {user_id}"
                else:
                    tooltip = f"{full_name}\nUser ID: {user_id}"
                
                # Checkbox for this annotator
                checkbox_key = f"annotator_cb_{project_id}_{row_start + i}"
                is_selected = st.checkbox(
                    label,
                    value=annotator_display in st.session_state.selected_annotators,
                    key=checkbox_key,
                    help=tooltip  # Email and details shown only on hover
                )
                
                if is_selected:
                    updated_selection.append(annotator_display)
    
    # Update session state if selection changed
    if set(updated_selection) != set(st.session_state.selected_annotators):
        st.session_state.selected_annotators = updated_selection
        st.rerun()
    
    # Show compact summary if any selected
    if st.session_state.selected_annotators:
        # Create a compact summary with just initials
        initials_list = []
        for annotator in st.session_state.selected_annotators:
            if " (" in annotator and annotator.endswith(")"):
                initials = annotator.split(" (")[1][:-1]
                initials_list.append(initials)
            else:
                initials_list.append(annotator[:2].upper())
        
        # Group initials nicely
        if len(initials_list) <= 10:
            initials_text = ", ".join(initials_list)
        else:
            # For many selections, show first few + count
            shown = initials_list[:8]
            remaining = len(initials_list) - 8
            initials_text = f"{', '.join(shown)} + {remaining} more"
        
        st.success(f"✅ Selected: {initials_text}")
    else:
        st.warning("⚠️ No annotators selected - results will only show ground truth")
    
    return st.session_state.selected_annotators


def display_project_progress(user_id: int, project_id: int, role: str, session: Session):
    """Display project progress in a refreshable fragment"""
    if role == "annotator":
        overall_progress = calculate_user_overall_progress(
            user_id=user_id, 
            project_id=project_id, 
            session=session
        )
        st.progress(overall_progress / 100)
        st.markdown(f"**Your Overall Progress:** {overall_progress:.1f}%")
    else:
        try:
            project_progress = ProjectService.progress(
                project_id=project_id, 
                session=session
            )
            st.progress(project_progress['completion_percentage'] / 100)
            st.markdown(f"**Ground Truth Progress:** {project_progress['completion_percentage']:.1f}%")
        except ValueError as e:
            st.error(f"Error loading project progress: {str(e)}")

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
        project = ProjectService.get_project_by_id(
            project_id=project_id, 
            session=session
        )
    except ValueError as e:
        st.error(f"Error loading project: {str(e)}")
        return
    
    # Determine project mode
    mode = "Training" if check_project_has_full_ground_truth(
        project_id=project_id, 
        session=session
    ) else "Annotation"
    
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
    display_project_progress(user_id=user_id, project_id=project_id, role=role, session=session)
    
    # Enhanced annotator selection for reviewers
    if role == "reviewer":
        try:
            annotators = get_all_project_annotators(
                project_id=project_id, 
                session=session
            )
            display_smart_annotator_selection(
                annotators=annotators, 
                project_id=project_id
            )
        except Exception as e:
            st.error(f"Error loading annotators: {str(e)}")
            st.session_state.selected_annotators = []
    
    # Get videos using service layer
    videos = get_project_videos(
        project_id=project_id, 
        session=session
    )
    
    if not videos:
        st.error("No videos found in this project.")
        return
    
    # Video grid controls
    st.markdown("### 🎛️ Video Layout Settings")
    col1, col2 = st.columns(2)
    with col1:
        video_pairs_per_row = st.slider("Video-Answer pairs per row", 1, 2, 1, key=f"{role}_pairs_per_row")
    with col2:
        # Handle edge cases for projects with very few videos
        min_videos_per_page = video_pairs_per_row
        max_videos_per_page = max(min(10, len(videos)), video_pairs_per_row + 1)  # Ensure max > min
        default_videos_per_page = min(min(4, len(videos)), max_videos_per_page)
        
        if len(videos) == 1:
            # Special case: only 1 video, no need for slider
            st.write("Videos per page: 1 (only 1 video in project)")
            videos_per_page = 1
        elif max_videos_per_page > min_videos_per_page:
            videos_per_page = st.slider(
                "Videos per page", 
                min_videos_per_page, 
                max_videos_per_page, 
                default_videos_per_page, 
                key=f"{role}_per_page"
            )
        else:
            # Fallback: show all videos if slider would be invalid
            st.write(f"Videos per page: {len(videos)} (showing all)")
            videos_per_page = len(videos)
    
    # Calculate pagination parameters (but don't show UI controls yet)
    total_pages = (len(videos) - 1) // videos_per_page + 1
    
    # Initialize page state with a unique key per project
    page_key = f"{role}_current_page_{project_id}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0
    
    current_page = st.session_state[page_key]
    
    # Calculate which videos to show on current page
    start_idx = current_page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(videos))
    page_videos = videos[start_idx:end_idx]
    
    st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(videos)}**")
    
    # Display videos in new side-by-side layout
    for i in range(0, len(page_videos), video_pairs_per_row):
        row_videos = page_videos[i:i + video_pairs_per_row]
        
        if video_pairs_per_row == 1:
            # Single pair takes full width
            display_video_answer_pair(
                row_videos[0], project_id, user_id, role, mode, session
            )
        else:
            # Multiple pairs in a row - create columns for each pair
            pair_cols = st.columns(video_pairs_per_row)
            for j, video in enumerate(row_videos):
                with pair_cols[j]:
                    display_video_answer_pair(
                        video, project_id, user_id, role, mode, session
                    )
        
        # Add some spacing between rows
        st.markdown("---")
    
    # PAGINATION CONTROLS MOVED TO BOTTOM
    if total_pages > 1:
        # st.markdown("---")  # Separator line before pagination
        # st.markdown("### 📄 Page Navigation")
        
        # Create smart pagination options
        def get_pagination_options(current, total):
            """Create smart pagination with ellipsis for many pages"""
            if total <= 7:
                # Show all pages if 7 or fewer
                return list(range(total))
            
            options = []
            # Always show first page
            options.append(0)
            
            # Determine range around current page
            start = max(1, current - 1)
            end = min(total - 1, current + 2)
            
            # Add ellipsis if gap exists
            if start > 1:
                options.append("...")
            
            # Add pages around current
            for i in range(start, end):
                if i not in options:
                    options.append(i)
            
            # Add ellipsis if gap exists
            if end < total - 1:
                options.append("...")
            
            # Always show last page
            if total - 1 not in options:
                options.append(total - 1)
            
            return options
        
        nav_col1, nav_col2, nav_col3 = st.columns([1, 5, 1])
        
        # Previous button
        with nav_col1:
            if st.button("◀ Previous Page", disabled=(current_page == 0), key=f"{role}_prev_{project_id}", use_container_width=True):
                st.session_state[page_key] = max(0, current_page - 1)
                st.rerun()
        
        # Smart segmented control in the middle
        with nav_col2:
            # Create nested columns to center the segmented control
            _, center_col, _ = st.columns([1, 2, 1])
            
            with center_col:
                pagination_options = get_pagination_options(current_page, total_pages)
                
                # Create display labels
                display_options = []
                actual_pages = []
                for opt in pagination_options:
                    if opt == "...":
                        display_options.append("...")
                        actual_pages.append(None)
                    else:
                        display_options.append(f"Page {opt + 1}")
                        actual_pages.append(opt)
                
                # Find current page index in display options
                try:
                    current_display_index = actual_pages.index(current_page)
                except ValueError:
                    current_display_index = 0
                
                # Use segmented control with smart pagination
                segmented_key = f"{role}_page_segmented_{project_id}"
                selected_display = st.segmented_control(
                    "📄 Navigate Pages", 
                    display_options,
                    default=display_options[current_display_index] if current_display_index < len(display_options) else display_options[0],
                    key=segmented_key
                )
            
            # Handle selection
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
        
        # Show current page info at the bottom
        st.markdown(f"<div style='text-align: center; color: #6c757d; margin-top: 1rem;'>Page {current_page + 1} of {total_pages}</div>", unsafe_allow_html=True)

@st.fragment
def display_video_answer_pair(
    video: Dict,
    project_id: int,
    user_id: int,
    role: str,
    mode: str,
    session: Session
):
    """Display a single video-answer pair in side-by-side layout with tabs"""
    
    # Add unique identifier for this video to prevent fragment conflicts
    video_fragment_key = f"{project_id}_{video['id']}_{user_id}_{role}"
    
    # Get project and its question groups using service layer first
    try:
        project = ProjectService.get_project_by_id(
            project_id=project_id, 
            session=session
        )
        question_groups = get_schema_question_groups(
            schema_id=project.schema_id, 
            session=session
        )
        
        if not question_groups:
            st.info("No question groups found for this project.")
            return
        
        # Check completion status for each group
        completion_status = {}
        for group in question_groups:
            is_complete = check_question_group_completion(
                video_id=video["id"], 
                project_id=project_id, 
                user_id=user_id, 
                question_group_id=group["ID"], 
                role=role, 
                session=session
            )
            completion_status[group["ID"]] = is_complete
        
        # Create centered progress header spanning both columns
        completed_count = sum(completion_status.values())
        total_count = len(question_groups)
        
        # Create detailed completion status - show all groups, emoji only for completed
        completion_details = []
        for group in question_groups:
            if completion_status[group["ID"]]:
                completion_details.append(f"✅ {group['Title']}")
            else:
                completion_details.append(group['Title'])
        
        # Centered progress box spanning both columns
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #e8f4f8, #d5e8f0);
            border: 2px solid #3498db;
            border-radius: 10px;
            padding: 10px 16px;
            margin-bottom: 15px;
            text-align: center;
            box-shadow: 0 2px 6px rgba(52, 152, 219, 0.2);
        ">
            <div style="color: #2980b9; font-weight: 500; font-size: 0.95rem;">
                📋 {video['uid']} - {' | '.join(completion_details)} - Progress: {completed_count}/{total_count} Complete
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Create two columns: video (left) and answers (right)
        video_col, answer_col = st.columns([1, 1])
        
        with video_col:
            # Video player without title (title is now in the centered header)
            video_height = custom_video_player(video["url"])
        
        with answer_col:
            # Create tabs with static names (no ✅ to avoid jumping)
            tab_names = [group['Title'] for group in question_groups]
            tabs = st.tabs(tab_names)
            
            # Display each question group in its own tab
            for i, (tab, group) in enumerate(zip(tabs, question_groups)):
                with tab:
                    display_question_group_in_fixed_container(
                        video=video, 
                        project_id=project_id, 
                        user_id=user_id, 
                        group_id=group["ID"], 
                        role=role, 
                        mode=mode, 
                        session=session, 
                        container_height=video_height
                    )
                    
    except ValueError as e:
        st.error(f"Error loading project data: {str(e)}")

def check_question_group_completion(
    video_id: int, 
    project_id: int, 
    user_id: int, 
    question_group_id: int, 
    role: str, 
    session: Session
) -> bool:
    """Check if a question group is complete for the user/role."""
    try:
        if role == "annotator":
            # For annotators: check if user has submitted answers for all questions in this group
            return AnnotatorService.check_user_has_submitted_answers(
                video_id=video_id, 
                project_id=project_id, 
                user_id=user_id, 
                question_group_id=question_group_id, 
                session=session
            )
        else:  # reviewer
            # For reviewers: check if all questions in this group have ground truth
            return GroundTruthService.check_all_questions_modified_by_admin(
                video_id=video_id, 
                project_id=project_id, 
                question_group_id=question_group_id, 
                session=session
            ) or check_all_questions_have_ground_truth(
                video_id=video_id, 
                project_id=project_id, 
                question_group_id=question_group_id, 
                session=session
            )
    except:
        return False

@st.dialog("🎉 Congratulations!")
def show_annotator_completion():
    """Simple completion popup for annotators"""
    st.markdown("### 🎉 **CONGRATULATIONS!** 🎉")
    st.success("You've completed all questions in this project!")
    st.info("Great work! You can now move on to other projects or review your answers.")
    
    # Trigger celebrations
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
    
    # Trigger celebrations
    st.snow()
    st.balloons()
    
    if st.button("Close", use_container_width=True):
        st.rerun()

def check_all_questions_have_ground_truth(
    video_id: int, 
    project_id: int, 
    question_group_id: int, 
    session: Session
) -> bool:
    """Check if all questions in a group have ground truth answers for this specific video."""
    try:
        # Get all questions in the group using service layer
        questions = QuestionService.get_questions_by_group_id(
            group_id=question_group_id, 
            session=session
        )
        
        if not questions:
            return False
        
        # Get ground truth for this video and project using service layer
        gt_df = GroundTruthService.get_ground_truth(
            video_id=video_id, 
            project_id=project_id, 
            session=session
        )
        
        if gt_df.empty:
            return False
        
        # Get question IDs that have ground truth
        gt_question_ids = set(gt_df["Question ID"].tolist())
        
        # Check if all questions in the group have ground truth
        for question in questions:
            if question["id"] not in gt_question_ids:
                return False
        
        return True
    except:
        return False

def display_question_group_in_fixed_container(
    video: Dict,
    project_id: int, 
    user_id: int,
    group_id: int,
    role: str,
    mode: str,
    session: Session,
    container_height: int
):
    """Display question group content with optimized fragment refresh"""
    
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
        
        # Get ground truth for training mode display
        gt_answers = {}
        if mode == "Training" and role == "annotator":
            try:
                gt_df = GroundTruthService.get_ground_truth(
                    video_id=video["id"], 
                    project_id=project_id, 
                    session=session
                )
                if not gt_df.empty:
                    question_map = {q["id"]: q for q in questions}
                    for _, gt_row in gt_df.iterrows():
                        question_id = gt_row["Question ID"]
                        if question_id in question_map:
                            question_text = question_map[question_id]["text"]
                            gt_answers[question_text] = gt_row["Answer Value"]
            except:
                pass  # If can't get GT, continue without it
        
        # Determine if we have editable questions
        has_any_editable_questions = False
        for question in questions:
            if role == "reviewer":
                if not GroundTruthService.check_question_modified_by_admin(
                    video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                ):
                    has_any_editable_questions = True
                    break
            else:
                has_any_editable_questions = True
                break
        
        # Check if this group is complete
        is_group_complete = check_question_group_completion(
            video_id=video["id"], project_id=project_id, user_id=user_id, question_group_id=group_id, role=role, session=session
        )
        
        # Get button configuration with improved logic
        button_text, button_disabled = _get_submit_button_config(
            role, form_disabled, all_questions_modified_by_admin, 
            has_any_editable_questions, is_group_complete, mode
        )
        
        # Create form for this question group
        form_key = f"form_{video['id']}_{group_id}_{role}"
        with st.form(form_key):
            answers = {}
            
            # Create a container for scrollable content 
            # Balance the height - not too short, not too tall
            content_height = max(350, container_height - 150)
            
            with st.container(height=content_height, border=False):
                # Display each question with sticky headers
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["text"]
                    existing_value = existing_answers.get(question_text, "")
                    gt_value = gt_answers.get(question_text, "")
                    
                    # Check if this specific question has been modified by admin
                    is_modified_by_admin = False
                    admin_info = None
                    if role == "reviewer":
                        is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                            video_id=video["id"], project_id=project_id, question_id=question_id, session=session
                        )
                        if is_modified_by_admin:
                            admin_info = GroundTruthService.get_admin_modification_details(
                                video_id=video["id"], project_id=project_id, question_id=question_id, session=session
                            )
                    
                    # Add spacing between questions (but not before the first one)
                    if i > 0:
                        st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                    
                    # Display question with clean sticky styling
                    if question["type"] == "single":
                        answers[question_text] = _display_clean_sticky_single_choice_question(
                            question, video["id"], project_id, role, existing_value,
                            is_modified_by_admin, admin_info, form_disabled, session,
                            gt_value, mode  # Added GT value and mode
                        )
                    else:  # description
                        answers[question_text] = _display_clean_sticky_description_question(
                            question, video["id"], project_id, role, existing_value,
                            is_modified_by_admin, admin_info, form_disabled, session,
                            gt_value, mode  # Added GT value and mode
                        )
            
            # Submit button outside the scrollable content - will be sticky
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            submitted = st.form_submit_button(
                button_text, 
                use_container_width=True,
                disabled=button_disabled
            )
            
            # Handle form submission
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
                        
                        # Check if user completed all questions in the project
                        try:
                            overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
                            if overall_progress >= 100:
                                show_annotator_completion()
                                return
                        except:
                            pass  # Don't let progress calculation errors break submission
                        
                        # Only show feedback in training mode
                        if mode == "Training":
                            show_training_feedback(video_id=video["id"], project_id=project_id, group_id=group_id, user_answers=answers, session=session)
                        else:
                            st.success("✅ Answers submitted!")
                        
                    else:  # reviewer
                        # Filter out admin-modified questions from answers
                        editable_answers = {}
                        for question in questions:
                            question_text = question["text"]
                            if not GroundTruthService.check_question_modified_by_admin(
                                video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
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
                            
                            # Check if project ground truth is complete
                            try:
                                project_progress = ProjectService.progress(project_id=project_id, session=session)
                                if project_progress['completion_percentage'] >= 100:
                                    show_reviewer_completion()
                                    return
                            except:
                                pass  # Don't let progress calculation errors break submission
                            
                            st.success("✅ Ground truth submitted!")
                        else:
                            st.warning("No editable questions to submit.")
                    
                    # Simple rerun - the fragment will handle the refresh
                    st.rerun(scope="fragment")
                    
                except ValueError as e:
                    st.error(f"Error: {str(e)}")
                    
                except ValueError as e:
                    st.error(f"Error: {str(e)}")
                    
    except ValueError as e:
        st.error(f"Error loading question group: {str(e)}")

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
    questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
    
    if not questions:
        return {"questions": [], "error": "No questions in this group."}
    
    # Check admin modification status using service layer
    all_questions_modified_by_admin = GroundTruthService.check_all_questions_modified_by_admin(
        video_id=video_id, project_id=project_id, question_group_id=group_id, session=session
    )
    
    # Get existing answers using service layer
    if role == "annotator":
        existing_answers = AnnotatorService.get_user_answers_for_question_group(
            video_id=video_id, project_id=project_id, user_id=user_id, question_group_id=group_id, session=session
        )
    else:  # reviewer
        # Get existing ground truth using service layer
        existing_answers = GroundTruthService.get_ground_truth_for_question_group(
            video_id=video_id, project_id=project_id, question_group_id=group_id, session=session
        )
    
    # Determine if form should be disabled for annotators
    form_disabled = False
    if role == "annotator":
        # Check if user already submitted answers in training mode using service layer
        form_disabled = (mode == "Training" and 
                       AnnotatorService.check_user_has_submitted_answers(
                           video_id=video_id, project_id=project_id, user_id=user_id, question_group_id=group_id, session=session
                       ))
    
    return {
        "questions": questions,
        "all_questions_modified_by_admin": all_questions_modified_by_admin,
        "existing_answers": existing_answers,
        "form_disabled": form_disabled,
        "error": None
    }

def _display_clean_sticky_single_choice_question(
    question: Dict,
    video_id: int,
    project_id: int,
    role: str,
    existing_value: str,
    is_modified_by_admin: bool,
    admin_info: Optional[Dict],
    form_disabled: bool,
    session: Session,
    gt_value: str = "",
    mode: str = ""
) -> str:
    """Display a single choice question with sticky header using Streamlit native components"""
    
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
    
    # Use Streamlit's subheader for better reliability
    if role == "reviewer" and is_modified_by_admin:
        st.error(f"🔒 {question_text}")
    else:
        st.success(f"❓ {question_text}")
    
    # Training mode feedback - positioned right after question, before options
    if mode == "Training" and form_disabled and gt_value and role == "annotator":
        if existing_value == gt_value:
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #d1f2eb, #a3e4d7);
                    border: 2px solid #27ae60;
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin: 10px 0 15px 0;
                    box-shadow: 0 2px 6px rgba(39, 174, 96, 0.2);
                ">
                    <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
                        ✅ Excellent! You selected the correct answer.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #fadbd8, #f1948a);
                    border: 2px solid #e74c3c;
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin: 10px 0 15px 0;
                    box-shadow: 0 2px 6px rgba(231, 76, 60, 0.2);
                ">
                    <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
                        ❌ Incorrect. The ground truth answer is highlighted below.
                    </span>
                </div>
            """, unsafe_allow_html=True)
    
    # Question content with inline radio buttons
    if role == "reviewer" and is_modified_by_admin and admin_info:
        # Show read-only version with admin modification info
        original_value = admin_info["original_value"]
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        original_display = value_to_display.get(original_value, original_value)
        current_display = value_to_display.get(current_value, current_value)
        
        st.warning(f"**Original Answer:** {original_display}")
        st.error(f"**Admin Override by {admin_name}:** {current_display}")
        
        result = current_value
        
    elif role == "reviewer":
        # Editable version for reviewers with enhanced options
        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id=video_id, project_id=project_id, question_id=question_id, options=options, display_values=display_values, session=session
        )
        
        # Debug info to help troubleshoot
        selected_annotators = st.session_state.get("selected_annotators", [])
        if not selected_annotators:
            st.caption("⚠️ No annotators selected - only ground truth will be shown")
        else:
            # Show which annotators are being used for this question
            try:
                # Extract initials from "Name (XX)" format for display
                shown_initials = []
                for name in selected_annotators[:3]:
                    if " (" in name and name.endswith(")"):
                        initials = name.split(" (")[1][:-1]  # Extract XX from "Name (XX)"
                        shown_initials.append(initials)
                    else:
                        shown_initials.append(name[:2])  # Fallback to first 2 chars
                
                st.caption(f"📊 Showing answers from: {', '.join(shown_initials)}")
            except Exception:
                st.caption(f"📊 Showing answers from {len(selected_annotators)} annotators")
        
        # Use a stable key that doesn't change with annotator selection to preserve user's choice
        radio_key = f"q_{video_id}_{question_id}_{role}_stable"
        
        # Find the index of the current value in the enhanced options
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
            horizontal=True  # Make radio buttons horizontal
        )
        result = options[selected_idx]
        
    else:
        # Regular display for annotators
        # ENHANCED FOR TRAINING MODE: Show ground truth after submission
        if mode == "Training" and form_disabled and gt_value:
            # In training mode after submission, enhance options to show ground truth
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
                key=f"q_{video_id}_{question_id}_{role}",
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True
            )
            
            result = display_to_value.get(selected_display.replace("🏆 ", "").replace(" (Ground Truth)", "").replace("❌ ", "").replace(" (Your Answer)", ""), existing_value)
        else:
            # Normal display or annotation mode
            selected_display = st.radio(
                "Select your answer:",
                options=display_values,
                index=default_idx,
                key=f"q_{video_id}_{question_id}_{role}",
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True  # Make radio buttons horizontal
            )
            result = display_to_value[selected_display]
    
    return result

def _display_clean_sticky_description_question(
    question: Dict,
    video_id: int,
    project_id: int,
    role: str,
    existing_value: str,
    is_modified_by_admin: bool,
    admin_info: Optional[Dict],
    form_disabled: bool,
    session: Session,
    gt_value: str = "",
    mode: str = ""
) -> str:
    """Display a description question with header using Streamlit native components"""
    
    question_id = question["id"]
    question_text = question["text"]
    
    # Use Streamlit's subheader for better reliability
    if role == "reviewer" and is_modified_by_admin:
        st.error(f"🔒 {question_text}")
    else:
        st.info(f"❓ {question_text}")
    
    # Training mode feedback - positioned right after question, before text area
    if mode == "Training" and form_disabled and gt_value and role == "annotator":
        if existing_value.strip().lower() == gt_value.strip().lower():
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #d1f2eb, #a3e4d7);
                    border: 2px solid #27ae60;
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin: 10px 0 15px 0;
                    box-shadow: 0 2px 6px rgba(39, 174, 96, 0.2);
                ">
                    <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
                        ✅ Excellent! Your answer matches the ground truth exactly.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        elif existing_value.strip():
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #fdeaa7, #f39c12);
                    border: 2px solid #f39c12;
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin: 10px 0 15px 0;
                    box-shadow: 0 2px 6px rgba(243, 156, 18, 0.2);
                ">
                    <span style="color: #d68910; font-weight: 600; font-size: 0.95rem;">
                        📝 Your answer differs from the ground truth. Compare them below to learn!
                    </span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #fadbd8, #f1948a);
                    border: 2px solid #e74c3c;
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin: 10px 0 15px 0;
                    box-shadow: 0 2px 6px rgba(231, 76, 60, 0.2);
                ">
                    <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
                        ❌ You didn't provide an answer.
                    </span>
                </div>
            """, unsafe_allow_html=True)
    
    # Question content
    if role == "reviewer" and is_modified_by_admin and admin_info:
        # Show read-only version with admin modification info
        original_value = admin_info["original_value"]
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        st.warning(f"**Original Answer:** {original_value}")
        st.error(f"**Admin Override by {admin_name}:** {current_value}")
        
        result = current_value
        
    elif role == "reviewer":
        # Use a stable key that doesn't change with annotator selection
        text_key = f"q_{video_id}_{question_id}_{role}_stable"
        
        # Editable version for reviewers
        answer = st.text_area(
            "Enter your answer:",
            value=existing_value,
            key=text_key,
            disabled=form_disabled,
            height=120,
            label_visibility="collapsed"
        )
        
        # Show existing annotator answers as helper text using service layer
        # This will update when annotator selection changes
        _display_helper_text_answers(video_id=video_id, project_id=project_id, question_id=question_id, session=session)
        
        result = answer
        
    else:
        # Regular text area for annotators
        result = st.text_area(
            "Enter your answer:",
            value=existing_value,
            key=f"q_{video_id}_{question_id}_{role}",
            disabled=form_disabled,
            height=120,
            label_visibility="collapsed"
        )
        
        # Show ground truth feedback in training mode after submission
        if mode == "Training" and form_disabled and gt_value:
            st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #e8f4f8, #d5e8f0);
                    border: 2px solid #3498db;
                    border-radius: 8px;
                    padding: 16px;
                    margin: 15px 0 10px 0;
                    box-shadow: 0 2px 6px rgba(52, 152, 219, 0.2);
                ">
                    <div style="color: #2980b9; font-weight: 700; font-size: 0.95rem; margin-bottom: 8px;">
                        🏆 Ground Truth Answer:
                    </div>
                    <div style="color: #34495e; font-size: 0.9rem; line-height: 1.4; background: white; padding: 12px; border-radius: 6px; border-left: 4px solid #3498db;">
            """ + gt_value + """
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    return result

def _get_enhanced_options_for_reviewer(
    video_id: int,
    project_id: int, 
    question_id: int,
    options: List[str],
    display_values: List[str],
    session: Session
) -> List[str]:
    """Get enhanced options showing who selected what for reviewers with percentage display"""
    
    # Get enhanced options showing who selected what using service layer
    selected_annotators = st.session_state.get("selected_annotators", [])
    
    try:
        # FIXED: Get annotator user IDs using service layer with proper format matching
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        # Show helpful info if no user IDs found
        if not annotator_user_ids and selected_annotators:
            st.caption(f"⚠️ Could not find user IDs for selected annotators")
        
        option_selections = GroundTruthService.get_question_option_selections(
            video_id=video_id, project_id=project_id, question_id=question_id, annotator_user_ids=annotator_user_ids, session=session
        )
        
    except Exception as e:
        st.caption(f"⚠️ Error getting annotator data: {str(e)}")
        option_selections = {}
        annotator_user_ids = []
    
    # Get total number of selected annotators for percentage calculation
    total_annotators = len(annotator_user_ids)
    
    # Create enhanced options with percentage display
    enhanced_options = []
    for i, display_val in enumerate(display_values):
        actual_val = options[i] if i < len(options) else display_val
        option_text = display_val
        
        if actual_val in option_selections:
            # Separate ground truth and annotators
            annotators = []
            has_gt = False
            
            for selector in option_selections[actual_val]:
                if selector["type"] == "ground_truth":
                    has_gt = True
                else:
                    annotators.append(selector["initials"])
            
            # Build selection info with cleaner percentage format
            selection_info = []
            if annotators and total_annotators > 0:
                count = len(annotators)
                percentage = (count / total_annotators) * 100
                # Clean formatting: show whole numbers without decimals
                if percentage == int(percentage):
                    percentage_str = f"{int(percentage)}%"
                else:
                    percentage_str = f"{percentage:.1f}%"
                selection_info.append(f"{percentage_str}: {', '.join(annotators)}")
            elif annotators:
                # Fallback if total_annotators is 0
                selection_info.append(f"{', '.join(annotators)}")
            
            if has_gt:
                selection_info.append("🏆 GT")
            
            if selection_info:
                option_text += f" — {' | '.join(selection_info)}"
        
        enhanced_options.append(option_text)
    
    return enhanced_options

def _display_helper_text_answers(video_id: int, project_id: int, question_id: int, session: Session):
    """Display helper text showing other annotator answers for description questions"""
    
    selected_annotators = st.session_state.get("selected_annotators", [])
    
    try:
        # Use proper service layer call with format matching
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        text_answers = GroundTruthService.get_question_text_answers(
            video_id=video_id, project_id=project_id, question_id=question_id, annotator_user_ids=annotator_user_ids, session=session
        )
        
        if text_answers:
            # Remove duplicates by creating a set of unique answer texts
            unique_answers = []
            seen_answers = set()
            
            for answer in text_answers:
                answer_key = f"{answer['initials']}:{answer['answer_value']}"
                if answer_key not in seen_answers:
                    seen_answers.add(answer_key)
                    # Use shortened format with just initials
                    unique_answers.append(f"**{answer['initials']}:** {answer['answer_value']}")
            
            if unique_answers:
                # Display each annotator's answer on a new line
                st.caption("Other answers:")
                for answer_text in unique_answers:
                    st.caption(answer_text)
    except Exception:
        pass  # Silently handle errors in helper text

def _get_submit_button_config(
    role: str,
    form_disabled: bool,
    all_questions_modified_by_admin: bool,
    has_any_editable_questions: bool,
    is_group_complete: bool,
    mode: str
) -> Tuple[str, bool]:
    """Get the submit button text and disabled state with improved logic"""
    
    if role == "annotator":
        if form_disabled:
            button_text = "🔒 Already Submitted"
            button_disabled = True
        elif is_group_complete and mode != "Training":
            # In annotation mode, if already complete, show re-submit option with checkmark
            button_text = "✅ Re-submit Answers"
            button_disabled = False
        elif is_group_complete:
            # In training mode, show completed status
            button_text = "✅ Completed"
            button_disabled = True
        else:
            button_text = "Submit Answers"
            button_disabled = False
    else:  # reviewer
        if all_questions_modified_by_admin:
            button_text = "🔒 All Questions Modified by Admin"
            button_disabled = True
        elif not has_any_editable_questions:
            button_text = "🔒 No Editable Questions"
            button_disabled = True
        elif is_group_complete:
            # If ground truth is complete but not all admin-modified, allow re-submit with checkmark
            button_text = "✅ Re-submit Ground Truth"
            button_disabled = False
        else:
            button_text = "Submit Ground Truth"
            button_disabled = False
    
    return button_text, button_disabled

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
            display_project_dashboard(user_id=user["id"], role="annotator", session=session)
        elif current_view == "project":
            # Display project view
            display_project_view(user_id=user["id"], role="annotator", session=session)

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
            display_project_dashboard(user_id=user["id"], role="reviewer", session=session)
        elif current_view == "project":
            # Display project view
            display_project_view(user_id=user["id"], role="reviewer", session=session)

###############################################################################
# ADMIN PORTAL  - OPTIMIZED WITH FRAGMENTS
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

@st.fragment
def admin_videos():
    st.subheader("📹 Video Management")
    
    with get_db_session() as session:
        videos_df = VideoService.get_videos_with_project_status(session=session)
        
        if not videos_df.empty:
            st.dataframe(videos_df, use_container_width=True)
        else:
            st.info("No videos in the database yet.")
        
        with st.expander("➕ Add Video"):
            url = st.text_input("Video URL", key="admin_video_url")
            metadata_json = st.text_area(
                "Metadata (JSON, optional)", 
                "{}",
                key="admin_video_metadata"
            )
            
            if st.button("Add Video", key="admin_add_video_btn"):
                if url:
                    try:
                        import json
                        metadata = json.loads(metadata_json) if metadata_json.strip() else {}
                        VideoService.add_video(
                            url=url, 
                            session=session, 
                            metadata=metadata
                        )
                        st.success("Video added!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

@st.fragment
def admin_projects():
    st.subheader("📁 Project Management")
    
    with get_db_session() as session:
        projects_df = ProjectService.get_all_projects(session=session)
        
        # Enhanced project display
        if not projects_df.empty:
            enhanced_projects = []
            for _, project in projects_df.iterrows():
                try:
                    progress = ProjectService.progress(
                        project_id=project["ID"], 
                        session=session
                    )
                    has_full_gt = check_project_has_full_ground_truth(
                        project_id=project["ID"], 
                        session=session
                    )
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
                        schema_id = SchemaService.get_schema_id_by_name(
                            name=schema_name, 
                            session=session
                        )
                        video_ids = ProjectService.get_video_ids_by_uids(
                            video_uids=selected_videos, 
                            session=session
                        )
                        ProjectService.create_project(
                            name=name, 
                            schema_id=schema_id, 
                            video_ids=video_ids, 
                            session=session
                        )
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
                        SchemaService.create_schema(
                            name=schema_name, 
                            question_group_ids=selected_groups, 
                            session=session
                        )
                        st.success("Schema created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

@st.fragment
def admin_questions():
    st.subheader("❓ Question & Group Management")
    
    with get_db_session() as session:
        q_tab1, q_tab2 = st.tabs(["Question Groups", "Individual Questions"])
        
        with q_tab1:
            groups_df = QuestionGroupService.get_all_groups(session=session)
            st.dataframe(groups_df, use_container_width=True)
            
            with st.expander("➕ Create Question Group"):
                title = st.text_input("Group Title", key="admin_group_title")
                description = st.text_area("Description", key="admin_group_description")
                is_reusable = st.checkbox("Reusable across schemas", key="admin_group_reusable")
                
                questions_df = QuestionService.get_all_questions(session=session)
                if not questions_df.empty:
                    available_questions = questions_df[~questions_df["Archived"]]
                    selected_questions = st.multiselect(
                        "Questions",
                        available_questions["ID"].tolist(),
                        format_func=lambda x: available_questions[available_questions["ID"]==x]["Text"].iloc[0],
                        key="admin_group_questions"
                    )
                else:
                    selected_questions = []
                    st.warning("No questions available.")
                
                if st.button("Create Group", key="admin_create_group_btn"):
                    if title and selected_questions:
                        try:
                            QuestionGroupService.create_group(
                                title=title, 
                                description=description, 
                                is_reusable=is_reusable, 
                                question_ids=selected_questions, 
                                verification_function=None, 
                                session=session
                            )
                            st.success("Question group created!")
                            st.rerun(scope="fragment")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
        
        with q_tab2:
            questions_df = QuestionService.get_all_questions(session=session)
            st.dataframe(questions_df, use_container_width=True)
            
            with st.expander("➕ Create Question"):
                text = st.text_input("Question Text", key="admin_question_text")
                q_type = st.selectbox("Type", ["single", "description"], key="admin_question_type")
                
                options = []
                default = None
                
                if q_type == "single":
                    st.write("**Options:**")
                    num_options = st.number_input("Number of options", 1, 10, 2, key="admin_question_num_options")
                    
                    for i in range(num_options):
                        option = st.text_input(f"Option {i+1}", key=f"admin_question_opt_{i}")
                        if option:
                            options.append(option)
                    
                    if options:
                        default = st.selectbox("Default option", [""] + options, key="admin_question_default")
                
                if st.button("Create Question", key="admin_create_question_btn"):
                    if text:
                        try:
                            QuestionService.add_question(
                                text=text, 
                                qtype=q_type, 
                                options=options if q_type == "single" else None,
                                default=default if q_type == "single" else None,
                                session=session
                            )
                            st.success("Question created!")
                            st.rerun(scope="fragment")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")

@st.fragment
def admin_users():
    st.subheader("👥 User Management")
    
    with get_db_session() as session:
        users_df = AuthService.get_all_users(session=session)
        st.dataframe(users_df, use_container_width=True)
        
        with st.expander("➕ Create User"):
            user_id = st.text_input("User ID", key="admin_user_id")
            email = st.text_input("Email", key="admin_user_email")
            password = st.text_input("Password", type="password", key="admin_user_password")
            user_type = st.selectbox("User Type", ["human", "model", "admin"], key="admin_user_type")
            
            if st.button("Create User", key="admin_create_user_btn"):
                if user_id and email and password:
                    try:
                        AuthService.create_user(
                            user_id=user_id, 
                            email=email, 
                            password_hash=password, 
                            user_type=user_type, 
                            session=session
                        )
                        st.success("User created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

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
                        project_id=project_id,
                        user_id=user_id,
                        role=role,
                        session=session
                    )
                else:  # remove
                    AuthService.archive_user_from_project(
                        user_id=user_id, 
                        project_id=project_id, 
                        session=session
                    )
                success_count += 1
            except Exception as e:
                error_count += 1
                # Only show first few errors to avoid spam
                if error_count <= 3:
                    st.error(f"Failed: User {user_id} {'to' if action_type == 'assign' else 'from'} Project {project_id}")
            
            operation_counter += 1
            progress = operation_counter / total_operations
            progress_bar.progress(progress)
            status_container.text(f"Processing: {operation_counter}/{total_operations}")
    
    # Final results
    action_word = "assignments" if action_type == "assign" else "removals"
    if success_count > 0:
        st.success(f"✅ Successfully completed {success_count} {action_word}!")
    if error_count > 0:
        st.warning(f"⚠️ {error_count} {action_word} failed")
    
    # Clear selections after successful operation
    if success_count > 0:
        st.session_state.selected_project_ids = []
        st.session_state.selected_user_ids = []
        st.rerun(scope="fragment")

@st.fragment
def display_assignment_management(session: Session):
    """Optimized assignment management with fragments to reduce refresh issues"""
    
    st.markdown("### 🎯 Assign Users to Projects")
    
    # Initialize session state for selections
    if "selected_project_ids" not in st.session_state:
        st.session_state.selected_project_ids = []
    if "selected_user_ids" not in st.session_state:
        st.session_state.selected_user_ids = []
    if "assignment_role" not in st.session_state:
        st.session_state.assignment_role = "annotator"
    
    # Project selection section
    st.markdown("**Step 1: Select Projects**")
    
    try:
        projects_df = ProjectService.get_all_projects(session=session)
        if projects_df.empty:
            st.warning("No projects available.")
            return
    except Exception as e:
        st.error(f"Error loading projects: {str(e)}")
        return
    
    # Project search
    project_search = st.text_input("🔍 Search projects", placeholder="Project name...", key="proj_search_mgmt")
    
    # Filter projects
    filtered_projects = []
    for _, project_row in projects_df.iterrows():
        if not project_search or project_search.lower() in project_row["Name"].lower():
            filtered_projects.append(project_row)
    
    if not filtered_projects:
        st.warning("No projects match the search criteria.")
        return
    
    st.info(f"Found {len(filtered_projects)} projects")
    
    # Quick select buttons
    select_col1, select_col2 = st.columns(2)
    with select_col1:
        if st.button("Select All Visible Projects", key="select_all_projects_mgmt"):
            st.session_state.selected_project_ids = [int(p["ID"]) for p in filtered_projects]
            st.rerun(scope="fragment")
    
    with select_col2:
        if st.button("Clear Project Selection", key="clear_projects_mgmt"):
            st.session_state.selected_project_ids = []
            st.rerun(scope="fragment")
    
    # Display project selection with simpler state management
    project_cols = st.columns(4)
    current_selections = []
    
    for i, project_row in enumerate(filtered_projects):
        with project_cols[i % 4]:
            project_id = int(project_row["ID"])
            project_name = project_row["Name"]
            
            # Use current session state value
            is_selected = project_id in st.session_state.selected_project_ids
            
            # Checkbox without auto-refresh
            checkbox_value = st.checkbox(
                project_name,
                value=is_selected,
                key=f"proj_cb_mgmt_{project_id}",
                help=f"Project ID: {project_id}"
            )
            
            if checkbox_value:
                current_selections.append(project_id)
    
    # Update session state only if changed
    if set(current_selections) != set(st.session_state.selected_project_ids):
        st.session_state.selected_project_ids = current_selections
        st.rerun(scope="fragment")
    
    if not st.session_state.selected_project_ids:
        st.info("Please select projects above to continue.")
        return
    
    st.success(f"✅ Selected {len(st.session_state.selected_project_ids)} projects")
    
    # User selection section
    st.markdown("**Step 2: Select Users**")
    
    try:
        users_df = AuthService.get_all_users(session=session)
        if users_df.empty:
            st.warning("No users available.")
            return
    except Exception as e:
        st.error(f"Error loading users: {str(e)}")
        return
    
    # User filters
    user_filter_col1, user_filter_col2 = st.columns(2)
    with user_filter_col1:
        user_search = st.text_input("Search users", placeholder="Name or email...", key="user_search_mgmt")
    with user_filter_col2:
        user_role_filter = st.selectbox("Filter by user role", ["All", "admin", "human", "model"], key="user_role_filter_mgmt")
    
    # Filter users
    filtered_users = []
    for _, user_row in users_df.iterrows():
        # Role filter
        if user_role_filter != "All" and user_row["Role"] != user_role_filter:
            continue
        
        # Search filter
        if user_search:
            if (user_search.lower() not in user_row["User ID"].lower() and 
                user_search.lower() not in user_row["Email"].lower()):
                continue
        
        filtered_users.append(user_row)
    
    if not filtered_users:
        st.warning("No users match the search criteria.")
        return
    
    st.info(f"Found {len(filtered_users)} users")
    
    # User selection with pagination
    users_per_page = 12
    total_pages = (len(filtered_users) - 1) // users_per_page + 1
    
    if total_pages > 1:
        page = st.selectbox(f"Page (showing {users_per_page} users per page)", 
                           range(1, total_pages + 1), key="user_page_mgmt") - 1
    else:
        page = 0
    
    start_idx = page * users_per_page
    end_idx = min(start_idx + users_per_page, len(filtered_users))
    page_users = filtered_users[start_idx:end_idx]
    
    # Quick select buttons for users
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
    
    # Display users
    user_cols = st.columns(4)
    current_user_selections = list(st.session_state.selected_user_ids)  # Copy current state
    
    for i, user_row in enumerate(page_users):
        with user_cols[i % 4]:
            user_id = int(user_row["ID"])
            user_name = user_row["User ID"]
            user_email = user_row["Email"]
            user_role = user_row["Role"]
            
            # Check current state
            is_selected = user_id in st.session_state.selected_user_ids
            
            checkbox_value = st.checkbox(
                user_name,
                value=is_selected,
                key=f"user_cb_mgmt_{user_id}",
                help=f"Email: {user_email}\nRole: {user_role}\nID: {user_id}"
            )
            
            # Update local selection list
            if checkbox_value and user_id not in current_user_selections:
                current_user_selections.append(user_id)
            elif not checkbox_value and user_id in current_user_selections:
                current_user_selections.remove(user_id)
    
    # Update session state only if changed
    if set(current_user_selections) != set(st.session_state.selected_user_ids):
        st.session_state.selected_user_ids = current_user_selections
        st.rerun(scope="fragment")
    
    if not st.session_state.selected_user_ids:
        st.info("Please select users above to continue.")
        return
    
    st.success(f"✅ Selected {len(st.session_state.selected_user_ids)} users")
    
    # Assignment role and actions
    st.markdown("**Step 3: Assignment Role & Actions**")
    
    role = st.selectbox("Assignment Role", ["annotator", "reviewer", "admin", "model"], 
                       index=["annotator", "reviewer", "admin", "model"].index(st.session_state.assignment_role),
                       key="assign_role_mgmt")
    
    if role != st.session_state.assignment_role:
        st.session_state.assignment_role = role
    
    # Show summary
    st.info(f"Ready to assign {len(st.session_state.selected_user_ids)} users as **{role}** to {len(st.session_state.selected_project_ids)} projects")
    
    # Action buttons
    action_col1, action_col2 = st.columns(2)
    
    with action_col1:
        if st.button("✅ Execute Assignments", key="execute_assignments", use_container_width=True):
            # Inline assignment logic to avoid scoping issues
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
                            project_id=project_id,
                            user_id=user_id,
                            role=role,
                            session=session
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
            
            # Final results
            if success_count > 0:
                st.success(f"✅ Successfully completed {success_count} assignments!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} assignments failed")
            
            # Clear selections after successful operation
            if success_count > 0:
                st.session_state.selected_project_ids = []
                st.session_state.selected_user_ids = []
                st.rerun(scope="fragment")
    
    with action_col2:
        if st.button("🗑️ Remove Assignments", key="execute_removals", use_container_width=True):
            # Inline removal logic to avoid scoping issues
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
                        AuthService.archive_user_from_project(
                            user_id=user_id, 
                            project_id=project_id, 
                            session=session
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 3:
                            st.error(f"Failed to remove user {user_id} from project {project_id}: {str(e)}")
                    
                    operation_counter += 1
                    progress = operation_counter / total_operations
                    progress_bar.progress(progress)
                    status_container.text(f"Processing: {operation_counter}/{total_operations}")
            
            # Final results
            if success_count > 0:
                st.success(f"🗑️ Successfully removed {success_count} assignments!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} removals failed")
            
            # Clear selections after successful operation
            if success_count > 0:
                st.session_state.selected_project_ids = []
                st.session_state.selected_user_ids = []
                st.rerun(scope="fragment")

@st.fragment 
def admin_assignments():
    st.subheader("🔗 Project Assignments")
    
    def get_user_role_emoji(role):
        """Get appropriate emoji for user role"""
        if role == "human":
            return "👤"
        elif role == "admin":
            return "👑"
        elif role == "model":
            return "🤖"
        else:
            return "❓"
    
    with get_db_session() as session:
        # Get raw assignments data
        assignments_df = AuthService.get_project_assignments(session=session)
        
        if not assignments_df.empty:
            # ASSIGNMENT MANAGEMENT SECTION - OPTIMIZED WITH FRAGMENT
            with st.expander("➕ **Manage Project Assignments**", expanded=False):
                display_assignment_management(session)
            
            st.markdown("---")  # Separator before assignment data
            
            # Get additional data for enhanced display
            users_df = AuthService.get_all_users(session=session)
            projects_df = ProjectService.get_all_projects(session=session)
            
            # Get project groups for organization
            try:
                project_groups = ProjectGroupService.list_project_groups(session=session)
                project_group_lookup = {}
                for group in project_groups:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=group.id, session=session)
                    for project in group_info["projects"]:
                        project_group_lookup[project.id] = group.name
            except:
                project_group_lookup = {}
            
            # Create lookup dictionaries
            user_lookup = {row["ID"]: {"name": row["User ID"], "email": row["Email"], "role": row["Role"]} for _, row in users_df.iterrows()}
            project_lookup = {row["ID"]: row["Name"] for _, row in projects_df.iterrows()}
            
            # Process assignments - consolidate by user and project, but keep role-specific dates
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
                        "is_archived": True  # Start as True, set False if any active assignment found
                    }
                
                # Store role assignments separately with their own dates
                project_key = project_id
                if project_key not in user_assignments[user_id]["projects"]:
                    user_assignments[user_id]["projects"][project_key] = {
                        "name": project_name,
                        "group": project_group,
                        "role_assignments": {}  # Changed to store each role separately
                    }
                
                # Store each role assignment with its specific dates
                role = assignment["Role"]
                if role not in user_assignments[user_id]["projects"][project_key]["role_assignments"]:
                    user_assignments[user_id]["projects"][project_key]["role_assignments"][role] = {
                        "assigned_date": None,
                        "completed_date": None,
                        "archived": assignment.get("Archived", False)
                    }
                
                role_data = user_assignments[user_id]["projects"][project_key]["role_assignments"][role]
                
                # Set assignment date
                if assignment.get("Assigned At"):
                    try:
                        if hasattr(assignment["Assigned At"], 'strftime'):
                            assigned_date = assignment["Assigned At"].strftime("%Y-%m-%d")
                        else:
                            assigned_date = str(assignment["Assigned At"])[:10]
                        role_data["assigned_date"] = assigned_date
                    except:
                        role_data["assigned_date"] = "Unknown"
                
                # Set completion date
                if assignment.get("Completed At"):
                    try:
                        if hasattr(assignment["Completed At"], 'strftime'):
                            completed_date = assignment["Completed At"].strftime("%Y-%m-%d")
                        else:
                            completed_date = str(assignment["Completed At"])[:10]
                        role_data["completed_date"] = completed_date
                    except:
                        role_data["completed_date"] = "Unknown"
                
                # Update user archive status
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
                
                # Project role filter - Updated for new structure
                if project_role_filter != "All":
                    has_role = False
                    for project_id, project_info in user_data["projects"].items():
                        if project_role_filter in project_info["role_assignments"]:
                            has_role = True
                            break
                    if not has_role:
                        continue
                
                filtered_assignments[user_id] = user_data
            
            # Display results count
            total_users = len(user_assignments)
            filtered_count = len(filtered_assignments)
            st.info(f"📊 Showing **{filtered_count}** of **{total_users}** total users")
            
            # Main assignments table
            if filtered_assignments:
                st.markdown("### 📋 Assignment Overview")
                
                # Prepare data for display with role-specific dates
                display_data = []
                for user_id, user_data in filtered_assignments.items():
                    # Group projects by project group
                    grouped_projects = {}
                    for project_id, project_info in user_data["projects"].items():
                        project_group = project_info["group"]
                        project_name = project_info["name"]
                        
                        if project_group not in grouped_projects:
                            grouped_projects[project_group] = []
                        
                        # Build clean, readable role display for this project
                        role_parts = []
                        
                        for role, role_data in project_info["role_assignments"].items():
                            # Check if role is completed (for annotator/reviewer only)
                            is_completed = (role != "admin" and 
                                          role_data["completed_date"] and 
                                          role_data["completed_date"] != "Unknown")
                            
                            # Add completion emoji for non-admin completed roles
                            completion_emoji = "✅ " if is_completed else ""
                            
                            # Build date info for this role
                            if role == "admin":
                                # Admin: just assignment date
                                if role_data["assigned_date"]:
                                    date_part = f"({role_data['assigned_date']})"
                                else:
                                    date_part = "(not set)"
                            else:
                                # Annotator/Reviewer: assignment → completion
                                assigned = role_data["assigned_date"] if role_data["assigned_date"] else "not set"
                                completed = role_data["completed_date"] if role_data["completed_date"] and role_data["completed_date"] != "Unknown" else None
                                
                                if completed:
                                    date_part = f"({assigned} → {completed})"
                                else:
                                    date_part = f"({assigned})"
                            
                            # Add archived indicator if needed
                            archived_indicator = " 🗄️" if role_data["archived"] else ""
                            
                            role_parts.append(f"{completion_emoji}{role.title()}{date_part}{archived_indicator}")
                        
                        # Create clean project display
                        roles_text = ", ".join(role_parts)
                        project_display = f"  • **{project_name}**: {roles_text}"
                        
                        grouped_projects[project_group].append({
                            "display": project_display,
                            "name": project_name,
                            "roles": list(project_info["role_assignments"].keys())
                        })
                    
                    # Format grouped projects with search data
                    projects_data = []
                    projects_display = ""
                    
                    for group_name in sorted(grouped_projects.keys()):
                        if projects_display:  # Add spacing between groups
                            projects_display += "\n---\n\n"
                        
                        if group_name != "Ungrouped":
                            projects_display += f"## 📁 {group_name}\n\n"
                        else:
                            projects_display += f"## 📄 Individual Projects\n\n"
                        
                        for project_info in grouped_projects[group_name]:
                            projects_display += f"{project_info['display']}\n\n"
                            # Store searchable data
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
                        "Projects Data": projects_data,  # For search functionality
                        "Total Projects": len(user_data["projects"])
                    })
                
                # Sort by name for consistency
                display_data.sort(key=lambda x: x["User Name"])
                
                # Display in a much cleaner, card-based format
                for i, user_data in enumerate(display_data):
                    # Create a styled card for each user with project preview
                    status_color = "#dc3545" if "Archived" in user_data["Status"] else "#28a745"
                    
                    # Get a few sample projects for preview
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
                        <div style="
                            border: 2px solid {status_color};
                            border-radius: 10px;
                            padding: 15px;
                            margin: 10px 0;
                            background: linear-gradient(135deg, #ffffff, #f8f9fa);
                            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                        ">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div style="flex: 1;">
                                    <h4 style="margin: 0; color: #1f77b4;">👤 {user_data['User Name']}</h4>
                                    <p style="margin: 5px 0; color: #6c757d;">📧 {user_data['Email']}</p>
                                    <p style="margin: 5px 0; color: #6c757d; font-size: 0.9rem; font-style: italic;">{sample_text}</p>
                                </div>
                                <div style="text-align: right;">
                                    <span style="
                                        background: {status_color};
                                        color: white;
                                        padding: 5px 10px;
                                        border-radius: 15px;
                                        font-weight: bold;
                                        font-size: 0.9rem;
                                        margin-left: 10px;
                                    ">{user_data["Status"]}</span>
                                    <br><br>
                                    <span style="color: #495057; font-weight: bold;">
                                        {get_user_role_emoji(user_data['User Role'].lower())} {user_data['User Role']}
                                    </span>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Count total projects for this user
                        total_projects = user_data["Total Projects"]
                        
                        # Projects in expandable section with search and pagination
                        with st.expander(f"📁 View assignments ({total_projects} projects)", expanded=False):
                            if user_data["Projects Data"]:
                                # Per-user search and filter
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
                                
                                # Filter projects based on search
                                filtered_projects = user_data["Projects Data"]
                                
                                if user_search:
                                    filtered_projects = [p for p in filtered_projects if user_search.lower() in p["name"].lower()]
                                
                                if user_role_filter != "All":
                                    filtered_projects = [p for p in filtered_projects if user_role_filter in p["roles"]]
                                
                                # Show filtered count
                                if len(filtered_projects) != total_projects:
                                    st.info(f"Showing {len(filtered_projects)} of {total_projects} projects")
                                
                                # Pagination for projects (show 10 per page)
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
                                
                                # Show projects for current page
                                start_idx = project_page * projects_per_page
                                end_idx = min(start_idx + projects_per_page, len(filtered_projects))
                                page_projects = filtered_projects[start_idx:end_idx]
                                
                                if page_projects:
                                    # Group projects by group for display
                                    display_groups = {}
                                    for project in page_projects:
                                        group = project["group"]
                                        if group not in display_groups:
                                            display_groups[group] = []
                                        display_groups[group].append(project["display"])
                                    
                                    # Display grouped projects
                                    for group_name in sorted(display_groups.keys()):
                                        if group_name != "Ungrouped":
                                            st.markdown(f"### 📁 {group_name}")
                                        else:
                                            st.markdown(f"### 📄 Individual Projects")
                                        
                                        for project_display in display_groups[group_name]:
                                            st.markdown(project_display)
                                        
                                        if len(display_groups) > 1:  # Add separator between groups
                                            st.markdown("---")
                                else:
                                    st.info("No projects match the current filters.")
                            else:
                                st.info("No project assignments")
            else:
                st.warning("No users match the current filters.")
        else:
            st.info("No project assignments found in the database.")
        
        # Raw data in collapsed expander
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
        # List existing project groups
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
        
        # Create new project group
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
                            name=group_name, 
                            description=group_description, 
                            project_ids=selected_projects, 
                            session=session
                        )
                        st.success("Project group created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        # Edit existing project groups
        with st.expander("✏️ Edit Project Group"):
            try:
                groups = ProjectGroupService.list_project_groups(session=session)
                if groups:
                    # Group selection
                    group_options = {f"{g.name} (ID: {g.id})": g.id for g in groups}
                    selected_group_name = st.selectbox(
                        "Select Group to Edit", 
                        list(group_options.keys()),
                        key="admin_edit_group_select"
                    )
                    
                    if selected_group_name:
                        selected_group_id = group_options[selected_group_name]
                        
                        # Get current group info
                        group_info = ProjectGroupService.get_project_group_by_id(
                            group_id=selected_group_id, 
                            session=session
                        )
                        current_group = group_info["group"]
                        current_projects = group_info["projects"]
                        
                        # Edit fields
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
                        
                        # Project management
                        st.markdown("**Project Management:**")
                        
                        # Show current projects
                        if current_projects:
                            st.markdown("**Current Projects:**")
                            current_project_ids = [p.id for p in current_projects]
                            for project in current_projects:
                                st.write(f"- {project.name} (ID: {project.id})")
                        else:
                            st.info("No projects currently in this group")
                            current_project_ids = []
                        
                        # Get all available projects
                        all_projects_df = ProjectService.get_all_projects(session=session)
                        
                        if not all_projects_df.empty:
                            # Projects to add
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
                            
                            # Projects to remove
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
                        
                        # Update button
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
    """Get question groups in a schema - using service layer"""
    try:
        return SchemaService.get_schema_question_groups_list(
            schema_id=schema_id, 
            session=session
        )
    except ValueError as e:
        st.error(f"Error loading schema question groups: {str(e)}")
        return []

def get_project_videos(project_id: int, session: Session) -> List[Dict]:
    """Get videos in a project - using service layer"""
    try:
        return VideoService.get_project_videos(
            project_id=project_id, 
            session=session
        )
    except ValueError as e:
        st.error(f"Error getting project videos: {str(e)}")
        return []

def calculate_user_overall_progress(user_id: int, project_id: int, session: Session) -> float:
    """Calculate user's overall progress - using service layer"""
    try:
        return AnnotatorService.calculate_user_overall_progress(
            user_id=user_id, 
            project_id=project_id, 
            session=session
        )
    except ValueError as e:
        st.error(f"Error calculating user progress: {str(e)}")
        return 0.0

def show_training_feedback(video_id: int, project_id: int, group_id: int, 
                          user_answers: Dict[str, str], session: Session):
    """Show training feedback comparing user answers to ground truth - using service layer"""
    
    try:
        gt_df = GroundTruthService.get_ground_truth(
            video_id=video_id, 
            project_id=project_id, 
            session=session
        )
        questions = QuestionService.get_questions_by_group_id(
            group_id=group_id, 
            session=session
        )
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
    
    # Custom CSS - clean design with improved styling and better radio buttons
    st.markdown("""
        <style>
        .stProgress > div > div > div > div {
            background-color: #4CAF50;
        }
        
        /* Clean tabs styling with better visual hierarchy */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 10px;
            padding: 6px;
            border: 1px solid #dee2e6;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .stTabs [data-baseweb="tab"] {
            height: 44px;
            white-space: pre-wrap;
            background-color: transparent;
            border-radius: 8px;
            color: #495057;
            font-weight: 600;
            border: none;
            padding: 10px 18px;
            transition: all 0.2s ease;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background-color: rgba(31, 119, 180, 0.1);
            transform: translateY(-1px);
        }
        
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #1f77b4, #4a90e2) !important;
            color: white !important;
            box-shadow: 0 3px 8px rgba(31, 119, 180, 0.3);
            transform: translateY(-1px);
        }
        
        /* Remove form borders completely */
        .stForm {
            border: none !important;
            padding: 0 !important;
            margin: 0 !important;
            background: transparent !important;
        }
        
        /* Enhanced inline radio button styling */
        .stRadio > div {
            gap: 1rem;
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
            margin-bottom: 0.7rem;
            font-size: 0.95rem;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 12px 16px;
            border-radius: 10px;
            border: 2px solid #e9ecef;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
            flex: 0 0 auto;
            min-width: fit-content;
            max-width: 320px;
            font-weight: 500;
        }
        
        .stRadio > div > label:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #1f77b4;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(31, 119, 180, 0.2);
        }
        
        /* Selected radio button styling with gradient */
        .stRadio > div > label[data-checked="true"] {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-color: #1f77b4;
            font-weight: 700;
            color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(31, 119, 180, 0.25);
        }
        
        /* Enhanced text areas */
        .stTextArea {
            background: transparent !important;
            padding: 0 !important;
            border: none !important;
            border-radius: 0 !important;
            margin: 0 !important;
        }
        
        .stTextArea > div > div > textarea {
            border-radius: 10px;
            border: 2px solid #e9ecef;
            font-size: 0.95rem;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 14px;
            transition: all 0.3s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }
        
        .stTextArea > div > div > textarea:focus {
            border-color: #1f77b4;
            box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.1), 0 4px 12px rgba(31, 119, 180, 0.15);
            background: #ffffff;
        }
        
        /* Enhanced navigation button styling */
        .stButton > button {
            border-radius: 10px;
            border: none;
            transition: all 0.3s ease;
            font-weight: 700;
            background: linear-gradient(135deg, #1f77b4, #4a90e2);
            color: white;
            padding: 14px 28px;
            font-size: 1.05rem;
            box-shadow: 0 4px 12px rgba(31, 119, 180, 0.3);
            position: relative;
            z-index: 100;
            letter-spacing: 0.5px;
        }
        
        .stButton > button:hover {
            box-shadow: 0 6px 20px rgba(31, 119, 180, 0.4);
            transform: translateY(-3px);
            background: linear-gradient(135deg, #1a6ca8, #4088d4);
        }
        
        .stButton > button:disabled {
            background: linear-gradient(135deg, #6c757d, #adb5bd);
            transform: none;
            box-shadow: 0 2px 6px rgba(0,0,0,0.2);
            opacity: 0.6;
        }
        
        /* Special styling for navigation buttons */
        .stButton > button[kind="secondary"] {
            background: linear-gradient(135deg, #28a745, #20c997);
            font-size: 0.95rem;
            padding: 10px 20px;
        }
        
        .stButton > button[kind="secondary"]:hover {
            background: linear-gradient(135deg, #218838, #1ea080);
        }
        
        /* Enhanced segmented control for navigation */
        .stSegmentedControl {
            margin: 16px 0;
            padding: 8px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 12px;
            border: 2px solid #dee2e6;
            box-shadow: 0 3px 8px rgba(0,0,0,0.1);
        }
        
        .stSegmentedControl > div {
            gap: 4px;
        }
        
        .stSegmentedControl button {
            border-radius: 8px !important;
            font-weight: 600 !important;
            transition: all 0.2s ease !important;
            border: 1px solid transparent !important;
        }
        
        .stSegmentedControl button:hover {
            transform: translateY(-1px) !important;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.2) !important;
        }
        
        .stSegmentedControl button[aria-selected="true"] {
            background: linear-gradient(135deg, #1f77b4, #4a90e2) !important;
            color: white !important;
            box-shadow: 0 3px 8px rgba(31, 119, 180, 0.3) !important;
        }
        
        /* Enhanced alert styling */
        .stAlert {
            border-radius: 10px;
            border: none;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            font-weight: 600;
        }
        
        /* Remove container borders globally */
        .element-container {
            border: none !important;
        }
        
        /* Custom scrollbars with gradient */
        ::-webkit-scrollbar {
            width: 10px;
        }
        
        ::-webkit-scrollbar-track {
            background: linear-gradient(180deg, #f1f1f1, #e9ecef);
            border-radius: 6px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: linear-gradient(180deg, #1f77b4, #4a90e2);
            border-radius: 6px;
            border: 2px solid #f1f1f1;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(180deg, #1a6ca8, #4088d4);
        }
        
        /* Enhanced success/celebration styling */
        .stSuccess {
            font-weight: 700;
            text-align: center;
            background: linear-gradient(135deg, #d4edda, #c3e6cb);
            border: 2px solid #28a745;
            border-radius: 10px;
        }
        
        /* Tab content with better padding */
        .stTabs [data-baseweb="tab-panel"] {
            padding: 20px 0;
        }
        
        /* Helper text with better styling */
        .stCaption {
            font-size: 0.85rem;
            color: #6c757d;
            font-style: italic;
            margin-top: 10px;
            padding: 8px 12px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 6px;
            border-left: 3px solid #1f77b4;
        }
        
        /* Container improvements */
        .stContainer {
            position: relative;
        }
        
        /* Form submit button area with gradient background */
        .stForm .stButton {
            position: sticky;
            bottom: 0;
            background: linear-gradient(180deg, rgba(255,255,255,0.9), rgba(255,255,255,1));
            padding: 12px 0;
            z-index: 100;
            border-radius: 10px 10px 0 0;
        }
        
        /* Enhanced checkbox styling for annotator selection */
        .stCheckbox > label {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 12px 16px;
            border-radius: 10px;
            border: 2px solid #e9ecef;
            margin-bottom: 8px;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
            display: block;
            font-weight: 500;
        }
        
        .stCheckbox > label:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(31, 119, 180, 0.2);
        }
        
        .stCheckbox > label[data-checked="true"] {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-color: #1f77b4;
            font-weight: 700;
            color: #1f77b4;
            box-shadow: 0 4px 12px rgba(31, 119, 180, 0.25);
        }
        
        /* Responsive improvements */
        @media (max-width: 768px) {
            .stRadio > div {
                gap: 0.7rem;
            }
            
            .stRadio > div > label {
                padding: 10px 14px;
                font-size: 0.9rem;
                max-width: 280px;
            }
            
            .stTabs [data-baseweb="tab"] {
                padding: 8px 14px;
                font-size: 0.9rem;
            }
            
            .stCheckbox > label {
                padding: 10px 14px;
                font-size: 0.9rem;
            }
        }
        
        /* Enhanced multiselect styling with better spacing */
        .stMultiSelect > div > div {
            border-radius: 10px;
            border: 2px solid #e9ecef;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            min-height: 45px;
        }
        
        .stMultiSelect > div > div:focus-within {
            border-color: #1f77b4;
            box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.1);
        }
        
        /* Custom styling for selected annotator badges */
        .annotator-badge {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border: 2px solid #1f77b4;
            border-radius: 8px;
            padding: 8px 12px;
            text-align: center;
            margin: 4px 0;
            font-size: 0.9rem;
            box-shadow: 0 2px 4px rgba(31, 119, 180, 0.1);
            transition: all 0.2s ease;
        }
        
        .annotator-badge:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 8px rgba(31, 119, 180, 0.2);
        }
        
        /* Remove unwanted borders and margins for cleaner look */
        .element-container > div > div > div > div {
            border: none !important;
        }
        
        /* Better spacing for form elements */
        .stForm > div {
            gap: 0 !important;
        }
        
        /* Enhanced pagination display */
        .stSegmentedControl button[disabled] {
            opacity: 0.5 !important;
            cursor: not-allowed !important;
            background: #f8f9fa !important;
            color: #6c757d !important;
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
        user_email = user.get('email', 'No email')
        # Fix email display - sometimes email might be stored differently
        if not user_email or user_email == 'No email':
            # Try alternative email field names
            user_email = user.get('Email', user.get('user_email', 'No email'))
        
        display_user_simple(user['name'], user_email, is_ground_truth=(user['role'] == 'admin'))
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