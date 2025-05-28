"""
Label Pizza Admin Streamlit App – Modular **v4**
================================================
*   Service layer           → business logic & data access
*   UI components           → one function per page/tab
*   Router / Auth           → handles login & role‑based menu

Run locally:
    streamlit run app.py

Admin seeded on first run:
    email    = zhiqiulin98@gmail.com
    password = zhiqiulin98

Requires:
    pip install streamlit sqlalchemy psycopg[binary] python-dotenv pandas
"""
from __future__ import annotations
import os
from typing import List, Optional, Dict, Callable

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from db import SessionLocal, engine
from models import Base
from services import (
    VideoService, ProjectService, SchemaService, 
    QuestionService, AuthService, QuestionGroupService
)

load_dotenv(".env")

###############################################################################
# 1.  CONSTANTS & ONE‑TIME SEED
###############################################################################

# Initialize database tables
Base.metadata.create_all(engine)

def _seed_admin() -> None:
    """Create hard‑coded admin if not present."""
    with SessionLocal() as s:
        AuthService.seed_admin(s)
_seed_admin()

###############################################################################
# 2.  UI COMPONENTS (Streamlit) ---------------------------------------------
###############################################################################

def page_login():
    st.title("Label Pizza :: Login")
    role_tabs = st.tabs(["Annotator", "Reviewer", "Admin"])
    roles = ["annotator", "reviewer", "admin"]
    for role, tab in zip(roles, role_tabs):
        with tab:
            email = st.text_input("Email", key=f"email_{role}")
            pwd   = st.text_input("Password", type="password", key=f"pwd_{role}")
            if st.button("Login", key=f"btn_{role}"):
                with SessionLocal() as s:
                    user = AuthService.authenticate(email, pwd, role, session=s)
                if user:
                    st.session_state["user"] = user
                    st.rerun()
                else:
                    st.error("Invalid credentials or role")

def tab_videos():
    st.subheader("Videos")
    with SessionLocal() as s:
        st.dataframe(VideoService.get_all_videos(s))
        with st.expander("Add video"):
            url = st.text_input("URL", key="v_url")
            if st.button("Save video") and url:
                try:
                    VideoService.add_video(url, session=s)
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

def tab_projects():
    st.subheader("Projects")
    with SessionLocal() as s:
        st.dataframe(ProjectService.get_all_projects(s))
        with st.expander("Create project"):
            pname = st.text_input("Name", key="p_name")
            schema_df = SchemaService.get_all_schemas(s)
            if len(schema_df) == 0:
                st.warning("No schemas available. Please create a schema first.")
                return
            sname = st.selectbox("Schema", schema_df["Name"])
            vids_df = VideoService.get_all_videos(s)
            if len(vids_df) == 0:
                st.warning("No videos available. Please add videos first.")
                return
            selected_vids = st.multiselect("Videos", vids_df["Video UID"])
            if st.button("Save project") and pname and sname:
                try:
                    sid = SchemaService.get_schema_id_by_name(sname, session=s)
                    v_ids = ProjectService.get_video_ids_by_uids(selected_vids, session=s)
                    ProjectService.create_project(pname, sid, v_ids, session=s)
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

def tab_schemas():
    st.subheader("Schemas")
    with SessionLocal() as s:
        df = SchemaService.get_all_schemas(s)
        st.dataframe(df)
        
        with st.expander("Create Schema"):
            schema_name = st.text_input("Schema Name", key="schema_name")
            if st.button("Create Schema") and schema_name:
                try:
                    SchemaService.create_schema(schema_name, {}, session=s)
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

        if len(df) == 0:
            st.warning("No schemas available. Create a schema first.")
            return

        sel_name = st.selectbox("Inspect", df["Name"].tolist())
        if sel_name:
            try:
                sid = SchemaService.get_schema_id_by_name(sel_name, session=s)
                questions_df = SchemaService.get_schema_questions(sid, session=s)
                st.write("Questions in this schema:", questions_df)
                
                # Add questions to schema
                all_questions = QuestionService.get_all_questions(s)
                if not questions_df.empty:
                    available_questions = all_questions[~all_questions["ID"].isin(questions_df["ID"])]
                else:
                    available_questions = all_questions
                
                if not available_questions.empty:
                    with st.expander("Add Questions to Schema"):
                        selected_qids = st.multiselect(
                            "Select questions to add",
                            options=available_questions["ID"].tolist(),
                            format_func=lambda x: f"{x}: {available_questions[available_questions['ID']==x]['Text'].iloc[0]}"
                        )
                        if st.button("Add Selected Questions"):
                            for qid in selected_qids:
                                SchemaService.add_question_to_schema(sid, qid, session=s)
                            st.rerun()
            except ValueError as e:
                st.error(str(e))

def tab_questions():
    st.subheader("Question Groups")
    with SessionLocal() as s:
        # Show question groups
        groups_df = QuestionGroupService.get_all_groups(s)
        st.dataframe(groups_df)
        
        # Create new group
        with st.expander("Create Question Group"):
            group_name = st.text_input("Group Name", key="group_name")
            group_desc = st.text_area("Description", key="group_desc")
            is_reusable = st.checkbox("Reusable Group", key="group_reusable")
            if st.button("Create Group") and group_name:
                try:
                    QuestionGroupService.create_group(group_name, group_desc, is_reusable, session=s)
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

        # Edit existing group
        with st.expander("Edit Question Group"):
            if not groups_df.empty:
                group_id = st.selectbox(
                    "Select Group to Edit",
                    options=groups_df["ID"].tolist(),
                    format_func=lambda x: f"{groups_df[groups_df['ID']==x]['Name'].iloc[0]}"
                )
                if group_id:
                    try:
                        group_details = QuestionGroupService.get_group_details(group_id, session=s)
                        new_title = st.text_input("New Title", value=group_details["title"], key="edit_group_title")
                        new_desc = st.text_area("New Description", value=group_details["description"], key="edit_group_desc")
                        new_reusable = st.checkbox("Reusable Group", value=group_details["is_reusable"], key="edit_group_reusable")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Update Group"):
                                try:
                                    QuestionGroupService.edit_group(
                                        group_id, new_title, new_desc, new_reusable, session=s
                                    )
                                    st.rerun()
                                except ValueError as e:
                                    st.error(str(e))
                        
                        with col2:
                            if group_details["is_archived"]:
                                if st.button("Unarchive Group"):
                                    try:
                                        QuestionGroupService.unarchive_group(group_id, session=s)
                                        st.rerun()
                                    except ValueError as e:
                                        st.error(str(e))
                            else:
                                if st.button("Archive Group"):
                                    try:
                                        QuestionGroupService.archive_group(group_id, session=s)
                                        st.rerun()
                                    except ValueError as e:
                                        st.error(str(e))
                    except ValueError as e:
                        st.error(str(e))

        # Questions section
        st.subheader("Add Question")
        qtext = st.text_input("Text", key="q_text")
        qtype = st.selectbox("Type", ["single", "description"], key="q_type")
        
        # Group selection with option to create new
        groups = groups_df[~groups_df["Archived"]]["Name"].tolist() if not groups_df.empty else []
        group_choice = st.selectbox(
            "Question Group",
            options=[""] + groups + ["+ Create New Group"],
            format_func=lambda x: x if x else "Use Question Text as Group"
        )
        if group_choice == "+ Create New Group":
            new_group = st.text_input("New Group Name")
            group_name = new_group if new_group else None
        else:
            group_name = group_choice if group_choice else None
        
        # Handle options based on question type
        options = []
        default = None
        if qtype == "single":
            st.write("Options:")
            # Add option boxes
            num_options = st.number_input("Number of options", min_value=1, max_value=10, value=2, key="num_opts")
            for i in range(num_options):
                option = st.text_input(f"Option {i+1}", key=f"opt_{i}")
                if option:
                    options.append(option)
            
            if options:
                default = st.selectbox("Default option", options=[""] + options)
        else:  # description type
            options = None
            default = None
        
        if st.button("Save question") and qtext:
            try:
                QuestionService.add_question(
                    qtext, qtype, group_name,
                    options if qtype == "single" else None,
                    default if qtype == "single" else None,
                    session=s
                )
                st.rerun()
            except ValueError as e:
                st.error(str(e))

        # View and edit questions in a group
        st.subheader("View Questions in Group")
        if not groups_df.empty:
            selected_group_id = st.selectbox(
                "Select Group to View",
                options=groups_df["ID"].tolist(),
                format_func=lambda x: f"{groups_df[groups_df['ID']==x]['Name'].iloc[0]}"
            )
            if selected_group_id:
                questions_df = QuestionGroupService.get_group_questions(selected_group_id, session=s)
                if not questions_df.empty:
                    st.dataframe(questions_df)
                    
                    # Edit selected question
                    question_text = st.selectbox(
                        "Select Question to Edit",
                        options=questions_df["Text"].tolist(),
                        format_func=lambda x: f"{x} ({questions_df[questions_df['Text']==x]['Type'].iloc[0]})"
                    )
                    
                    if question_text:
                        q = QuestionService.get_question_by_text(question_text, session=s)
                        if not q:
                            st.error("Question not found")
                            return
                            
                        new_text = st.text_input("New text", value=q.text, key="edit_text")
                        
                        # Handle options based on question type
                        new_opts = None
                        new_default = None
                        if q.type == "single":
                            st.write("Options:")
                            # Add option boxes
                            num_options = st.number_input("Number of options", min_value=1, max_value=10, 
                                                        value=len(q.options) if q.options else 2, key="edit_num_opts")
                            new_opts = []
                            for i in range(num_options):
                                option = st.text_input(
                                    f"Option {i+1}", 
                                    value=q.options[i] if q.options and i < len(q.options) else "",
                                    key=f"edit_opt_{i}"
                                )
                                if option:
                                    new_opts.append(option)
                            
                            if new_opts:
                                new_default = st.selectbox(
                                    "Default option",
                                    options=[""] + new_opts,
                                    index=new_opts.index(q.default_option) + 1 if q.default_option in new_opts else 0
                                )
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Update"):
                                try:
                                    QuestionService.edit_question(
                                        question_text, new_text, groups_df[groups_df["ID"]==selected_group_id]["Name"].iloc[0],
                                        new_opts if q.type == "single" else None,
                                        new_default if q.type == "single" else None,
                                        session=s
                                    )
                                    st.rerun()
                                except ValueError as e:
                                    st.error(str(e))
                        
                        with col2:
                            if q.is_archived:
                                if st.button("Unarchive Question"):
                                    try:
                                        QuestionService.unarchive_question(q.id, session=s)
                                        st.rerun()
                                    except ValueError as e:
                                        st.error(str(e))
                            else:
                                if st.button("Archive Question"):
                                    try:
                                        QuestionService.archive_question(q.id, session=s)
                                        st.rerun()
                                    except ValueError as e:
                                        st.error(str(e))
                else:
                    st.info("No questions in this group yet.")

def tab_users():
    st.subheader("User Management")
    with SessionLocal() as s:
        # Display all users
        users_df = AuthService.get_all_users(s)
        st.dataframe(users_df)
        
        # User management section
        with st.expander("Manage User"):
            if not users_df.empty:
                user_id = st.selectbox(
                    "Select User",
                    options=users_df["ID"].tolist(),
                    format_func=lambda x: f"{users_df[users_df['ID']==x]['User ID'].iloc[0]} ({users_df[users_df['ID']==x]['Email'].iloc[0]})"
                )
                
                if user_id:
                    user = users_df[users_df["ID"] == user_id].iloc[0]
                    
                    # Display all user details
                    st.write("### User Details")
                    st.write(f"**ID:** {user['ID']}")
                    st.write(f"**User ID:** {user['User ID']}")
                    st.write(f"**Email:** {user['Email']}")
                    st.write(f"**Password Hash:** {user['Password Hash']}")
                    st.write(f"**Role:** {user['Role']}")
                    st.write(f"**Active:** {user['Active']}")
                    st.write(f"**Created At:** {user['Created At']}")
                    
                    # Edit user details
                    st.write("### Edit User")
                    new_user_id = st.text_input("New User ID", value=user['User ID'])
                    new_email = st.text_input("New Email", value=user['Email'])
                    new_password = st.text_input("New Password", type="password")
                    new_role = st.selectbox(
                        "Role",
                        options=["human", "model", "admin"],
                        index=["human", "model", "admin"].index(user["Role"])
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Update User"):
                            try:
                                # Update user ID if changed
                                if new_user_id != user['User ID']:
                                    AuthService.update_user_id(user_id, new_user_id, session=s)
                                
                                # Update email if changed
                                if new_email != user['Email']:
                                    AuthService.update_user_email(user_id, new_email, session=s)
                                
                                # Update password if provided
                                if new_password:
                                    AuthService.update_user_password(user_id, new_password, session=s)
                                
                                # Update role if changed
                                if new_role != user['Role']:
                                    AuthService.update_user_role(user_id, new_role, session=s)
                                
                                st.success("User updated successfully!")
                                st.rerun()
                            except ValueError as e:
                                st.error(str(e))
                    
                    with col2:
                        status = "Active" if user["Active"] else "Inactive"
                        if st.button(f"Toggle {status}"):
                            try:
                                AuthService.toggle_user_active(user_id, session=s)
                                st.rerun()
                            except ValueError as e:
                                st.error(str(e))

        # Create new user section
        with st.expander("Create New User"):
            new_user_id = st.text_input("User ID")
            new_email = st.text_input("Email")
            new_password = st.text_input("Password", type="password")
            new_user_type = st.selectbox("User Type", options=["human", "model", "admin"])
            
            if st.button("Create User"):
                if not all([new_user_id, new_email, new_password]):
                    st.error("Please fill in all fields")
                else:
                    try:
                        AuthService.create_user(
                            user_id=new_user_id,
                            email=new_email,
                            password_hash=new_password,  # Note: In production, this should be hashed
                            user_type=new_user_type,
                            session=s
                        )
                        st.success("User created successfully!")
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))

def tab_project_assignments():
    st.subheader("Project Assignments")
    with SessionLocal() as s:
        # Display current assignments
        assignments_df = AuthService.get_project_assignments(s)
        
        # Add filtering options only if there are assignments
        if not assignments_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                filter_role = st.multiselect(
                    "Filter by Role",
                    options=["annotator", "reviewer", "admin", "model"],
                    default=[]
                )
            with col2:
                filter_project = st.multiselect(
                    "Filter by Project",
                    options=assignments_df["Project Name"].unique().tolist(),
                    default=[]
                )
            
            # Apply filters
            if filter_role:
                assignments_df = assignments_df[assignments_df["Role"].isin(filter_role)]
            if filter_project:
                assignments_df = assignments_df[assignments_df["Project Name"].isin(filter_project)]
        
        # Display assignments or message if empty
        if assignments_df.empty:
            st.info("No project assignments found.")
        else:
            st.dataframe(assignments_df)
        
        # Assignment management section
        with st.expander("Manage Assignments"):
            # Get all projects and users
            projects_df = ProjectService.get_all_projects(s)
            users_df = AuthService.get_all_users(s)
            
            if projects_df.empty:
                st.warning("No projects available. Please create a project first.")
                return
                
            if users_df.empty:
                st.warning("No users available. Please create users first.")
                return
            
            # Select project
            project_id = st.selectbox(
                "Select Project",
                options=projects_df["ID"].tolist(),
                format_func=lambda x: f"{projects_df[projects_df['ID']==x]['Name'].iloc[0]}"
            )
            
            # Select role
            role = st.selectbox(
                "Role",
                options=["annotator", "reviewer", "admin", "model"]
            )
            
            # Single user assignment
            st.subheader("Single User Assignment")
            user_id = st.selectbox(
                "Select User",
                options=users_df["ID"].tolist(),
                format_func=lambda x: f"{users_df[users_df['ID']==x]['User ID'].iloc[0]} ({users_df[users_df['ID']==x]['Email'].iloc[0]})"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Assign User"):
                    try:
                        AuthService.assign_user_to_project(user_id, project_id, role, session=s)
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))
            
            with col2:
                if st.button("Remove Assignment"):
                    try:
                        AuthService.remove_user_from_project(user_id, project_id, session=s)
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))
            
            # Bulk assignment
            st.subheader("Bulk Assignment")
            selected_users = st.multiselect(
                "Select Multiple Users",
                options=users_df["ID"].tolist(),
                format_func=lambda x: f"{users_df[users_df['ID']==x]['User ID'].iloc[0]} ({users_df[users_df['ID']==x]['Email'].iloc[0]})"
            )
            
            if st.button("Bulk Assign Users"):
                if not selected_users:
                    st.warning("Please select at least one user")
                else:
                    try:
                        AuthService.bulk_assign_users_to_project(selected_users, project_id, role, session=s)
                        st.success(f"Successfully assigned {len(selected_users)} users to project")
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))
            
            if st.button("Bulk Remove Users"):
                if not selected_users:
                    st.warning("Please select at least one user")
                else:
                    try:
                        AuthService.bulk_remove_users_from_project(selected_users, project_id, session=s)
                        st.success(f"Successfully removed {len(selected_users)} users from project")
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))

###############################################################################
# 3.  ROUTER  ----------------------------------------------------------------
###############################################################################

def admin_portal():
    tab_funcs: Dict[str, Callable[[], None]] = {
        "Videos": tab_videos,
        "Projects": tab_projects,
        "Schemas": tab_schemas,
        "Questions": tab_questions,
        "Users": tab_users,
        "Project Assignments": tab_project_assignments,
    }
    tab_labels = list(tab_funcs.keys())
    pages = st.tabs(tab_labels)
    for label, page in zip(tab_labels, pages):
        with page:
            tab_funcs[label]()

def main():
    st.set_page_config(page_title="Label Pizza Admin", layout="wide")

    if "user" not in st.session_state:
        page_login()
        return

    user = st.session_state["user"]
    st.sidebar.write(f"Logged in as {user['name']} ({user['role']})")
    if st.sidebar.button("Logout"):
        del st.session_state["user"]
        st.rerun()

    if user["role"] == "admin":
        admin_portal()
    else:
        st.write("🚧 Workflows for annotators/reviewers not yet implemented.")

if __name__ == "__main__":
    main()