import argparse
import os
import json
from pathlib import Path

def run_label_pizza_setup(database_url_name, folder_path):
    """
    Run the complete label pizza setup process.
    Only processes files/folders that exist.
    
    Args:
        database_url_name (str): Database URL name
        folder_path (str): Base folder path containing all data files
    """
    from label_pizza.verification_registry import register_workspace
    register_workspace(folder_path)
    update_verification_config(folder_path)
    
    # Initialize database (Important to do this before importing utils which uses the database session)
    from label_pizza.db import init_database
    init_database(database_url_name) # This will initialize the database; importantly to do this before importing utils which uses the database session

    # from label_pizza.upload_utils import upload_videos, upload_users, upload_question_groups, upload_schemas, create_projects, bulk_assign_users, batch_upload_annotations, batch_upload_reviews, apply_simple_video_configs

    from label_pizza.sync_utils import sync_videos
    sync_videos(videos_path=os.path.join(folder_path, "videos.json"))
    
    from label_pizza.sync_utils import sync_users
    sync_users(users_path=os.path.join(folder_path, "users.json"))
    
    from label_pizza.sync_utils import sync_question_groups
    sync_question_groups(question_groups_folder=os.path.join(folder_path, "question_groups"))
    
    from label_pizza.sync_utils import sync_schemas
    sync_schemas(schemas_path=os.path.join(folder_path, "schemas.json"))

    from label_pizza.sync_utils import sync_projects
    sync_projects(projects_path=os.path.join(folder_path, "projects.json"))
    
    from label_pizza.sync_utils import sync_project_groups
    sync_project_groups(project_groups_path=os.path.join(folder_path, "project_groups.json"))
    
    from label_pizza.sync_utils import sync_users_to_projects
    sync_users_to_projects(assignment_path=os.path.join(folder_path, "assignments.json"))
    
    from label_pizza.sync_utils import sync_annotations
    sync_annotations(annotations_folder=os.path.join(folder_path, "annotations"), max_workers=8)

    from label_pizza.sync_utils import sync_ground_truths
    sync_ground_truths(ground_truths_folder=os.path.join(folder_path, "ground_truths"), max_workers=10)

def update_verification_config(workspace_path: str):
    """Add workspace path to verification config if not already present"""
    config_file = Path("verification_config.json")
    
    # Load existing config
    if config_file.exists():
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except json.JSONDecodeError:
            config = {"workspace_paths": []}
    else:
        config = {"workspace_paths": []}
    
    # Ensure workspace_paths is a list
    if "workspace_paths" not in config:
        config["workspace_paths"] = []
    
    # Normalize workspace path for duplicate detection
    workspace_abs = Path(workspace_path).resolve()
    project_root = Path.cwd().resolve()
    
    # Try to make it relative to project root, otherwise keep absolute
    try:
        workspace_rel = workspace_abs.relative_to(project_root)
        workspace_to_store = str(workspace_rel)
    except ValueError:
        # Path is outside project root, store as absolute
        workspace_to_store = str(workspace_abs)
    
    # Check for duplicates by resolving all existing paths
    existing_resolved = set()
    for existing_path in config["workspace_paths"]:
        try:
            # Resolve existing path (could be relative or absolute)
            if Path(existing_path).is_absolute():
                resolved = Path(existing_path).resolve()
            else:
                resolved = (project_root / existing_path).resolve()
            existing_resolved.add(resolved)
        except (OSError, ValueError):
            # Keep invalid paths as-is for manual cleanup
            pass
    
    # Only add if this workspace isn't already present
    if workspace_abs not in existing_resolved:
        config["workspace_paths"].append(workspace_to_store)
        
        # Save config
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"Added workspace to config: {workspace_to_store}")
    else:
        print(f"Workspace already in config: {workspace_to_store}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--database-url-name", default="DBURL")
    parser.add_argument("--folder-path", default="./workspace", help="Folder path containing data files")
    args, _ = parser.parse_known_args()
    
    run_label_pizza_setup(args.database_url_name, args.folder_path)
