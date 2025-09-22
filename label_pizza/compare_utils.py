"""
JSON File Comparison Functions for Label Pizza
==============================================
Functions to compare JSON files between two folders and generate diff reports.
"""

import json
import os
from typing import Dict, List, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime


def compare_videos(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare videos.json files between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing videos.json
        folder2_path (str): Path to second folder containing videos.json
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if files are identical, False if differences found
        
    Output:
        Creates diff_videos.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Videos only in folder1
            "folder2_only": [...],  # Videos only in folder2
            "different": [...],     # Videos with same video_uid but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
    """
    file1_path = Path(folder1_path) / "videos.json"
    file2_path = Path(folder2_path) / "videos.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["video_uid"]: item for item in data1}
    dict2 = {item["video_uid"]: item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "video_uid": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_videos.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_users(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare users.json files between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing users.json
        folder2_path (str): Path to second folder containing users.json
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if files are identical, False if differences found
        
    Output:
        Creates diff_users.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Users only in folder1
            "folder2_only": [...],  # Users only in folder2  
            "different": [...],     # Users with same user_id but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
    """
    file1_path = Path(folder1_path) / "users.json"
    file2_path = Path(folder2_path) / "users.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["user_id"]: item for item in data1}
    dict2 = {item["user_id"]: item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "user_id": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_users.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_schemas(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare schemas.json files between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing schemas.json
        folder2_path (str): Path to second folder containing schemas.json
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if files are identical, False if differences found
        
    Output:
        Creates diff_schemas.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Schemas only in folder1
            "folder2_only": [...],  # Schemas only in folder2
            "different": [...],     # Schemas with same schema_name but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int, 
                "differences_found": int
            }
        }
    """
    file1_path = Path(folder1_path) / "schemas.json"
    file2_path = Path(folder2_path) / "schemas.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["schema_name"]: item for item in data1}
    dict2 = {item["schema_name"]: item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "schema_name": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_schemas.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_projects(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare projects.json files between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing projects.json
        folder2_path (str): Path to second folder containing projects.json
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if files are identical, False if differences found
        
    Output:
        Creates diff_projects.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Projects only in folder1
            "folder2_only": [...],  # Projects only in folder2
            "different": [...],     # Projects with same project_name but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
    """
    file1_path = Path(folder1_path) / "projects.json"
    file2_path = Path(folder2_path) / "projects.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["project_name"]: item for item in data1}
    dict2 = {item["project_name"]: item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "project_name": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_projects.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_project_groups(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare project_groups.json files between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing project_groups.json
        folder2_path (str): Path to second folder containing project_groups.json
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if files are identical, False if differences found
        
    Output:
        Creates diff_project_groups.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Project groups only in folder1
            "folder2_only": [...],  # Project groups only in folder2
            "different": [...],     # Project groups with same project_group_name but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
    """
    file1_path = Path(folder1_path) / "project_groups.json"
    file2_path = Path(folder2_path) / "project_groups.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["project_group_name"]: item for item in data1}
    dict2 = {item["project_group_name"]: item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "project_group_name": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_project_groups.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_assignments(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare assignments.json files between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing assignments.json
        folder2_path (str): Path to second folder containing assignments.json
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if files are identical, False if differences found
        
    Output:
        Creates diff_assignments.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Assignments only in folder1
            "folder2_only": [...],  # Assignments only in folder2
            "different": [...],     # Assignments with same key but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
        
    Note:
        For assignments, the unique key is a combination of user_name + project_name + role
        since a user can have multiple assignments to different projects.
    """
    file1_path = Path(folder1_path) / "assignments.json"
    file2_path = Path(folder2_path) / "assignments.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    def make_assignment_key(item):
        return f"{item['user_name']}|{item['project_name']}|{item['role']}"
    
    dict1 = {make_assignment_key(item): item for item in data1}
    dict2 = {make_assignment_key(item): item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "key": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_assignments.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_question_groups(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare question_groups folders between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing question_groups folder
        folder2_path (str): Path to second folder containing question_groups folder
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if all question groups are identical, False if differences found
        
    Output:
        Creates diff_question_groups.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Question groups only in folder1
            "folder2_only": [...],  # Question groups only in folder2
            "different": [...],     # Question groups with same title but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
    """
    qg_folder1 = Path(folder1_path) / "question_groups"
    qg_folder2 = Path(folder2_path) / "question_groups"
    
    # Load all question groups from both folders
    data1 = _load_all_files_in_folder(qg_folder1)
    data2 = _load_all_files_in_folder(qg_folder2)
    
    # Use title as unique key for question groups
    dict1 = {item["title"]: item for item in data1}
    dict2 = {item["title"]: item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "title": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_question_groups.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_annotations(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare annotations folders between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing annotations folder
        folder2_path (str): Path to second folder containing annotations folder
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if all annotations are identical, False if differences found
        
    Output:
        Creates diff_annotations.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Annotations only in folder1
            "folder2_only": [...],  # Annotations only in folder2
            "different": [...],     # Annotations with same key but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
        
    Note:
        For annotations, the unique key is a combination of video_uid + project_name + question_group_title + user_name
    """
    ann_folder1 = Path(folder1_path) / "annotations"
    ann_folder2 = Path(folder2_path) / "annotations"
    
    # Load all annotations from both folders
    data1 = _load_all_files_in_folder(ann_folder1)
    data2 = _load_all_files_in_folder(ann_folder2)
    
    def make_annotation_key(item):
        return f"{item['video_uid']}|{item['project_name']}|{item['question_group_title']}|{item['user_name']}"
    
    dict1 = {make_annotation_key(item): item for item in data1}
    dict2 = {make_annotation_key(item): item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "key": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_annotations.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_ground_truths(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare ground_truths folders between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first folder containing ground_truths folder
        folder2_path (str): Path to second folder containing ground_truths folder
        output_folder (str, optional): Folder to save diff report. If None, no file is written.
        
    Returns:
        bool: True if all ground truths are identical, False if differences found
        
    Output:
        Creates diff_ground_truths.json in output_folder with structure:
        {
            "identical": false,
            "folder1_only": [...],  # Ground truths only in folder1
            "folder2_only": [...],  # Ground truths only in folder2
            "different": [...],     # Ground truths with same key but different content
            "summary": {
                "folder1_count": int,
                "folder2_count": int,
                "differences_found": int
            }
        }
        
    Note:
        For ground truths, the unique key is a combination of video_uid + project_name + question_group_title + user_name
    """
    gt_folder1 = Path(folder1_path) / "ground_truths"
    gt_folder2 = Path(folder2_path) / "ground_truths"
    
    # Load all ground truths from both folders
    data1 = _load_all_files_in_folder(gt_folder1)
    data2 = _load_all_files_in_folder(gt_folder2)
    
    def make_ground_truth_key(item):
        return f"{item['video_uid']}|{item['project_name']}|{item['question_group_title']}|{item['user_name']}"
    
    dict1 = {make_ground_truth_key(item): item for item in data1}
    dict2 = {make_ground_truth_key(item): item for item in data2}
    
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    folder1_only = [dict1[key] for key in keys1 - keys2]
    folder2_only = [dict2[key] for key in keys2 - keys1]
    different = []
    
    for key in keys1 & keys2:
        if dict1[key] != dict2[key]:
            different.append({
                "key": key,
                "folder1_data": dict1[key],
                "folder2_data": dict2[key]
            })
    
    is_identical = len(folder1_only) == 0 and len(folder2_only) == 0 and len(different) == 0
    
    diff_report = {
        "identical": is_identical,
        "folder1_only": folder1_only,
        "folder2_only": folder2_only,
        "different": different,
        "summary": {
            "folder1_count": len(data1),
            "folder2_count": len(data2),
            "differences_found": len(different)
        }
    }
    
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)
        output_path = Path(output_folder) / "diff_ground_truths.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    return is_identical


def compare_workspace(folder1_path: str = None, folder2_path: str = None, output_folder: str = None) -> bool:
    """
    Compare workspace between two folders and generate diff report.
    
    Args:
        folder1_path (str): Path to first workspace folder
        folder2_path (str): Path to second workspace folder
        output_folder (str, optional): Folder to save all diff reports. If None, no files are written.
        
    Returns:
        bool: True if all components are identical, False if differences found
    """
    is_identical = compare_videos(folder1_path, folder2_path, output_folder)
    is_identical = compare_users(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_schemas(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_projects(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_project_groups(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_assignments(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_question_groups(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_annotations(folder1_path, folder2_path, output_folder) and is_identical
    is_identical = compare_ground_truths(folder1_path, folder2_path, output_folder) and is_identical
    return is_identical


def _load_all_files_in_folder(folder_path: Path) -> List[Dict]:
    """Load all JSON files in a folder and return combined list of items."""
    all_items = []
    
    if not folder_path.exists():
        return all_items
    
    for json_file in folder_path.glob("*.json"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both single dict and list of dicts
            if isinstance(data, dict):
                all_items.append(data)
            elif isinstance(data, list):
                all_items.extend(data)
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
    
    return all_items


def _read_json_file(file_path: Path) -> List[Dict]:
    """Read and parse JSON file."""
    if not file_path.exists():
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError(f"JSON file {file_path} does not contain a list")
    
    return data