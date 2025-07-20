import json
import os

def create_projects_json():
    """
    创建一个包含34个项目的JSON文件，每个项目都是一个字典
    """
    
    # 读取原始视频列表
    with open('CameraShotcomp/ShotComposition_videos.json', 'r', encoding='utf-8') as f:
        videos = json.load(f)
    
    print(f"总视频数量: {len(videos)}")
    
    # 计算分割参数
    total_videos = len(videos)
    videos_per_project = 30
    num_projects = 44
    
    # 计算最后一个项目的视频数量
    videos_in_last_project = total_videos - (videos_per_project * (num_projects - 1))
    
    print(f"前33个项目每个包含: {videos_per_project} 个视频")
    print(f"最后一个项目(33)包含: {videos_in_last_project} 个视频")
    
    # 创建项目列表
    projects = []
    start_index = 0
    
    for project_num in range(num_projects):
        # 计算当前项目的视频数量
        if project_num == num_projects - 1:  # 最后一个项目
            end_index = start_index + videos_in_last_project
        else:
            end_index = start_index + videos_per_project
        
        # 获取当前项目的视频列表
        project_videos = videos[start_index:end_index]
        cur_num = project_num + 38
        # 创建项目字典
        project = {
            "project_name": f"Shot Composition {cur_num}",
            "description": "Videos that shot composition has been done.",
            "schema_name": "Shot Composition",
            "videos": project_videos,
            "is_active": True
        }
        
        projects.append(project)
        
        print(f"项目 {project_num}: {len(project_videos)} 个视频")
        
        # 更新起始索引
        start_index = end_index
    
    # 保存到JSON文件
    output_file = "./CameraShotcomp/ShotComposition_Projects.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(projects, f, indent=2, ensure_ascii=False)
    
    print(f"\n所有项目已保存到 '{output_file}'")
    
    # 验证结果
    print("\n验证分割结果:")
    total_after_split = sum(len(project["videos"]) for project in projects)
    print(f"分割后总视频数: {total_after_split}")
    print(f"原始视频数: {len(videos)}")
    print(f"分割是否正确: {total_after_split == len(videos)}")
    
    # 显示前几个项目的结构示例
    print(f"\n项目结构示例 (前3个项目):")
    for i, project in enumerate(projects[:3]):
        print(f"\n项目 {i}:")
        print(f"  项目名称: {project['project_name']}")
        print(f"  描述: {project['description']}")
        print(f"  模式名称: {project['schema_name']}")
        print(f"  视频数量: {len(project['videos'])}")
        print(f"  是否激活: {project['is_active']}")
        print(f"  前3个视频: {project['videos'][:3]}")

def split_json_file(input_file_path, output_dir, items_per_chunk=500):
    """
    Split JSON file into multiple smaller files with specified number of items per file
    
    Args:
        input_file_path (str): Path to input JSON file
        output_dir (str): Path to output directory
        items_per_chunk (int): Number of items per file, default 500
    """
    from pathlib import Path
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Read JSON file
    with open(input_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Get base filename without extension
    base_filename = Path(input_file_path).stem
    
    # Calculate number of chunks needed
    total_items = len(data)
    num_chunks = (total_items + items_per_chunk - 1) // items_per_chunk
    
    print(f"Total items: {total_items}")
    print(f"Items per file: {items_per_chunk}")
    print(f"Will split into {num_chunks} files")
    
    # Split data and save
    for i in range(num_chunks):
        start_idx = i * items_per_chunk
        end_idx = min((i + 1) * items_per_chunk, total_items)
        
        chunk_data = data[start_idx:end_idx]
        
        # Generate output filename
        output_filename = f"{base_filename}_part_{i+1:03d}.json"
        output_path = os.path.join(output_dir, output_filename)
        
        # Save split data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved: {output_filename} (contains {len(chunk_data)} items)")
    
    print(f"\nSplit complete! Files saved in: {output_dir}")



def create_assignments_from_users(users_file_path, output_file_path, start_project_num=0, end_project_num=90):
    """
    Create assignments.json items from users.json, excluding admin users
    Creates assignments for multiple projects from Camera Movement 0 to Camera Movement 90
    
    Args:
        users_file_path (str): Path to users.json file
        output_file_path (str): Path to output assignments file
        start_project_num (int): Starting project number
        end_project_num (int): Ending project number
    """
    
    # Read users file
    with open(users_file_path, 'r', encoding='utf-8') as f:
        users = json.load(f)
    
    # Filter out admin users
    non_admin_users = [user for user in users if user.get('user_type') != 'admin']
    
    # Create assignments for each user and each project
    assignments = []
    
    for user in non_admin_users:
        for project_num in range(start_project_num, end_project_num + 1):
            project_name = f"Camera Movement {project_num}"
            
            # Create assignment item
            assignment = {
                "user_name": user['user_id'],
                "project_name": project_name,
                "role": "annotator",
                "user_weight": 1.0,
                "is_active": user.get('is_active', True)
            }
            
            assignments.append(assignment)
    
    # Save assignments to file
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(assignments, f, indent=2, ensure_ascii=False)
    
    # Print summary
    admin_count = sum(1 for user in users if user.get('user_type') == 'admin')
    total_users = len(users)
    non_admin_users_count = len(non_admin_users)
    total_projects = end_project_num - start_project_num + 1
    total_assignments = len(assignments)
    
    print(f"Created {total_assignments} assignments")
    print(f"Output saved to: {output_file_path}")
    
    print(f"\nSummary:")
    print(f"Total users: {total_users}")
    print(f"Admin users (excluded): {admin_count}")
    print(f"Non-admin users (included): {non_admin_users_count}")
    print(f"Projects: Camera Movement {start_project_num} to Camera Movement {end_project_num} ({total_projects} projects)")
    print(f"Total assignments: {non_admin_users_count} users × {total_projects} projects = {total_assignments}")
    

if __name__ == "__main__":
    
    # Make tannotations corresponding to the projects
    import glob
    import json
    import os
    # create_projects_json()

    
    paths = glob.glob('./CameraLighting/reviews_project_1/*_shadow_patterns*.json')
    for path in paths:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for item in data:
            ans = item['answers']['Describe shadows and gobes']
            del item['answers']['Describe shadows and gobes']
            item['answers']['Describe shadows and gobos'] = ans
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    # from label_pizza.verify import check_camera_movement
    # with open('./CameraMotion/annotations/CameraMotion_Camera_Movement.json', 'r', encoding='utf-8') as f:
    #     data = json.load(f)
    # for item in data:
    #     answers = item['answers']
    #     try:
    #         check_camera_movement(answers)
    #     except ValueError as e:
    #         print('--------------------------------')
    #         print(e)
    #         print(item['video_uid'])
    #         print(item['user_name'])
    #         print('--------------------------------')
    

    # # read all video: project mapping
    # camera_project = {}
    # with open('./CameraMotion/Camera_Motion_Done_Projects.json', 'r', encoding='utf-8') as f:
    #     projects = json.load(f)
    # for project in projects:
    #     for video in project['videos']:
    #         camera_project[video] = project['project_name']
    # with open('./CameraMotion/Camera_Motion_Projects.json', 'r', encoding='utf-8') as f:
    #     done_projects = json.load(f)
    # for project in done_projects:
    #     for video in project['videos']:
    #         camera_project[video] = project['project_name']
            
            
    # paths = glob.glob('./CameraMotion/annotations/*.json')
    # for path in paths:
    #     res = []
    #     with open(path, 'r', encoding='utf-8') as f:
    #         data = json.load(f)
    #     for item in data:
    #         if item['video_uid'] not in camera_project:
    #             continue
    #         item['project_name'] = camera_project[item['video_uid']]
    #         res.append(item)
    #     with open(path, 'w', encoding='utf-8') as f:
    #         json.dump(res, f, indent=2, ensure_ascii=False)
    
    # with open('./CameraMotion/annotations/CameraMotion_Camera_Effects.json', 'r', encoding='utf-8') as f:
    #     data = json.load(f)
    # videos = set()
    # for item in data:
    #     videos.add(item['video_uid'])
    # with open('./CameraMotion/annotations/Camera_Movement_shot_transtion.json', 'r', encoding='utf-8') as f:
    #     data = json.load(f)
    #     res = []
    #     cur_videos = set()
    #     for item in data:
    #         if item['video_uid'] not in videos:
    #             continue
    #         res.append(item)
    #     with open('./CameraMotion/annotations/Camera_Movement_shot_transtion.json', 'w', encoding='utf-8') as f:
    #         json.dump(res, f, indent=2, ensure_ascii=False)
            
    # paths = glob.glob('./CameraMotion/annotations/*.json')
    # for path in paths:
    #     res = []
    #     with open(path, 'r', encoding='utf-8') as f:
    #         data = json.load(f)
            
    #         for item in data:
    #             video_uid = item['video_uid']
    #             if video_uid not in camera_project:
    #                 continue
    #             project_name = camera_project[video_uid]
    #             item['project_name'] = project_name
    #             res.append(item)
    #     target_path = path.replace('annotations', 'annotations_processed')
    #     os.makedirs(os.path.dirname(target_path), exist_ok=True)
    #     with open(target_path, 'w', encoding='utf-8') as f:
    #         json.dump(res, f, indent=2, ensure_ascii=False)

    # create_assignments_from_users('./CameraMotion/users.json', './CameraMotion/assignments.json', 0, 90)