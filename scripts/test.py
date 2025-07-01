

import sys
from pathlib import Path
import os
import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from label_pizza.db import init_database

init_database()
from sqlalchemy import select, insert, update, func, delete, exists, join, distinct, and_, or_, case, text
from sqlalchemy.orm import Session, selectinload, joinedload, contains_eager  

from label_pizza.db import engine
from label_pizza.models import Base
from label_pizza.services import AuthService

from upload_utils import *
from import_annotations import *
from label_pizza.models import *
import json
import glob
from collections import defaultdict


def reset_database():
    """
    Drop all tables and recreate them using existing session.
    """
    print("🗑️ Dropping all tables...")
    
    # 设置更长的超时时间
    with engine.connect() as conn:
        # 设置语句超时为5分钟
        conn.execute(text("SET statement_timeout = '300000'"))  # 300秒 = 5分钟
        conn.commit()
    
    try:
        # 先禁用外键约束检查
        with engine.connect() as conn:
            conn.execute(text("SET session_replication_role = replica"))
            conn.commit()
        
        # Drop all tables
        Base.metadata.drop_all(engine)
        print("✅ Dropped all tables")
        
        # 重新启用外键约束检查
        with engine.connect() as conn:
            conn.execute(text("SET session_replication_role = DEFAULT"))
            conn.commit()
        
    except Exception as e:
        print(f"⚠️ Error during drop_all: {e}")
        # 尝试手动删除表
        print("🔄 Trying manual table deletion...")
        manual_drop_tables()
    
    # Create all tables
    print("🏗️ Creating all tables...")
    Base.metadata.create_all(engine)
    print("✅ Created all tables")
    
    print("🎉 Database reset complete!")

def manual_drop_tables():
    """
    手动删除表，处理超时问题
    """
    try:
        with engine.connect() as conn:
            # 获取所有表名
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """))
            tables = [row[0] for row in result]
            
            print(f"📋 Found {len(tables)} tables to drop")
            
            # 逐个删除表
            for table in tables:
                try:
                    print(f"��️ Dropping table: {table}")
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    conn.commit()
                    print(f"✅ Dropped table: {table}")
                except Exception as e:
                    print(f"⚠️ Failed to drop table {table}: {e}")
                    # 继续尝试其他表
                    continue
            
            print("✅ Manual table deletion completed")
            
    except Exception as e:
        print(f"❌ Manual table deletion failed: {e}")
        raise

def reset_and_create_admin(email="admin@example.com", password="password123", user_id="Admin"):
    """
    Reset database and create admin user.
    """
    # Reset database structure
    from label_pizza.init_or_reset_db import create_all_tables
    
    # Create admin user using service
    with SessionLocal() as session:
        try:
            create_all_tables(engine)
            AuthService.seed_admin(session, email, password, user_id)
            print(f"✅ Admin user created: {email}")
        except Exception as e:
            print(f"⚠️ Admin user creation failed: {e}")


def delete_all_table_data(confirm=True, use_truncate=True):
    """
    删除所有表的数据
    
    Args:
        confirm (bool): 是否需要确认
        use_truncate (bool): 是否使用TRUNCATE（更快但不可回滚）
    """
    print("��️ Deleting all data from all tables...")
    
    try:
        with engine.connect() as conn:
            # 设置超时时间
            conn.execute(text("SET statement_timeout = '300000'"))  # 5分钟
            conn.commit()
            
            # 获取所有表名
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """))
            tables = [row[0] for row in result]
            
            print(f"📋 Found {len(tables)} tables to clear")
            
            if confirm:
                response = input(f"⚠️ Are you sure you want to delete ALL data from ALL {len(tables)} tables? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("❌ Operation cancelled")
                    return False
            
            # 先禁用外键约束检查
            print("🔄 Disabling foreign key constraints...")
            conn.execute(text("SET session_replication_role = replica"))
            conn.commit()
            
            deleted_tables = []
            failed_tables = []
            
            for table in tables:
                try:
                    print(f"��️ Clearing table: {table}")
                    
                    if use_truncate:
                        # 使用TRUNCATE（更快）
                        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                        conn.commit()
                        print(f"✅ Truncated: {table}")
                    else:
                        # 使用DELETE
                        result = conn.execute(text(f"DELETE FROM {table}"))
                        deleted_count = result.rowcount
                        conn.commit()
                        print(f"✅ Deleted {deleted_count:,} records from: {table}")
                    
                    deleted_tables.append(table)
                    
                except Exception as e:
                    print(f"❌ Failed to clear {table}: {e}")
                    failed_tables.append(table)
                    # 继续处理其他表
                    continue
            
            # 重新启用外键约束检查
            print("🔄 Re-enabling foreign key constraints...")
            conn.execute(text("SET session_replication_role = DEFAULT"))
            conn.commit()
            
            print("\n" + "="*50)
            print("📊 Summary:")
            print(f"✅ Successfully cleared: {len(deleted_tables)} tables")
            if failed_tables:
                print(f"❌ Failed to clear: {len(failed_tables)} tables")
                for table in failed_tables:
                    print(f"   - {table}")
            
            return len(failed_tables) == 0
            
    except Exception as e:
        print(f"❌ Failed to delete all table data: {e}")
        return False

def delete_all_table_data_batch(confirm=True):
    """
    分批删除所有表的数据，适用于大表
    """
    print("🗑️ Batch deleting all data from all tables...")
    
    try:
        with engine.connect() as conn:
            # 设置超时时间
            conn.execute(text("SET statement_timeout = '300000'"))
            conn.commit()
            
            # 获取所有表名
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """))
            tables = [row[0] for row in result]
            
            print(f"📋 Found {len(tables)} tables to clear")
            
            if confirm:
                response = input(f"⚠️ Are you sure you want to delete ALL data from ALL {len(tables)} tables? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("❌ Operation cancelled")
                    return False
            
            # 先禁用外键约束检查
            print("🔄 Disabling foreign key constraints...")
            conn.execute(text("SET session_replication_role = replica"))
            conn.commit()
            
            deleted_tables = []
            failed_tables = []
            
            for table in tables:
                try:
                    print(f"🗑️ Batch clearing table: {table}")
                    
                    # 获取记录数
                    try:
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        total_count = count_result.scalar()
                    except:
                        total_count = "unknown"
                    
                    if total_count == 0:
                        print(f"ℹ️ {table} is already empty")
                        deleted_tables.append(table)
                        continue
                    
                    count_str = f"{total_count:,}" if isinstance(total_count, int) else str(total_count)
                    print(f"   �� Records to delete: {count_str}")
                    
                    # 分批删除
                    deleted_count = 0
                    batch_size = 1000
                    
                    while True:
                        result = conn.execute(text(f"DELETE FROM {table} LIMIT {batch_size}"))
                        batch_deleted = result.rowcount
                        
                        if batch_deleted == 0:
                            break
                        
                        deleted_count += batch_deleted
                        conn.commit()
                        
                        if isinstance(total_count, int):
                            print(f"   🔄 Deleted {deleted_count:,}/{total_count:,} records...")
                        else:
                            print(f"   🔄 Deleted {deleted_count:,} records...")
                        
                        if batch_deleted < batch_size:
                            break
                    
                    print(f"✅ Cleared {deleted_count:,} records from: {table}")
                    deleted_tables.append(table)
                    
                except Exception as e:
                    print(f"❌ Failed to clear {table}: {e}")
                    failed_tables.append(table)
                    continue
            
            # 重新启用外键约束检查
            print("🔄 Re-enabling foreign key constraints...")
            conn.execute(text("SET session_replication_role = DEFAULT"))
            conn.commit()
            
            print("\n" + "="*50)
            print("📊 Summary:")
            print(f"✅ Successfully cleared: {len(deleted_tables)} tables")
            if failed_tables:
                print(f"❌ Failed to clear: {len(failed_tables)} tables")
                for table in failed_tables:
                    print(f"   - {table}")
            
            return len(failed_tables) == 0
            
    except Exception as e:
        print(f"❌ Failed to delete all table data: {e}")
        return False

def clear_database_completely(confirm=True):
    """
    完全清空数据库（删除所有表并重新创建）
    """
    print("🗑️ Completely clearing database...")
    
    if confirm:
        response = input("⚠️ This will DELETE ALL TABLES and recreate them. Are you sure? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ Operation cancelled")
            return False
    
    try:
        # 使用现有的reset_database函数
        reset_database()
        print("✅ Database completely cleared and recreated")
        return True
        
    except Exception as e:
        print(f"❌ Failed to clear database: {e}")
        return False

def delete_table_data_with_progress(table_name, confirm=True, batch_size=1000):
    """
    删除指定表的所有数据，每删除1000条提醒一次
    
    Args:
        table_name (str): 要删除数据的表名
        confirm (bool): 是否需要确认
        batch_size (int): 每批删除的记录数，默认1000
    """
    print(f"️ Deleting all data from table: {table_name}")
    
    try:
        with engine.connect() as conn:
            # 设置超时时间
            conn.execute(text("SET statement_timeout = '300000'"))  # 5分钟
            conn.commit()
            
            # 先检查表是否存在
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            """))
            
            if result.scalar() == 0:
                print(f"❌ Table '{table_name}' does not exist")
                return False
            
            # 获取当前记录数
            try:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                current_count = count_result.scalar()
            except Exception as e:
                print(f"⚠️ Could not count records in {table_name}: {e}")
                print("🔄 Proceeding with deletion anyway...")
                current_count = "unknown"
            
            if current_count == 0:
                print(f"ℹ️ Table '{table_name}' is already empty")
                return True
            
            count_str = f"{current_count:,}" if isinstance(current_count, int) else str(current_count)
            print(f" Current records in {table_name}: {count_str}")
            
            if confirm:
                response = input(f"⚠️ Are you sure you want to delete ALL records from '{table_name}'? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("❌ Operation cancelled")
                    return False
            
            # 分批删除，每1000条提醒一次
            deleted_count = 0
            batch_count = 0
            
            while True:
                result = conn.execute(text(f"DELETE FROM {table_name} LIMIT {batch_size}"))
                batch_deleted = result.rowcount
                
                if batch_deleted == 0:
                    break
                
                deleted_count += batch_deleted
                batch_count += 1
                conn.commit()
                
                # 每删除1000条提醒一次
                if batch_count % 1 == 0:  # 每批都提醒（因为batch_size=1000）
                    if isinstance(current_count, int):
                        remaining = current_count - deleted_count
                        print(f"�� Progress: {deleted_count:,}/{current_count:,} records deleted ({remaining:,} remaining)")
                    else:
                        print(f"�� Progress: {deleted_count:,} records deleted so far...")
                
                if batch_deleted < batch_size:
                    break
            
            print(f"✅ Successfully deleted {deleted_count:,} records from '{table_name}'")
            return True
            
    except Exception as e:
        print(f"❌ Failed to delete data from {table_name}: {e}")
        return False

def delete_all_table_data_with_progress(confirm=True, batch_size=1000):
    """
    删除所有表的数据，每删除1000条提醒一次
    
    Args:
        confirm (bool): 是否需要确认
        batch_size (int): 每批删除的记录数，默认1000
    """
    print("️ Deleting all data from all tables...")
    
    try:
        with engine.connect() as conn:
            # 设置超时时间
            conn.execute(text("SET statement_timeout = '300000'"))  # 5分钟
            conn.commit()
            
            # 获取所有表名
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """))
            tables = [row[0] for row in result]
            
            print(f"📋 Found {len(tables)} tables to clear")
            
            if confirm:
                response = input(f"⚠️ Are you sure you want to delete ALL data from ALL {len(tables)} tables? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("❌ Operation cancelled")
                    return False
            
            # 先禁用外键约束检查
            print("🔄 Disabling foreign key constraints...")
            conn.execute(text("SET session_replication_role = replica"))
            conn.commit()
            
            deleted_tables = []
            failed_tables = []
            
            for table in tables:
                try:
                    print(f"\n��️ Processing table: {table}")
                    
                    # 获取记录数
                    try:
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        total_count = count_result.scalar()
                    except:
                        total_count = "unknown"
                    
                    if total_count == 0:
                        print(f"ℹ️ {table} is already empty")
                        deleted_tables.append(table)
                        continue
                    
                    count_str = f"{total_count:,}" if isinstance(total_count, int) else str(total_count)
                    print(f"   Records to delete: {count_str}")
                    
                    # 分批删除，每1000条提醒一次
                    deleted_count = 0
                    batch_count = 0
                    
                    while True:
                        result = conn.execute(text(f"DELETE FROM {table} LIMIT {batch_size}"))
                        batch_deleted = result.rowcount
                        
                        if batch_deleted == 0:
                            break
                        
                        deleted_count += batch_deleted
                        batch_count += 1
                        conn.commit()
                        
                        # 每删除1000条提醒一次
                        if batch_count % 1 == 0:  # 每批都提醒
                            if isinstance(total_count, int):
                                remaining = total_count - deleted_count
                                print(f"   �� Progress: {deleted_count:,}/{total_count:,} records deleted ({remaining:,} remaining)")
                            else:
                                print(f"   �� Progress: {deleted_count:,} records deleted so far...")
                        
                        if batch_deleted < batch_size:
                            break
                    
                    print(f"✅ Cleared {deleted_count:,} records from: {table}")
                    deleted_tables.append(table)
                    
                except Exception as e:
                    print(f"❌ Failed to clear {table}: {e}")
                    failed_tables.append(table)
                    continue
            
            # 重新启用外键约束检查
            print("🔄 Re-enabling foreign key constraints...")
            conn.execute(text("SET session_replication_role = DEFAULT"))
            conn.commit()
            
            print("\n" + "="*50)
            print("📊 Summary:")
            print(f"✅ Successfully cleared: {len(deleted_tables)} tables")
            if failed_tables:
                print(f"❌ Failed to clear: {len(failed_tables)} tables")
                for table in failed_tables:
                    print(f"   - {table}")
            
            return len(failed_tables) == 0
            
    except Exception as e:
        print(f"❌ Failed to delete all table data: {e}")
        return False

# Usage
if __name__ == "__main__":

    # name_email_mapping = {
    #     'Zhiqiu Lin': 'zhiqiul@andrew.cmu.edu',
    #     'Zhiqiu Lin ': 'zhiqiul@andrew.cmu.edu',
    #     'Hewei Wang': 'stephenw@andrew.cmu.edu',
    #     'Kewen Wu': 'kewenwu@andrew.cmu.edu',
    #     'Siyuan Cen': 'siyuancen096@gmail.com',
    #     'Tiffany Ling': 'ttiffanyyllingg@gmail.com',
    #     'Daniel Jiang': 'drjiang@andrew.cmu.edu',
    #     'hewen': 'chwen.1@outlook.com',
    #     'yongbo': 'yangyongbo716@outlook.com'
    # }
    
    # paths = glob.glob('./reviews/movement/*.json')
    
    # for path in paths:
    #     res = []
    #     with open(path, 'r') as f:
    #         data = json.load(f)
    #     for item in data:
    #         item['reviewer_email'] = name_email_mapping[item['reviewer_email']]
    #         res.append(item)
    #     with open(path, 'w') as f:
    #         json.dump(res, f, indent=2)

    
    # all_videos = {}
    # with open('./annotations/movement/Camera_Movement_motion_attributes.json', 'r') as f:
    #     data = json.load(f)
    #     for item in data:
    #         all_videos[item['video_uid']] = item['project_name']
    # print(len(all_videos))
    # res = []
    # with open('./annotations/movement/Camera_Movement_motion_effects.json', 'r') as f:
    #     data = json.load(f)
    #     for item in data:
    #         item['project_name'] = all_videos[item['video_uid']] if item['video_uid'] in all_videos else ''
    #         res.append(item)
    # with open('./annotations/movement/Camera_Movement_motion_effects.json', 'w') as f:
    #     json.dump(res, f, indent=2)
    
    # # 0. Reset database
    # reset_and_create_admin()
    
    # # 1. Upload videos
    # upload_videos()
    
    # # 2. Upload Users
    # upload_users()
    
    # # 3. Upload Schemas / Question Groups / Questions
    # create_schemas()
    
    # # 4. Create Projects with Annotations
    # create_projects_from_annotations_json(json_path='./annotations/movement/Camera_Movement_motion_attributes.json', schema_name='Camera Movement')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_color.json', schema_name='Color Grading')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_color.json', schema_name='Color Grading')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_color.json', schema_name='Color Grading')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_color.json', schema_name='Color Grading')
    
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_light_setup.json', schema_name='Light Setup')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_light_setup.json', schema_name='Light Setup')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_light_setup.json', schema_name='Light Setup')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_light_setup.json', schema_name='Light Setup')
    
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_subject_light.json', schema_name='Subject Light')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_subject_light.json', schema_name='Subject Light')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_subject_light.json', schema_name='Subject Light')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_subject_light.json', schema_name='Subject Light')
    
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_cinematic_motion.json', schema_name='Cinematic Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_cinematic_motion.json', schema_name='Cinematic Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_cinematic_motion.json', schema_name='Cinematic Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_cinematic_motion.json', schema_name='Cinematic Effects')
    
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_special_lighting.json', schema_name="Special Effects")
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_special_lighting.json', schema_name="Special Effects")
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_special_lighting.json', schema_name="Special Effects")
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_special_lighting.json', schema_name="Special Effects")
    
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_natural_effects.json', schema_name='Natural Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_natural_effects.json', schema_name='Natural Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_natural_effects.json', schema_name='Natural Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_natural_effects.json', schema_name='Natural Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_0_special_effects.json', schema_name='Special Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_0_special_effects.json', schema_name='Special Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_1_special_effects.json', schema_name='Special Light Effects')
    # create_projects_from_annotations_json(json_path='./annotations/lighting/Lightingtest_2_3_special_effects.json', schema_name='Special Light Effects')
    
    # 5. Assign Users to Projects
    # bulk_assign_users()
    
    # 6. Import Annotations
    # import_annotations()
    
    # # 7. Import Reviews
    # import_reviews()
    
    
    # Let Tiffany be the ground truth
    # lighting_paths = glob.glob('./annotations/lighting/*.json')
    # for path in lighting_paths:
    #     res = []
    #     base_name = os.path.basename(path)
    #     with open(path, 'r') as f:
    #         data = json.load(f)
    #     for item in data:
    #         if item['user_email'] == 'ttiffanyyllingg@gmail.com':
    #             item['reviewer_email'] = 'ttiffanyyllingg@gmail.com'
    #             res.append(item)
        
    #     target_path = './reviews/lighting/' + base_name
    #     with open(target_path, 'w') as f:
    #         json.dump(res, f, indent=2)
    
    
    # annotators
    annotators = set()

    paths = glob.glob(os.path.join('./CameraLighting/annotations', '*.json'))
    for path in paths:
        with open(path, 'r') as f:
            data = json.load(f)
        for item in data:
            annotators.add((item['user_email'], item['project_name']))
    
    # # reviewers
    # folders = glob.glob('./reviews/*')
    # reviewers = set()
    # for folder in folders:
    #     paths = glob.glob(os.path.join(folder, '*.json'))
    #     for path in paths:
    #         with open(path, 'r') as f:
    #             data = json.load(f)
    #         for item in data:
    #             item['reviewer_email'] = 'siyuancen096@gmail.com' if item['reviewer_email'] == '' else item['reviewer_email']
    #             reviewers.add((item['reviewer_email'], item['project_name']))
    
    final = []
    for annotator in annotators:
        final.append({"user_email": annotator[0], "project_name": annotator[1], "role": "annotator"})
    # for reviewer in reviewers:
    #     final.append({"user_email": reviewer[0], "project_name": reviewer[1], "role": "reviewer"})
    with open('./CameraLighting/user_project_assignments.json', 'w') as f:
        json.dump(final, f, indent=2)
    
    # double_checked = {}
    # with open('./annotations/movement/double_checked.json', 'r') as f:
    #     data = json.load(f)
    # for item in data:
    #     double_checked[item['video_uid']] = item['reviewer_email']
    
    # import glob
    # paths = glob.glob('./annotations/movement/*.json')
    # for path in paths:
    #     if 'double_checked' in path:
    #         continue
    #     res = []
    #     with open(path, wo xiang a
    #         data = json.load(f)
    #     for item in data:
    #         if item['video_uid'] in double_checked:
    #             item['reviewer_email'] = double_checked[item['video_uid']]
    #             res.append(item)
    #     os.makedirs('./reviews/movement', exist_ok=True)
    #     target_path = os.path.join('./reviews/movement', os.path.basename(path))
    #     with open(target_path, 'w') as f:
    #         json.dump(res, f, indent=2)

    # with open('./captions/movement/Camera_Movement_motion_attributes.json', 'r') as f:
    #     data = json.load(f)
    # reference = {}
    # for item in data:
    #     reference[item['video_uid']] = item
    # with open('./videos_data.json', 'r') as f:
    #     data = json.load(f)
    # for item in data:
    #     video_uid = item['video_name']
    #     attributes = item['cam_motion']
    #     if video_uid not in reference:
    #         continue
    #     reference_attributes = reference[video_uid]['cam_motion']
    #     for key, value in attributes.items():
    #         if key not in reference_attributes:
    #             continue
    #         else:
    #             ref_attribute = reference_attributes[key]
    #             # print(ref_attribute, value)
    #             if ref_attribute != value:
    #                 print(ref_attribute, value)
    #                 print(video_uid)
    #                 print('--------------------------------')
    #                 break


    