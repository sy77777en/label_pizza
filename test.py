from label_pizza.db import init_database

init_database()

from label_pizza.upload_utils import *
from label_pizza.import_annotations import import_annotations, import_reviews

import glob
import json
import os

trusted_approvers = [
    "ipi@andrew.cmu.edu",
    "eanoh4111@gmail.com", 
    "rater.4@grandecenter.org",
    "gela.xu@outlook.com",
    "wangluxuan59@gmail.com",
    "rater.64@grandecenter.org",
    "yangeve4967@gmail.com",
    "zixidai185@gmail.com",
    "huangyuhan1130@gmail.com",
    "qsydfzly@outlook.com",
    "sunnyg@andrew.cmu.edu",
    "sarahchenart@gmail.com",
    "aalucythejellyfish@hotmail.com",
    "xieyi0756@gmail.com",
    "1340464709w@gmail.com",
    "wanqi.xing@alumni.uts.edu.au",
    "zouyoumei1131@gmail.com",
    "supervisor@grandecenter.org",
    "zhiqiulin98@gmail.com",
    "ttiffanyyllingg@gmail.com",
    "zhiqiul@andrew.cmu.edu"
]

user_email_to_id = {
    "siyuancen096@gmail.com": "Siyuan Cen",
    "stephenw@andrew.cmu.edu": "Hewei Wang (cmu)",
    "ttiffanyyllingg@gmail.com": "Yu Tong Tiffany Ling",
    "zhiqiulin98@gmail.com": "Zhiqiu Lin",
    "zhanghaochi22@gmail.com": "Haochi Zhang",
    "whan55751@gmail.com": "Han Wang",
    "hhwangyubo@gmail.com": "Yubo Wang",
    "sarahchenart@gmail.com": "sarahchenart",
    "thebluesoil@hotmail.com": "Yang Wang",
    "wukw8016@gmail.com": "Kewen Wu",
    "chwen.1@outlook.com": "Hewen Chi",
    "yangyongbo716@outlook.com": "Yongbo Yang",
    "drjiang@andrew.cmu.edu": "Daniel Jiang",
    "rater.62@grandecenter.org": "rater.62",
    "rater.51@grandecenter.org": "rater.51",
    "rater.67@grandecenter.org": "rater.67",
    "rater.70@grandecenter.org": "rater.70",
    "zhiqiul@andrew.cmu.edu": "Zhiqiu Lin (cmu)",
    "stephenw0516@gmail.com": "Hewei Wang",
    "kewenwu@andrew.cmu.edu": "Kewen Wu (cmu)",
    "rater.32@grandecenter.org": "rater.32",
    "rater.68@grandecenter.org": "rater.68",
    "rater.39@grandecenter.org": "rater.39",
    "shi.4123123@gmail.com": "Shaozhou Shi",
    "w1831302677@gmail.com": "Jialin Wang",
    "edzee1701@gmail.com": "Shihang Zhu",
    "rater.4@grandecenter.org": "rater.4",
    "huangyuhan1130@gmail.com": "Yuhan Huang"
}

# videos = set()
# paths = glob.glob('./CameraLighting/annotations/*.json')
# for path in paths:
#     for path in paths:
#         with open(path, 'r') as f:
#             data = json.load(f)
#             for item in data:
#                 videos.add(item['video_uid'])
# with open('./CameraLighting/videos.json', 'w') as f:
    
# paths = glob.glob('./CameraShotcomp/annotations/Shotcomp1_Trusted*.json')
# for path in paths:
#     base_name = os.path.basename(path)
#     base_name = base_name[17:]
#     trusted_videos = []
#     untrusted_videos = []
#     with open(path, 'r') as f:
#         data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if item['user_email'] in trusted_approvers:
#             trusted_videos.append(item)
#         else:
#             untrusted_videos.append(item)
#     print(len(trusted_videos), len(untrusted_videos))
#     with open(path, 'w') as f:
#         json.dump(trusted_videos, f, indent=2)
#     target_path = os.path.join('./CameraShotcomp/annotations', "Shotcomp1_Untrusted"+base_name)
#     with open(target_path, 'w') as f:
#         json.dump(untrusted_videos, f, indent=2)
    
    
# videos = {}
# paths = glob.glob('./CameraShotcomp/captions/Shotcomp1_Trusted_*.json')
# for path in paths:
#     with open(path, 'r') as f:
#         data = json.load(f)
#         for item in data:
#             video_uid = item['video_uid']
#             if video_uid not in videos:
#                 videos[video_uid] = {'cam_setup': {}}
#             videos[video_uid]['cam_setup'].update(item['shotcomp'])
# with open('./CameraShotcomp/annotations.json', 'w') as f:
#     json.dump(videos, f, indent=2)

# with open('./CameraShotcomp/annotations.json', 'r') as f:
#     videos = json.load(f)
    
# with open('./CameraShotcomp/refannotations.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_name']
#         if video_uid in videos:
#             annotations = videos[video_uid]['cam_setup']
#             ref_annotations = item['cam_setup']
#             for key, ref_value in ref_annotations.items():
#                 if key in ['shot_transition']:
#                     continue
#                 value = annotations[key]
#                 if value != ref_value:
#                     print(video_uid, key, value, ref_value)
#                     break



# Generate reviews
# paths = glob.glob('./CameraLighting/annotations/*.json')
# for path in paths:
#     with open(path, 'r') as f:
#         data = json.load(f)
#         items = []
#         for item in data:
#             if item['user_name'] == 'Yu Tong Tiffany Ling':
#                 item["is_ground_truth"] = True
#                 items.append(item)
#     base_name = os.path.basename(path)
#     os.makedirs('./CameraLighting/reviews', exist_ok=True)
#     target_path = os.path.join('./CameraLighting/reviews', base_name)
#     with open(target_path, 'w') as f:
#         json.dump(items, f, indent=2)


# Get Video annotations from Shotcomp1
# paths = glob.glob('./CameraShotcomp/annotations/Shotcomp1_Trusted*.json')
# for path in paths:
#     base_name = os.path.basename(path)
#     base_name = base_name[17:]
#     trusted_videos = []
#     untrusted_videos = []
#     with open(path, 'r') as f:
#         data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if item['user_email'] in trusted_approvers:
#             trusted_videos.append(item)
#         else:
#             untrusted_videos.append(item)
#     print(len(trusted_videos), len(untrusted_videos))
#     with open(path, 'w') as f:
#         json.dump(trusted_videos, f, indent=2)
#     target_path = os.path.join('./CameraShotcomp/annotations', "Shotcomp1_Untrusted"+base_name)
#     with open(target_path, 'w') as f:
#         json.dump(untrusted_videos, f, indent=2)

# paths = glob.glob('./CameraShotcomp/annotations/Shotcomp1*.json')
# for path in paths:
#     with open(path, 'r') as f:
#         data = json.load(f)
#         items = []
#         for item in data:
#             if "user_email" in item:
#                 user_name = user_email_to_id[item['user_email']]
#                 value = item.pop("user_email", None)
#             else:
#                 user_name = item['user_name']
#             item['user_name'] = user_name
#             items.append(item)
#     with open(path, 'w') as f:
#         json.dump(data, f, indent=2)

# with open('./CameraLighting/videos.json', 'r') as f:
#     videos = json.load(f)
#     items = []
#     for video in videos:
#         item = {}
#         item['metadata'] = video['metadata']
#         item['url'] = video['video_url']
#         items.append(item)
# with open('./CameraLighting/videos.json', 'w') as f:
#     json.dump(items, f, indent=2)


motion_without_485 = set()
with open('./CameraShotcomp/annotations/ShotcompSuppl_Inreview_Camera_Angle.json', 'r') as f:
    data = json.load(f)
    for item in data:
        video_uid = item['video_uid']
        motion_without_485.add(video_uid)
# Excluding 485 videos
supplement_videos = []
with open('./videos4labelpizza/485_videos.txt', 'r') as f:
    videos = f.readlines()
    for video in videos:
        video = video.strip()
        video = video.replace(',', '')
        # supplement_videos.append(video)
        if video not in motion_without_485:
            supplement_videos.append(video)
print(len(motion_without_485))
with open('./Shotcomp_Supplement/videos.json', 'w') as f:
    json.dump(list(motion_without_485), f, indent=2)



# # All Motion Done videos Excluding 485
# all_motion_double_checked = set()
# all_motion_done = set()
# all_motion_annotations = set()
# with open('./CameraMotion/reviews/CameraMovementDone_motion_attributes.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_motion_double_checked.add(video_uid)
#             all_motion_done.add(video_uid)
#             all_motion_annotations.add(video_uid)
# with open('./CameraMotion/annotations/CameraMovement_motion_attributes.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_motion_done.add(video_uid)
#             all_motion_annotations.add(video_uid)
# with open('./CameraMotion/annotations/CameraMotionInreview_motion_attributes.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_motion_annotations.add(video_uid)
# print('motion done (double check) and done numbers are:')
# print(len(all_motion_double_checked))
# print(len(all_motion_done))
# print('annotations numbers are:')
# print(len(all_motion_annotations))
# print('--------------------------------')
# # All Shot Comp done / trusted videos Excluding 485
# all_shotcomp_trusted = set()
# all_shotcomp_done = set()
# all_shotcomp_annotations = set()
# with open('./CameraShotcomp/annotations/Shotcomp1_Trusted_Camera_Angle.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_shotcomp_trusted.add(video_uid)
#             all_shotcomp_done.add(video_uid)
#             all_shotcomp_annotations.add(video_uid)
# with open('./CameraShotcomp/annotations/Shotcomp2_Done_Camera_Angle.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_shotcomp_trusted.add(video_uid)
#             all_shotcomp_done.add(video_uid)
#             all_shotcomp_annotations.add(video_uid)
# with open('./CameraShotcomp/annotations/Shotcomp1_Untrusted_Camera_Angle.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_shotcomp_done.add(video_uid)
#             all_shotcomp_annotations.add(video_uid)
# with open('./CameraShotcomp/annotations/Shotcomp2_Inreview_Camera_Angle.json', 'r') as f:
#     data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             all_shotcomp_annotations.add(video_uid)
# print('shotcomp trusted and done numbers are:')
# print(len(all_shotcomp_trusted))
# print(len(all_shotcomp_done))
# print('annotations numbers are:')
# print(len(all_shotcomp_annotations))
# print('--------------------------------')

# # Both Motion and Shot Comp Done videos Excluding 485
# both_done = all_motion_double_checked.intersection(all_shotcomp_trusted)
# print('both done numbers are:')
# print(len(both_done))
# # with open('./motion_Double_Checked_and_Shotcomp_Trusted/videos.json', 'w') as f:
# #     json.dump(list(both_done), f, indent=2)
# print('--------------------------------')

# # Only Shot Comp Trusted but not motion double checked
# shotcomp_trusted_not_motion_double_checked = all_motion_done.intersection(all_shotcomp_trusted) - both_done
# print('shotcomp trusted but not motion double checked numbers are:')
# print(len(shotcomp_trusted_not_motion_double_checked))
# # with open('./Shotcomp_Trusted_Motion_Done_Without_Double_Checked/videos.json', 'w') as f:
# #     json.dump(list(shotcomp_trusted_not_motion_double_checked), f, indent=2)
# print('--------------------------------')
# # Only shot Comp Trusted with motion annotations (Excluding motion done)
# shotcomp_trusted_with_motion_annotations = all_motion_annotations.intersection(all_shotcomp_trusted) - all_motion_done.intersection(all_shotcomp_trusted)
# print('shotcomp trusted with motion annotations numbers are (Excluding motion done):')
# print(len(shotcomp_trusted_with_motion_annotations))
# print('--------------------------------')
# # Only shot Comp Trusted with no motion annotations
# shotcomp_trusted_no_motion_annotations = all_shotcomp_trusted - all_motion_annotations.intersection(all_shotcomp_trusted)
# print('shotcomp trusted with no motion annotations numbers are:')
# print(len(shotcomp_trusted_no_motion_annotations))
# print('--------------------------------')

# # Only Motion Double Checked but not shotcomp trusted
# motion_double_checked_not_shotcomp_trusted = all_motion_double_checked.intersection(all_shotcomp_done) - both_done
# print('motion double checked but not shotcomp trusted numbers are:')
# print(len(motion_double_checked_not_shotcomp_trusted))
# print('--------------------------------')
# motion_double_checked_with_annotations = all_motion_double_checked.intersection(all_shotcomp_annotations) - all_motion_double_checked.intersection(all_shotcomp_done)
# print('motion double checked with shotcomp annotations numbers are:')
# print(len(motion_double_checked_with_annotations))
# # with open('./Motion_Double_Checked_Without_Shotcomp_Done/videos.json', 'w') as f:
# #     json.dump(list(motion_double_checked_with_annotations), f, indent=2)
# print('--------------------------------')
# motion_double_checked_no_shotcomp_annotations = all_motion_double_checked - all_shotcomp_annotations.intersection(all_motion_double_checked)
# print('motion double checked with no shotcomp annotations numbers are:')
# print(len(motion_double_checked_no_shotcomp_annotations))
# print('--------------------------------')




# # Only Motion Double Checked with shotcomp annotations (Excluding shotcomp done)
# motion_double_checked_with_shotcomp_annotations = all_motion_double_checked.intersection(all_shotcomp_annotations) - both_done - shotcomp_trusted_not_motion_double_checked
# print('motion double checked with shotcomp annotations numbers are:')
# print(len(motion_double_checked_with_shotcomp_annotations))
# print('--------------------------------')





# supplement_videos = []
# with open('./videos4labelpizza/485_videos.txt', 'r') as f:
#     videos = f.readlines()
#     for video in videos:
#         video = video.strip()
#         video = video.replace(',', '')
#         supplement_videos.append(video)

# paths = glob.glob('./CameraShotcomp/annotations/Shotcomp2*.json')
# for path in paths:
#     base_name = os.path.basename(path)
#     base_name = base_name[10:]
#     tmp = []
#     with open(path, 'r') as f:
#         data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             tmp.append(item)

# Get Video annotations from Shotcomp1
# paths = glob.glob('./CameraShotcomp/annotations/Shotcomp2*.json')
# for path in paths:
#     base_name = os.path.basename(path)
#     base_name = base_name[10:]
#     tmp = []
#     with open(path, 'r') as f:
#         data = json.load(f)
#     for item in data:
#         video_uid = item['video_uid']
#         if video_uid not in supplement_videos:
#             tmp.append(item)




