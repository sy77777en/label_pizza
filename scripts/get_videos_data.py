import json
import re
from collections import defaultdict

if __name__ == "__main__":
    videos = []
    with open("segment_review_reorganized.json", 'r') as f:
        data = json.load(f)
    for item in data:
        base_name = item['base_name']
        youtube_id = base_name[:11]
        base_start_frame = base_name[12:].split('_')[0]
        if "full" in base_name:
            base_start_frame = 0
        segments = item['segments']
        for segment in segments:
            segement_data = {}
            ext_id = segment['external_id']
            segment_idxes = segment['frame_idxes']
            for i in range(1, len(segment_idxes), 2):
                if i+1 >= len(segment_idxes):
                    break
                idx = i // 2
                start_frame = segment_idxes[i] + int(base_start_frame)
                end_frame = segment_idxes[i+1] + int(base_start_frame)
                external_id = ext_id[:-3] + str(idx) + ".mp4"
                segment_data = {
                    "url": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/" + external_id,
                    "metadata": {
                        "original_youtube_id": youtube_id,
                        "start_frame": start_frame,
                        "end_frame": end_frame,
                        "license": "YouTube Educational License"
                    }
                }
                videos.append(segment_data)
            base_start_frame = int(base_start_frame) + segment['frame_idxes'][-1]
    with open("videos_data.json", 'w') as f:
        json.dump(videos, f, indent=4)
