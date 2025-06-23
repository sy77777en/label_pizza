import sys
from pathlib import Path
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from functools import lru_cache
from typing import Dict, Any, Optional, List, Tuple
from tqdm import tqdm
from sqlalchemy.orm import Session

from label_pizza.services import (
    VideoService, ProjectService, QuestionGroupService,
    AuthService, AnnotatorService, GroundTruthService
)
from label_pizza.db import SessionLocal


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _resolve_ids(
    *,
    session: Session,
    question_group_title: str,
    user_email: str,
    video_ref: str,
    project_name: str,
) -> Tuple[int, int, int, int]:
    """Return (video_id, project_id, user_id, group_id) or raise ValueError."""
    group_id = QuestionGroupService.get_group_by_name(question_group_title, session).id
    user_id  = AuthService.get_user_by_email(user_email, session).id

    video_uid  = video_ref.split("/")[-1]
    video_id   = VideoService.get_video_by_uid(video_uid, session).id

    project_id = ProjectService.get_project_by_name(project_name, session).id
    return video_id, project_id, user_id, group_id


def _verification_passes(
    *,
    session: Session,
    video_id: int,
    project_id: int,
    user_id: int,
    group_id: int,
    answers: Dict[str, str],
) -> None:
    """
    Validate one label *without* writing to DB.
    Missing answers are tolerated for questions where `is_required` is False.
    """
    # 1. project & user existence / role checks ------------------------
    AnnotatorService._validate_project_and_user(project_id, user_id, session)
    AnnotatorService._validate_user_role(user_id, project_id, "annotator", session)

    # 2. fetch group + questions ---------------------------------------
    group, questions = AnnotatorService._validate_question_group(group_id, session)

    # ---- build two helper sets ---------------------------------------
    required_q_texts = {q.text for q in questions if getattr(q, "required", True)}
    provided_q_texts = set(answers)
    missing = required_q_texts - provided_q_texts
    extra   = provided_q_texts  - {q.text for q in questions}

    if missing or extra:
        raise ValueError(
            f"Answers do not match questions in group. "
            f"Missing: {missing}. Extra: {extra}"
        )

    # 3. run optional verification hook -------------------------------
    AnnotatorService._run_verification(group, answers)

    # 4. per-question value validation (only for keys we have) ---------
    q_lookup = {q.text: q for q in questions}
    for q_text in provided_q_texts:
        AnnotatorService._validate_answer_value(q_lookup[q_text], answers[q_text])


# ──────────────────────────────────────────────────────────────────────
# Main routine
# ──────────────────────────────────────────────────────────────────────
# ── small helper: cache the legal question keys for each group_id ────────────
@lru_cache(maxsize=None)
def _legal_keys_for_group(group_id: int, session: Session) -> set[str]:
    """Return the set of Question.text keys that live in <group_id>."""
    qs = QuestionGroupService.get_group_questions(group_id, session)
    return {row["Text"] for _, row in qs.iterrows()}


def upload_annotations_from_json(
    rows: List[Dict[str, Any]],
) -> None:
    """Verify every entry; upload only if all entries are valid."""
    errors: list[str] = []
    valid_cache: list[dict[str, Any]] = []

    with SessionLocal() as session:
        for idx, row in enumerate(tqdm(rows, desc="verifying"), start=1):
            try:
                # ----- resolve IDs -------------------------------------------------
                video_id, project_id, user_id, group_id = _resolve_ids(
                    session=session,
                    question_group_title=row["question_group_title"],
                    user_email=row["user_email"],
                    video_ref=row.get("video_uid") or row["video_uid"],
                    project_name=row["project_name"],
                )

                # ----- keep only questions that exist in this group ---------------
                legal_keys = _legal_keys_for_group(group_id, session)
                answers = {k: v for k, v in row["answers"].items() if k in legal_keys}

                # (optional) warn if something was dropped
                dropped = set(row["answers"]) - legal_keys
                if dropped:
                    print(f"[WARN] {row['video_uid']} | {row['user_email']} "
                          f"dropped keys: {dropped}")

                # ----- verify remaining answers -----------------------------------
                _verification_passes(
                    session=session,
                    video_id=video_id,
                    project_id=project_id,
                    user_id=user_id,
                    group_id=group_id,
                    answers=answers,
                )

                # ----- cache for phase-2 upload -----------------------------------
                valid_cache.append({
                    "video_id":   video_id,
                    "project_id": project_id,
                    "user_id":    user_id,
                    "group_id":   group_id,
                    "answers":    answers,
                    "confidence": row.get("confidence_scores") or {},
                    "notes":      row.get("notes") or {},
                    "video_uid":  row.get("video_uid", "<unknown>"),
                    "email":      row["user_email"],
                })

            except Exception as exc:
                errors.append(f"[{idx}] {row.get('video_uid')} | "
                              f"{row.get('user_email')}: {exc}")

    if errors:
        print("\nVERIFICATION FAILED – nothing uploaded.")
        for e in errors[:20]:
            print(e)
        if len(errors) > 20:
            print(f"...and {len(errors)-20} more")
        return

    print(f"\nVERIFICATION PASSED ({len(valid_cache)} records). Starting upload...")

    # -------- phase 2 – real upload -----------------------------------
    ok, fail = 0, 0
    with SessionLocal() as session:
        for rec in tqdm(valid_cache, desc="uploading"):
            try:
                AnnotatorService.submit_answer_to_question_group(
                    video_id=rec["video_id"],
                    project_id=rec["project_id"],
                    user_id=rec["user_id"],
                    question_group_id=rec["group_id"],
                    answers=rec["answers"],
                    session=session,
                    confidence_scores=rec["confidence"],
                    notes=rec["notes"],
                )
                ok += 1
            except Exception as exc:
                print(f"[FAIL] {rec['video_uid']} | {rec['email']}: {exc}")
                fail += 1

    print(f"\nUpload finished – {ok} succeeded, {fail} failed.")

def _resolve_ids_for_reviews(
    *,
    session: Session,
    question_group_title: str,
    reviewer_email: str,  # 改为 reviewer_email
    video_ref: str,
    project_name: str,
) -> Tuple[int, int, int, int]:
    """Return (video_id, project_id, reviewer_id, group_id) or raise ValueError."""
    group_id = QuestionGroupService.get_group_by_name(question_group_title, session).id
    
    # 通过邮箱获取 reviewer_id
    reviewer_id = AuthService.get_user_by_email(reviewer_email, session).id

    video_uid  = video_ref.split("/")[-1]
    video_id   = VideoService.get_video_by_uid(video_uid, session).id

    project_id = ProjectService.get_project_by_name(project_name, session).id
    return video_id, project_id, reviewer_id, group_id

def upload_reviews_from_json(
    rows: List[Dict[str, Any]],
) -> None:
    """Verify every review entry; upload only if all entries are valid."""
    errors: list[str] = []
    valid_cache: list[dict[str, Any]] = []

    with SessionLocal() as session:
        for idx, row in enumerate(tqdm(rows, desc="verifying reviews"), start=1):
            try:
                # ----- resolve IDs -------------------------------------------------
                video_id, project_id, reviewer_id, group_id = _resolve_ids_for_reviews(
                    session=session,
                    question_group_title=row["question_group_title"],
                    reviewer_email=row["reviewer_email"],  # 使用 reviewer_email 字段
                    video_ref=row.get("video_uid") or row["video_uid"],
                    project_name=row["project_name"],
                )

                # ----- keep only questions that exist in this group ---------------
                legal_keys = _legal_keys_for_group(group_id, session)
                answers = {k: v for k, v in row["answers"].items() if k in legal_keys}

                # (optional) warn if something was dropped
                dropped = set(row["answers"]) - legal_keys
                if dropped:
                    print(f"[WARN] {row['video_uid']} | reviewer:{row['reviewer_email']} "
                          f"dropped keys: {dropped}")

                # ----- verify remaining answers -----------------------------------
                _verification_passes_reviews(
                    session=session,
                    video_id=video_id,
                    project_id=project_id,
                    reviewer_id=reviewer_id,
                    group_id=group_id,
                    answers=answers,
                )

                # ----- cache for phase-2 upload -----------------------------------
                valid_cache.append({
                    "video_id":   video_id,
                    "project_id": project_id,
                    "reviewer_id": reviewer_id,
                    "group_id":   group_id,
                    "answers":    answers,
                    "confidence": row.get("confidence_scores") or {},
                    "notes":      row.get("notes") or {},
                    "video_uid":  row.get("video_uid", "<unknown>"),
                    "reviewer_email": row["reviewer_email"],  # 保存原始邮箱
                })

            except Exception as exc:
                errors.append(f"[{idx}] {row.get('video_uid')} | "
                              f"reviewer:{row.get('reviewer_email')}: {exc}")

    if errors:
        print("\nVERIFICATION FAILED – nothing uploaded.")
        for e in errors[:20]:
            print(e)
        if len(errors) > 20:
            print(f"...and {len(errors)-20} more")
        return

    print(f"\nVERIFICATION PASSED ({len(valid_cache)} records). Starting upload...")

    # -------- phase 2 – real upload -----------------------------------
    ok, fail = 0, 0
    with SessionLocal() as session:
        for rec in tqdm(valid_cache, desc="uploading reviews"):
            try:
                GroundTruthService.submit_ground_truth_to_question_group(
                    video_id=rec["video_id"],
                    project_id=rec["project_id"],
                    reviewer_id=rec["reviewer_id"],
                    question_group_id=rec["group_id"],
                    answers=rec["answers"],
                    session=session,
                    confidence_scores=rec["confidence"],
                    notes=rec["notes"],
                )
                ok += 1
            except Exception as exc:
                print(f"[FAIL] {rec['video_uid']} | reviewer:{rec['reviewer_email']}: {exc}")
                fail += 1

    print(f"\nUpload finished – {ok} succeeded, {fail} failed.")


def _verification_passes_reviews(
    *,
    session: Session,
    video_id: int,  # 改为 int
    project_id: int,
    reviewer_id: int,
    group_id: int,
    answers: Dict[str, str],
) -> None:
    """
    Validate one review label *without* writing to DB.
    Missing answers are tolerated for questions where `is_required` is False.
    """
    # 1. project & reviewer existence / role checks ------------------------
    GroundTruthService._validate_project_and_user(project_id, reviewer_id, session)
    GroundTruthService._validate_user_role(reviewer_id, project_id, "reviewer", session)

    # 2. fetch group + questions ---------------------------------------
    group, questions = GroundTruthService._validate_question_group(group_id, session)

    # ---- build two helper sets ---------------------------------------
    required_q_texts = {q.text for q in questions if getattr(q, "required", True)}
    provided_q_texts = set(answers)
    missing = required_q_texts - provided_q_texts
    extra   = provided_q_texts  - {q.text for q in questions}

    if missing or extra:
        raise ValueError(
            f"Answers do not match questions in group. "
            f"Missing: {missing}. Extra: {extra}"
        )

    # 3. run optional verification hook -------------------------------
    GroundTruthService._run_verification(group, answers)

    # 4. per-question value validation (only for keys we have) ---------
    q_lookup = {q.text: q for q in questions}
    for q_text in provided_q_texts:
        GroundTruthService._validate_answer_value(q_lookup[q_text], answers[q_text])


def import_annotations() -> None:
    base_dir = './annotations'
    import glob
    folders = glob.glob(os.path.join(base_dir, '*'))
    for folder in folders:
        paths = glob.glob(os.path.join(folder, '*.json'))
        for path in paths:
            with open(path, 'r') as f:
                data = json.load(f)
            upload_annotations_from_json(data)
    
def import_reviews() -> None:
    base_dir = './reviews'
    import glob
    folders = glob.glob(os.path.join(base_dir, '*'))
    for folder in folders:
        if 'movement' in folder:
            continue
        paths = glob.glob(os.path.join(folder, '*.json'))
        for path in paths:
            with open(path, 'r') as f:
                data = json.load(f)
            upload_reviews_from_json(data)

# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser(description="Verify, then upload annotations JSON → DB")
#     parser.add_argument("--rows", required=True, help="Path to JSON file")
#     args = parser.parse_args()
#     with open(args.rows, 'r') as f:
#         rows = json.load(f)
#     upload_annotations_from_json(rows)