from pathlib import Path

from .file_utils import safe_name, write_json_atomic, read_json


def get_school_file_path(data_root, school_info):
    data_root = Path(data_root)
    province = safe_name(school_info.get("province") or "未知省份")
    school_name = safe_name(
        school_info.get("name") or school_info.get("school_id") or "未知学校"
    )
    return data_root / province / f"{school_name}.json"


def save_school_json(data_root, school_info, now_str):
    file_path = get_school_file_path(data_root, school_info)

    payload = {
        "update_time": now_str(),
        "school_id": str(school_info.get("school_id") or ""),
        "province": school_info.get("province"),
        "name": school_info.get("name"),
        "data": school_info,
    }

    write_json_atomic(file_path, payload)


def get_existing_school_ids_from_files(data_root):
    data_root = Path(data_root)
    existing = set()

    if not data_root.exists():
        return existing

    for file in data_root.rglob("*.json"):
        try:
            payload = read_json(file, default={}) or {}
            school_id = payload.get("school_id") or payload.get("data", {}).get("school_id")
            if school_id:
                existing.add(str(school_id))
        except Exception:
            continue

    return existing
