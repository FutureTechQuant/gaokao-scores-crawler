import os
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseCrawler
from utils.file_utils import safe_name, write_json_atomic, read_json


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


class PlanCrawler(BaseCrawler):
    CRAWLER_NAME = "plans"
    ENTITY_TYPE = "school_year_plan"
    CURSOR_TYPE = "school_cursor"

    REQUEST_TIMEOUT = 10
    FAILURE_PAUSE_THRESHOLD = 8

    def __init__(self):
        super().__init__()
        self._first_logged = False
        self.debug = parse_bool(os.getenv("PLAN_DEBUG", "false"), False)
        self.skip_existing = parse_bool(os.getenv("PLAN_SKIP_EXISTING", "true"), True)

        self.school_data_dir = Path(os.getenv("SCHOOL_DATA_DIR", "data/schools"))
        self.school_source_file = Path(os.getenv("SCHOOL_SOURCE_FILE", "data/schools.json"))
        self.plan_data_dir = Path(os.getenv("PLAN_DATA_DIR", "data/plans"))
        self.progress_dir = Path(os.getenv("PLAN_PROGRESS_DIR", "data/plans_progress"))
        self.completed_dir = Path(os.getenv("PLAN_COMPLETED_DIR", "data/plans_progress/completed"))

        self.max_schools_per_run = int(os.getenv("PLAN_MAX_SCHOOLS_PER_RUN", "20"))
        self.province_workers = max(1, int(os.getenv("PLAN_PROVINCE_WORKERS", "4")))

        self.school_shard = str(os.getenv("PLAN_SCHOOL_SHARD", "all")).strip().lower()
        if self.school_shard not in {"all", "upper", "lower"}:
            self.school_shard = "all"

        self._thread_local = threading.local()

        self.province_dict = {
            "11": "北京", "12": "天津", "13": "河北", "14": "山西", "15": "内蒙古",
            "21": "辽宁", "22": "吉林", "23": "黑龙江",
            "31": "上海", "32": "江苏", "33": "浙江", "34": "安徽", "35": "福建", "36": "江西", "37": "山东",
            "41": "河南", "42": "湖北", "43": "湖南",
            "44": "广东", "45": "广西", "46": "海南",
            "50": "重庆", "51": "四川", "52": "贵州", "53": "云南", "54": "西藏",
            "61": "陕西", "62": "甘肃", "63": "青海", "64": "宁夏", "65": "新疆",
            "71": "台湾", "81": "香港", "82": "澳门",
        }

        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=max(self.province_workers, 4),
            pool_maxsize=max(self.province_workers, 4),
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def build_worker_session(self):
        session = requests.Session()
        session.headers.update(self.headers)

        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=max(self.province_workers, 4),
            pool_maxsize=max(self.province_workers, 4),
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get_worker_session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self.build_worker_session()
            self._thread_local.session = session
        return session

    def parse_years(self, years_input):
        if isinstance(years_input, list):
            return [str(y).strip() for y in years_input if str(y).strip()]

        if isinstance(years_input, str):
            raw = years_input.strip()
            if not raw:
                return []

            if "-" in raw:
                start, end = raw.split("-", 1)
                start = int(start.strip())
                end = int(end.strip())
                if start <= end:
                    return [str(y) for y in range(end, start - 1, -1)]
                return [str(y) for y in range(start, end - 1, -1)]

            if "," in raw:
                return [y.strip() for y in raw.split(",") if y.strip()]

            return [raw]

        return years_input or []

    def normalize_school_target(self, school_id, school_name=None):
        if not school_id:
            return None
        return {
            "school_id": str(school_id),
            "school_name": school_name,
        }

    def _add_school_target(self, targets, seen_ids, school_id, school_name=None):
        target = self.normalize_school_target(school_id, school_name)
        if not target:
            return

        if target["school_id"] in seen_ids:
            return

        seen_ids.add(target["school_id"])
        targets.append(target)

    def _extract_targets_from_payload(self, payload, targets, seen_ids, fallback_name=None):
        if isinstance(payload, dict):
            data = payload.get("data")

            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    school_id = item.get("school_id") or item.get("id")
                    school_name = item.get("name") or item.get("school_name")
                    self._add_school_target(targets, seen_ids, school_id, school_name)
                return

            if isinstance(data, dict):
                school = data
                school_id = school.get("school_id") or payload.get("school_id")
                school_name = school.get("name") or payload.get("name") or fallback_name
                self._add_school_target(targets, seen_ids, school_id, school_name)
                return

            school_id = payload.get("school_id") or payload.get("id")
            school_name = payload.get("name") or payload.get("school_name") or fallback_name
            self._add_school_target(targets, seen_ids, school_id, school_name)
            return

        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                school_id = item.get("school_id") or item.get("id")
                school_name = item.get("name") or item.get("school_name") or fallback_name
                self._add_school_target(targets, seen_ids, school_id, school_name)

    def apply_school_shard(self, targets):
        if not targets:
            return targets

        if self.school_shard == "all":
            return targets

        midpoint = (len(targets) + 1) // 2

        if self.school_shard == "upper":
            result = targets[:midpoint]
        else:
            result = targets[midpoint:]

        print(f"学校分片: {self.school_shard}，分片学校数 {len(result)} / {len(targets)}")
        return result

    def load_school_targets_from_files(self):
        sample_count = int(os.getenv("SAMPLE_SCHOOLS", "3"))
        targets = []
        seen_ids = set()

        if self.school_data_dir.exists():
            for path in self.school_data_dir.rglob("*.json"):
                try:
                    payload = read_json(path, default={}) or {}
                    self._extract_targets_from_payload(
                        payload=payload,
                        targets=targets,
                        seen_ids=seen_ids,
                        fallback_name=path.stem,
                    )
                except Exception as e:
                    print(f"⚠️ 读取学校文件失败: {path} - {e}")
        else:
            print(f"ℹ️ 未找到学校目录: {self.school_data_dir}")

        if self.school_source_file.exists():
            try:
                payload = read_json(self.school_source_file, default={}) or {}
                self._extract_targets_from_payload(
                    payload=payload,
                    targets=targets,
                    seen_ids=seen_ids,
                    fallback_name=self.school_source_file.stem,
                )
            except Exception as e:
                print(f"⚠️ 读取学校聚合文件失败: {self.school_source_file} - {e}")
        else:
            print(f"ℹ️ 未找到学校聚合文件: {self.school_source_file}")

        def sort_key(x):
            sid = str(x["school_id"])
            return (0, int(sid)) if sid.isdigit() else (1, sid)

        targets.sort(key=sort_key)
        total_before_shard = len(targets)
        targets = self.apply_school_shard(targets)

        if sample_count > 0:
            targets = targets[:sample_count]

        if targets:
            print(f"从学校数据源读取到 {total_before_shard} 所学校，本次目标学校数 {len(targets)}")
        else:
            print("⚠️ 未读取到有效学校目标")

        return targets

    def load_school_targets(self, school_targets=None):
        if school_targets is not None:
            result = []
            for item in school_targets:
                if isinstance(item, dict):
                    target = self.normalize_school_target(
                        item.get("school_id"),
                        item.get("school_name") or item.get("name"),
                    )
                else:
                    target = self.normalize_school_target(item, None)
                if target:
                    result.append(target)

            result = self.apply_school_shard(result)
            return result

        file_targets = self.load_school_targets_from_files()
        if file_targets:
            return file_targets

        print("⚠️ 没有可用学校数据")
        return []

    def get_progress_file(self, year_label):
        custom = os.getenv("PLAN_PROGRESS_FILE", "").strip()
        if custom:
            return Path(custom)
        if self.school_shard == "all":
            return self.progress_dir / f"{year_label}.json"
        return self.progress_dir / f"{year_label}-{self.school_shard}.json"

    def load_year_progress(self, year_label, target_school_ids):
        path = self.get_progress_file(year_label)
        if not path.exists():
            return set(), {}

        progress = read_json(path, default={}) or {}

        saved_year = str(progress.get("year", ""))
        if saved_year and saved_year != str(year_label):
            print("⚠️ progress 年份不一致，忽略旧断点")
            return set(), {}

        saved_shard = str(progress.get("school_shard", "") or "").strip().lower()
        if saved_shard and saved_shard != self.school_shard:
            print("⚠️ progress 分片不一致，忽略旧断点")
            return set(), {}

        saved_target_ids = [str(x) for x in progress.get("target_school_ids", [])]
        current_target_ids = [str(x) for x in target_school_ids]

        if saved_target_ids and saved_target_ids != current_target_ids:
            print("⚠️ progress 的目标学校集合与当前不一致，忽略旧断点")
            return set(), {}

        completed_school_ids = {str(x) for x in progress.get("completed_school_ids", [])}
        print(
            f"↻ 检测到断点：year={year_label}，shard={self.school_shard}，"
            f"已完成学校 {len(completed_school_ids)} / {len(current_target_ids)}"
        )
        return completed_school_ids, progress

    def save_year_progress(
        self,
        year_label,
        target_school_ids,
        completed_school_ids,
        last_school_id=None,
        consecutive_failures=0,
        last_error=None,
    ):
        path = self.get_progress_file(year_label)
        payload = {
            "year": str(year_label),
            "school_shard": self.school_shard,
            "target_school_ids": [str(x) for x in target_school_ids],
            "completed_school_ids": sorted(str(x) for x in completed_school_ids),
            "school_total": len(target_school_ids),
            "school_done": len(completed_school_ids),
            "last_school_id": str(last_school_id) if last_school_id else None,
            "consecutive_failures": int(consecutive_failures),
            "last_error": last_error,
            "updated_at": self.now_str(),
        }
        write_json_atomic(path, payload)

    def clear_year_progress(self, year_label):
        path = self.get_progress_file(year_label)
        if path.exists():
            path.unlink()

    def get_completed_marker_file(self, year_label, school_id):
        return self.completed_dir / str(year_label) / f"{school_id}.json"

    def mark_school_completed(self, year_label, school_id, school_name, province_ids, hit_province_ids, record_count):
        payload = {
            "year": str(year_label),
            "school_shard": self.school_shard,
            "school_id": str(school_id),
            "school_name": school_name,
            "checked_province_ids": [str(x) for x in province_ids],
            "hit_province_ids": sorted(str(x) for x in hit_province_ids),
            "record_count": int(record_count),
            "completed_at": self.now_str(),
        }
        write_json_atomic(self.get_completed_marker_file(year_label, school_id), payload)

    def get_completed_school_ids_from_files(self, year_label):
        root = self.completed_dir / str(year_label)
        result = set()
        if not root.exists():
            return result

        for path in root.rglob("*.json"):
            try:
                payload = read_json(path, default={}) or {}
                school_id = payload.get("school_id")
                if school_id:
                    result.add(str(school_id))
            except Exception:
                continue

        return result

    def get_primary_plan_file_path(self, year_label, province_id, school_name):
        province_name = safe_name(self.province_dict.get(str(province_id), f"省份{province_id}"))
        school_file_name = safe_name(school_name or "未知学校")
        return self.plan_data_dir / str(year_label) / province_name / f"{school_file_name}.json"

    def get_suffix_plan_file_path(self, year_label, province_id, school_name, school_id):
        province_name = safe_name(self.province_dict.get(str(province_id), f"省份{province_id}"))
        school_file_name = f"{safe_name(school_name or school_id or '未知学校')}__{school_id}"
        return self.plan_data_dir / str(year_label) / province_name / f"{school_file_name}.json"

    def find_existing_plan_file(self, year_label, province_id, school_name, school_id):
        primary = self.get_primary_plan_file_path(year_label, province_id, school_name)
        suffix = self.get_suffix_plan_file_path(year_label, province_id, school_name, school_id)

        if primary.exists():
            try:
                payload = read_json(primary, default={}) or {}
                if str(payload.get("school_id")) == str(school_id):
                    return primary
            except Exception:
                pass

        if suffix.exists():
            try:
                payload = read_json(suffix, default={}) or {}
                if str(payload.get("school_id")) == str(school_id):
                    return suffix
            except Exception:
                pass

        return None

    def get_plan_file_path(self, year_label, province_id, school_name, school_id):
        existing = self.find_existing_plan_file(year_label, province_id, school_name, school_id)
        if existing:
            return existing

        primary = self.get_primary_plan_file_path(year_label, province_id, school_name)
        if primary.exists():
            return self.get_suffix_plan_file_path(year_label, province_id, school_name, school_id)

        return primary

    def plan_file_exists(self, year_label, province_id, school_name, school_id):
        return self.find_existing_plan_file(year_label, province_id, school_name, school_id) is not None

    def get_existing_province_ids_for_school(self, year_label, school_id, school_name, province_ids):
        existing = set()
        for province_id in province_ids:
            if self.plan_file_exists(year_label, province_id, school_name, school_id):
                existing.add(str(province_id))
        return existing

    def build_document_payload(
        self,
        school_id,
        school_name,
        year_label,
        province_id,
        raw_data,
        records,
    ):
        province_name = self.province_dict.get(str(province_id), f"省份{province_id}")
        return {
            "update_time": self.now_str(),
            "school_id": str(school_id),
            "school_name": school_name,
            "year": str(year_label),
            "province_id": str(province_id),
            "province": province_name,
            "record_count": len(records),
            "records": records,
            "data": raw_data,
        }

    def save_plan_json(self, year_label, province_id, school_id, school_name, payload):
        file_path = self.get_plan_file_path(
            year_label=year_label,
            province_id=province_id,
            school_name=school_name,
            school_id=school_id,
        )
        write_json_atomic(file_path, payload)

    def get_plan_data(self, school_id, year, province_id, session=None):
        url = f"https://static-data.gaokao.cn/www/2.0/schoolspecialplan/{school_id}/{year}/{province_id}.json"
        result = self.get_json_with_session(
            session=session or self.session,
            url=url,
            retry=2,
            delay=2,
            timeout=self.REQUEST_TIMEOUT,
            allow_404=True,
        )

        if result == "no_data":
            return "no_data"

        if isinstance(result, dict) and self.is_success_code(result.get("code")) and "data" in result:
            return result["data"]

        return None

    def get_json_with_session(self, session, url, retry=3, delay=2, timeout=15, allow_404=False):
        for attempt in range(retry):
            try:
                response = session.get(url, timeout=timeout)

                if response.status_code == 200:
                    try:
                        return response.json()
                    except json.JSONDecodeError as e:
                        print(f"⚠️ JSON解析失败: {str(e)}")
                        print(f" URL: {url}")
                        print(f" 响应前200字符: {response.text[:200]}")
                        return None

                if allow_404 and response.status_code == 404:
                    return "no_data"

                if response.status_code in (429, 500, 502, 503, 504):
                    pass

            except requests.exceptions.Timeout:
                pass
            except requests.exceptions.RequestException:
                pass

        return None

    def log_first_structure(self, data):
        if self._first_logged or not self.debug or not data or data == "no_data":
            return

        print(f"\n{'─' * 60}")
        print("首次响应数据结构")
        print(f"{'─' * 60}")
        print(f"data类型: {type(data).__name__}")

        if isinstance(data, dict):
            print(f"data包含键: {list(data.keys())}")
            sample_item = None
            sample_type = None

            for plan_type, plan_info in data.items():
                if isinstance(plan_info, dict):
                    items = plan_info.get("item", [])
                    if items:
                        sample_type = plan_type
                        sample_item = items[0]
                        break

            if sample_type:
                print(f"招生类型: {sample_type}")

            if isinstance(sample_item, dict):
                fields = list(sample_item.keys())
                print(f"字段数: {len(fields)}")
                for i, field in enumerate(fields[:25], 1):
                    value = sample_item.get(field)
                    if value is None:
                        preview = "None"
                    elif isinstance(value, str):
                        preview = f'"{value[:40]}..."' if len(value) > 40 else f'"{value}"'
                    elif isinstance(value, (list, dict)):
                        preview = f"{type(value).__name__}({len(value)})"
                    else:
                        preview = str(value)
                    print(f"{i:2}. {field:25} = {preview}")

        print(f"{'─' * 60}\n")
        self._first_logged = True

    def extract_records(self, school_id, year, province_id, province_name, data):
        records = []

        if not data or data == "no_data" or not isinstance(data, dict):
            return records

        for plan_type, plan_info in data.items():
            if not isinstance(plan_info, dict):
                continue

            items = plan_info.get("item", [])
            for item in items:
                if not isinstance(item, dict):
                    continue

                records.append({
                    "school_id": str(school_id),
                    "year": str(year),
                    "province_id": str(province_id),
                    "province": province_name,
                    "plan_type": plan_type,
                    "batch": item.get("local_batch_name"),
                    "type": item.get("type"),
                    "major": item.get("sp_name") or item.get("spname"),
                    "major_code": item.get("spcode"),
                    "major_group": item.get("sg_name"),
                    "major_group_code": item.get("sg_code"),
                    "major_group_info": item.get("sg_info"),
                    "level1_name": item.get("level1_name"),
                    "level2_name": item.get("level2_name"),
                    "level3_name": item.get("level3_name"),
                    "plan_number": item.get("num") or item.get("plan_num"),
                    "years": item.get("length") or item.get("years"),
                    "tuition": item.get("tuition"),
                    "note": item.get("note") or item.get("remark"),
                })

        return records

    def fetch_one_province(self, school_id, school_name, year_label, province_id):
        province_id = str(province_id)
        province_name = self.province_dict.get(province_id, f"省份{province_id}")

        try:
            session = self.get_worker_session()
            data = self.get_plan_data(school_id, year_label, province_id, session=session)

            if data == "no_data":
                return {
                    "province_id": province_id,
                    "province_name": province_name,
                    "status": "no_data",
                    "records": [],
                    "payload": None,
                    "raw_data": None,
                }

            if data is None:
                return {
                    "province_id": province_id,
                    "province_name": province_name,
                    "status": "error",
                    "records": [],
                    "payload": None,
                    "raw_data": None,
                }

            records = self.extract_records(school_id, year_label, province_id, province_name, data)
            payload = self.build_document_payload(
                school_id=school_id,
                school_name=school_name,
                year_label=year_label,
                province_id=province_id,
                raw_data=data,
                records=records,
            )

            return {
                "province_id": province_id,
                "province_name": province_name,
                "status": "ok",
                "records": records,
                "payload": payload,
                "raw_data": data,
            }
        except Exception:
            return {
                "province_id": province_id,
                "province_name": province_name,
                "status": "error",
                "records": [],
                "payload": None,
                "raw_data": None,
            }

    def crawl_one_year(self, year_label, school_targets=None, province_ids=None, mode=None):
        province_ids = [str(x) for x in (province_ids or list(self.province_dict.keys()))]
        school_targets = self.load_school_targets(school_targets)

        if not school_targets:
            return {
                "year": str(year_label),
                "school_shard": self.school_shard,
                "status": "skipped",
                "saved_documents": 0,
                "completed_schools": 0,
            }

        target_school_ids = [t["school_id"] for t in school_targets]
        scope_key = f"{year_label}:{self.school_shard}"

        if self.db_enabled:
            self.job_id = self.start_job(
                crawler_name=self.CRAWLER_NAME,
                mode=mode or os.getenv("CRAWL_MODE", "full"),
                scope_key=scope_key,
                year=str(year_label),
                meta_json={
                    "school_shard": self.school_shard,
                    "target_school_count": len(target_school_ids),
                    "province_count": len(province_ids),
                },
            )

        progress_completed_ids, progress_meta = self.load_year_progress(year_label, target_school_ids)

        file_completed_ids = set()
        if self.skip_existing:
            file_completed_ids = self.get_completed_school_ids_from_files(year_label)
            if file_completed_ids:
                print(f"✓ 已存在 {len(file_completed_ids)} 所学校的 {year_label} 年招生计划完成标记，自动跳过")

        completed_school_ids = set(progress_completed_ids) | set(file_completed_ids)
        pending_targets = [t for t in school_targets if t["school_id"] not in completed_school_ids]

        if not pending_targets:
            self.clear_year_progress(year_label)
            if self.db_enabled:
                self.mark_job_done(
                    meta_json={
                        "year": str(year_label),
                        "school_shard": self.school_shard,
                        "saved_documents": 0,
                        "completed_schools": len(completed_school_ids),
                        "target_school_count": len(target_school_ids),
                    }
                )
            print(f"\n{'=' * 60}")
            print("✅ 该年份分片已全部完成，无需继续爬取")
            print(f" 年份: {year_label}")
            print(f" 分片: {self.school_shard}")
            print(f" 学校完成: {len(completed_school_ids)} / {len(target_school_ids)}")
            print(f"{'=' * 60}\n")
            return {
                "year": str(year_label),
                "school_shard": self.school_shard,
                "status": "done",
                "saved_documents": 0,
                "completed_schools": len(completed_school_ids),
            }

        consecutive_failures = int(progress_meta.get("consecutive_failures", 0) or 0)

        print(f"\n{'=' * 60}")
        print("开始爬取招生计划")
        print(f"年份: {year_label}")
        print(f"分片: {self.school_shard}")
        print(f"学校总数: {len(target_school_ids)}")
        print(f"已完成学校: {len(completed_school_ids)}")
        print(f"待爬学校: {len(pending_targets)}")
        print(f"省份数: {len(province_ids)}")
        print(f"学校目录: {self.school_data_dir}")
        print(f"学校聚合文件: {self.school_source_file}")
        print(f"输出目录: {self.plan_data_dir}")
        print(f"进度目录: {self.progress_dir}")
        print(f"完成标记目录: {self.completed_dir}")
        print(f"数据库启用: {'✓' if self.db_enabled else '✗'}")
        print(f"省份并发数: {self.province_workers}")
        print(f"单次最多完成学校数: {self.max_schools_per_run}")
        print(f"{'=' * 60}\n")

        saved_documents = 0
        processed_this_run = 0
        has_incomplete_school = False

        try:
            for idx, target in enumerate(pending_targets, 1):
                school_id = target["school_id"]
                school_name = target.get("school_name")

                print(f"\n[{idx}/{len(pending_targets)}] 学校ID: {school_id}" + (f" ({school_name})" if school_name else ""))

                existing_province_ids = set()
                if
