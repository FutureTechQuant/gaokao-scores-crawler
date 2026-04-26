import gzip
import hashlib
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

import requests

from .base import BaseCrawler


class PlanCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

        self._first_logged = False
        self._first_log_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._local = threading.local()

        self.state_file = Path("data/plans_progress.json")
        self.manifest_file = Path("data/manifest.json")
        self.chunk_dir = Path("data/chunks")

        self.max_workers = max(1, int(os.getenv("PLAN_MAX_WORKERS", "6")))
        self.flush_every = max(20, int(os.getenv("PLAN_FLUSH_EVERY", "200")))
        self.chunk_record_limit = max(
            1000, int(os.getenv("PLAN_CHUNK_RECORD_LIMIT", "200000"))
        )
        self.time_limit_seconds = int(
            os.getenv("PLAN_TIME_LIMIT_SECONDS", str(5 * 60 * 60 - 15 * 60))
        )  # 默认 4小时45分钟
        self.start_time = time.time()

        self.completed_keys = set()
        self.chunk_files = []
        self.current_chunk_index = 0
        self.current_chunk_records = 0
        self.current_chunk_file = None
        self.total_records = 0
        self.scope_signature = None
        self.scope_name = None

        self.province_dict = {
            "11": "北京",
            "12": "天津",
            "13": "河北",
            "14": "山西",
            "15": "内蒙古",
            "21": "辽宁",
            "22": "吉林",
            "23": "黑龙江",
            "31": "上海",
            "32": "江苏",
            "33": "浙江",
            "34": "安徽",
            "35": "福建",
            "36": "江西",
            "37": "山东",
            "41": "河南",
            "42": "湖北",
            "43": "湖南",
            "44": "广东",
            "45": "广西",
            "46": "海南",
            "50": "重庆",
            "51": "四川",
            "52": "贵州",
            "53": "云南",
            "54": "西藏",
            "61": "陕西",
            "62": "甘肃",
            "63": "青海",
            "64": "宁夏",
            "65": "新疆",
            "71": "台湾",
            "81": "香港",
            "82": "澳门",
        }

    def get_thread_session(self):
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update(self.headers)
            self._local.session = session
        return self._local.session

    def parse_years(self, years_input):
        if isinstance(years_input, list):
            return [str(y).strip() for y in years_input if str(y).strip()]

        if years_input is None:
            return []

        if isinstance(years_input, (int, float)):
            return [str(int(years_input))]

        years_input = str(years_input).strip()
        if not years_input:
            return []

        if "-" in years_input:
            start, end = [x.strip() for x in years_input.split("-", 1)]
            start_year, end_year = int(start), int(end)
            step = -1 if start_year > end_year else 1
            return [str(y) for y in range(start_year, end_year + step, step)]

        if "," in years_input:
            return [y.strip() for y in years_input.split(",") if y.strip()]

        return [years_input]

    def load_school_ids(self, school_ids=None):
        if school_ids is not None:
            return [str(sid) for sid in school_ids]

        with open("data/schools.json", "r", encoding="utf-8") as f:
            schools_data = json.load(f)

        if isinstance(schools_data, list):
            schools = schools_data
        elif isinstance(schools_data, dict):
            schools = schools_data.get("data", []) or [schools_data]
        else:
            raise ValueError(f"schools.json 数据格式错误: {type(schools_data)}")

        sample_count = int(os.getenv("SAMPLE_SCHOOLS", "0") or 0)
        if sample_count > 0:
            schools = schools[:sample_count]

        school_ids = [
            str(s["school_id"])
            for s in schools
            if isinstance(s, dict) and s.get("school_id")
        ]

        if not school_ids:
            raise ValueError("未找到有效的学校ID")

        return school_ids

    def build_scope_signature(self, school_ids, years, province_ids):
        raw = json.dumps(
            {
                "school_ids": school_ids,
                "years": years,
                "province_ids": province_ids,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def build_scope_name(self, years):
        joined = "-".join(years)
        return joined[:120]

    def task_key(self, school_id, year, province_id):
        return f"{school_id}_{year}_{province_id}"

    def build_tasks(self, school_ids, years, province_ids, completed_keys=None):
        completed_keys = completed_keys or set()
        tasks = []

        for school_id in school_ids:
            for year in years:
                for province_id in province_ids:
                    key = self.task_key(school_id, year, province_id)
                    if key not in completed_keys:
                        tasks.append((str(school_id), str(year), str(province_id)))

        return tasks

    def get_plan_data(self, school_id, year, province_id):
        url = f"https://static-data.gaokao.cn/www/2.0/schoolspecialplan/{school_id}/{year}/{province_id}.json"
        session = self.get_thread_session()

        try:
            response = session.get(url, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if result.get("code") == "0000" and "data" in result:
                    return result["data"]
            elif response.status_code == 404:
                return "no_data"
        except Exception:
            return None

        return None

    def load_progress(self):
        if not self.state_file.exists():
            return None

        with open(self.state_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def reset_scope_files(self):
        if self.chunk_dir.exists():
            for file in self.chunk_dir.glob("*.jsonl.gz"):
                file.unlink()

        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_files = []
        self.current_chunk_index = 0
        self.current_chunk_records = 0
        self.current_chunk_file = None
        self.total_records = 0

    def restore_progress(self, progress):
        self.completed_keys = set(progress.get("completed", []))
        self.chunk_files = progress.get("chunk_files", [])
        self.current_chunk_index = int(progress.get("current_chunk_index", 0) or 0)
        self.current_chunk_records = int(progress.get("current_chunk_records", 0) or 0)
        self.total_records = int(progress.get("total_records", 0) or 0)

        current_name = progress.get("current_chunk")
        self.current_chunk_file = self.chunk_dir / current_name if current_name else None
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

    def next_chunk_path(self):
        self.current_chunk_index += 1
        filename = f"plans-{self.scope_name}-{self.current_chunk_index:05d}.jsonl.gz"

        if filename not in self.chunk_files:
            self.chunk_files.append(filename)

        self.current_chunk_file = self.chunk_dir / filename
        self.current_chunk_records = 0
        return self.current_chunk_file

    def ensure_chunk_file(self):
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

        if self.current_chunk_file is None:
            return self.next_chunk_path()

        if self.current_chunk_records >= self.chunk_record_limit:
            return self.next_chunk_path()

        return self.current_chunk_file

    def append_records(self, records):
        if not records:
            return

        with self._write_lock:
            for record in records:
                chunk_path = self.ensure_chunk_file()
                with gzip.open(chunk_path, "at", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                self.current_chunk_records += 1
                self.total_records += 1

    def save_manifest(self, school_ids, years, province_ids, meta):
        payload = {
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scope_signature": self.scope_signature,
            "scope_name": self.scope_name,
            "years": years,
            "school_count": len(school_ids),
            "province_count": len(province_ids),
            "total_records": self.total_records,
            "chunk_record_limit": self.chunk_record_limit,
            "chunk_files": self.chunk_files,
            "meta": meta,
        }
        with open(self.manifest_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def save_progress(self, school_ids, years, province_ids, meta):
        payload = {
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scope_signature": self.scope_signature,
            "scope_name": self.scope_name,
            "completed_count": len(self.completed_keys),
            "completed": sorted(self.completed_keys),
            "chunk_dir": str(self.chunk_dir),
            "chunk_files": self.chunk_files,
            "current_chunk": self.current_chunk_file.name if self.current_chunk_file else None,
            "current_chunk_index": self.current_chunk_index,
            "current_chunk_records": self.current_chunk_records,
            "total_records": self.total_records,
            "meta": meta,
        }

        with self._state_lock:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        self.save_manifest(school_ids, years, province_ids, meta)

    def should_stop(self):
        return (time.time() - self.start_time) >= self.time_limit_seconds

    def parse_plan_records(self, school_id, year, province_id, data):
        province_name = self.province_dict.get(province_id, f"省份{province_id}")
        records = []

        if not isinstance(data, dict):
            return records

        with self._first_log_lock:
            if not self._first_logged:
                print("\n" + "─" * 60)
                print("首次成功响应结构")
                print(f"school_id={school_id}, year={year}, province={province_name}")
                print(f"data keys: {list(data.keys())}")
                print("─" * 60 + "\n")
                self._first_logged = True

        for plan_type, plan_info in data.items():
            if not isinstance(plan_info, dict):
                continue

            items = plan_info.get("item", [])
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue

                records.append(
                    {
                        "school_id": school_id,
                        "year": year,
                        "province_id": province_id,
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
                    }
                )

        return records

    def worker(self, task):
        school_id, year, province_id = task

        if self.should_stop():
            return {"task": task, "status": "stopped", "records": []}

        data = self.get_plan_data(school_id, year, province_id)
        time.sleep(random.uniform(0.05, 0.25))

        if data == "no_data":
            return {"task": task, "status": "no_data", "records": []}

        if data is None:
            return {"task": task, "status": "failed", "records": []}

        records = self.parse_plan_records(school_id, year, province_id, data)
        return {"task": task, "status": "success", "records": records}

    def create_meta(self, school_ids, years, province_ids):
        remaining_tasks = self.build_tasks(
            school_ids, years, province_ids, self.completed_keys
        )

        remaining_years = []
        seen = set()
        for _, year, _ in remaining_tasks:
            if year not in seen:
                seen.add(year)
                remaining_years.append(year)

        return {
            "finished": len(remaining_tasks) == 0,
            "resume_required": len(remaining_tasks) > 0,
            "total_tasks": len(school_ids) * len(years) * len(province_ids),
            "completed_tasks": len(self.completed_keys),
            "total_records": self.total_records,
            "max_workers": self.max_workers,
            "years": years,
            "remaining_tasks": len(remaining_tasks),
            "remaining_years": remaining_years,
            "years_arg": ",".join(remaining_years) if remaining_years else ",".join(years),
            "chunk_files_count": len(self.chunk_files),
            "scope_signature": self.scope_signature,
            "scope_name": self.scope_name,
        }

    def crawl(self, school_ids=None, years=None, province_ids=None):
        if years is None:
            years = self.parse_years(os.getenv("PLAN_YEARS", "2025,2024,2023"))
        else:
            years = self.parse_years(years)

        if not years:
            raise ValueError("years 不能为空")

        province_ids = [str(p) for p in (province_ids or list(self.province_dict.keys()))]
        school_ids = self.load_school_ids(school_ids)

        self.scope_signature = self.build_scope_signature(school_ids, years, province_ids)
        self.scope_name = self.build_scope_name(years)

        progress = self.load_progress() if os.getenv("PLAN_RESUME", "1") == "1" else None

        if progress and progress.get("scope_signature") == self.scope_signature:
            self.restore_progress(progress)
        else:
            self.completed_keys = set()
            self.reset_scope_files()

        pending_tasks = self.build_tasks(
            school_ids, years, province_ids, self.completed_keys
        )

        print("=" * 60)
        print("开始爬取招生计划")
        print(f"学校数: {len(school_ids)}")
        print(f"年份: {', '.join(years)}")
        print(f"省份数: {len(province_ids)}")
        print(f"线程数: {self.max_workers}")
        print(f"已完成任务数: {len(self.completed_keys)}")
        print(f"待处理任务数: {len(pending_tasks)}")
        print("=" * 60)

        if not pending_tasks:
            meta = self.create_meta(school_ids, years, province_ids)
            self.save_progress(school_ids, years, province_ids, meta)
            return meta

        task_queue = list(pending_tasks)
        inflight = {}
        processed_since_flush = 0

        executor = ThreadPoolExecutor(max_workers=self.max_workers)

        try:
            while task_queue and len(inflight) < self.max_workers and not self.should_stop():
                task = task_queue.pop(0)
                future = executor.submit(self.worker, task)
                inflight[future] = task

            while inflight:
                done, _ = wait(inflight.keys(), timeout=5, return_when=FIRST_COMPLETED)

                if not done:
                    if self.should_stop():
                        print("接近 5 小时限制，准备停止并保存进度...")
                        break
                    continue

                for future in done:
                    task = inflight.pop(future)
                    school_id, year, province_id = task
                    key = self.task_key(school_id, year, province_id)

                    try:
                        result = future.result()
                    except Exception as e:
                        print(f"⚠️  任务异常 {key}: {e}")
                        result = {"task": task, "status": "failed", "records": []}

                    if result["status"] in {"success", "no_data"}:
                        self.completed_keys.add(key)
                        self.append_records(result["records"])
                    elif result["status"] == "failed":
                        print(f"⚠️  请求失败，留待下轮续跑: {key}")

                    processed_since_flush += 1

                    if processed_since_flush >= self.flush_every:
                        meta = self.create_meta(school_ids, years, province_ids)
                        self.save_progress(school_ids, years, province_ids, meta)
                        processed_since_flush = 0
                        print(
                            f"✓ 已落盘：completed={meta['completed_tasks']}, records={meta['total_records']}, remaining={meta['remaining_tasks']}, chunks={meta['chunk_files_count']}"
                        )

                while task_queue and len(inflight) < self.max_workers and not self.should_stop():
                    task = task_queue.pop(0)
                    future = executor.submit(self.worker, task)
                    inflight[future] = task

                if self.should_stop():
                    print("接近 5 小时限制，停止提交新任务...")
                    break

        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        meta = self.create_meta(school_ids, years, province_ids)
        self.save_progress(school_ids, years, province_ids, meta)

        print("\n" + "=" * 60)
        print("招生计划爬取结束")
        print(f"已完成任务: {meta['completed_tasks']} / {meta['total_tasks']}")
        print(f"总记录数: {meta['total_records']}")
        print(f"分片数: {meta['chunk_files_count']}")
        print(f"是否需要续跑: {meta['resume_required']}")
        print(f"剩余任务数: {meta['remaining_tasks']}")
        print("=" * 60)

        return meta


if __name__ == "__main__":
    years_arg = sys.argv[1] if len(sys.argv) > 1 else None
    crawler = PlanCrawler()
    meta = crawler.crawl(years=years_arg)
    print(json.dumps(meta, ensure_ascii=False))
