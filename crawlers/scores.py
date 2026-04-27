import json
import os
import time
from pathlib import Path

from .base import BaseCrawler


class ScoreCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self._first_logged = False
        self.progress_dir = Path(os.getenv('SCORE_PROGRESS_DIR', 'data/scores_progress'))
        self.data_dir = Path(os.getenv('SCORE_DATA_DIR', 'data/scores'))
        self.run_deadline_seconds = int(os.getenv('SCORE_RUN_DEADLINE_SECONDS', '17400'))
        self.flush_schools = max(1, int(os.getenv('SCORE_FLUSH_SCHOOLS', '25')))

        self.province_dict = {
            '11': '北京', '12': '天津', '13': '河北', '14': '山西', '15': '内蒙古',
            '21': '辽宁', '22': '吉林', '23': '黑龙江',
            '31': '上海', '32': '江苏', '33': '浙江', '34': '安徽', '35': '福建', '36': '江西', '37': '山东',
            '41': '河南', '42': '湖北', '43': '湖南',
            '44': '广东', '45': '广西', '46': '海南',
            '50': '重庆', '51': '四川', '52': '贵州', '53': '云南', '54': '西藏',
            '61': '陕西', '62': '甘肃', '63': '青海', '64': '宁夏', '65': '新疆',
            '71': '台湾', '81': '香港', '82': '澳门',
        }

    def now_str(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    def write_json_atomic(self, path, payload):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + '.tmp')
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)

    def format_duration(self, seconds):
        seconds = max(0, float(seconds))
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f'{hours}小时{minutes}分{secs}秒'
        if minutes > 0:
            return f'{minutes}分{secs}秒'
        return f'{seconds:.2f}秒'

    def parse_years(self, years_input):
        if isinstance(years_input, list):
            return [str(y).strip() for y in years_input if str(y).strip()]
        if isinstance(years_input, str):
            raw = years_input.strip()
            if not raw:
                return []
            if '-' in raw:
                start, end = raw.split('-', 1)
                start = int(start.strip())
                end = int(end.strip())
                if start >= end:
                    return [str(y) for y in range(start, end - 1, -1)]
                return [str(y) for y in range(end, start - 1, -1)]
            if ',' in raw:
                return [y.strip() for y in raw.split(',') if y.strip()]
            return [raw]
        return years_input or []

    def load_default_school_ids(self):
        schools_file = Path(os.getenv('SCHOOL_DATA_FILE', 'data/schools.json'))
        if not schools_file.exists():
            print(f'⚠️  未找到 schools.json: {schools_file}')
            return []

        with open(schools_file, 'r', encoding='utf-8') as f:
            payload = json.load(f)

        if isinstance(payload, list):
            schools = payload
        elif isinstance(payload, dict):
            schools = payload.get('data', [])
            if not schools and payload.get('school_id'):
                schools = [payload]
        else:
            schools = []

        school_ids = []
        for item in schools:
            if isinstance(item, dict) and item.get('school_id'):
                school_ids.append(str(item['school_id']))

        def sort_key(x):
            return (0, int(x)) if x.isdigit() else (1, x)

        school_ids = sorted(dict.fromkeys(school_ids), key=sort_key)
        sample_count = int(os.getenv('SAMPLE_SCHOOLS', '0') or 0)
        if sample_count > 0:
            school_ids = school_ids[:sample_count]
        return school_ids

    def get_progress_file(self, year, province_id):
        custom = os.getenv('SCORE_PROGRESS_FILE', '').strip()
        if custom:
            return Path(custom)
        return self.progress_dir / f'{year}.{province_id}.json'

    def load_progress(self, year, province_id, target_school_ids):
        path = self.get_progress_file(year, province_id)
        base = {
            'year': str(year),
            'province_id': str(province_id),
            'target_school_ids': [str(x) for x in target_school_ids],
            'current_school_index': 0,
            'updated_at': None,
            'last_error': None,
            'status': 'new',
        }
        if not path.exists():
            return base
        try:
            with open(path, 'r', encoding='utf-8') as f:
                progress = json.load(f)
        except Exception:
            return base

        saved_year = str(progress.get('year', ''))
        saved_province_id = str(progress.get('province_id', ''))
        saved_targets = [str(x) for x in progress.get('target_school_ids', [])]
        current_targets = [str(x) for x in target_school_ids]
        if saved_year != str(year) or saved_province_id != str(province_id) or saved_targets != current_targets:
            return base
        return progress

    def save_progress(self, year, province_id, target_school_ids, current_school_index, last_error=None, status='running'):
        payload = {
            'year': str(year),
            'province_id': str(province_id),
            'target_school_ids': [str(x) for x in target_school_ids],
            'current_school_index': int(current_school_index),
            'updated_at': self.now_str(),
            'last_error': last_error,
            'status': status,
        }
        self.write_json_atomic(self.get_progress_file(year, province_id), payload)

    def clear_progress(self, year, province_id):
        path = self.get_progress_file(year, province_id)
        if path.exists():
            path.unlink()

    def get_province_file_path(self, year, province_id):
        province_name = self.province_dict.get(str(province_id), f'省份{province_id}')
        return self.data_dir / str(year) / f'{province_name}.json'

    def build_record_key(self, item):
        return (
            str(item.get('school_id') or ''),
            str(item.get('year') or ''),
            str(item.get('province_id') or ''),
            str(item.get('major_type') or ''),
            str(item.get('batch') or ''),
            str(item.get('type') or ''),
            str(item.get('recruit_type') or ''),
            str(item.get('major') or ''),
            str(item.get('major_code') or ''),
            str(item.get('major_group') or ''),
        )

    def load_province_records(self, year, province_id):
        path = self.get_province_file_path(year, province_id)
        province_name = self.province_dict.get(str(province_id), f'省份{province_id}')
        records = []
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                if isinstance(payload, dict):
                    records = payload.get('data', []) or []
                elif isinstance(payload, list):
                    records = payload
            except Exception as e:
                print(f'⚠️  读取已有省份文件失败，改为重建: {path} - {e}')
                records = []
        existing_keys = {self.build_record_key(item) for item in records if isinstance(item, dict)}
        return {
            'year': str(year),
            'province_id': str(province_id),
            'province': province_name,
            'data': records,
            'existing_keys': existing_keys,
        }

    def save_province_records(self, year, province_id, payload):
        file_path = self.get_province_file_path(year, province_id)
        body = {
            'update_time': self.now_str(),
            'year': str(year),
            'province_id': str(province_id),
            'province': payload.get('province'),
            'count': len(payload.get('data', [])),
            'data': payload.get('data', []),
        }
        self.write_json_atomic(file_path, body)

    def get_score_data(self, school_id, year, province_id):
        url = f'https://static-data.gaokao.cn/www/2.0/schoolspecialscore/{school_id}/{year}/{province_id}.json'
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == '0000' and 'data' in result:
                    return result['data']
            elif response.status_code == 404:
                return 'no_data'
        except Exception:
            pass
        return None

    def extract_records(self, school_id, year, province_id, province_name, data):
        records = []
        if not data or data == 'no_data' or not isinstance(data, dict):
            return records

        for major_type, major_info in data.items():
            if not isinstance(major_info, dict):
                continue

            items = major_info.get('item', [])
            for item in items:
                if not isinstance(item, dict):
                    continue

                records.append({
                    'school_id': str(school_id),
                    'year': str(year),
                    'province_id': str(province_id),
                    'province': province_name,
                    'major_type': major_type,
                    'batch': item.get('local_batch_name'),
                    'type': item.get('type'),
                    'recruit_type': item.get('zslx_name'),
                    'major': item.get('sp_name') or item.get('spname'),
                    'major_code': item.get('spcode'),
                    'major_group': item.get('sg_name'),
                    'major_group_info': item.get('sg_info'),
                    'level1_name': item.get('level1_name'),
                    'level2_name': item.get('level2_name'),
                    'level3_name': item.get('level3_name'),
                    'min_score': item.get('min'),
                    'max_score': item.get('max'),
                    'avg_score': item.get('average') or item.get('avg'),
                    'min_rank': item.get('min_section'),
                    'proscore': item.get('proscore'),
                    'enrollment': item.get('lq_num') or item.get('sg_info'),
                })
        return records

    def merge_records(self, province_payload, new_records):
        added = 0
        for item in new_records:
            key = self.build_record_key(item)
            if key in province_payload['existing_keys']:
                continue
            province_payload['existing_keys'].add(key)
            province_payload['data'].append(item)
            added += 1
        return added

    def should_stop(self, started_at):
        return (time.time() - started_at) >= self.run_deadline_seconds

    def crawl_one_year(self, year, school_ids=None, province_ids=None):
        school_ids = [str(x) for x in (school_ids or self.load_default_school_ids())]
        province_ids = [str(x) for x in (province_ids or list(self.province_dict.keys()))]

        if not school_ids:
            print('⚠️  没有可用学校ID')
            return {
                'year': str(year),
                'status': 'skipped',
                'saved_documents': 0,
                'completed_schools': 0,
            }

        if len(province_ids) != 1:
            raise ValueError('当前版本要求每次只传入一个省份')

        started_at = time.time()
        province_id = province_ids[0]
        province_name = self.province_dict.get(province_id, f'省份{province_id}')

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.progress_dir.mkdir(parents=True, exist_ok=True)

        progress = self.load_progress(year, province_id, school_ids)
        start_index = int(progress.get('current_school_index', 0) or 0)
        province_payload = self.load_province_records(year, province_id)
        province_added_records = 0

        print(f'
{'=' * 60}')
        print('启动分数线爬虫')
        print(f'年份: {year}')
        print(f'省份: {province_name} ({province_id})')
        print(f'学校数: {len(school_ids)}')
        print(f'软截止: {self.format_duration(self.run_deadline_seconds)}')
        print(f'学校起始索引: {start_index + 1}/{len(school_ids)}')
        print(f'{'=' * 60}
')

        for school_index in range(start_index, len(school_ids)):
            if self.should_stop(started_at):
                self.save_province_records(year, province_id, province_payload)
                self.save_progress(
                    year=year,
                    province_id=province_id,
                    target_school_ids=school_ids,
                    current_school_index=school_index,
                    last_error='run deadline reached',
                    status='partial',
                )
                print(f'⏸️ 接近 5 小时上限，已保存 {province_name} 和 progress，准备下一轮续跑')
                return {
                    'year': str(year),
                    'status': 'partial',
                    'saved_documents': 0,
                    'completed_schools': school_index,
                }

            school_id = school_ids[school_index]
            data = self.get_score_data(school_id, year, province_id)

            if not self._first_logged and data and data != 'no_data' and isinstance(data, dict):
                print(f'
{'─' * 50}')
                print('首次响应数据结构:')
                print(f'{'─' * 50}')
                print(f'data类型: {type(data).__name__}')
                print(f'data包含键: {list(data.keys())}')
                print(f'{'─' * 50}
')
                self._first_logged = True

            if data and data != 'no_data' and isinstance(data, dict):
                records = self.extract_records(school_id, year, province_id, province_name, data)
                added = self.merge_records(province_payload, records)
                province_added_records += added

            if (school_index + 1) % self.flush_schools == 0:
                self.save_province_records(year, province_id, province_payload)
                self.save_progress(
                    year=year,
                    province_id=province_id,
                    target_school_ids=school_ids,
                    current_school_index=school_index + 1,
                    last_error=None,
                    status='running',
                )
                print(f'   ↻ 已阶段性保存 {province_name}: 学校进度 {school_index + 1}/{len(school_ids)}，当前 {len(province_payload["data"])} 条')

            self.polite_sleep(0.2, 0.6)

        self.save_province_records(year, province_id, province_payload)
        self.clear_progress(year, province_id)

        print(f'✅ 省份完成: {province_name}，本轮新增 {province_added_records} 条，累计 {len(province_payload["data"])} 条')
        return {
            'year': str(year),
            'status': 'done',
            'saved_documents': 1,
            'completed_schools': len(school_ids),
        }

    def crawl(self, school_ids=None, years=None, province_ids=None):
        if years is None:
            years_env = os.getenv('SCORE_YEARS', '2025,2024,2023')
            years = self.parse_years(years_env)
        else:
            years = self.parse_years(years)

        if not years:
            print('⚠️  未提供有效年份')
            return {
                'year': '',
                'status': 'skipped',
                'saved_documents': 0,
                'completed_schools': 0,
            }

        result = None
        for year in years:
            result = self.crawl_one_year(year=str(year), school_ids=school_ids, province_ids=province_ids)
            if result.get('status') in {'partial', 'paused'}:
                return result
        return result or {
            'year': '',
            'status': 'skipped',
            'saved_documents': 0,
            'completed_schools': 0,
        }


if __name__ == '__main__':
    import sys
    years_arg = sys.argv[1] if len(sys.argv) > 1 else None
    crawler = ScoreCrawler()
    crawler.crawl(years=years_arg)
