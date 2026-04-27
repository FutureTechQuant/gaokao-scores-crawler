import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from crawlers.scores import ScoreCrawler


def write_output(path: str, key: str, value):
    if not path:
        return
    with open(path, 'a', encoding='utf-8') as f:
        f.write(f'{key}={value}\n')


def load_school_ids():
    schools_file = PROJECT_ROOT / os.getenv('SCHOOL_DATA_FILE', 'data/schools.json')
    if not schools_file.exists():
        raise FileNotFoundError(f'未找到学校文件: {schools_file}')

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=True)
    parser.add_argument('--province-id', required=True)
    parser.add_argument('--province-index', required=True)
    parser.add_argument('--mode', required=True)
    parser.add_argument('--github-output', default='')
    args = parser.parse_args()

    school_ids = load_school_ids()

    if not school_ids:
        write_output(args.github_output, 'run_year', args.year)
        write_output(args.github_output, 'run_province_id', args.province_id)
        write_output(args.github_output, 'run_province_index', args.province_index)
        write_output(args.github_output, 'run_status', 'skipped')
        write_output(args.github_output, 'saved_documents', 0)
        write_output(args.github_output, 'completed_schools', 0)
        print({'year': args.year, 'province_id': args.province_id, 'status': 'skipped'})
        return

    crawler = ScoreCrawler()
    result = crawler.crawl(
        school_ids=school_ids,
        years=args.year,
        province_ids=[args.province_id],
    )

    write_output(args.github_output, 'run_year', result.get('year', args.year))
    write_output(args.github_output, 'run_province_id', args.province_id)
    write_output(args.github_output, 'run_province_index', args.province_index)
    write_output(args.github_output, 'run_status', result.get('status', 'skipped'))
    write_output(args.github_output, 'saved_documents', result.get('saved_documents', 0))
    write_output(args.github_output, 'completed_schools', result.get('completed_schools', 0))

    print(result)


if __name__ == '__main__':
    main()
