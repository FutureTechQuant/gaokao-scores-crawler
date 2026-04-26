import argparse


VALID_SHARDS = {"upper", "lower"}


def parse_years(raw: str):
    if not raw:
        return []
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def normalize_shard(value: str):
    shard = (value or "upper").strip().lower()
    if shard not in VALID_SHARDS:
        raise ValueError(f"无效分片: {value}")
    return shard


def next_pair(years, current_year, current_shard):
    if current_shard == "upper":
        return current_year, "lower"

    try:
        index = years.index(current_year)
    except ValueError as exc:
        raise ValueError(f"当前年份不在计划范围内: {current_year}") from exc

    if index + 1 >= len(years):
        return "", ""

    return years[index + 1], "upper"


def write_output(path: str, key: str, value: str):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", required=True)
    parser.add_argument("--current-year", default="")
    parser.add_argument("--current-shard", default="upper")
    parser.add_argument("--github-output", default="")
    args = parser.parse_args()

    years = parse_years(args.years)
    if not years:
        raise SystemExit("没有可执行年份")

    current_year = (args.current_year or "").strip()
    current_shard = normalize_shard(args.current_shard)

    if not current_year:
        current_year = years[0]
        current_shard = "upper"
    elif current_year not in years:
        raise SystemExit(f"当前年份不在计划范围内: {current_year}")

    next_year, next_shard = next_pair(years, current_year, current_shard)

    outputs = {
        "current_year": current_year,
        "current_shard": current_shard,
        "next_year": next_year,
        "next_shard": next_shard,
    }

    for key, value in outputs.items():
        write_output(args.github_output, key, value)

    for key, value in outputs.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
