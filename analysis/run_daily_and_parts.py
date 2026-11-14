"""
Pokreće analyze_weather_impact dan po dan i (opciono) po delovima dana,
upisuje korelacije u CSV za poređenje.

Korišćenje (iz project root):
  PYTHONPATH=. .venv/bin/python analysis/run_daily_and_parts.py --daily
  PYTHONPATH=. .venv/bin/python analysis/run_daily_and_parts.py --parts
  PYTHONPATH=. .venv/bin/python analysis/run_daily_and_parts.py --daily --parts
"""
import os
import sys
import subprocess
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
DAILY_CSV = os.path.join(OUTPUT_DIR, "daily_correlations.csv")
PARTS_CSV = os.path.join(OUTPUT_DIR, "part_of_day_correlations.csv")

START = datetime(2026, 1, 30, 0, 0, 0, tzinfo=timezone.utc)
END   = datetime(2026, 2, 20, 23, 59, 59, tzinfo=timezone.utc)
PARTS_OF_DAY = ["0-4", "4-8", "8-12", "12-16", "16-20", "20-24"]


def date_range(start: datetime, end: datetime):
    d = start.date()
    end_d = end.date()
    while d <= end_d:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def run_analyze(start_date: str, end_date: str, summary_csv: str, part_of_day: str | None = None):
    cmd = [
        sys.executable,
        os.path.join(SCRIPT_DIR, "analyze_weather_impact.py"),
        "--mode", "single",
        "--start-date", start_date,
        "--end-date", end_date,
        "--summary-csv", summary_csv,
    ]
    if part_of_day:
        cmd.extend(["--part-of-day", part_of_day])
    env = os.environ.copy()
    env["PYTHONPATH"] = PROJECT_ROOT
    try:
        subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        print(f"  Timeout: {start_date} {part_of_day or ''}")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--daily", action="store_true", help="Run one run per day, write to daily_correlations.csv")
    p.add_argument("--parts", action="store_true", help="Run per day × part-of-day, write to part_of_day_correlations.csv")
    p.add_argument("--start", type=str, default="2026-01-30", help="Start date YYYY-MM-DD")
    p.add_argument("--end", type=str, default="2026-02-20", help="End date YYYY-MM-DD")
    args = p.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_d = datetime.strptime(args.end, "%Y-%m-%d").date()
    days = list(date_range(
        datetime(start_d.year, start_d.month, start_d.day, tzinfo=timezone.utc),
        datetime(end_d.year, end_d.month, end_d.day, tzinfo=timezone.utc),
    ))

    if args.daily:
        print(f"Daily runs: {len(days)} days -> {DAILY_CSV}")
        for i, d in enumerate(days):
            print(f"  [{i+1}/{len(days)}] {d}")
            run_analyze(d, d, DAILY_CSV, part_of_day=None)
        print(f"Done. Summary: {DAILY_CSV}")

    if args.parts:
        print(f"Part-of-day runs: {len(days)} days × {len(PARTS_OF_DAY)} parts -> {PARTS_CSV}")
        n = 0
        for d in days:
            for part in PARTS_OF_DAY:
                n += 1
                print(f"  [{n}] {d} {part}")
                run_analyze(d, d, PARTS_CSV, part_of_day=part)
        print(f"Done. Summary: {PARTS_CSV}")

    if not args.daily and not args.parts:
        p.print_help()
        print("\nSpecify --daily and/or --parts.")


if __name__ == "__main__":
    main()
