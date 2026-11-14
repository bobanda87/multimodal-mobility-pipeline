"""
Regenerate avinor/active_airports.json: the airports that had flights in earlier raw data, most important first.

  PYTHONPATH=. .venv/bin/python avinor/refresh_active_airports.py 2026-11-10 2026-11-12 2026-11-14

Pass the local dates to scan (any number, a few spread over the period is enough). An airport counts as
active if at least one raw file for it exists on any of the days. They are ordered by the total size of
their raw files (a proxy for the number of flights), so the collector asks for the busiest airports first and
a cycle that has to be cut short still has them. Check the result into git and redeploy.
"""
import collections
import json
import os
import re
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()
from utils import s3_upload as s  # noqa: E402


def main(days):
    base = s.S3_PARQUET_PATH.rstrip("/")
    found = collections.Counter()
    for day in days:
        for page in s.s3_client.get_paginator("list_objects_v2").paginate(Bucket=s.S3_BUCKET_NAME, Prefix=f"{base}/avinor/{day}/"):
            for obj in page.get("Contents", []):
                m = re.search(r"/([A-Z]{3})_\d{6}\.parquet$", obj["Key"])
                if m:
                    found[m.group(1)] += obj["Size"]
        print(f"{day}: {len(found)} airports so far")
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "active_airports.json")
    with open(path, "w") as f:
        json.dump({"source": "airports with at least one raw file on " + ", ".join(days) + ", busiest first (total raw file size)",
                   "airports": [code for code, _ in found.most_common()]}, f, indent=1)
    print(f"{len(found)} airports -> {path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
