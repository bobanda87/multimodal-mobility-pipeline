"""
Upload precomputed analysis tables (CSV) to s3://<bucket>/results/ so the API can serve them.

Writes only under the results/ prefix, never to the collected or normalized data.
Usage: python api/publish_results.py [--dry-run] file.csv [file.csv ...]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import s3_upload  # noqa: E402  (initialises the S3 client from .env)
from api import config  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    for path in args.files:
        key = f"{config.RESULTS_PREFIX}{path.name}"
        print(f"{path} -> s3://{config.BUCKET}/{key}")
        if not args.dry_run:
            s3_upload.s3_client.upload_file(str(path), config.BUCKET, key)


if __name__ == "__main__":
    main()
