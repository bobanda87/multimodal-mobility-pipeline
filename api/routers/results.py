import re

import boto3
from fastapi import APIRouter, HTTPException, Query

from .. import config, db

router = APIRouter(prefix="/results", tags=["analysis results"])

_NAME = re.compile(r"^[A-Za-z0-9_.-]+\.csv$")


def _names() -> list[str]:
    s3 = boto3.client("s3", region_name=config.REGION)
    names = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=config.BUCKET, Prefix=config.RESULTS_PREFIX):
        for obj in page.get("Contents", []):
            name = obj["Key"][len(config.RESULTS_PREFIX):]
            if "/" not in name and _NAME.match(name):
                names.append(name)
    return sorted(names)


@router.get("")
def list_results():
    """Precomputed analysis result tables (CSV) published by api/publish_results.py."""
    return _names()


@router.get("/{name}")
def get_result(name: str, limit: int = Query(1000, ge=1, le=10000)):
    if not _NAME.match(name) or name not in _names():
        raise HTTPException(status_code=404, detail="Unknown result")
    path = f"s3://{config.BUCKET}/{config.RESULTS_PREFIX}{name}"
    return db.query("SELECT * FROM read_csv_auto(?) LIMIT ?", [path, limit])
