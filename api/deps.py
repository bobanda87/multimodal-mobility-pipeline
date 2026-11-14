"""Shared request parameters and authentication."""
import functools
import hmac
from datetime import datetime, timedelta, timezone
from typing import Literal

import duckdb
from fastapi import HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from . import config

Bucket = Literal["hour", "day"]


api_key_header = APIKeyHeader(
    name="X-API-Key", auto_error=False,
    description="The API key. In Swagger UI press Authorize once and every request carries it.")


def require_api_key(x_api_key: str | None = Security(api_key_header)):
    """401 unless the key matches. Declared as a security scheme, so /docs gets an Authorize button."""
    if config.API_KEY and not hmac.compare_digest(x_api_key or "", config.API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


class TimeRange:
    """`start` inclusive, `end` exclusive; naive values are taken as UTC."""

    def __init__(
        self,
        start: datetime = Query(..., description="ISO 8601, inclusive"),
        end: datetime = Query(..., description="ISO 8601, exclusive"),
    ):
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if end <= start:
            raise HTTPException(status_code=422, detail="end must be after start")
        if end - start > timedelta(days=config.MAX_RANGE_DAYS):
            raise HTTPException(
                status_code=422, detail=f"range is limited to {config.MAX_RANGE_DAYS} days")
        self.start = start
        self.end = end


def retry_once_on_missing_file(endpoint):
    """
    Run an endpoint again, listing the files afresh, if S3 no longer has one it listed.

    The compaction deletes the small files after writing the merged one, which can happen between this
    API listing the files and DuckDB opening them. One retry sees the merged file instead.
    """
    @functools.wraps(endpoint)
    def wrapper(*args, **kwargs):
        try:
            return endpoint(*args, **kwargs)
        except (duckdb.IOException, duckdb.HTTPException):
            return endpoint(*args, **kwargs)
    return wrapper
