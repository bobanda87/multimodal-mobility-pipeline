"""Shared request parameters and authentication."""
import hmac
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import Header, HTTPException, Query

from . import config

Bucket = Literal["hour", "day"]


def require_api_key(x_api_key: str = Header(default="")):
    if config.API_KEY and not hmac.compare_digest(x_api_key, config.API_KEY):
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
