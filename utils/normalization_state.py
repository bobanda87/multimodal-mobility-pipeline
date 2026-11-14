"""
Normalization progress as one watermark per source, replacing normalization_tracking.json.

The old tracking file listed every normalized raw key (~0.5 million keys, 30 MB) and was read and
rewritten on each run. A watermark is the LastModified of the newest raw file that was normalized, so
the state stays a few hundred bytes however long the collectors run.

Raw files that share the watermark's second (S3 LastModified has one-second resolution) are kept in
`boundary_keys`, so a file written a moment after the watermark was set is not skipped and an
already normalized one is not processed twice.
"""
from datetime import datetime, timezone

from utils.s3_upload import S3_JSON_PATH, read_json_from_s3, save_json_to_s3

WATERMARK_KEY = f"{S3_JSON_PATH.rstrip('/')}/normalization_watermark.json"


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


class Watermarks:
    """Per-source watermarks: {source: {"last_modified": ISO-8601 UTC, "boundary_keys": [...]}}."""

    def __init__(self):
        data = read_json_from_s3(WATERMARK_KEY)
        self._data = data if isinstance(data, dict) else {}

    def last_modified(self, source: str) -> datetime | None:
        entry = self._data.get(source)
        return _parse(entry["last_modified"]) if entry else None

    def is_new(self, source: str, file_info: dict) -> bool:
        """True if a raw file ({'key', 'last_modified'}) has not been normalized yet."""
        entry = self._data.get(source)
        if not entry:
            return True
        wm = _parse(entry["last_modified"])
        lm = file_info["last_modified"].astimezone(timezone.utc)
        return lm > wm or (lm == wm and file_info["key"] not in entry["boundary_keys"])

    def advance(self, source: str, attempted: list[dict], failed_keys: set[str]) -> bool:
        """
        Move the watermark over the raw files of this run, but not past the oldest failed one,
        so a file that could not be read is retried by the next run. Never moves backwards.
        Returns True if the watermark changed (and was saved).
        """
        files = sorted(attempted, key=lambda f: f["last_modified"])
        failed_lms = [f["last_modified"] for f in files if f["key"] in failed_keys]
        if failed_lms:
            files = [f for f in files if f["last_modified"] < min(failed_lms)]
        if not files:
            return False
        newest = files[-1]["last_modified"].astimezone(timezone.utc)
        boundary = {f["key"] for f in files if f["last_modified"].astimezone(timezone.utc) == newest}

        current = self._data.get(source)
        if current:
            old = _parse(current["last_modified"])
            if newest < old:
                return False
            if newest == old:
                boundary |= set(current["boundary_keys"])
        self._data[source] = {"last_modified": newest.isoformat(), "boundary_keys": sorted(boundary)}
        return save_json_to_s3(self._data, WATERMARK_KEY)

    def set(self, source: str, last_modified: datetime, boundary_keys: list[str]) -> None:
        """Used by the migration from the old tracking file; call save() afterwards."""
        self._data[source] = {
            "last_modified": last_modified.astimezone(timezone.utc).isoformat(),
            "boundary_keys": sorted(boundary_keys),
        }

    def save(self) -> bool:
        return save_json_to_s3(self._data, WATERMARK_KEY)
