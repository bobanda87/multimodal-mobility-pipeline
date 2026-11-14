"""Lambda entry points: incremental normalization and daily compaction of the normalized files."""
from analysis import compact_normalized, normalize_data


def normalize_handler(event, context):
    """Normalize the raw files that arrived since each source's watermark."""
    normalize_data.run_normalization(only_today=True)
    return {"status": "ok"}


def compact_handler(event, context):
    """Merge the small per-run files of closed days into one file per source and day."""
    compact_normalized.run()
    return {"status": "ok"}
