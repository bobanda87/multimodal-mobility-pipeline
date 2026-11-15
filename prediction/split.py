"""Time-ordered train / validation / test split at trip level, with an embargo before each boundary."""
import numpy as np
import pandas as pd

TRIP_KEY = ["datasource", "trip_id", "data_frame_ref"]


def trip_start(df: pd.DataFrame) -> pd.Series:
    """Aimed time of the first stop of each trip that is in the data, per row."""
    return df.groupby(TRIP_KEY)["aimed"].transform("min")


def split_by_time(df: pd.DataFrame, fractions=(0.6, 0.2, 0.2), embargo_s: int = 3 * 3600):
    """
    Returns (train, val, test) index arrays.

    A trip belongs to the segment its first row falls in, so no trip is in two sets. Trips that start within
    `embargo_s` before a boundary are dropped from the earlier set: they run into the next one, and weather,
    traffic and the delay of the previous trips of the same line are shared across the boundary.
    Boundaries are quantiles of the trip start times, not of the row counts, so a busy hour does not move them.
    """
    assert abs(sum(fractions) - 1) < 1e-9 and len(fractions) == 3
    start = trip_start(df)
    trips = start.groupby([df[k] for k in TRIP_KEY]).first()
    b1, b2 = np.quantile(trips.values, [fractions[0], fractions[0] + fractions[1]])
    s = start.values
    train = np.flatnonzero(s < b1 - embargo_s)
    val = np.flatnonzero((s >= b1) & (s < b2 - embargo_s))
    test = np.flatnonzero(s >= b2)
    return train, val, test, (b1, b2)
