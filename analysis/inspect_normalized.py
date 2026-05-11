"""
Quick inspection of normalized parquet files (run from project root with pandas/pyarrow).
Usage: python analysis/inspect_normalized.py [path_to_folder_or_files...]
Default: inspects files in current dir matching *_normalized_*.parquet
"""
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Install pandas: pip install pandas pyarrow")
    sys.exit(1)

def inspect(path: Path) -> None:
    if not path.suffix == ".parquet":
        return
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"  ERROR: {e}")
        return
    name = path.name
    print(f"\n=== {name} ===")
    print(f"  Rows: {len(df):,}   Columns: {list(df.columns)}")
    if "timestamp" in df.columns:
        ts = df["timestamp"]
        print(f"  timestamp: min={ts.min()}  max={ts.max()}")
    if "road_temperature" in df.columns:
        rt = df["road_temperature"].dropna()
        if len(rt):
            print(f"  road_temperature: min={rt.min():.2f}  max={rt.max():.2f}  count={len(rt)}")
    if "source" in df.columns:
        print(f"  source: {df['source'].iloc[0]}")
                                                                                                  
    wants = {
        "avinor": ["timestamp", "location", "metric_value", "source"],
        "entur_siri_et": ["timestamp", "departure_timestamp", "max_departure_delay_sec", "max_arrival_delay_sec"],
        "oslobysykkel": ["timestamp", "location", "metric_value", "source"],
        "vegvesen_normalized_": ["timestamp", "location", "road_temperature", "source"],                
        "vegvesen_travel_times": ["location_id", "period_start", "period_end", "free_flow_travel_time_sec", "traffic_status", "source"],
    }
    for key, cols in wants.items():
        if key in name:
            missing = [c for c in cols if c not in df.columns]
            if missing:
                print(f"  WARNING (expected columns): missing {missing}")
            else:
                note = " (not used by weather-impact; for congestion/speed analysis)" if "travel_times" in name else ""
                print(f"  OK for analysis (has required columns){note}")
            break
    print("  Sample:")
    print(df.head(2).to_string(index=False))

def main():
    if len(sys.argv) > 1:
        paths = []
        for a in sys.argv[1:]:
            p = Path(a)
            if p.is_file():
                paths.append(p)
            elif p.is_dir():
                paths.extend(p.glob("*_normalized_*.parquet"))
        if not paths:
            print("No *_normalized_*.parquet files found.")
            return
    else:
        paths = list(Path(".").glob("*_normalized_*.parquet"))
        if not paths:
            print("Usage: python analysis/inspect_normalized.py <file.parquet> [file2.parquet ...]")
            print("   or: put *_normalized_*.parquet in current dir and run again.")
            return
    for p in sorted(paths):
        inspect(p)

if __name__ == "__main__":
    main()
