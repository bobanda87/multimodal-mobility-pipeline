"""
Tabela Pearson r(temperatura, indikator) odvojeno za commute peak vs off-peak.

Peak definicija: ista kao u analyze_weather_impact.correlations_by_commute_peak
(pon–pet, UTC 07:00–09:59 i 15:00–17:59).

Izlaz: analysis/output/weather_pearson_peak_offpeak_wa1_wa2_wb_wc.csv

Pokretanje (iz project root):
  POLARS_SKIP_CPU_CHECK=1 .venv/bin/python analysis/weather_pearson_peak_offpeak_table.py
"""
from __future__ import annotations

import csv
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from analysis.analyze_weather_impact import (  # noqa: E402
    PERIODS_WA1_WA2_WB_WC,
    analyze_weather_impact,
)


def main() -> None:
    rows: list[dict] = []
    for period in PERIODS_WA1_WA2_WB_WC:
        name = period["name"]
        print(f"Analiza: {name} …")
        res = analyze_weather_impact(
            start_date=period["start_date"],
            end_date=period["end_date"],
            read_from_s3=True,
        )
        cpo = res.get("correlations_peak_offpeak")
        if not cpo:
            print(f"  (nema peak/off-peak rezultata za {name})")
            continue
        for segment in ("peak", "offpeak"):
            seg = cpo[segment]
            rows.append(
                {
                    "period": name,
                    "segment": segment,
                    "pearson_temp_vs_bikes": seg.get("pearson_temp_vs_bikes"),
                    "pearson_temp_vs_public_transport": seg.get("pearson_temp_vs_public_transport"),
                    "pearson_temp_vs_delay": seg.get("pearson_temp_vs_delay"),
                    "n_minutes": seg.get("n_minutes"),
                    "n_temp_bikes": seg.get("n_temp_bikes"),
                    "n_temp_pt": seg.get("n_temp_pt"),
                    "n_temp_delay": seg.get("n_temp_delay"),
                    "peak_definition": cpo.get("definition", ""),
                }
            )

    out_dir = os.path.join(PROJECT_ROOT, "analysis", "output")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "weather_pearson_peak_offpeak_wa1_wa2_wb_wc.csv")
    fieldnames = [
        "period",
        "segment",
        "pearson_temp_vs_bikes",
        "pearson_temp_vs_public_transport",
        "pearson_temp_vs_delay",
        "n_minutes",
        "n_temp_bikes",
        "n_temp_pt",
        "n_temp_delay",
        "peak_definition",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Sačuvano: {out_path} ({len(rows)} redova)")


if __name__ == "__main__":
    main()
