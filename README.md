# Weather Impact on Norwegian Urban Mobility

A data collection and analysis pipeline investigating the correlation between weather conditions and multimodal urban transport performance in Oslo, Norway. The study integrates real-time data streams from aviation, public transit, cycling infrastructure, and road traffic to quantify weather-driven disruptions across transport modes.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Data Collection                   │
│  Avinor · Entur SIRI ET · Oslo Bysykkel · Vegvesen  │
└────────────────────┬────────────────────────────────┘
                     │  real-time polling (JSON / Parquet)
                     ▼
┌─────────────────────────────────────────────────────┐
│    AWS S3  (s3://...)     │
│   normalized/         schema-aligned Parquet files  │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│                  Normalization                      │
│            analysis/normalize_data.py               │
│   Aligns schemas, casts types, writes normalized/   │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│               Weather Impact Analysis               │
│          analysis/analyze_weather_impact.py         │
│   Correlates weather with delays, availability,     │
│   travel times, and traffic incidents               │
└─────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Data | API / Format |
|--------|------|--------------|
| [Avinor](https://avinor.no/konsern/om-oss/åpne-data/) | Flight status, delays, cancellations at Norwegian airports | REST / JSON |
| [Entur](https://developer.entur.org/) | Public transport real-time estimated timetable (SIRI ET) | SIRI / Protobuf |
| [Oslo Bysykkel](https://oslobysykkel.no/apne-data/sanntid) | Bike-sharing station availability (GBFS) | REST / JSON |
| [Statens vegvesen](https://datex2.vegvesen.no/) | Road traffic situations and travel times | DATEX II / XML |

Weather data is fetched from the [MET Norway Frost API](https://frost.met.no/) and joined on timestamp and Oslo bounding box (lat 59.85–60.00, lon 10.60–10.85).

---

## Setup

```bash
git clone https://github.com/bobanda87/multimodal-mobility-pipeline
cd multimodal-mobility-pipeline

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # fill in AWS credentials and S3 bucket details
```

---

## Running the Pipeline

### 1. Data Collection

Start all four collectors in parallel (each logs to `logs/`):

```bash
bash run_all_collectors.sh
```

Stop collectors:

```bash
bash run_all_collectors.sh stop
```

Run a single collector:

```bash
PYTHONPATH=. python3 avinor/avinor_collect.py
PYTHONPATH=. python3 entur/entur_siri_et_collect.py
PYTHONPATH=. python3 oslobysykkel/oslobysykkel_collect.py
PYTHONPATH=. python3 vegvesen/vegvesen_collect.py
```

### 2. Normalization

Reads raw Parquet files from S3, aligns schemas across sources, and writes normalized files back to S3 under `data/normalized/`:

```bash
cd analysis && python3 normalize_data.py
```

### 3. Analysis

```bash
cd analysis

# Single period (last 7 days)
python3 analyze_weather_impact.py --mode single --period-days 7

# Compare across three phases
python3 analyze_weather_impact.py --mode compare --periods three_phases

# Weekday vs. weekend breakdown
python3 analyze_weather_impact.py --mode single --period-days 30 --weekday-only
python3 analyze_weather_impact.py --mode single --period-days 30 --weekend-only
```

---

## Project Structure

```
phdproject/
├── avinor/                    # Avinor flight data collector
├── entur/                     # Entur SIRI ET public transport collector
├── oslobysykkel/              # Oslo Bysykkel bike-sharing collector
├── vegvesen/                  # Vegvesen road traffic collector
├── analysis/
│   ├── normalize_data.py      # Schema normalization across sources
│   ├── analyze_weather_impact.py   # Correlation analysis + figures
│   └── inspect_normalized.py  # Data inspection utilities
├── utils/
│   └── s3_upload.py           # S3 read/write helpers
├── logs/                      # Per-run collector logs
├── run_all_collectors.sh      # Orchestration script
├── requirements.txt
└── .env.example
```

---

## Data Access

### Sample data (public, no AWS account required)

A sample of the dataset is publicly available for exploration and research purposes:

| | URL |
|---|---|
| Normalized | https://urban-mobility-research-data-sample-173471018037-eu-north-1-an.s3.eu-north-1.amazonaws.com/normalized/ |
| Raw | https://urban-mobility-research-data-sample-173471018037-eu-north-1-an.s3.eu-north-1.amazonaws.com/raw/ |

### Full dataset (requires AWS account)

The complete normalized dataset is archived on AWS S3 (region `eu-north-1`) with **Requester Pays** enabled:

```
https://urban-mobility-research-data-173471018037-eu-north-1-an.s3.eu-north-1.amazonaws.com/normalized/
```

You must have a valid AWS account and authenticate before accessing:

```bash
# CLI
aws s3 ls s3://urban-mobility-research-data-173471018037-eu-north-1-an/normalized/ --request-payer requester

# Python (boto3)
import boto3
s3 = boto3.client("s3")
s3.get_object(Bucket="urban-mobility-research-data-173471018037-eu-north-1-an",
              Key="normalized/...", RequestPayer="requester")
```

---

## Reproducibility

To reproduce the analysis from the archived data:

1. **Clone and install**
   ```bash
   git clone https://github.com/bobanda87/multimodal-mobility-pipeline && cd multimodal-mobility-pipeline
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure S3 access** — copy `.env.example` to `.env` and fill in credentials.

3. **Run normalization** (only needed if reproducing from raw data; skip if using pre-normalized files)
   ```bash
   cd analysis && python3 normalize_data.py
   ```

4. **Reproduce figures and tables**
   ```bash
   cd analysis
   python3 analyze_weather_impact.py --mode compare --periods three_phases
   ```
   Figures are written to the working directory as `.png` files.

5. **Verify data coverage** — use `inspect_normalized.py` to inspect date ranges and record counts across sources:
   ```bash
   cd analysis && python3 inspect_normalized.py
   ```

> All results are deterministic given the same normalized Parquet files. Random seeds are not used; analysis is fully aggregation-based.
