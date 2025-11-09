# Traffic Data Collection Project

A project for collecting traffic data from various sources in Norway.

## Data Sources

1. **Avinor** - Flight data (every 3 minutes)
2. **Entur** - GTFS-RT public transport data (every 3 minutes)
3. **Oslo Bysykkel** - GBFS bike sharing station data (every 3 minutes)
4. **Vegvesen** - DATEX traffic, weather, and travel time data (every 3 minutes)

## Installation

Set up a virtual environment and install all dependencies from the root directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

This will install all required packages for all collectors and the normalizer.

## Running Collectors

### Running All Collectors Simultaneously

Use the bash script `run_all_collectors.sh` which runs all collectors in the background:

```bash
bash run_all_collectors.sh
```

**Note:** On Ubuntu/Linux systems, make sure to use `bash` instead of `sh`:
```bash
bash run_all_collectors.sh
# or
./run_all_collectors.sh  # if the script has execute permissions
```

The script automatically:
- Runs all collectors in the background
- Saves each collector's output to a separate log file with timestamp
- Stores PID files for easy process termination

### Log Files

Logs are saved in the `logs/` directory with the format:
- `avinor_{timestamp}.log`
- `entur_{timestamp}.log`
- `oslobysykkel_{timestamp}.log`
- `vegvesen_{timestamp}.log`

### Monitoring Logs in Real-Time

```bash
tail -f logs/avinor_20250120_120000.log
tail -f logs/entur_20250120_120000.log
tail -f logs/oslobysykkel_20250120_120000.log
tail -f logs/vegvesen_20250120_120000.log
```

### Stopping All Collectors

```bash
bash run_all_collectors.sh stop
```

## Running Individual Collectors

If you want to run only one collector:

```bash
cd avinor
python3 avinor_collect.py
```

```bash
cd entur
python3 entur_collect.py
```

```bash
cd oslobysykkel
python3 oslobysykkel_collect.py
```

```bash
cd vegvesen
python3 vegvesen_collect.py
```

## Configuration

All collectors automatically determine where to store data:
- **Locally**: Data is saved in the `{source}_data/` directory
- **On server**: Data is saved in the `/mnt/volume_ams3_01/{source}_data/` directory

Collectors automatically detect if they are running on the server (by checking for the existence of the `/mnt/volume_ams3_01` directory).

## Collection Intervals

All collectors gather data every **3 minutes** (180 seconds) for easier data combination and analysis.

## Data Normalization

To normalize collected data, use:

```bash
cd normalizer
python3 normalize_data.py
```

## Project Structure

```
phdproject/
├── avinor/              # Avinor collector
├── entur/               # Entur collector
├── oslobysykkel/        # Oslo Bysykkel collector
├── vegvesen/            # Vegvesen collector
├── normalizer/          # Data normalization
├── logs/                # Log files (created automatically)
└── run_all_collectors.sh # Script to run all collectors
```
