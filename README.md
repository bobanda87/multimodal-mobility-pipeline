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

This will install all required packages for all collectors and the analysis tools.

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

### Local Storage

All collectors automatically determine where to store data:
- **Locally**: Data is saved in the `{source}_data/` directory
- **On server**: Data is saved in the `/mnt/volume_ams3_01/{source}_data/` directory

Collectors automatically detect if they are running on the server (by checking for the existence of the `/mnt/volume_ams3_01` directory).

### S3 Storage

The project supports automatic upload of data files to AWS S3. To enable S3 upload:

#### Step 1: Create AWS Account and S3 Bucket

1. **Create AWS Account** (if you don't have one):
   - Go to https://aws.amazon.com/ and sign up for a free account
   - You'll need a credit card, but AWS Free Tier includes 5GB of S3 storage for 12 months

2. **Create S3 Bucket**:
   - Log in to AWS Console: https://console.aws.amazon.com/
   - Go to S3 service
   - Click "Create bucket"
   - Choose a unique bucket name (e.g., `phdproject-data`)
   - Select a region (e.g., `eu-north-1` for Stockholm, or choose one closer to you)
   - Leave other settings as default or configure as needed
   - Click "Create bucket"

#### Step 2: Create IAM User with S3 Access

1. **Create IAM User**:
   - Go to IAM service in AWS Console
   - Click "Users" → "Create user"
   - Enter username (e.g., `s3-uploader`)
   - Click "Next"

2. **Attach S3 Policy**:
   - Select "Attach policies directly"
   - Search for and select `AmazonS3FullAccess` (or create a custom policy with only PutObject permission for security)
   - Click "Next" → "Create user"

3. **Create Access Keys**:
   - Click on the newly created user
   - Go to "Security credentials" tab
   - Scroll to "Access keys" section
   - Click "Create access key"
   - Select "Application running outside AWS" as use case
   - Click "Next" → "Create access key"
   - **IMPORTANT**: Copy both:
     - **Access key ID** (starts with `AKIA...`)
     - **Secret access key** (click "Show" to reveal it)
   - Save these credentials securely - you won't be able to see the secret key again!

#### Step 3: Configure Environment Variables

1. Copy `env.example` to `.env`:
   ```bash
   cp env.example .env
   ```

2. Edit `.env` and add your AWS credentials:
   ```bash
   AWS_ACCESS_KEY_ID=AKIA...your_access_key_id
   AWS_SECRET_ACCESS_KEY=your_secret_access_key
   AWS_REGION=eu-north-1  # Use the region where you created your bucket
   S3_BUCKET_NAME=phdproject-data  # Your bucket name
   S3_JSON_PATH=data/json
   S3_PARQUET_PATH=data/parquet
   ```

#### Step 4: Verify Configuration

Data files are automatically uploaded to S3:
- **JSON files** → `s3://{bucket}/{S3_JSON_PATH}/{source}/{date}/filename.json`
- **Parquet files** → `s3://{bucket}/{S3_PARQUET_PATH}/{source}/{date}/filename.parquet`
- **Normalized data** → `s3://{bucket}/{S3_PARQUET_PATH}/normalized/{date}/filename.parquet`

**Note**: If S3 credentials are not configured, the collectors will continue to work normally but will skip S3 uploads (you'll see warning messages in the logs).

## Collection Intervals

All collectors gather data every **3 minutes** (180 seconds) for easier data combination and analysis.

## Data Normalization

To normalize collected data, use:

```bash
cd analysis
python3 normalize_data.py
```

This will process all collected parquet files and create normalized versions in the `normalized_data/` directory.

## Data Analysis

### Weather Impact Analysis

Analyze the correlation between weather conditions and transportation systems:

```bash
cd analysis
python3 analyze_weather_impact.py
```

This analysis examines:
- **Oslo Bysykkel**: Correlation between temperature and bike availability
- **Public Transport (Entur)**: Correlation between temperature and vehicle activity in Oslo area
- **Aviation (Avinor)**: Correlation between temperature and flight activity

The analysis uses normalized data and provides correlation coefficients and interpretations.

## Project Structure

```
phdproject/
├── avinor/              # Avinor collector
├── entur/               # Entur collector
├── oslobysykkel/        # Oslo Bysykkel collector
├── vegvesen/            # Vegvesen collector
├── analysis/            # Data normalization and analysis
├── logs/                # Log files (created automatically)
└── run_all_collectors.sh # Script to run all collectors
```
