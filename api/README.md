# Data API (FastAPI on Lambda)

Read-only service over the normalized Parquet files on S3. DuckDB queries the files in place;
nothing is loaded wholesale into memory and nothing is written to the collected data.

| Endpoint | Source |
|---|---|
| `GET /health`, `GET /sources` | |
| `GET /bikes/availability?start=&end=&bucket=hour&station=` | oslobysykkel |
| `GET /weather/observations?start=&end=&bucket=hour&location=` | vegvesen |
| `GET /flights/delays?start=&end=&bucket=day&airport=` | avinor |
| `GET /transit/delays?start=&end=&bucket=hour&route_id=` | entur_siri_et |
| `GET /results`, `GET /results/{name}.csv` | tables uploaded with `publish_results.py` |

Requests are limited to 31 days. Interactive docs are at `/docs`.

## Run locally (uses the AWS credentials from `.env`)

```
pip install -r api/requirements.txt
uvicorn api.main:app --reload
```

## Deploy

Needs AWS credentials that may create Lambda, IAM roles and ECR (like the collectors' stack) and Docker.

```
cd api
sam build
sam deploy --guided --parameter-overrides ApiKey=<at-least-16-chars>
```

Call it with `curl -H "X-API-Key: ..." "<ApiUrl>/sources"`.
