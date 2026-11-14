"""FastAPI service layer over the normalized Parquet data. Runs on AWS Lambda through Mangum."""
from fastapi import Depends, FastAPI
from mangum import Mangum

from .deps import require_api_key
from .routers import bikes, flights, meta, results, siri, transit, weather

TAGS = [
    {"name": "meta", "description": "Health and the list of data sources."},
    {"name": "micromobility", "description": "Oslo Bysykkel station status."},
    {"name": "infrastructure", "description": "Statens vegvesen road weather."},
    {"name": "aviation", "description": "Avinor flights and delays."},
    {"name": "public transport", "description": "Entur SIRI ET delays collected per operator (normalized)."},
    {"name": "public transport (SIRI ET, all operators)",
     "description": "Entur SIRI ET for every operator once a minute: journey state and how a delay develops stop by stop."},
    {"name": "analysis results", "description": "Precomputed result tables."},
]

app = FastAPI(
    title="Smart traffic data API",
    description=(
        "Read-only access to multimodal traffic data collected from Entur, Statens vegvesen, Avinor and Oslo Bysykkel.\n\n"
        "Every request needs the `X-API-Key` header: press **Authorize** and enter the key once. "
        "Times are UTC. Ranges are limited to 31 days (72 hours for `/siri/delays`)."
    ),
    version="0.2.0",
    openapi_tags=TAGS,
    dependencies=[Depends(require_api_key)],
)

for module in (meta, bikes, weather, flights, transit, siri, results):
    app.include_router(module.router)

handler = Mangum(app, lifespan="off")
