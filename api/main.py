"""FastAPI service layer over the normalized Parquet data. Runs on AWS Lambda through Mangum."""
from fastapi import Depends, FastAPI
from mangum import Mangum

from .deps import require_api_key
from .routers import bikes, flights, meta, results, siri, transit, weather

app = FastAPI(
    title="Smart traffic data API",
    description="Read-only access to normalized multimodal traffic data and precomputed analysis results.",
    version="0.1.0",
    dependencies=[Depends(require_api_key)],
)

for module in (meta, bikes, weather, flights, transit, siri, results):
    app.include_router(module.router)

handler = Mangum(app, lifespan="off")
