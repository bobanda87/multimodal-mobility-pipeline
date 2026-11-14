from fastapi import APIRouter

router = APIRouter(tags=["meta"])

SOURCES = {
    "oslobysykkel": "Oslo Bysykkel station status (bikes and docks per station)",
    "vegvesen": "Statens vegvesen road weather observations",
    "avinor": "Avinor flights (scheduled time, status time, delay)",
    "entur_siri_et": "Entur SIRI ET trip delays (per-operator collector, normalized)",
    "entur_siri_national": "Entur SIRI ET for every operator, journeys and stop calls per minute (raw)",
}


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/sources")
def sources():
    return [{"source": name, "description": description} for name, description in SOURCES.items()]
