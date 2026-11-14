"""The API key is a security scheme (Authorize button in /docs), still enforced, and the docs stay reachable."""
import os, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
os.environ["API_KEY"] = "k" * 20
from fastapi.testclient import TestClient
from api import config
config.API_KEY = "k" * 20
from api.main import app
c = TestClient(app)
spec = c.get("/openapi.json").json()
scheme = spec["components"]["securitySchemes"]
print("security schemes:", {k: (v["type"], v["in"], v["name"]) for k, v in scheme.items()})
assert any(v["type"] == "apiKey" and v["name"] == "X-API-Key" for v in scheme.values())
params = [p["name"] for p in spec["paths"]["/siri/journeys/latest"]["get"]["parameters"]]
assert "x-api-key" not in params, params
assert c.get("/docs").status_code == 200 and c.get("/redoc").status_code == 200
assert c.get("/health").status_code == 401 and c.get("/health", headers={"X-API-Key": "wrong"}).status_code == 401
assert c.get("/health", headers={"X-API-Key": "k" * 20}).status_code == 200
print("tags:", [t["name"] for t in spec["tags"]][:4], "...", "| title:", spec["info"]["title"], spec["info"]["version"])
print("swagger auth tests passed")
