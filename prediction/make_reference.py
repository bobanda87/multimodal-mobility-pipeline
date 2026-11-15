"""
One-off: small static reference tables used for the spatial joins (written to prediction/reference/).

  weather_sites.parquet  Vegvesen weather station id -> lat/lon     (DATEX II GetMeasurementWeatherSiteTable)
  stops.parquet          NSR quay/stop id -> lat/lon                (stops.txt of the Entur GTFS Norway export)
  routes.parquet         line id -> GTFS route_type, gives the mode where SIRI has none (routes.txt of the same export)
  bike_stations.parquet  Oslo Bysykkel station id -> lat/lon        (latest station_information file on S3)
  traffic_vertices.parquet  Vegvesen travel time location id -> vertices of its road line, lat/lon
                         (DATEX II GetPredefinedTravelTimeLocations, EPSG:32633 converted with pyproj)

    PYTHONPATH=. python -m prediction.make_reference weather
    PYTHONPATH=. python -m prediction.make_reference stops <gtfs.zip>
    PYTHONPATH=. python -m prediction.make_reference bikes
    PYTHONPATH=. python -m prediction.make_reference traffic
"""
import io
import os
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

OUT = Path(__file__).parent / "reference"
SITE_URL = "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetMeasurementWeatherSiteTable/pullsnapshotdata"
NS6 = "{http://datex2.eu/schema/3/roadTrafficData}"
NS9 = "{http://datex2.eu/schema/3/locationReferencing}"


def parse_weather_sites(xml: bytes) -> pd.DataFrame:
    rows = []
    for _, el in ET.iterparse(io.BytesIO(xml), events=("end",)):
        if el.tag != NS6 + "measurementSite":
            continue
        lat, lon = el.find(f".//{NS9}latitude"), el.find(f".//{NS9}longitude")
        name = el.find(f"{NS6}measurementSiteName//value")
        if lat is not None and lon is not None:
            rows.append({"site_id": el.get("id"), "name": name.text if name is not None else None,
                         "lat": float(lat.text), "lon": float(lon.text)})
        el.clear()
    return pd.DataFrame(rows).drop_duplicates("site_id")


def weather():
    load_dotenv()
    r = requests.get(SITE_URL, auth=HTTPBasicAuth(os.getenv("VEGVESEN_USER"), os.getenv("VEGVESEN_PASS")), timeout=120)
    r.raise_for_status()
    df = parse_weather_sites(r.content)
    df.to_parquet(OUT / "weather_sites.parquet", index=False)
    print("weather sites:", len(df))


TT_URL = "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetPredefinedTravelTimeLocations/pullsnapshotdata"
_LINE = re.compile(r'predefinedLocationReference xsi:type="ns9:PredefinedLocation" id="(\d+)".*?<ns9:posList>([^<]+)</ns9:posList>',
                   re.S)


def parse_traffic_vertices(xml: str) -> pd.DataFrame:
    from pyproj import Transformer
    to_wgs84 = Transformer.from_crs(32633, 4326, always_xy=True)
    rows = []
    for m in _LINE.finditer(xml):
        nums = list(map(float, m.group(2).split()))
        lon, lat = to_wgs84.transform(nums[0::2], nums[1::2])
        rows += [(m.group(1), a, b) for a, b in zip(lat, lon)]
    return pd.DataFrame(rows, columns=["location_id", "lat", "lon"])


def traffic():
    load_dotenv()
    r = requests.get(TT_URL, auth=HTTPBasicAuth(os.getenv("VEGVESEN_USER"), os.getenv("VEGVESEN_PASS")), timeout=120)
    r.raise_for_status()
    df = parse_traffic_vertices(r.text)
    df.to_parquet(OUT / "traffic_vertices.parquet", index=False)
    print("traffic vertices:", len(df), "locations:", df.location_id.nunique())


def stops(zip_path: str):
    with zipfile.ZipFile(zip_path) as z, z.open("stops.txt") as f:
        df = pd.read_csv(f, usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"], dtype={"stop_id": str})
    df.columns = ["stop_id", "name", "lat", "lon"]
    df.to_parquet(OUT / "stops.parquet", index=False)
    print("stops:", len(df), df.stop_id.head(3).tolist())
    with zipfile.ZipFile(zip_path) as z, z.open("routes.txt") as f:
        routes = pd.read_csv(f, usecols=["route_id", "route_type"], dtype=str)
    routes.to_parquet(OUT / "routes.parquet", index=False)  # SIRI gives no vehicle mode for RUT, TRO, AKT ...
    print("routes:", len(routes))


def bikes():
    from prediction.s3db import ROOT, connect
    con = connect()
    day = pd.Timestamp.now(tz="Europe/Oslo").strftime("%Y-%m-%d")
    df = con.execute(f"""SELECT DISTINCT station_id, name, lat, lon, capacity
        FROM read_parquet('{ROOT}/data/parquet/oslobysykkel/{day}/station_information_0*.parquet', union_by_name=true)
        WHERE lat IS NOT NULL""").df()
    df.to_parquet(OUT / "bike_stations.parquet", index=False)
    print("bike stations:", len(df))


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    {"weather": weather, "bikes": bikes, "traffic": traffic}.get(sys.argv[1], lambda: stops(sys.argv[2]))()
