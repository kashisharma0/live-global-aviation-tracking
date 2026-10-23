
import json
import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

LIVE = "data/live"

st.set_page_config(page_title="Live Global Aviation Tracking", layout="wide")


def load(name):
    with open(os.path.join(LIVE, name), encoding="utf-8") as f:
        return json.load(f)


def demo_data():
    positions = [
        ["a10001", "UAL101", "United States", 40.7, -74.0, 35000, 850, 90, False],
        ["a10002", "BAW218", "United Kingdom", 51.5, -0.1, 37000, 820, 180, False],
        ["a10003", "UAE202", "United Arab Emirates", 25.2, 55.3, 36000, 880, 270, False],
        ["a10004", "IGO123", "India", 28.6, 77.2, 33000, 790, 120, False],
        ["a10005", "SIA305", "Singapore", 1.35, 103.8, 34000, 830, 160, False],
        ["a10006", "DLH400", "Germany", 50.1, 8.7, 38000, 810, 210, False],
        ["a10007", "QFA12", "Australia", -33.9, 151.2, 36000, 870, 300, False],
        ["a10008", "AFR220", "France", 48.9, 2.4, 32000, 800, 150, False],
        ["a10009", "JAL505", "Japan", 35.7, 139.7, 39000, 840, 80, False],
        ["a10010", "ACA789", "Canada", 43.7, -79.4, 31000, 780, 240, False],
        ["a10011", "THA610", "Thailand", 13.8, 100.5, 29000, 760, 190, False],
        ["a10012", "KLM601", "Netherlands", 52.3, 4.8, 37000, 825, 100, False],
    ]

    countries = pd.DataFrame({
        "country": [
            "United States", "India", "United Kingdom", "Germany",
            "United Arab Emirates", "Singapore", "Japan", "France"
        ],
        "aircraft": [3200, 1800, 1200, 950, 780, 620, 590, 560]
    })

    bands = pd.DataFrame({
        "band": ["0–10k ft", "10–20k ft", "20–30k ft", "30–40k ft", "40k+ ft"],
        "aircraft": [850, 1400, 2600, 6100, 900]
    })

    clusters = {
        "silhouette": 0.72,
        "clusters": [
            {"prediction": 0, "aircraft": 4, "description": "Lower altitude"},
            {"prediction": 1, "aircraft": 5, "description": "Cruising"},
            {"prediction": 2, "aircraft": 3, "description": "High altitude"},
        ]
    }

    kpis = {
        "total_aircraft": 12,
        "airborne": 12,
        "grounded": 0,
        "avg_altitude_ft": 35000,
        "avg_speed_kmh": 820,
        "snapshot_time": datetime.now(timezone.utc).timestamp()
    }

    return kpis, countries, bands, clusters, positions, True


st.title("Live Global Aviation and Flight Tracking")
st.caption(
    "OpenSky API → Kafka → Apache Spark Structured Streaming → "
    "Parquet Data Lake → Streamlit"
)

try:
    k = load("kpis.json")
    countries = pd.DataFrame(load("countries.json"))
    bands = pd.DataFrame(load("altitude_bands.json"))
    km = load("clusters.json")
    pos = pd.DataFrame(
        load("positions.json"),
        columns=[
            "hex", "callsign", "country", "latitude", "longitude",
            "altitude", "velocity", "heading", "on_ground"
        ]
    )
    demo = False

except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
    k, countries, bands, km, positions, demo = demo_data()
    pos = pd.DataFrame(
        positions,
        columns=[
            "hex", "callsign", "country", "latitude", "longitude",
            "altitude", "velocity", "heading", "on_ground"
        ]
    )

if demo:
    st.info(
        "DEMO MODE — Sample aircraft data is displayed. "
        "These are illustrative positions, not live flight locations."
    )
else:
    st.success("Pipeline data loaded.")

if not demo:
    snap = datetime.fromtimestamp(k["snapshot_time"], tz=timezone.utc)
    st.write(f"Data snapshot: **{snap:%Y-%m-%d %H:%M:%S UTC}**")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Aircraft tracked", f"{k['total_aircraft']:,}")
c2.metric("Airborne", f"{k['airborne']:,}")
c3.metric("On ground", f"{k['grounded']:,}")
c4.metric("Avg altitude (ft)", f"{int(k['avg_altitude_ft']):,}")
c5.metric("Avg speed (km/h)", f"{int(k['avg_speed_kmh']):,}")

st.subheader("Aircraft map")
st.map(pos, latitude="latitude", longitude="longitude")

left, right = st.columns(2)
with left:
    st.subheader("Aircraft by country")
    st.bar_chart(countries.set_index("country")["aircraft"])

with right:
    st.subheader("Aircraft by altitude band")
    st.bar_chart(bands.set_index("band")["aircraft"])

st.subheader(f"K-means clusters (silhouette score = {km['silhouette']})")
st.dataframe(pd.DataFrame(km["clusters"]))

st.subheader("Country statistics")
st.dataframe(countries, use_container_width=True)
