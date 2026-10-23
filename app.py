import json
import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

LIVE = "data/live"
st.set_page_config(page_title="Live Flight Dashboard", layout="wide")


def load(name):
    with open(os.path.join(LIVE, name)) as f:
        return json.load(f)


st.title("Live Global Aviation and Flight Tracking")
st.caption("Pipeline: OpenSky API → Kafka → Apache Spark cluster (SQL + MLlib) → Streamlit")

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
    info = load("run_info.json")
except FileNotFoundError:
    st.error("No results yet. Run the Spark job first (.\\run_job.ps1).")
    st.stop()

snap = datetime.fromtimestamp(k["snapshot_time"], tz=timezone.utc)
st.write(
    f"Snapshot time: **{snap:%Y-%m-%d %H:%M:%S} UTC** | "
    "Data source: **Kafka + Spark Structured Streaming**"
)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Aircraft tracked", f"{k['total_aircraft']:,}")
c2.metric("Airborne", f"{k['airborne']:,}")
c3.metric("On ground", f"{k['grounded']:,}")
c4.metric("Avg altitude (ft)", f"{int(k['avg_altitude_ft']):,}")
c5.metric("Avg speed (km/h)", f"{int(k['avg_speed_kmh']):,}")

st.subheader("Aircraft map (live positions)")

if not pos.empty and {"latitude", "longitude"}.issubset(pos.columns):
    pos = pos.dropna(subset=["latitude", "longitude"])
    pos = pos[
        pos["latitude"].between(-90, 90)
        & pos["longitude"].between(-180, 180)
    ]

    st.map(
        pos.sample(min(len(pos), 4000), random_state=42)
        if len(pos) > 4000 else pos,
        latitude="latitude",
        longitude="longitude",
    )
else:
    st.info("No valid aircraft positions are available yet.")

left, right = st.columns(2)
with left:
    st.subheader("Top 15 countries by aircraft")
    st.bar_chart(countries.set_index("country")["aircraft"])
with right:
    st.subheader("Airborne aircraft by altitude band")
    st.bar_chart(bands.set_index("band")["aircraft"])

st.subheader(f"K-means clusters (silhouette score = {km['silhouette']})")
st.dataframe(pd.DataFrame(km["clusters"]).rename(columns={"prediction": "cluster"}))

st.subheader("Country statistics")
st.dataframe(countries)