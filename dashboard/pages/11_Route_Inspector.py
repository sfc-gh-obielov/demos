import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Route Inspector", page_icon="🔍", layout="wide")
session = get_active_session()

st.title("Route Inspector")
st.markdown("Inspect raw GPS points and constructed linestrings for individual trips")

col_truck, col_date = st.columns(2)

with col_truck:
    truck_query = """
    SELECT DISTINCT TRUCK_ID FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS ORDER BY TRUCK_ID
    """
    trucks = session.sql(truck_query).to_pandas()["TRUCK_ID"].tolist()
    selected_truck = st.selectbox("Truck", trucks)

with col_date:
    date_query = f"""
    SELECT DISTINCT TRIP_DATE 
    FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS 
    WHERE TRUCK_ID = '{selected_truck}'
    ORDER BY TRIP_DATE
    """
    dates = session.sql(date_query).to_pandas()["TRIP_DATE"].tolist()
    selected_date = st.selectbox("Date", dates)

trip_query = f"""
SELECT TRIP_ID, ORIGIN_CITY, DEST_CITY, DISTANCE_DEVIATION_PCT, IS_ROUTE_DEVIATION
FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS
WHERE TRUCK_ID = '{selected_truck}' AND TRIP_DATE = '{selected_date}'
ORDER BY TRIP_ID
"""
trip_df = session.sql(trip_query).to_pandas()

if trip_df.empty:
    st.warning("No trips found for this truck/date combination")
    st.stop()

selected_trip = st.selectbox(
    "Trip",
    trip_df["TRIP_ID"].tolist(),
    format_func=lambda x: (
        f"{x} | "
        f"{trip_df[trip_df['TRIP_ID']==x]['ORIGIN_CITY'].iloc[0]} → "
        f"{trip_df[trip_df['TRIP_ID']==x]['DEST_CITY'].iloc[0]} | "
        f"Dev: {trip_df[trip_df['TRIP_ID']==x]['DISTANCE_DEVIATION_PCT'].iloc[0]:.1f}%"
    )
)

show_expected = st.toggle("Show Expected Route", value=True)
filter_teleportation = st.toggle("Filter GPS Teleportation", value=True, help="Remove GPS points that jump >50km between consecutive pings")
render_mode = st.radio("Actual Path Rendering", ["Points", "LineString (timestamp-ordered)"], horizontal=True)

points_query = f"""
WITH ordered AS (
    SELECT 
        TS,
        ST_X(GEOMETRY) AS LNG,
        ST_Y(GEOMETRY) AS LAT,
        ROW_NUMBER() OVER (ORDER BY TS) AS RN
    FROM SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.FACT_TRUCK_TELEMETRY_V2
    WHERE TRIP_ID = '{selected_trip}'
)
SELECT 
    a.TS, a.LNG, a.LAT, a.RN AS SEQ,
    COALESCE(ST_DISTANCE(ST_MAKEPOINT(a.LNG, a.LAT), ST_MAKEPOINT(b.LNG, b.LAT)), 0) AS DIST_M,
    COALESCE(TIMESTAMPDIFF('SECOND', b.TS, a.TS), 0) AS DT_SEC
FROM ordered a
LEFT JOIN ordered b ON b.RN = a.RN - 1
ORDER BY a.RN
"""
points_df = session.sql(points_query).to_pandas()

if points_df.empty:
    st.warning("No GPS points found for this trip")
    st.stop()

raw_count = len(points_df)
if filter_teleportation and len(points_df) > 1:
    points_df['SPEED_KMH'] = np.where(
        points_df['DT_SEC'] > 0,
        (points_df['DIST_M'] / points_df['DT_SEC']) * 3.6,
        0
    )
    points_df = points_df[points_df['SPEED_KMH'] <= 250].reset_index(drop=True)
    for _ in range(3):
        if len(points_df) < 2:
            break
        lngs = points_df['LNG'].values
        lats = points_df['LAT'].values
        dlat = np.radians(np.diff(lats))
        dlng = np.radians(np.diff(lngs))
        lat1 = np.radians(lats[:-1])
        lat2 = np.radians(lats[1:])
        a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlng/2)**2
        dists = 6371000 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        keep = np.concatenate([[True], dists < 50000])
        if keep.all():
            break
        points_df = points_df[keep].reset_index(drop=True)

col1, col2, col3, col4 = st.columns(4)
with col1:
    label = f"{len(points_df):,}"
    if filter_teleportation and len(points_df) < raw_count:
        label += f" (filtered from {raw_count:,})"
    st.metric("GPS Points", label)
with col2:
    st.metric("First Ping", str(points_df["TS"].iloc[0])[:19])
with col3:
    st.metric("Last Ping", str(points_df["TS"].iloc[-1])[:19])
with col4:
    duration_min = (points_df["TS"].iloc[-1] - points_df["TS"].iloc[0]).total_seconds() / 60
    st.metric("Duration", f"{duration_min:.0f} min")

layers = []

if render_mode == "Points":
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=points_df,
            get_position="[LNG, LAT]",
            get_color="[50, 100, 255, 180]",
            get_radius=80,
            pickable=True,
        )
    )
else:
    path_coords = points_df[["LNG", "LAT"]].values.tolist()
    layers.append(
        pdk.Layer(
            "PathLayer",
            data=[{"path": path_coords, "name": "Actual (timestamp-ordered)"}],
            get_path="path",
            get_color=[50, 100, 255, 200],
            get_width=5,
            width_min_pixels=3,
            pickable=True,
        )
    )

if show_expected:
    expected_query = f"""
    SELECT ST_ASGEOJSON(EXPECTED_PATH)::STRING AS geojson
    FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS
    WHERE TRIP_ID = '{selected_trip}'
    """
    expected_df = session.sql(expected_query).to_pandas()
    if not expected_df.empty and expected_df["GEOJSON"].iloc[0]:
        geo = json.loads(expected_df["GEOJSON"].iloc[0])
        if "coordinates" in geo:
            expected_coords = [[c[0], c[1]] for c in geo["coordinates"]]
            layers.append(
                pdk.Layer(
                    "PathLayer",
                    data=[{"path": expected_coords, "name": "Expected (ORS)"}],
                    get_path="path",
                    get_color=[255, 50, 50, 180],
                    get_width=4,
                    width_min_pixels=2,
                    pickable=True,
                )
            )

center_lat = points_df["LAT"].mean()
center_lng = points_df["LNG"].mean()
lat_range = points_df["LAT"].max() - points_df["LAT"].min()
lng_range = points_df["LNG"].max() - points_df["LNG"].min()
max_range = max(lat_range, lng_range)

if max_range < 0.05:
    zoom = 13
elif max_range < 0.2:
    zoom = 11
elif max_range < 0.5:
    zoom = 10
elif max_range < 1.0:
    zoom = 9
elif max_range < 2.0:
    zoom = 8
elif max_range < 4.0:
    zoom = 7
else:
    zoom = 6

view_state = pdk.ViewState(latitude=center_lat, longitude=center_lng, zoom=zoom, pitch=0)
deck = pdk.Deck(layers=layers, initial_view_state=view_state, map_style="light", tooltip={"text": "{name}\n{TS}\nSeq: {SEQ}"})
st.pydeck_chart(deck, use_container_width=True)

legend_parts = ["**Blue** = Actual GPS"]
if show_expected:
    legend_parts.append("**Red** = Expected ORS Route")
st.markdown(" | ".join(legend_parts))

with st.expander("GPS Points Table"):
    st.dataframe(points_df, use_container_width=True, hide_index=True)
