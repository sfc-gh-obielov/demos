import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import pydeck as pdk
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Route Deviations - Fleet Analytics",
    page_icon="🔀",
    layout="wide"
)

session = get_active_session()

st.title("Route Deviation Intelligence")
st.markdown("Comparing actual truck routes against ORS-expected routes to detect deviations")

kpi_query = """
SELECT 
    COUNT(*) AS total_trips,
    SUM(CASE WHEN IS_ROUTE_DEVIATION THEN 1 ELSE 0 END) AS deviation_trips,
    SUM(CASE WHEN IS_DISTANCE_DEVIATION THEN 1 ELSE 0 END) AS distance_dev_trips,
    SUM(CASE WHEN IS_DURATION_DEVIATION THEN 1 ELSE 0 END) AS duration_dev_trips,
    ROUND(SUM(CASE WHEN IS_ROUTE_DEVIATION THEN DISTANCE_DEVIATION_KM ELSE 0 END), 0) AS total_excess_km,
    ROUND(SUM(CASE WHEN IS_ROUTE_DEVIATION THEN DURATION_DEVIATION_MIN ELSE 0 END), 0) AS total_excess_min
FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS
"""
kpi_df = session.sql(kpi_query).to_pandas()

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Trips Analyzed", f"{kpi_df['TOTAL_TRIPS'].iloc[0]:,}")
with col2:
    dev_trips = kpi_df['DEVIATION_TRIPS'].iloc[0]
    total = kpi_df['TOTAL_TRIPS'].iloc[0]
    st.metric("Route Deviations", f"{dev_trips:,}", f"{dev_trips/total*100:.1f}%")
with col3:
    st.metric("Distance Deviations", f"{kpi_df['DISTANCE_DEV_TRIPS'].iloc[0]:,}")
with col4:
    st.metric("Duration Deviations", f"{kpi_df['DURATION_DEV_TRIPS'].iloc[0]:,}")
with col5:
    st.metric("Total Excess Distance", f"{kpi_df['TOTAL_EXCESS_KM'].iloc[0]:,.0f} km")

st.divider()

with st.sidebar:
    st.header("Filters")

    variation_query = "SELECT DISTINCT ROUTE_VARIATION FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS ORDER BY ROUTE_VARIATION"
    variations_df = session.sql(variation_query).to_pandas()
    selected_variations = st.multiselect(
        "Route Variation",
        options=variations_df["ROUTE_VARIATION"].tolist(),
        default=None
    )

    trip_type_query = "SELECT DISTINCT TRIP_TYPE FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS ORDER BY TRIP_TYPE"
    trip_types_df = session.sql(trip_type_query).to_pandas()
    selected_trip_types = st.multiselect(
        "Trip Type",
        options=trip_types_df["TRIP_TYPE"].tolist(),
        default=None
    )

    deviation_only = st.toggle("Show Deviations Only", value=False)

where_clauses = []
if selected_variations:
    vl = ", ".join([f"'{v}'" for v in selected_variations])
    where_clauses.append(f"ROUTE_VARIATION IN ({vl})")
if selected_trip_types:
    tl = ", ".join([f"'{t}'" for t in selected_trip_types])
    where_clauses.append(f"TRIP_TYPE IN ({tl})")
if deviation_only:
    where_clauses.append("IS_ROUTE_DEVIATION = TRUE")
where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""

tab1, tab2, tab3 = st.tabs(["Driver Rankings", "Daily Trends", "Trip Detail"])

with tab1:
    st.subheader("Top Deviating Drivers")

    driver_query = f"""
    SELECT
        d.TRUCK_ID,
        d.DRIVER_ID,
        d.DRIVER_PROFILE,
        d.TRUCK_TYPE,
        d.HOME_CITY,
        d.TOTAL_TRIPS,
        d.DEVIATION_TRIPS,
        d.DEVIATION_RATE_PCT,
        d.TOTAL_EXCESS_KM,
        d.TOTAL_TIME_LOST_MIN,
        d.AVG_DISTANCE_DEVIATION_PCT,
        d.MAX_DISTANCE_DEVIATION_PCT
    FROM FLEET_DEMOS.ROUTE_DEVIATIONS.DRIVER_DEVIATION_SUMMARY d
    WHERE d.TOTAL_TRIPS >= 1
    ORDER BY d.TOTAL_EXCESS_KM DESC
    LIMIT 50
    """
    drivers_df = session.sql(driver_query).to_pandas()

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**Excess Distance by Driver (Top 20)**")
            top20 = drivers_df.head(20)
            chart = alt.Chart(top20).mark_bar().encode(
                x=alt.X("TOTAL_EXCESS_KM:Q", title="Total Excess Distance (km)"),
                y=alt.Y("TRUCK_ID:N", title="Truck", sort="-x"),
                color=alt.Color("DRIVER_PROFILE:N", title="Profile"),
                tooltip=["TRUCK_ID", "DRIVER_PROFILE", "TOTAL_TRIPS", "DEVIATION_TRIPS", "TOTAL_EXCESS_KM"]
            ).properties(height=500)
            st.altair_chart(chart, use_container_width=True)

    with col2:
        with st.container(border=True):
            st.markdown("**Deviation Rate by Driver Profile**")
            profile_query = """
            SELECT 
                DRIVER_PROFILE,
                COUNT(*) AS driver_count,
                ROUND(AVG(DEVIATION_RATE_PCT), 2) AS avg_deviation_rate,
                ROUND(AVG(TOTAL_EXCESS_KM), 2) AS avg_excess_km,
                ROUND(AVG(AVG_DISTANCE_DEVIATION_PCT), 2) AS avg_dist_dev_pct
            FROM FLEET_DEMOS.ROUTE_DEVIATIONS.DRIVER_DEVIATION_SUMMARY
            WHERE TOTAL_TRIPS >= 1
            GROUP BY DRIVER_PROFILE
            ORDER BY avg_deviation_rate DESC
            """
            profile_df = session.sql(profile_query).to_pandas()
            chart2 = alt.Chart(profile_df).mark_bar().encode(
                x=alt.X("AVG_DEVIATION_RATE:Q", title="Avg Deviation Rate (%)"),
                y=alt.Y("DRIVER_PROFILE:N", title="Profile", sort="-x"),
                color=alt.Color("DRIVER_PROFILE:N", legend=None),
                tooltip=["DRIVER_PROFILE", "DRIVER_COUNT", "AVG_DEVIATION_RATE", "AVG_EXCESS_KM"]
            ).properties(height=200)
            st.altair_chart(chart2, use_container_width=True)

            st.markdown("**Deviation Rate by Truck Type**")
            type_query = """
            SELECT 
                TRUCK_TYPE,
                COUNT(*) AS driver_count,
                ROUND(AVG(DEVIATION_RATE_PCT), 2) AS avg_deviation_rate,
                ROUND(AVG(TOTAL_EXCESS_KM), 2) AS avg_excess_km
            FROM FLEET_DEMOS.ROUTE_DEVIATIONS.DRIVER_DEVIATION_SUMMARY
            WHERE TOTAL_TRIPS >= 1
            GROUP BY TRUCK_TYPE
            ORDER BY avg_deviation_rate DESC
            """
            type_df = session.sql(type_query).to_pandas()
            chart3 = alt.Chart(type_df).mark_bar().encode(
                x=alt.X("AVG_DEVIATION_RATE:Q", title="Avg Deviation Rate (%)"),
                y=alt.Y("TRUCK_TYPE:N", title="Truck Type", sort="-x"),
                color=alt.Color("TRUCK_TYPE:N", legend=None),
                tooltip=["TRUCK_TYPE", "DRIVER_COUNT", "AVG_DEVIATION_RATE", "AVG_EXCESS_KM"]
            ).properties(height=200)
            st.altair_chart(chart3, use_container_width=True)

    st.markdown("**Full Driver Table**")
    st.dataframe(drivers_df, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("Daily Deviation Trends")

    daily_query = f"""
    SELECT 
        TRIP_DATE,
        DAY_OF_WEEK,
        TOTAL_TRIPS,
        DEVIATION_TRIPS,
        DEVIATION_RATE_PCT,
        TOTAL_EXCESS_DISTANCE_KM,
        AVG_DISTANCE_DEVIATION_PCT,
        AVG_DURATION_DEVIATION_PCT
    FROM FLEET_DEMOS.ROUTE_DEVIATIONS.DAILY_DEVIATION_TRENDS
    ORDER BY TRIP_DATE
    """
    daily_df = session.sql(daily_query).to_pandas()

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**Trips & Deviations by Day**")
            base = alt.Chart(daily_df).encode(x=alt.X("TRIP_DATE:T", title="Date"))
            bars = base.mark_bar(opacity=0.3, color="#4A90D9").encode(
                y=alt.Y("TOTAL_TRIPS:Q", title="Total Trips")
            )
            line = base.mark_line(color="#E74C3C", strokeWidth=2).encode(
                y=alt.Y("DEVIATION_TRIPS:Q", title="Deviation Trips")
            )
            chart = alt.layer(bars, line).resolve_scale(y='independent').properties(height=350)
            st.altair_chart(chart, use_container_width=True)

    with col2:
        with st.container(border=True):
            st.markdown("**Deviation Rate Over Time**")
            chart2 = alt.Chart(daily_df).mark_area(
                opacity=0.4,
                color="#E74C3C"
            ).encode(
                x=alt.X("TRIP_DATE:T", title="Date"),
                y=alt.Y("DEVIATION_RATE_PCT:Q", title="Deviation Rate (%)"),
                tooltip=["TRIP_DATE", "DEVIATION_RATE_PCT", "TOTAL_TRIPS"]
            ).properties(height=350)
            line_overlay = alt.Chart(daily_df).mark_line(color="#E74C3C", strokeWidth=2).encode(
                x="TRIP_DATE:T",
                y="DEVIATION_RATE_PCT:Q"
            )
            st.altair_chart(chart2 + line_overlay, use_container_width=True)

    with st.container(border=True):
        st.markdown("**Excess Distance by Day**")
        chart3 = alt.Chart(daily_df).mark_bar(color="#F39C12").encode(
            x=alt.X("TRIP_DATE:T", title="Date"),
            y=alt.Y("TOTAL_EXCESS_DISTANCE_KM:Q", title="Excess Distance (km)"),
            tooltip=["TRIP_DATE", "TOTAL_EXCESS_DISTANCE_KM", "DEVIATION_TRIPS"]
        ).properties(height=250)
        st.altair_chart(chart3, use_container_width=True)

    with st.expander("View Daily Data"):
        st.dataframe(daily_df, use_container_width=True, hide_index=True)

with tab3:
    st.subheader("Trip Detail & Route Map")

    trips_query = f"""
    SELECT 
        TRIP_ID,
        TRUCK_ID,
        DRIVER_ID,
        TRIP_DATE,
        ROUTE_VARIATION,
        TRIP_TYPE,
        ACTUAL_DISTANCE_KM,
        EXPECTED_DISTANCE_KM,
        DISTANCE_DEVIATION_PCT,
        ACTUAL_DURATION_MIN,
        EXPECTED_DURATION_MIN,
        DURATION_DEVIATION_PCT,
        IS_ROUTE_DEVIATION,
        ORIGIN_CITY,
        DEST_CITY
    FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS
    WHERE 1=1 {where_clause}
    ORDER BY ABS(DISTANCE_DEVIATION_PCT) DESC
    LIMIT 200
    """
    trips_df = session.sql(trips_query).to_pandas()

    if trips_df.empty:
        st.info("No trips match the selected filters")
    else:
        selected_trip = st.selectbox(
            "Select Trip",
            trips_df["TRIP_ID"].tolist(),
            format_func=lambda x: (
                f"{x} | "
                f"{trips_df[trips_df['TRIP_ID']==x]['ORIGIN_CITY'].iloc[0]} → "
                f"{trips_df[trips_df['TRIP_ID']==x]['DEST_CITY'].iloc[0]} | "
                f"Dist Dev: {trips_df[trips_df['TRIP_ID']==x]['DISTANCE_DEVIATION_PCT'].iloc[0]:.1f}%"
            )
        )

        trip_row = trips_df[trips_df["TRIP_ID"] == selected_trip].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Actual Distance", f"{trip_row['ACTUAL_DISTANCE_KM']:.1f} km")
        with col2:
            diff_km = trip_row['ACTUAL_DISTANCE_KM'] - trip_row['EXPECTED_DISTANCE_KM']
            st.metric(
                "Expected Distance",
                f"{trip_row['EXPECTED_DISTANCE_KM']:.1f} km",
                delta=f"{diff_km:+.1f} km",
                delta_color="inverse"
            )
        with col3:
            st.metric("Actual Duration", f"{trip_row['ACTUAL_DURATION_MIN']:.1f} min")
        with col4:
            diff_min = trip_row['ACTUAL_DURATION_MIN'] - trip_row['EXPECTED_DURATION_MIN']
            st.metric(
                "Expected Duration",
                f"{trip_row['EXPECTED_DURATION_MIN']:.1f} min",
                delta=f"{diff_min:+.1f} min",
                delta_color="inverse"
            )

        col5, col6, col7 = st.columns(3)
        with col5:
            st.metric("Distance Deviation", f"{trip_row['DISTANCE_DEVIATION_PCT']:.1f}%")
        with col6:
            st.metric("Duration Deviation", f"{trip_row['DURATION_DEVIATION_PCT']:.1f}%")
        with col7:
            is_dev = trip_row['IS_ROUTE_DEVIATION']
            st.metric("Route Deviation", "YES" if is_dev else "NO")

        st.markdown(
            f"**Route:** {trip_row['ORIGIN_CITY']} → {trip_row['DEST_CITY']} | "
            f"**Variation:** {trip_row['ROUTE_VARIATION']} | **Type:** {trip_row['TRIP_TYPE']}"
        )

        with st.spinner("Loading route geometries..."):
            try:
                expected_query = f"""
                WITH trip_od AS (
                    SELECT TRIP_ID,
                        FIRST_VALUE(LOCATION_ID) OVER (PARTITION BY TRIP_ID ORDER BY TS) AS origin_loc_id,
                        LAST_VALUE(LOCATION_ID) OVER (PARTITION BY TRIP_ID ORDER BY TS
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dest_loc_id
                    FROM SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.FACT_TRUCK_TELEMETRY
                    WHERE TRIP_ID = '{selected_trip}'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIP_ID ORDER BY TS) = 1
                )
                SELECT 
                    ST_ASGEOJSON(t.EXPECTED_PATH) AS expected_geojson,
                    e.ORIGIN_LAT AS start_lat,
                    e.ORIGIN_LNG AS start_lng,
                    e.DEST_LAT AS end_lat,
                    e.DEST_LNG AS end_lng
                FROM FLEET_DEMOS.ROUTE_DEVIATIONS.TRIP_DEVIATION_ANALYSIS t
                JOIN trip_od od ON t.TRIP_ID = od.TRIP_ID
                JOIN FLEET_DEMOS.ROUTE_DEVIATIONS.OD_EXPECTED_ROUTES e 
                    ON e.ORIGIN_ID = od.origin_loc_id AND e.DEST_ID = od.dest_loc_id
                WHERE t.TRIP_ID = '{selected_trip}'
                """
                geom_df = session.sql(expected_query).to_pandas()

                actual_pts_query = f"""
                WITH ordered AS (
                    SELECT 
                        ST_X(GEOMETRY) AS LNG, 
                        ST_Y(GEOMETRY) AS LAT,
                        TS,
                        ROW_NUMBER() OVER (ORDER BY TS) AS RN
                    FROM SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.FACT_TRUCK_TELEMETRY
                    WHERE TRIP_ID = '{selected_trip}'
                )
                SELECT 
                    a.LNG, a.LAT, a.TS, a.RN,
                    COALESCE(ST_DISTANCE(ST_MAKEPOINT(a.LNG, a.LAT), ST_MAKEPOINT(b.LNG, b.LAT)), 0) AS DIST_M,
                    COALESCE(TIMESTAMPDIFF('SECOND', b.TS, a.TS), 0) AS DT_SEC
                FROM ordered a
                LEFT JOIN ordered b ON b.RN = a.RN - 1
                ORDER BY a.RN
                """
                actual_pts_df = session.sql(actual_pts_query).to_pandas()
                if not actual_pts_df.empty and len(actual_pts_df) > 1:
                    actual_pts_df['SPEED_KMH'] = np.where(
                        actual_pts_df['DT_SEC'] > 0,
                        (actual_pts_df['DIST_M'] / actual_pts_df['DT_SEC']) * 3.6,
                        0
                    )
                    actual_pts_df = actual_pts_df[actual_pts_df['SPEED_KMH'] <= 250].reset_index(drop=True)
                    for _ in range(3):
                        if len(actual_pts_df) < 2:
                            break
                        lngs = actual_pts_df['LNG'].values
                        lats = actual_pts_df['LAT'].values
                        dlat = np.radians(np.diff(lats))
                        dlng = np.radians(np.diff(lngs))
                        lat1 = np.radians(lats[:-1])
                        lat2 = np.radians(lats[1:])
                        a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlng/2)**2
                        dists = 6371000 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
                        keep = np.concatenate([[True], dists < 50000])
                        if keep.all():
                            break
                        actual_pts_df = actual_pts_df[keep].reset_index(drop=True)

                if not geom_df.empty and geom_df['EXPECTED_GEOJSON'].iloc[0]:
                    expected_geo = json.loads(geom_df['EXPECTED_GEOJSON'].iloc[0])
                    start_lat = float(geom_df['START_LAT'].iloc[0])
                    start_lng = float(geom_df['START_LNG'].iloc[0])
                    end_lat = float(geom_df['END_LAT'].iloc[0])
                    end_lng = float(geom_df['END_LNG'].iloc[0])

                    center_lat = (start_lat + end_lat) / 2
                    center_lng = (start_lng + end_lng) / 2

                    layers = []

                    if 'coordinates' in expected_geo:
                        expected_path = [[c[0], c[1]] for c in expected_geo['coordinates']]
                        layers.append(
                            pdk.Layer(
                                "PathLayer",
                                data=[{"path": expected_path, "tooltip": "Expected (ORS) Route"}],
                                get_path="path",
                                get_color=[255, 50, 50, 200],
                                get_width=5,
                                width_min_pixels=3,
                                pickable=True,
                            )
                        )

                    if not actual_pts_df.empty and len(actual_pts_df) > 1:
                        actual_path = actual_pts_df[["LNG", "LAT"]].values.tolist()
                        layers.append(
                            pdk.Layer(
                                "PathLayer",
                                data=[{"path": actual_path, "tooltip": "Actual GPS Path"}],
                                get_path="path",
                                get_color=[50, 100, 255, 200],
                                get_width=4,
                                width_min_pixels=2,
                                pickable=True,
                            )
                        )

                    layers.append(
                        pdk.Layer(
                            "ScatterplotLayer",
                            data=[{"lon": start_lng, "lat": start_lat, "label": "Origin"}],
                            get_position='[lon, lat]',
                            get_color='[0, 200, 0, 255]',
                            get_radius=200,
                            pickable=True,
                        )
                    )
                    layers.append(
                        pdk.Layer(
                            "ScatterplotLayer",
                            data=[{"lon": end_lng, "lat": end_lat, "label": "Destination"}],
                            get_position='[lon, lat]',
                            get_color='[200, 0, 0, 255]',
                            get_radius=200,
                            pickable=True,
                        )
                    )

                    lat_diff = abs(start_lat - end_lat)
                    lon_diff = abs(start_lng - end_lng)
                    max_diff = max(lat_diff, lon_diff)
                    if max_diff < 0.5:
                        zoom = 10
                    elif max_diff < 1.0:
                        zoom = 9
                    elif max_diff < 2.0:
                        zoom = 8
                    elif max_diff < 4.0:
                        zoom = 7
                    else:
                        zoom = 6

                    view_state = pdk.ViewState(
                        latitude=center_lat,
                        longitude=center_lng,
                        zoom=zoom,
                        pitch=0,
                    )

                    deck = pdk.Deck(
                        layers=layers,
                        initial_view_state=view_state,
                        map_style='light',
                        tooltip={"text": "{tooltip}\n{label}"}
                    )

                    st.pydeck_chart(deck, use_container_width=True)

                    st.markdown("""
**Legend:** 🔴 **Red** = Expected ORS Route | 🔵 **Blue** = Actual GPS Path | 🟢 Origin | 🔴 Destination
""")
                else:
                    st.info("No route geometry available for this trip")
            except Exception as e:
                st.warning(f"Could not load route map: {str(e)}")

        with st.expander("View All Filtered Trips"):
            st.dataframe(trips_df, use_container_width=True, hide_index=True)
