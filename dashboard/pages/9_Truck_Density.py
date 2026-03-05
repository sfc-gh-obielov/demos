import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
from datetime import date, timedelta
import hashlib

st.set_page_config(
    page_title="Truck Location Density - Fleet Analytics",
    page_icon="🚛",
    layout="wide"
)

session = get_active_session()

st.title("🚛 Truck Location Density")
st.markdown("Visualize truck telemetry density using H3 hexagons")

st.sidebar.header("Filters")

@st.cache_data(ttl=300)
def get_filter_options():
    trucks = session.sql("""
        SELECT DISTINCT TRUCK_ID FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST ORDER BY TRUCK_ID LIMIT 100
    """).collect()
    
    date_range = session.sql("""
        SELECT MIN(DATE(TS)) as min_date, MAX(DATE(TS)) as max_date 
        FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST
    """).collect()[0]
    
    return (
        [row['TRUCK_ID'] for row in trucks],
        date_range['MIN_DATE'],
        date_range['MAX_DATE']
    )

truck_ids, min_date, max_date = get_filter_options()

selected_trucks = st.sidebar.multiselect(
    "Truck ID (optional)",
    options=truck_ids,
    default=[],
    help="Filter by specific trucks. Leave empty for all trucks."
)

trip_id_input = st.sidebar.text_input(
    "Trip ID (optional)",
    value="",
    help="Filter by specific trip ID"
)

use_date_filter = st.sidebar.checkbox("Filter by Date", value=True)

if use_date_filter:
    date_range_selected = st.sidebar.date_input(
        "Date Range",
        value=(min_date, min_date),
        min_value=min_date,
        max_value=max_date,
        help="Select date range to analyze"
    )
    if isinstance(date_range_selected, tuple) and len(date_range_selected) == 2:
        start_date, end_date = date_range_selected
    else:
        start_date = end_date = date_range_selected
else:
    start_date = min_date
    end_date = max_date

st.sidebar.divider()
st.sidebar.header("Visualization")

h3_resolution = st.sidebar.selectbox(
    "H3 Resolution",
    options=[4, 5, 6, 7, 8],
    index=2,
    help="H3 hexagon size: 4=largest (~1,300km²), 6=medium (~36km²), 8=small (~0.7km²)"
)

color_by = st.sidebar.selectbox(
    "Color By",
    options=["Point Count", "Unique Trucks", "Unique Trips", "Avg Speed", "Origin Location", "Destination Location"],
    index=0
)

opacity = st.sidebar.slider(
    "Opacity",
    min_value=0.1,
    max_value=1.0,
    value=0.7,
    step=0.1
)

extruded = st.sidebar.checkbox("3D Extrusion", value=False)

generate_button = st.sidebar.button("🗺️ Generate Map", type="primary", use_container_width=True)

if not generate_button:
    st.info("👈 Configure filters and click 'Generate Map' to visualize truck density")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        count = session.sql("SELECT COUNT(*) as cnt FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST").collect()[0]['CNT']
        st.metric("Total Records", f"{count:,}")
    with col2:
        trucks = session.sql("SELECT COUNT(DISTINCT TRUCK_ID) as cnt FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST").collect()[0]['CNT']
        st.metric("Unique Trucks", trucks)
    with col3:
        trips = session.sql("SELECT COUNT(DISTINCT TRIP_ID) as cnt FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST").collect()[0]['CNT']
        st.metric("Unique Trips", f"{trips:,}")
    
    st.stop()

where_clauses = []

if selected_trucks:
    trucks_list = ",".join([f"'{t}'" for t in selected_trucks])
    where_clauses.append(f"TRUCK_ID IN ({trucks_list})")

if trip_id_input.strip():
    where_clauses.append(f"TRIP_ID = '{trip_id_input.strip()}'")

if use_date_filter:
    where_clauses.append(f"DATE(TS) BETWEEN '{start_date}' AND '{end_date}'")

where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

LOCATION_COLORS = [
    [231, 76, 60],    # Red
    [46, 204, 113],   # Green
    [52, 152, 219],   # Blue
    [155, 89, 182],   # Purple
    [241, 196, 15],   # Yellow
    [230, 126, 34],   # Orange
    [26, 188, 156],   # Teal
    [52, 73, 94],     # Dark blue
    [149, 165, 166],  # Gray
    [192, 57, 43],    # Dark red
    [39, 174, 96],    # Dark green
    [41, 128, 185],   # Dark blue
    [142, 68, 173],   # Dark purple
    [243, 156, 18],   # Dark yellow
    [211, 84, 0],     # Dark orange
]

def get_location_color(location_id):
    if not location_id:
        return [128, 128, 128]
    hash_val = int(hashlib.md5(str(location_id).encode()).hexdigest()[:8], 16)
    return LOCATION_COLORS[hash_val % len(LOCATION_COLORS)]

if color_by in ["Origin Location", "Destination Location"]:
    location_type = "DWELL_WAREHOUSE" if color_by == "Origin Location" else "DWELL_DESTINATION"
    
    query = f"""
    WITH trip_locations AS (
        SELECT 
            TRIP_ID,
            LOCATION_ID as loc_id
        FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST
        WHERE LOCATION_TYPE = '{location_type}'
          AND LOCATION_ID IS NOT NULL
          AND {where_sql}
        GROUP BY TRIP_ID, LOCATION_ID
    ),
    points_with_location AS (
        SELECT 
            t.LATITUDE,
            t.LONGITUDE,
            t.TRUCK_ID,
            t.TRIP_ID,
            t.SPEED_KMH,
            COALESCE(tl.loc_id, 'UNKNOWN') as location_id
        FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST t
        LEFT JOIN trip_locations tl ON t.TRIP_ID = tl.TRIP_ID
        WHERE {where_sql}
    )
    SELECT 
        H3_LATLNG_TO_CELL_STRING(LATITUDE, LONGITUDE, {h3_resolution}) as h3_cell,
        MODE(location_id) as dominant_location,
        COUNT(*) as point_count,
        COUNT(DISTINCT TRUCK_ID) as truck_count,
        COUNT(DISTINCT TRIP_ID) as trip_count,
        AVG(SPEED_KMH) as avg_speed,
        AVG(LATITUDE) as center_lat,
        AVG(LONGITUDE) as center_lon,
        COUNT(DISTINCT location_id) as location_count
    FROM points_with_location
    GROUP BY h3_cell
    HAVING h3_cell IS NOT NULL
    ORDER BY point_count DESC
    """
    
    with st.spinner(f"Calculating density by {color_by.lower()}..."):
        try:
            result = session.sql(query).collect()
        except Exception as e:
            st.error(f"Query error: {e}")
            st.code(query)
            st.stop()
    
    if not result:
        st.warning("No data found for the selected filters")
        st.stop()
    
    locations = list(set([row['DOMINANT_LOCATION'] for row in result if row['DOMINANT_LOCATION']]))
    location_color_map = {loc: get_location_color(loc) for loc in locations}
    
    data = []
    for row in result:
        loc = row['DOMINANT_LOCATION']
        color = location_color_map.get(loc, [128, 128, 128])
        elevation = row['POINT_COUNT'] / max([r['POINT_COUNT'] for r in result]) * 50000 if extruded else 0
        
        data.append({
            'hex_id': row['H3_CELL'],
            'color': color,
            'elevation': elevation,
            'point_count': row['POINT_COUNT'],
            'truck_count': row['TRUCK_COUNT'],
            'trip_count': row['TRIP_COUNT'],
            'avg_speed': round(row['AVG_SPEED'], 1) if row['AVG_SPEED'] else 0,
            'location': loc[:20] + '...' if loc and len(loc) > 20 else loc,
            'location_count': row['LOCATION_COUNT']
        })
    
    df = pd.DataFrame(data)
    
    center_lat = sum([row['CENTER_LAT'] for row in result]) / len(result)
    center_lon = sum([row['CENTER_LON'] for row in result]) / len(result)
    
    tooltip = {
        "html": """
        <b>Location:</b> {location}<br/>
        <b>Points:</b> {point_count}<br/>
        <b>Trucks:</b> {truck_count}<br/>
        <b>Trips:</b> {trip_count}<br/>
        <b>Locations in hex:</b> {location_count}
        """,
        "style": {
            "backgroundColor": "#2d3436",
            "color": "white",
            "padding": "10px",
            "borderRadius": "5px"
        }
    }

else:
    if color_by == "Point Count":
        metric_col = "COUNT(*)"
    elif color_by == "Unique Trucks":
        metric_col = "COUNT(DISTINCT TRUCK_ID)"
    elif color_by == "Unique Trips":
        metric_col = "COUNT(DISTINCT TRIP_ID)"
    else:
        metric_col = "AVG(SPEED_KMH)"

    query = f"""
    SELECT 
        H3_LATLNG_TO_CELL_STRING(LATITUDE, LONGITUDE, {h3_resolution}) as h3_cell,
        {metric_col} as metric_value,
        COUNT(*) as point_count,
        COUNT(DISTINCT TRUCK_ID) as truck_count,
        COUNT(DISTINCT TRIP_ID) as trip_count,
        AVG(SPEED_KMH) as avg_speed,
        AVG(LATITUDE) as center_lat,
        AVG(LONGITUDE) as center_lon
    FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST
    WHERE {where_sql}
    GROUP BY h3_cell
    HAVING h3_cell IS NOT NULL
    ORDER BY metric_value DESC
    """

    with st.spinner("Calculating density..."):
        try:
            result = session.sql(query).collect()
        except Exception as e:
            st.error(f"Query error: {e}")
            st.code(query)
            st.stop()

    if not result:
        st.warning("No data found for the selected filters")
        st.stop()

    max_value = max([row['METRIC_VALUE'] for row in result])
    min_value = min([row['METRIC_VALUE'] for row in result])

    def get_color(value, min_val, max_val):
        if max_val == min_val:
            normalized = 0.5
        else:
            normalized = (value - min_val) / (max_val - min_val)
        
        if normalized <= 0.25:
            t = normalized / 0.25
            r, g, b = int(171 + (102 - 171) * t), int(217 + (194 - 217) * t), int(233 + (165 - 233) * t)
        elif normalized <= 0.5:
            t = (normalized - 0.25) / 0.25
            r, g, b = int(102 + (254 - 102) * t), int(194 + (224 - 194) * t), int(165 + (139 - 165) * t)
        elif normalized <= 0.75:
            t = (normalized - 0.5) / 0.25
            r, g, b = int(254 + (253 - 254) * t), int(224 + (174 - 224) * t), int(139 + (97 - 139) * t)
        else:
            t = (normalized - 0.75) / 0.25
            r, g, b = int(253 + (215 - 253) * t), int(174 + (48 - 174) * t), int(97 + (39 - 97) * t)
        
        return [r, g, b]

    data = []
    for row in result:
        color = get_color(row['METRIC_VALUE'], min_value, max_value)
        elevation = row['METRIC_VALUE'] / max_value * 50000 if extruded else 0
        
        data.append({
            'hex_id': row['H3_CELL'],
            'color': color,
            'elevation': elevation,
            'point_count': row['POINT_COUNT'],
            'truck_count': row['TRUCK_COUNT'],
            'trip_count': row['TRIP_COUNT'],
            'avg_speed': round(row['AVG_SPEED'], 1) if row['AVG_SPEED'] else 0
        })

    df = pd.DataFrame(data)

    center_lat = sum([row['CENTER_LAT'] for row in result]) / len(result)
    center_lon = sum([row['CENTER_LON'] for row in result]) / len(result)

    tooltip = {
        "html": """
        <b>Points:</b> {point_count}<br/>
        <b>Trucks:</b> {truck_count}<br/>
        <b>Trips:</b> {trip_count}<br/>
        <b>Avg Speed:</b> {avg_speed} km/h
        """,
        "style": {
            "backgroundColor": "#2d3436",
            "color": "white",
            "padding": "10px",
            "borderRadius": "5px"
        }
    }

layer = pdk.Layer(
    'H3HexagonLayer',
    df,
    get_hexagon='hex_id',
    get_fill_color='color',
    get_elevation='elevation' if extruded else None,
    elevation_scale=1 if extruded else 0,
    pickable=True,
    stroked=True,
    filled=True,
    extruded=extruded,
    opacity=opacity,
    auto_highlight=True
)

view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=6,
    pitch=45 if extruded else 0
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip=tooltip,
    map_style='road'
)

st.pydeck_chart(deck, use_container_width=True, height=600)

st.divider()

col1, col2, col3, col4 = st.columns(4)
with col1:
    total_points = sum([row['POINT_COUNT'] for row in result])
    st.metric("Total Points", f"{total_points:,}")
with col2:
    st.metric("Hexagons", f"{len(result):,}")
with col3:
    unique_trucks = session.sql(f"SELECT COUNT(DISTINCT TRUCK_ID) as cnt FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST WHERE {where_sql}").collect()[0]['CNT']
    st.metric("Unique Trucks", unique_trucks)
with col4:
    avg_speed_overall = sum([row['AVG_SPEED'] * row['POINT_COUNT'] for row in result]) / total_points if total_points > 0 else 0
    st.metric("Avg Speed", f"{avg_speed_overall:.1f} km/h")

if color_by in ["Origin Location", "Destination Location"]:
    with st.expander("🎨 Location Legend"):
        legend_data = []
        for loc in sorted(locations)[:20]:
            color = location_color_map.get(loc, [128, 128, 128])
            hex_color = '#{:02x}{:02x}{:02x}'.format(*color)
            legend_data.append({'Location': loc, 'Color': hex_color})
        
        legend_df = pd.DataFrame(legend_data)
        
        cols = st.columns(4)
        for i, row in legend_df.iterrows():
            with cols[i % 4]:
                st.markdown(f"<span style='color:{row['Color']};font-size:20px;'>●</span> {row['Location'][:25]}", unsafe_allow_html=True)

with st.expander("📊 Top 10 Hexagons"):
    if color_by in ["Origin Location", "Destination Location"]:
        top_df = pd.DataFrame([{
            'H3 Cell': row['H3_CELL'],
            'Location': row['DOMINANT_LOCATION'][:30] if row['DOMINANT_LOCATION'] else 'N/A',
            'Points': row['POINT_COUNT'],
            'Trucks': row['TRUCK_COUNT'],
            'Trips': row['TRIP_COUNT']
        } for row in result[:10]])
    else:
        top_df = pd.DataFrame([{
            'H3 Cell': row['H3_CELL'],
            'Points': row['POINT_COUNT'],
            'Trucks': row['TRUCK_COUNT'],
            'Trips': row['TRIP_COUNT'],
            'Avg Speed (km/h)': round(row['AVG_SPEED'], 1) if row['AVG_SPEED'] else 0
        } for row in result[:10]])
    st.dataframe(top_df, use_container_width=True)

with st.expander("🔍 Query Used"):
    st.code(query, language="sql")
