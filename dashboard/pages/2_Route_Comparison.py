import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Route Comparison - Fleet Analytics",
    page_icon="🗺️",
    layout="wide"
)

session = get_active_session()

st.title("Route Comparison")
st.markdown("Compare actual GPS trajectories with OpenRouteService calculated routes")

# Get list of trips
trips_query = """
SELECT 
    CONCAT(UID, '-', TID) as trip_id,
    UID,
    TID,
    transportation_mode,
    country_name,
    COUNT(*) as points,
    ROUND(trip_avg_speed, 2) as avg_speed
FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
WHERE transportation_mode IS NOT NULL
GROUP BY UID, TID, transportation_mode, country_name, trip_avg_speed
ORDER BY UID, TID
"""

trips_df = session.sql(trips_query).to_pandas()

with st.sidebar:
    st.header("Trip Selection")
    
    # Filter by country (required)
    countries = sorted(trips_df['COUNTRY_NAME'].dropna().unique().tolist())
    selected_country = st.selectbox(
        "Filter by Country", 
        countries, 
        key="country_filter",
        index=None,
        placeholder="Choose a country..."
    )
    
    # Filter by transportation mode (required)
    modes = sorted(trips_df['TRANSPORTATION_MODE'].dropna().unique().tolist())
    selected_mode = st.selectbox(
        "Filter by Mode", 
        modes, 
        key="mode_filter",
        index=None,
        placeholder="Choose a mode..."
    )
    
    # Only show trip selector if both country and mode are selected
    if selected_country and selected_mode:
        # Apply filters
        filtered_df = trips_df[
            (trips_df['COUNTRY_NAME'] == selected_country) & 
            (trips_df['TRANSPORTATION_MODE'] == selected_mode)
        ]
        
        # Trip selector
        trip_options = filtered_df['TRIP_ID'].tolist()
        
        if not trip_options:
            st.warning("No trips match the selected filters")
            st.stop()
        
        # Show filtered count
        st.caption(f"Found {len(trip_options)} trips")
        
        selected_trip = st.selectbox(
            "Select Trip ID",
            trip_options,
            format_func=lambda x: f"{x} ({filtered_df[filtered_df['TRIP_ID']==x]['POINTS'].iloc[0]} pts, {filtered_df[filtered_df['TRIP_ID']==x]['AVG_SPEED'].iloc[0]} km/h)",
            index=0  # Always default to first trip in filtered list
        )
    else:
        st.info("Please select both country and transportation mode to see available trips")
        st.stop()
    
    st.divider()
    
    # Profile selector for ORS
    ors_profile_map = {
        'car': 'driving-car',
        'taxi': 'driving-car',
        'bus': 'driving-car',
        'motorcycle': 'driving-car',
        'train': 'driving-car',
        'subway': 'driving-car',
        'bike': 'cycling-electric',
        'walk': 'foot-walking',
        'run': 'foot-walking'
    }
    
    trip_info = filtered_df[filtered_df['TRIP_ID'] == selected_trip].iloc[0]
    default_profile = ors_profile_map.get(trip_info['TRANSPORTATION_MODE'], 'driving-car')
    
    ors_profile = st.selectbox(
        "ORS Routing Profile",
        ['driving-car', 'driving-hgv', 'cycling-electric', 'foot-walking'],
        index=['driving-car', 'driving-hgv', 'cycling-electric', 'foot-walking'].index(default_profile)
    )
    
    st.info(f"**Trip Info:**\n- Mode: {trip_info['TRANSPORTATION_MODE']}\n- Points: {trip_info['POINTS']}\n- Avg Speed: {trip_info['AVG_SPEED']} km/h\n- Country: {trip_info['COUNTRY_NAME']}")

# Get actual trip data with geospatial aggregation and metrics
uid, tid = selected_trip.split('-')

# Get trip segments with speed-based color coding AND trip metrics
segments_query = f"""
WITH ordered_points AS (
    SELECT 
        LAT,
        LNG,
        SPEED,
        GEOMETRY as point,
        EVENT_TIMESTAMP,
        TIME_LAG,
        ROW_NUMBER() OVER (ORDER BY EVENT_TIMESTAMP) as rn
    FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
    WHERE UID = '{uid}' AND TID = '{tid}'
    ORDER BY EVENT_TIMESTAMP
),
segments AS (
    SELECT 
        p1.LAT as lat1,
        p1.LNG as lon1,
        p2.LAT as lat2,
        p2.LNG as lon2,
        TO_GEOGRAPHY(
            OBJECT_CONSTRUCT(
                'type', 'LineString',
                'coordinates', ARRAY_CONSTRUCT(
                    ARRAY_CONSTRUCT(p1.LNG, p1.LAT),
                    ARRAY_CONSTRUCT(p2.LNG, p2.LAT)
                )
            )
        ) as segment_geom,
        (p1.SPEED + p2.SPEED) / 2 as avg_speed,
        p2.TIME_LAG as segment_duration_seconds,
        p1.rn
    FROM ordered_points p1
    JOIN ordered_points p2 ON p2.rn = p1.rn + 1
),
trip_metrics AS (
    SELECT 
        SUM(TIME_LAG) as total_duration_seconds
    FROM ordered_points
    WHERE TIME_LAG IS NOT NULL
)
SELECT 
    s.lat1,
    s.lon1,
    s.lat2,
    s.lon2,
    ST_ASGEOJSON(s.segment_geom) as segment_geojson,
    ROUND(s.avg_speed, 2) as avg_speed,
    s.segment_duration_seconds,
    -- Color coding based on speed (Red=fast, Yellow=medium, Green=slow)
    CASE 
        WHEN s.avg_speed > 60 THEN '[255, 0, 0, 200]'    -- Red for > 60 km/h
        WHEN s.avg_speed > 30 THEN '[255, 165, 0, 200]'  -- Orange for 30-60 km/h
        WHEN s.avg_speed > 10 THEN '[255, 255, 0, 200]'  -- Yellow for 10-30 km/h
        ELSE '[0, 255, 0, 200]'                         -- Green for < 10 km/h
    END as color,
    tm.total_duration_seconds,
    ROUND(SUM(s.avg_speed * (s.segment_duration_seconds / 3600.0)) OVER (), 2) as total_distance_km
FROM segments s
CROSS JOIN trip_metrics tm
ORDER BY s.rn
"""

segments_df = session.sql(segments_query).to_pandas()

if segments_df.empty:
    st.error("No data found for selected trip")
    st.stop()

# Get start and end points
start_lat = segments_df.iloc[0]['LAT1']
start_lon = segments_df.iloc[0]['LON1']
end_lat = segments_df.iloc[-1]['LAT2']
end_lon = segments_df.iloc[-1]['LON2']

# Get actual trip metrics (already calculated from summing TIME_LAG)
actual_duration_seconds = segments_df.iloc[0]['TOTAL_DURATION_SECONDS']
actual_duration_minutes = round(actual_duration_seconds / 60, 2)
actual_distance_km = segments_df.iloc[0]['TOTAL_DISTANCE_KM']

st.divider()

st.subheader("Route Details")

col1, col2 = st.columns(2)

with col1:
    st.write("**Start Point**")
    st.write(f"Lat: {start_lat:.5f}, Lon: {start_lon:.5f}")
    st.write("**End Point**")
    st.write(f"Lat: {end_lat:.5f}, Lon: {end_lon:.5f}")

with col2:
    st.write("**Actual GPS Trajectory**")
    st.write(f"Distance: {actual_distance_km} km")
    st.write(f"Duration: {actual_duration_minutes} min")
    st.write(f"Segments: {len(segments_df)}")

# Calculate ORS route automatically
with st.spinner("Calculating OpenRouteService route..."):
    try:
        ors_query = f"""
        WITH result AS (
            SELECT OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS(
                '{ors_profile}',
                ARRAY_CONSTRUCT({start_lon}, {start_lat}),
                ARRAY_CONSTRUCT({end_lon}, {end_lat})
            ) AS response
        )
        SELECT 
            TO_GEOGRAPHY(response:features[0]:geometry) AS geometry,
            response:features[0]:geometry AS geometry_geojson,
            ROUND(response:features[0]:properties:summary:distance::NUMBER / 1000, 2) AS distance_km,
            ROUND(response:features[0]:properties:summary:duration::NUMBER / 60, 2) AS duration_minutes
        FROM result
        """
        
        ors_result = session.sql(ors_query).to_pandas()
        
        if not ors_result.empty:
            st.session_state['ors_result'] = ors_result
            st.session_state['ors_calculated'] = True
        else:
            st.error("No route returned from ORS")
            st.session_state['ors_calculated'] = False
    except Exception as e:
        st.error(f"Error calculating route: {str(e)}")
        st.session_state['ors_calculated'] = False

st.divider()

# Comparison metrics
if 'ors_calculated' in st.session_state and st.session_state['ors_calculated']:
    st.subheader("Route Comparison")
    
    ors_result = st.session_state['ors_result']
    ors_distance = ors_result['DISTANCE_KM'].iloc[0]
    ors_duration = ors_result['DURATION_MINUTES'].iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Actual Distance", 
            f"{actual_distance_km} km"
        )
    
    with col2:
        distance_diff = actual_distance_km - ors_distance
        st.metric(
            "ORS Distance", 
            f"{ors_distance} km",
            delta=f"{distance_diff:+.2f} km",
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            "Actual Duration", 
            f"{actual_duration_minutes} min"
        )
    
    with col4:
        duration_diff = actual_duration_minutes - ors_duration
        st.metric(
            "ORS Duration", 
            f"{ors_duration} min",
            delta=f"{duration_diff:+.2f} min",
            delta_color="inverse"
        )

# Calculate bounds and center for map
import math

min_lat = min(start_lat, end_lat)
max_lat = max(start_lat, end_lat)
min_lon = min(start_lon, end_lon)
max_lon = max(start_lon, end_lon)

# Add padding
lat_padding = (max_lat - min_lat) * 0.1
lon_padding = (max_lon - min_lon) * 0.1

# Calculate center
center_lat = (min_lat + max_lat) / 2
center_lon = (min_lon + max_lon) / 2

# Calculate zoom level based on bounds
lat_diff = max_lat - min_lat + lat_padding
lon_diff = max_lon - min_lon + lon_padding

# Approximate zoom level (simplified calculation)
max_diff = max(lat_diff, lon_diff)
if max_diff > 0:
    zoom_level = max(1, min(15, 10 - math.log2(max_diff * 100)))
else:
    zoom_level = 12

# Prepare segment data for PathLayer with color coding
segment_paths = []
for _, row in segments_df.iterrows():
    segment_paths.append({
        "path": [[row['LON1'], row['LAT1']], [row['LON2'], row['LAT2']]],
        "color": eval(row['COLOR']),  # Convert string representation to list
        "speed": row['AVG_SPEED'],
        "tooltip": f"Actual route - Speed: {row['AVG_SPEED']:.1f} km/h"
    })

# Create layers
layers = [
    # Speed-colored segments for actual route
    pdk.Layer(
        "PathLayer",
        data=segment_paths,
        get_path="path",
        get_color="color",
        get_width=5,
        width_min_pixels=3,
        pickable=True,
    ),
    # Start marker
    pdk.Layer(
        "ScatterplotLayer",
        data=[{"lon": start_lon, "lat": start_lat, "label": "Start"}],
        get_position='[lon, lat]',
        get_color='[0, 255, 0, 255]',
        get_radius=80,
        pickable=True,
    ),
    # End marker
    pdk.Layer(
        "ScatterplotLayer",
        data=[{"lon": end_lon, "lat": end_lat, "label": "End"}],
        get_position='[lon, lat]',
        get_color='[255, 0, 0, 255]',
        get_radius=80,
        pickable=True,
    ),
]

# Add ORS route if calculated
if 'ors_calculated' in st.session_state and st.session_state['ors_calculated']:
    try:
        ors_result = st.session_state['ors_result']
        geometry_geojson = ors_result['GEOMETRY_GEOJSON'].iloc[0]
        
        # Parse if it's a string
        if isinstance(geometry_geojson, str):
            import json
            geometry_geojson = json.loads(geometry_geojson)
        
        if 'coordinates' in geometry_geojson:
            ors_path = [[lon, lat] for lon, lat in geometry_geojson['coordinates']]
            
            layers.append(
                pdk.Layer(
                    "PathLayer",
                    data=[{
                        "path": ors_path,
                        "tooltip": "Optimal Route"
                    }],
                    get_path="path",
                    get_color=[0, 100, 255, 220],  # Blue for ORS route
                    get_width=4,
                    width_min_pixels=2,
                    pickable=True,
                )
            )
    except Exception as e:
        st.warning(f"Could not display ORS route on map: {str(e)}")

# Create deck
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom_level,
    pitch=0,
)

r = pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    map_style='light',
    tooltip={"text": "{tooltip}"}
)

st.pydeck_chart(r)

# Legend
st.markdown("""
**Legend:**
- **Speed Color Coding** (Actual GPS trajectory):
  - 🔴 **Red**: > 60 km/h (fast)
  - 🟠 **Orange**: 30-60 km/h (medium)
  - 🟡 **Yellow**: 10-30 km/h (slow)
  - 🟢 **Green**: < 10 km/h (very slow)
- 🔵 **Blue Line**: OpenRouteService calculated route
- 🟢 **Green Marker**: Trip start point
- 🔴 **Red Marker**: Trip end point
""")
