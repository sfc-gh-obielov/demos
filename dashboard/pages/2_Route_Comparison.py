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

st.title("🗺️ Route Comparison")
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
    
    # Filter by country
    countries = ['All'] + sorted(trips_df['COUNTRY_NAME'].dropna().unique().tolist())
    selected_country = st.selectbox("Filter by Country", countries, key="country_filter")
    
    # Filter by transportation mode
    modes = ['All'] + sorted(trips_df['TRANSPORTATION_MODE'].dropna().unique().tolist())
    selected_mode = st.selectbox("Filter by Mode", modes, key="mode_filter")
    
    # Apply filters
    filtered_df = trips_df.copy()
    if selected_country != 'All':
        filtered_df = filtered_df[filtered_df['COUNTRY_NAME'] == selected_country]
    if selected_mode != 'All':
        filtered_df = filtered_df[filtered_df['TRANSPORTATION_MODE'] == selected_mode]
    
    # Trip selector
    trip_options = filtered_df['TRIP_ID'].tolist()
    if not trip_options:
        st.warning("No trips match the selected filters")
        st.stop()
    
    selected_trip = st.selectbox(
        "Select Trip ID",
        trip_options,
        format_func=lambda x: f"{x} ({filtered_df[filtered_df['TRIP_ID']==x]['TRANSPORTATION_MODE'].iloc[0]}, {filtered_df[filtered_df['TRIP_ID']==x]['POINTS'].iloc[0]} pts)",
        key="trip_selector"
    )
    
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

# Get actual trip data
uid, tid = selected_trip.split('-')
actual_route_query = f"""
SELECT 
    LATITUDE,
    LONGITUDE,
    TIMESTAMP_LOCAL,
    SPEED
FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
WHERE UID = '{uid}' AND TID = '{tid}'
ORDER BY TIMESTAMP_LOCAL
"""

actual_route_df = session.sql(actual_route_query).to_pandas()

if actual_route_df.empty:
    st.error("No data found for selected trip")
    st.stop()

# Get start and end points
start_lat = actual_route_df.iloc[0]['LATITUDE']
start_lon = actual_route_df.iloc[0]['LONGITUDE']
end_lat = actual_route_df.iloc[-1]['LATITUDE']
end_lon = actual_route_df.iloc[-1]['LONGITUDE']

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("📍 Route Details")
    st.write(f"**Start:** {start_lat:.5f}, {start_lon:.5f}")
    st.write(f"**End:** {end_lat:.5f}, {end_lon:.5f}")
    st.write(f"**Total Points:** {len(actual_route_df)}")

with col2:
    st.subheader("🔄 Calculate ORS Route")
    if st.button("Calculate OpenRouteService Route", type="primary"):
        with st.spinner("Calculating route..."):
            try:
                # Call ORS DIRECTIONS function
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
                    ROUND(response:features[0]:properties:summary:distance::NUMBER / 1000, 2) AS distance_km,
                    ROUND(response:features[0]:properties:summary:duration::NUMBER / 60, 2) AS duration_minutes,
                    response AS full_payload
                FROM result
                """
                
                ors_result = session.sql(ors_query).to_pandas()
                
                if not ors_result.empty:
                    st.session_state['ors_result'] = ors_result
                    st.session_state['ors_calculated'] = True
                    st.success(f"✅ Route calculated: {ors_result['DISTANCE_KM'].iloc[0]} km, {ors_result['DURATION_MINUTES'].iloc[0]} min")
                else:
                    st.error("No route returned from ORS")
            except Exception as e:
                st.error(f"Error calculating route: {str(e)}")
                st.session_state['ors_calculated'] = False

st.divider()

# Prepare map data
if 'ors_calculated' in st.session_state and st.session_state['ors_calculated']:
    st.subheader("📊 Comparison")
    
    ors_result = st.session_state['ors_result']
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("ORS Distance", f"{ors_result['DISTANCE_KM'].iloc[0]} km")
    with col2:
        st.metric("ORS Duration", f"{ors_result['DURATION_MINUTES'].iloc[0]} min")
    with col3:
        actual_distance = actual_route_df['SPEED'].sum() * 0.01  # Rough estimate
        st.metric("Actual Points", len(actual_route_df))

# Create map visualization
st.subheader("🗺️ Map Visualization")

# Prepare actual route as line
actual_route_coords = actual_route_df[['LONGITUDE', 'LATITUDE']].values.tolist()

# Calculate center
center_lat = actual_route_df['LATITUDE'].mean()
center_lon = actual_route_df['LONGITUDE'].mean()

# Create layers
layers = [
    pdk.Layer(
        "ScatterplotLayer",
        data=actual_route_df,
        get_position='[LONGITUDE, LATITUDE]',
        get_color='[0, 0, 255, 160]',
        get_radius=20,
        pickable=True,
    ),
    pdk.Layer(
        "PathLayer",
        data=[{"path": actual_route_coords}],
        get_path="path",
        get_color=[0, 0, 255, 180],
        get_width=3,
        width_min_pixels=2,
        pickable=False,
    ),
    # Start marker
    pdk.Layer(
        "ScatterplotLayer",
        data=[{"lon": start_lon, "lat": start_lat}],
        get_position='[lon, lat]',
        get_color='[0, 255, 0, 200]',
        get_radius=50,
        pickable=True,
    ),
    # End marker
    pdk.Layer(
        "ScatterplotLayer",
        data=[{"lon": end_lon, "lat": end_lat}],
        get_position='[lon, lat]',
        get_color='[255, 0, 0, 200]',
        get_radius=50,
        pickable=True,
    ),
]

# Add ORS route if calculated
if 'ors_calculated' in st.session_state and st.session_state['ors_calculated']:
    try:
        ors_geom = st.session_state['ors_result']['GEOMETRY'].iloc[0]
        
        # Parse ORS geometry and create layer
        ors_coords_query = f"""
        SELECT 
            ST_X(value) AS lon,
            ST_Y(value) AS lat
        FROM TABLE(FLATTEN(ST_ASGEOJSON('{ors_geom}')::VARIANT:coordinates))
        """
        
        ors_coords_df = session.sql(ors_coords_query).to_pandas()
        
        if not ors_coords_df.empty:
            ors_path = ors_coords_df[['LON', 'LAT']].values.tolist()
            
            layers.append(
                pdk.Layer(
                    "PathLayer",
                    data=[{"path": ors_path}],
                    get_path="path",
                    get_color=[255, 165, 0, 200],
                    get_width=5,
                    width_min_pixels=3,
                    pickable=True,
                )
            )
    except Exception as e:
        st.warning(f"Could not display ORS route on map: {str(e)}")

# Create deck
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=12,
    pitch=0,
)

r = pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    tooltip={
        "text": "Lat: {LATITUDE}\nLon: {LONGITUDE}\nSpeed: {SPEED} km/h"
    }
)

st.pydeck_chart(r)

# Legend
st.markdown("""
**Legend:**
- 🔵 **Blue Line/Points**: Actual GPS trajectory
- 🟠 **Orange Line**: OpenRouteService calculated route
- 🟢 **Green Marker**: Trip start point
- 🔴 **Red Marker**: Trip end point
""")
