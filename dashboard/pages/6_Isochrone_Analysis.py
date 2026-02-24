import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
import json

st.set_page_config(
    page_title="Isochrone Analysis - Fleet Analytics",
    page_icon="⏱️",
    layout="wide"
)

session = get_active_session()

st.title("⏱️ Isochrone Analysis")
st.markdown("Visualize areas reachable within specific travel times from any location")

st.sidebar.header("Location Settings")

location_name = st.sidebar.text_input(
    "Location Name",
    value="Green Bay, Wisconsin",
    help="Name of the origin location"
)

col1, col2 = st.sidebar.columns(2)
with col1:
    origin_lon = st.number_input(
        "Longitude",
        value=-88.0133,
        format="%.4f",
        help="Origin longitude coordinate"
    )
with col2:
    origin_lat = st.number_input(
        "Latitude",
        value=44.5133,
        format="%.4f",
        help="Origin latitude coordinate"
    )

st.sidebar.divider()

st.sidebar.header("Isochrone Settings")

profile = st.sidebar.selectbox(
    "Transportation Profile",
    options=[
        "driving-hgv",
        "driving-car",
        "cycling-electric",
        "foot-walking"
    ],
    index=0,
    help="Select the routing profile for isochrone calculation"
)

profile_labels = {
    "driving-hgv": "🚛 Truck",
    "driving-car": "🚗 Car",
    "cycling-electric": "🚴 E-Bike",
    "foot-walking": "🚶 Walking"
}

time_intervals = st.sidebar.multiselect(
    "Travel Time Intervals (hours)",
    options=[0.5, 1, 1.5, 2, 2.5, 3, 4, 5],
    default=[1, 2, 3],
    help="Select time intervals for isochrone generation"
)

if not time_intervals:
    st.warning("⚠️ Please select at least one time interval")
    st.stop()

time_intervals = sorted(time_intervals)

st.sidebar.divider()

color_scheme = st.sidebar.selectbox(
    "Color Scheme",
    options=["Blue Gradient", "Purple Gradient", "Green-Yellow-Red", "Rainbow"],
    index=1,
    help="Color scheme for isochrones"
)

opacity = st.sidebar.slider(
    "Opacity",
    min_value=0.0,
    max_value=1.0,
    value=0.5,
    step=0.1,
    help="Transparency of isochrone polygons"
)

st.sidebar.divider()

def get_color_for_interval(interval_hours, max_hours, color_scheme):
    """Generate color based on time interval"""
    normalized = interval_hours / max_hours
    
    if color_scheme == "Blue Gradient":
        if normalized < 0.33:
            return [0, 0, 139, int(255 * opacity)]  # Dark blue
        elif normalized < 0.67:
            return [65, 105, 225, int(255 * opacity)]  # Royal blue
        else:
            return [135, 206, 250, int(255 * opacity)]  # Light sky blue
    
    elif color_scheme == "Purple Gradient":
        if normalized < 0.33:
            return [75, 0, 130, int(255 * opacity)]  # Indigo (dark purple)
        elif normalized < 0.67:
            return [138, 43, 226, int(255 * opacity)]  # Blue violet
        else:
            return [216, 191, 216, int(255 * opacity)]  # Thistle (light purple)
    
    elif color_scheme == "Green-Yellow-Red":
        if normalized < 0.33:
            return [34, 139, 34, int(255 * opacity)]  # Forest green
        elif normalized < 0.67:
            return [255, 215, 0, int(255 * opacity)]  # Gold
        else:
            return [220, 20, 60, int(255 * opacity)]  # Crimson
    
    else:  # Rainbow
        if normalized < 0.25:
            return [0, 0, 255, int(255 * opacity)]  # Blue
        elif normalized < 0.5:
            return [0, 255, 0, int(255 * opacity)]  # Green
        elif normalized < 0.75:
            return [255, 255, 0, int(255 * opacity)]  # Yellow
        else:
            return [255, 0, 0, int(255 * opacity)]  # Red

if st.sidebar.button("🚀 Generate Isochrones", type="primary", use_container_width=True):
    st.session_state.generate_isochrones = True
    st.session_state.iso_params = {
        'origin_lon': origin_lon,
        'origin_lat': origin_lat,
        'profile': profile,
        'time_intervals': time_intervals,
        'color_scheme': color_scheme,
        'location_name': location_name
    }
else:
    if 'generate_isochrones' not in st.session_state:
        st.info("👈 Configure settings and click 'Generate Isochrones' to visualize travel time areas")
        st.stop()
    
    origin_lon = st.session_state.iso_params['origin_lon']
    origin_lat = st.session_state.iso_params['origin_lat']
    profile = st.session_state.iso_params['profile']
    time_intervals = st.session_state.iso_params['time_intervals']
    color_scheme = st.session_state.iso_params['color_scheme']
    location_name = st.session_state.iso_params['location_name']

try:
    with st.spinner(f"Generating isochrones for {location_name}..."):
        time_intervals_minutes = [int(t * 60) for t in time_intervals]
        
        queries = []
        for i, time_min in enumerate(time_intervals_minutes):
            queries.append(f"""
            SELECT 
                {time_min} as time_minutes,
                OPENROUTESERVICE_NATIVE_APP.CORE.ISOCHRONES(
                    '{profile}',
                    {origin_lon},
                    {origin_lat},
                    {time_min}
                ) as isochrone_result
            """)
        
        combined_query = " UNION ALL ".join(queries)
        
        result = session.sql(combined_query).collect()
        
        if not result:
            st.error("❌ Failed to generate isochrones")
            st.info("Make sure OpenRouteService is running and has data for the selected region")
            st.stop()
        
        all_features = []
        for row in result:
            isochrone_geojson = row['ISOCHRONE_RESULT']
            
            if isinstance(isochrone_geojson, str):
                iso_data = json.loads(isochrone_geojson)
            else:
                iso_data = isochrone_geojson
            
            features = iso_data.get('features', [])
            for feature in features:
                all_features.append(feature)
        
        isochrone_data = {
            'type': 'FeatureCollection',
            'features': all_features
        }
    
    st.success(f"✅ Generated {len(time_intervals)} isochrones for {location_name}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Origin", location_name)
    
    with col2:
        st.metric("Profile", profile_labels.get(profile, profile))
    
    with col3:
        st.metric("Isochrones", len(time_intervals))
    
    with col4:
        max_time = max(time_intervals)
        st.metric("Max Range", f"{max_time} hours")
    
    st.divider()
    
    features = isochrone_data.get('features', [])
    
    if not features:
        st.error("No isochrone features returned")
        st.stop()
    
    isochrone_polygons = []
    max_hours = max(time_intervals)
    
    for feature in features:
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        value_seconds = properties.get('value', 0)
        value_hours = value_seconds / 3600
        
        center = properties.get('center', [origin_lon, origin_lat])
        
        color = get_color_for_interval(value_hours, max_hours, color_scheme)
        line_color = [color[0], color[1], color[2], 255]
        
        polygon_coords = geometry.get('coordinates', [])
        
        isochrone_polygons.append({
            'polygon': polygon_coords,
            'fill_color': color,
            'line_color': line_color,
            'time_hours': value_hours,
            'time_minutes': value_hours * 60,
            'center_lon': center[0],
            'center_lat': center[1]
        })
    
    isochrone_polygons = sorted(isochrone_polygons, key=lambda x: x['time_hours'], reverse=True)
    
    polygon_df = pd.DataFrame(isochrone_polygons)
    
    polygon_layer = pdk.Layer(
        'PolygonLayer',
        polygon_df,
        get_polygon='polygon',
        get_fill_color='fill_color',
        get_line_color='line_color',
        line_width_min_pixels=2,
        pickable=True,
        opacity=opacity,
        filled=True,
        stroked=True,
        auto_highlight=True
    )
    
    origin_df = pd.DataFrame([{
        'lon': origin_lon,
        'lat': origin_lat,
        'color': [255, 0, 0, 255],
        'name': location_name
    }])
    
    origin_layer = pdk.Layer(
        'ScatterplotLayer',
        origin_df,
        get_position='[lon, lat]',
        get_fill_color='color',
        get_radius=300,
        pickable=True,
        opacity=1.0,
        filled=True
    )
    
    view_state = pdk.ViewState(
        latitude=origin_lat,
        longitude=origin_lon,
        zoom=7,
        pitch=0,
        bearing=0
    )
    
    tooltip = {
        "html": """
        <b>Travel Time:</b> {time_hours:.1f} hours ({time_minutes:.0f} minutes)<br/>
        """,
        "style": {
            "backgroundColor": "steelblue",
            "color": "white",
            "padding": "10px"
        }
    }
    
    deck = pdk.Deck(
        layers=[polygon_layer, origin_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style='light'
    )
    
    st.pydeck_chart(deck, use_container_width=True)
    
    st.subheader("📊 Isochrone Summary")
    
    summary_data = []
    for iso in sorted(isochrone_polygons, key=lambda x: x['time_hours']):
        summary_data.append({
            'Time Interval': f"{iso['time_hours']:.1f} hours",
            'Minutes': f"{iso['time_minutes']:.0f} min",
            'Color': f"RGB({iso['fill_color'][0]}, {iso['fill_color'][1]}, {iso['fill_color'][2]})"
        })
    
    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    st.markdown(f"""
    **Interpretation:**
    - 🔴 Red marker shows the origin point: **{location_name}**
    - Colored polygons show areas reachable within specified travel times
    - Darker colors = shorter travel time, Lighter colors = longer travel time
    - Profile used: **{profile_labels.get(profile, profile)}**
    """)
    
    with st.expander("📋 View Raw GeoJSON"):
        st.json(isochrone_data)
    
    with st.expander("🔍 Technical Details"):
        api_calls = "\n".join([
            f"-- {t} hour isochrone\nOPENROUTESERVICE_NATIVE_APP.CORE.ISOCHRONES('{profile}', {origin_lon}, {origin_lat}, {int(t * 3600)})"
            for t in time_intervals
        ])
        
        st.markdown(f"""
        **Query Parameters:**
        - **Origin Coordinates:** `{origin_lon:.4f}, {origin_lat:.4f}`
        - **Profile:** `{profile}`
        - **Time Intervals:** `{', '.join([f'{t}h' for t in time_intervals])}`
        - **Color Scheme:** `{color_scheme}`
        - **Opacity:** `{opacity}`
        
        **API Calls:**
        ```sql
{api_calls}
        ```
        
        **Note:** Each time interval requires a separate API call. Results are combined into a single visualization.
        """)

except Exception as e:
    st.error(f"❌ Error generating isochrones: {str(e)}")
    st.markdown("**Common issues:**")
    st.markdown("- OpenRouteService may not be running - check the ORS Profile Manager")
    st.markdown("- Selected region may not have map data loaded")
    st.markdown("- Profile may not be enabled or configured")
    st.markdown("- Network connectivity issues")
    
    with st.expander("💡 View Error Details"):
        st.exception(e)

st.divider()

st.caption("💡 **Tip:** Isochrones show realistic travel time areas based on actual road networks. Use different profiles to compare truck, car, bike, and walking travel times.")
