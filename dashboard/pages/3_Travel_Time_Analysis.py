import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Travel Time Analysis - Fleet Analytics",
    page_icon="🗺️",
    layout="wide"
)

session = get_active_session()

st.title("🗺️ Travel Time Analysis")
st.markdown("Explore e-bike travel times from any hexagon to its nearest neighbors (San Francisco)")

# Helper function to get color based on travel time
def get_color_for_time(minutes):
    """Return RGB color based on travel time in minutes"""
    if minutes is None or pd.isna(minutes):
        return [128, 128, 128, 180]  # Gray for no data
    elif minutes < 10:
        # Green (fast)
        return [34, 139, 34, 200]
    elif minutes < 20:
        # Yellow-green
        return [154, 205, 50, 200]
    elif minutes < 30:
        # Orange
        return [255, 165, 0, 200]
    elif minutes < 40:
        # Orange-red
        return [255, 69, 0, 200]
    else:
        # Red (slow)
        return [220, 20, 60, 200]

# Helper function to calculate k-ring neighbors using Snowflake H3 functions
@st.cache_data
def get_k_ring_neighbors(hex_id, k):
    """Get all hexagons within k rings using Snowflake's H3_GRID_DISK function"""
    query = f"""
    WITH grid_disk AS (
        SELECT 
            VALUE::STRING AS neighbor_hex,
            H3_GRID_DISTANCE('{hex_id}', VALUE::STRING) AS ring_number
        FROM TABLE(FLATTEN(H3_GRID_DISK('{hex_id}', {k})))
    )
    SELECT 
        neighbor_hex,
        ring_number
    FROM grid_disk
    ORDER BY ring_number, neighbor_hex
    """
    
    df = session.sql(query).to_pandas()
    neighbors = df['NEIGHBOR_HEX'].tolist()
    hex_to_ring = dict(zip(df['NEIGHBOR_HEX'], df['RING_NUMBER']))
    
    return neighbors, hex_to_ring

# Query available hexagons
@st.cache_data
def get_available_hexagons():
    """Get list of all SF hexagons"""
    query = """
    SELECT 
        HEX_ID,
        LATITUDE,
        LONGITUDE
    FROM FLEET_DEMOS.ROUTING.SF_HEXAGONS
    ORDER BY HEX_ID
    """
    df = session.sql(query).to_pandas()
    return df

# Load available hexagons
with st.spinner("Loading hexagons..."):
    hexagons_df = get_available_hexagons()

st.sidebar.header("Settings")

# Hexagon selector
selected_hex = st.sidebar.selectbox(
    "Select Origin Hexagon",
    options=hexagons_df['HEX_ID'].tolist(),
    index=0,
    help="Choose the hexagon to analyze travel times from"
)

# K-ring slider
k_rings = st.sidebar.slider(
    "Number of Neighbor Rings",
    min_value=1,
    max_value=10,
    value=10,
    help="Number of hexagon rings to visualize (Ring 1 = 6 neighbors, Ring 2 = 12 more, etc.)"
)

st.sidebar.divider()

# Color legend
st.sidebar.markdown("### Travel Time Legend")
st.sidebar.markdown("🟢 **< 10 min** - Very close")
st.sidebar.markdown("🟡 **10-20 min** - Close")
st.sidebar.markdown("🟠 **20-30 min** - Medium")
st.sidebar.markdown("🔴 **30-40 min** - Far")
st.sidebar.markdown("🔴 **> 40 min** - Very far")

# Get k-ring neighbors
neighbors, hex_to_ring = get_k_ring_neighbors(selected_hex, k_rings)

st.info(f"**Selected Hexagon:** `{selected_hex}` | **Analyzing {len(neighbors)} hexagons** (1 origin + {len(neighbors)-1} neighbors across {k_rings} rings)")

# Query travel times
@st.cache_data
def get_travel_times(origin_hex, neighbor_hexes):
    """Get travel times from origin to all neighbors"""
    # Create a temporary table or use array for IN clause
    hex_list = ", ".join([f"'{h}'" for h in neighbor_hexes])
    
    query = f"""
    SELECT 
        m.ORIGIN_HEX,
        m.DEST_HEX,
        m.DISTANCE_KM,
        m.DURATION_MINUTES,
        h_origin.LATITUDE AS ORIGIN_LAT,
        h_origin.LONGITUDE AS ORIGIN_LON,
        h_dest.LATITUDE AS DEST_LAT,
        h_dest.LONGITUDE AS DEST_LON
    FROM FLEET_DEMOS.ROUTING.SF_TRAVEL_TIME_MATRIX m
    JOIN FLEET_DEMOS.ROUTING.SF_HEXAGONS h_origin 
        ON m.ORIGIN_HEX = h_origin.HEX_ID
    JOIN FLEET_DEMOS.ROUTING.SF_HEXAGONS h_dest 
        ON m.DEST_HEX = h_dest.HEX_ID
    WHERE m.ORIGIN_HEX = '{origin_hex}'
        AND m.DEST_HEX IN ({hex_list})
    """
    
    df = session.sql(query).to_pandas()
    return df

# Load travel times
with st.spinner("Loading travel times..."):
    travel_times_df = get_travel_times(selected_hex, neighbors)

# Add origin hexagon itself with 0 travel time
origin_row = hexagons_df[hexagons_df['HEX_ID'] == selected_hex].copy()
origin_row['ORIGIN_HEX'] = selected_hex
origin_row['DEST_HEX'] = selected_hex
origin_row['DISTANCE_KM'] = 0.0
origin_row['DURATION_MINUTES'] = 0.0
origin_row['ORIGIN_LAT'] = origin_row['LATITUDE']
origin_row['ORIGIN_LON'] = origin_row['LONGITUDE']
origin_row['DEST_LAT'] = origin_row['LATITUDE']
origin_row['DEST_LON'] = origin_row['LONGITUDE']

travel_times_df = pd.concat([travel_times_df, origin_row[['ORIGIN_HEX', 'DEST_HEX', 'DISTANCE_KM', 'DURATION_MINUTES', 'ORIGIN_LAT', 'ORIGIN_LON', 'DEST_LAT', 'DEST_LON']]], ignore_index=True)

# Add ring information to dataframe
travel_times_df['RING'] = travel_times_df['DEST_HEX'].map(hex_to_ring)

# Add color based on travel time
travel_times_df['COLOR'] = travel_times_df['DURATION_MINUTES'].apply(get_color_for_time)

# Summary statistics
st.subheader("📊 Travel Time Summary")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Neighbors Analyzed",
        f"{len(travel_times_df) - 1:,}"
    )

with col2:
    avg_time = travel_times_df[travel_times_df['DEST_HEX'] != selected_hex]['DURATION_MINUTES'].mean()
    st.metric(
        "Avg Travel Time",
        f"{avg_time:.1f} min"
    )

with col3:
    max_time = travel_times_df[travel_times_df['DEST_HEX'] != selected_hex]['DURATION_MINUTES'].max()
    st.metric(
        "Max Travel Time",
        f"{max_time:.1f} min"
    )

with col4:
    avg_dist = travel_times_df[travel_times_df['DEST_HEX'] != selected_hex]['DISTANCE_KM'].mean()
    st.metric(
        "Avg Distance",
        f"{avg_dist:.1f} km"
    )

# Statistics by ring
st.subheader("📈 Travel Time by Ring")

ring_stats = travel_times_df[travel_times_df['DEST_HEX'] != selected_hex].groupby('RING').agg({
    'DURATION_MINUTES': ['min', 'mean', 'max', 'count'],
    'DISTANCE_KM': 'mean'
}).round(1)

ring_stats.columns = ['Min Time (min)', 'Avg Time (min)', 'Max Time (min)', 'Hexagons', 'Avg Distance (km)']
ring_stats = ring_stats.reset_index()
ring_stats.columns = ['Ring', 'Min Time (min)', 'Avg Time (min)', 'Max Time (min)', 'Hexagons', 'Avg Distance (km)']

st.dataframe(
    ring_stats,
    use_container_width=True,
    hide_index=True
)

# Map visualization
st.subheader("🗺️ Interactive Map")

# Prepare data for PyDeck
map_data = travel_times_df[['DEST_HEX_ID', 'DURATION_MINUTES', 'DISTANCE_KM', 'RING', 'COLOR', 'DEST_LAT', 'DEST_LON']].copy()
map_data = map_data.rename(columns={
    'DEST_HEX_ID': 'hex',
    'DURATION_MINUTES': 'time',
    'DISTANCE_KM': 'distance',
    'RING': 'ring'
})

# Create PyDeck layer
h3_layer = pdk.Layer(
    'H3HexagonLayer',
    map_data,
    get_hexagon='hex',
    get_fill_color='COLOR',
    get_line_color=[255, 255, 255],
    line_width_min_pixels=2,
    opacity=0.7,
    pickable=True,
    stroked=True,
    filled=True,
    extruded=False,
    auto_highlight=True
)

# Get center coordinates
center_lat = origin_row['LATITUDE'].iloc[0]
center_lon = origin_row['LONGITUDE'].iloc[0]

# Create view state
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=11,
    pitch=0,
    bearing=0
)

# Tooltip
tooltip = {
    "html": """
    <b>Hexagon:</b> {hex}<br/>
    <b>Ring:</b> {ring}<br/>
    <b>Travel Time:</b> {time:.1f} min<br/>
    <b>Distance:</b> {distance:.1f} km
    """,
    "style": {
        "backgroundColor": "steelblue",
        "color": "white",
        "padding": "10px"
    }
}

# Create deck
deck = pdk.Deck(
    layers=[h3_layer],
    initial_view_state=view_state,
    tooltip=tooltip,
    map_style='mapbox://styles/mapbox/light-v10'
)

st.pydeck_chart(deck, use_container_width=True)

# Detailed data table
with st.expander("📋 View Detailed Data"):
    detailed_data = travel_times_df[travel_times_df['DEST_HEX_ID'] != selected_hex][
        ['DEST_HEX_ID', 'RING', 'DURATION_MINUTES', 'DISTANCE_KM', 'DEST_LAT', 'DEST_LON']
    ].copy()
    detailed_data = detailed_data.sort_values(['RING', 'DURATION_MINUTES'])
    detailed_data.columns = ['Destination Hex', 'Ring', 'Time (min)', 'Distance (km)', 'Latitude', 'Longitude']
    
    st.dataframe(
        detailed_data,
        use_container_width=True,
        hide_index=True
    )

st.divider()

st.caption("💡 **Tip:** Select different hexagons to compare travel time patterns across San Francisco. Adjust the number of rings to focus on closer or more distant neighbors.")
