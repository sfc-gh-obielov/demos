import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Parking Accessibility - Fleet Analytics",
    page_icon="🅿️",
    layout="wide"
)

session = get_active_session()

st.title("🅿️ Store Parking Accessibility Analysis")
st.markdown("Analyze delivery logistics by evaluating HGV parking proximity to retail stores")

available_brands = ['EDEKA', 'NETTO', 'REWE', 'ALDI', 'LIDL', 'PENNY']
accessibility_categories = ['Excellent', 'Good', 'Moderate', 'Poor']

st.sidebar.header("Analysis Settings")

selected_brand = st.sidebar.selectbox(
    "Select Brand",
    options=available_brands,
    index=2,
    help="Choose a retail brand to analyze parking accessibility"
)

distance_threshold = st.sidebar.selectbox(
    "Distance Threshold",
    options=[5, 10, 15],
    index=0,
    help="Distance in kilometers to count nearby parkings"
)

accessibility_filter = st.sidebar.multiselect(
    "Show Categories",
    options=accessibility_categories,
    default=accessibility_categories,
    help="Filter stores by accessibility category"
)

show_parkings = st.sidebar.checkbox(
    "Show Parking Locations",
    value=True,
    help="Display HGV parking facilities on the map"
)

generate_button = st.sidebar.button("🗺️ Generate Map", type="primary", use_container_width=True)

st.sidebar.divider()
st.sidebar.markdown("### Color Legend")
st.sidebar.markdown("🟢 **Excellent** - 5+ parkings within threshold")
st.sidebar.markdown("🟡 **Good** - 3-4 parkings within threshold")
st.sidebar.markdown("🟠 **Moderate** - 1-2 parkings within threshold")
st.sidebar.markdown("🔴 **Poor** - 0 parkings within threshold")

if not generate_button:
    st.info("👈 Select a brand and click 'Generate Map' to analyze parking accessibility")
    st.markdown("""
    ### About This Analysis
    
    This dashboard helps identify delivery logistics challenges by analyzing the proximity of retail stores to HGV (Heavy Goods Vehicle) parking facilities.
    
    **Key Metrics**:
    - **Parking Count**: Number of HGV parkings within selected distance
    - **Accessibility Score**: Categorization based on parking availability
    - **Nearest Parking**: Distance to closest parking facility
    
    **Use Cases**:
    - Identify stores with poor parking access (delivery challenges)
    - Plan alternative delivery strategies for problematic locations
    - Compare parking accessibility across different regions
    - Evaluate new store site locations for logistics feasibility
    
    **Data Sources**:
    - 36,673 retail stores across Germany (6 brands)
    - 4,187 HGV parking facilities from Overture Maps
    """)
    st.stop()

with st.spinner(f"Analyzing parking accessibility for {selected_brand} stores..."):
    query = f"""
    WITH store_parking_counts AS (
      SELECT 
        s.id AS store_id,
        s.store_name,
        s.canonical_name AS brand,
        ST_X(s.geometry) AS store_lon,
        ST_Y(s.geometry) AS store_lat,
        COUNT(CASE WHEN ST_DISTANCE(s.geometry, p.geometry) <= 5000 THEN 1 END) AS parkings_5km,
        COUNT(CASE WHEN ST_DISTANCE(s.geometry, p.geometry) <= 10000 THEN 1 END) AS parkings_10km,
        COUNT(CASE WHEN ST_DISTANCE(s.geometry, p.geometry) <= 15000 THEN 1 END) AS parkings_15km,
        MIN(ST_DISTANCE(s.geometry, p.geometry)) AS nearest_parking_meters
      FROM FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES s
      CROSS JOIN FLEET_DEMOS.ROUTING.GERMANY_HGV_PARKINGS p
      WHERE s.canonical_name = '{selected_brand}'
      GROUP BY s.id, s.store_name, s.canonical_name, ST_X(s.geometry), ST_Y(s.geometry)
    )
    SELECT 
      store_id,
      store_name,
      brand,
      store_lon,
      store_lat,
      parkings_5km,
      parkings_10km,
      parkings_15km,
      ROUND(nearest_parking_meters / 1000, 2) AS nearest_parking_km,
      CASE 
        WHEN parkings_5km >= 5 THEN 'Excellent'
        WHEN parkings_5km >= 3 THEN 'Good'
        WHEN parkings_5km >= 1 THEN 'Moderate'
        ELSE 'Poor'
      END AS accessibility_category
    FROM store_parking_counts
    ORDER BY parkings_5km DESC
    """
    
    store_result = session.sql(query).collect()
    
    if not store_result:
        st.error(f"No stores found for {selected_brand}")
        st.stop()

def get_accessibility_color(category):
    colors = {
        'Excellent': [34, 139, 34, 200],
        'Good': [255, 215, 0, 200],
        'Moderate': [255, 140, 0, 200],
        'Poor': [220, 20, 60, 200]
    }
    return colors.get(category, [128, 128, 128, 200])

store_data = []
for row in store_result:
    accessibility = row['ACCESSIBILITY_CATEGORY']
    
    if accessibility not in accessibility_filter:
        continue
    
    parking_count_col = f'PARKINGS_{distance_threshold}KM'
    parking_count = row[parking_count_col]
    
    color = get_accessibility_color(accessibility)
    
    store_data.append({
        'store_name': row['STORE_NAME'],
        'brand': row['BRAND'],
        'store_lon': row['STORE_LON'],
        'store_lat': row['STORE_LAT'],
        'parkings_5km': row['PARKINGS_5KM'],
        'parkings_10km': row['PARKINGS_10KM'],
        'parkings_15km': row['PARKINGS_15KM'],
        'parking_count': parking_count,
        'nearest_parking_km': row['NEAREST_PARKING_KM'],
        'accessibility_category': accessibility,
        'color': color
    })

store_df = pd.DataFrame(store_data)

if len(store_df) == 0:
    st.warning("No stores match the selected accessibility filters")
    st.stop()

layers = []

store_layer = pdk.Layer(
    'ScatterplotLayer',
    store_df,
    get_position='[store_lon, store_lat]',
    get_fill_color='color',
    get_radius=300,
    pickable=True,
    filled=True,
    auto_highlight=True
)
layers.append(store_layer)

if show_parkings:
    parking_query = """
    SELECT 
      ST_X(geometry) AS lon,
      ST_Y(geometry) AS lat
    FROM FLEET_DEMOS.ROUTING.GERMANY_HGV_PARKINGS
    """
    
    parking_result = session.sql(parking_query).collect()
    parking_df = pd.DataFrame([{
        'lon': row['LON'],
        'lat': row['LAT']
    } for row in parking_result])
    
    parking_layer = pdk.Layer(
        'ScatterplotLayer',
        parking_df,
        get_position='[lon, lat]',
        get_fill_color=[100, 149, 237, 150],
        get_radius=200,
        pickable=True,
        filled=True
    )
    layers.append(parking_layer)

center_lat = store_df['store_lat'].mean()
center_lon = store_df['store_lon'].mean()

view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=6,
    pitch=0
)

tooltip = {
    "html": """
    <b>{store_name}</b><br/>
    <b>Brand:</b> {brand}<br/>
    <b>Accessibility:</b> {accessibility_category}<br/>
    <b>Parkings within {distance_threshold}km:</b> {parking_count}<br/>
    <b>Parkings (5km/10km/15km):</b> {parkings_5km}/{parkings_10km}/{parkings_15km}<br/>
    <b>Nearest parking:</b> {nearest_parking_km} km
    """.replace("{distance_threshold}", str(distance_threshold)),
    "style": {
        "backgroundColor": "steelblue",
        "color": "white",
        "padding": "10px"
    }
}

deck = pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    tooltip=tooltip,
    map_style='road'
)

st.pydeck_chart(deck, use_container_width=True)

st.divider()

st.subheader("📊 Accessibility Statistics")

total_stores = len(store_df)
poor_count = len(store_df[store_df['accessibility_category'] == 'Poor'])
moderate_count = len(store_df[store_df['accessibility_category'] == 'Moderate'])
good_count = len(store_df[store_df['accessibility_category'] == 'Good'])
excellent_count = len(store_df[store_df['accessibility_category'] == 'Excellent'])
avg_parkings = store_df['parking_count'].mean()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Stores", f"{total_stores:,}")

with col2:
    st.metric("Poor Access", f"{poor_count:,}", 
              delta=f"{(poor_count/total_stores*100):.1f}%",
              delta_color="inverse")

with col3:
    st.metric("Moderate", f"{moderate_count:,}",
              delta=f"{(moderate_count/total_stores*100):.1f}%",
              delta_color="off")

with col4:
    st.metric("Good", f"{good_count:,}",
              delta=f"{(good_count/total_stores*100):.1f}%",
              delta_color="off")

with col5:
    st.metric("Excellent", f"{excellent_count:,}",
              delta=f"{(excellent_count/total_stores*100):.1f}%",
              delta_color="normal")

col6, col7, col8 = st.columns(3)

with col6:
    st.metric(f"Avg Parkings ({distance_threshold}km)", f"{avg_parkings:.1f}")

with col7:
    worst_store = store_df.nsmallest(1, 'parking_count')
    if not worst_store.empty:
        st.metric("Worst Store", worst_store.iloc[0]['store_name'][:20], 
                  delta=f"{worst_store.iloc[0]['parking_count']} parkings",
                  delta_color="inverse")

with col8:
    best_store = store_df.nlargest(1, 'parking_count')
    if not best_store.empty:
        st.metric("Best Store", best_store.iloc[0]['store_name'][:20], 
                  delta=f"{best_store.iloc[0]['parking_count']} parkings",
                  delta_color="normal")

st.divider()

with st.expander("📋 View Detailed Store Data"):
    display_df = store_df[[
        'store_name', 
        'accessibility_category', 
        'parkings_5km', 
        'parkings_10km', 
        'parkings_15km', 
        'nearest_parking_km'
    ]].copy()
    
    display_df.columns = [
        'Store Name',
        'Accessibility',
        'Parkings (5km)',
        'Parkings (10km)',
        'Parkings (15km)',
        'Nearest Parking (km)'
    ]
    
    display_df = display_df.sort_values('Parkings (5km)')
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )
    
    st.caption(f"Showing {len(display_df)} stores sorted by parking accessibility (worst to best)")

st.divider()

st.markdown("### 💡 Key Insights")

poor_pct = (poor_count / total_stores * 100)
if poor_pct > 10:
    st.warning(f"⚠️ **{poor_count} stores ({poor_pct:.1f}%)** have poor parking access. Consider alternative delivery strategies for these locations.")
elif poor_pct > 5:
    st.info(f"ℹ️ **{poor_count} stores ({poor_pct:.1f}%)** have poor parking access. Monitor delivery performance at these locations.")
else:
    st.success(f"✅ Only **{poor_count} stores ({poor_pct:.1f}%)** have poor parking access. Overall logistics infrastructure is good.")

if excellent_count + good_count > total_stores * 0.7:
    st.success(f"✅ **{excellent_count + good_count} stores ({(excellent_count + good_count)/total_stores*100:.1f}%)** have good or excellent parking access.")

st.caption("💡 **Tip:** Use the distance threshold slider to analyze parking accessibility at different ranges. Toggle parking locations on the map to visualize spatial distribution.")
