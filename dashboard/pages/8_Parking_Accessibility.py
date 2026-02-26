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

h3_resolution = st.sidebar.selectbox(
    "H3 Resolution",
    options=[4, 5, 6, 7],
    index=1,
    help="H3 hexagon size: 4 = largest (~1,300km²), 5 = large (~250km²), 6 = medium (~36km²), 7 = small (~5km²)"
)

distance_threshold = st.sidebar.selectbox(
    "Distance Threshold",
    options=[5, 10, 15],
    index=0,
    help="Distance in kilometers to count nearby parkings"
)

show_parkings = st.sidebar.checkbox(
    "Show Parking Locations",
    value=True,
    help="Display HGV parking facilities on the map"
)

generate_button = st.sidebar.button("🗺️ Generate Map", type="primary", use_container_width=True)

st.sidebar.divider()
st.sidebar.markdown("### Distance Legend")
st.sidebar.markdown("🟢 **Green** - Parking within 5 km")
st.sidebar.markdown("🟡 **Yellow** - Parking within 10 km")
st.sidebar.markdown("🟠 **Orange** - Parking within 20 km")
st.sidebar.markdown("🔴 **Red** - Parking beyond 20 km")

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
        H3_POINT_TO_CELL_STRING(s.geometry, {h3_resolution}) AS h3_cell,
        s.canonical_name AS brand,
        COUNT(CASE WHEN ST_DISTANCE(s.geometry, p.geometry) <= 5000 THEN 1 END) AS parkings_5km,
        COUNT(CASE WHEN ST_DISTANCE(s.geometry, p.geometry) <= 10000 THEN 1 END) AS parkings_10km,
        COUNT(CASE WHEN ST_DISTANCE(s.geometry, p.geometry) <= 15000 THEN 1 END) AS parkings_15km,
        MIN(ST_DISTANCE(s.geometry, p.geometry)) AS nearest_parking_meters
      FROM FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES s
      CROSS JOIN FLEET_DEMOS.ROUTING.GERMANY_HGV_PARKINGS p
      WHERE s.canonical_name = '{selected_brand}'
      GROUP BY H3_POINT_TO_CELL_STRING(s.geometry, {h3_resolution}), s.canonical_name
    ),
    h3_aggregation AS (
      SELECT 
        h3_cell,
        brand,
        COUNT(*) AS store_count,
        AVG(parkings_5km) AS avg_parkings_5km,
        AVG(parkings_10km) AS avg_parkings_10km,
        AVG(parkings_15km) AS avg_parkings_15km,
        AVG(nearest_parking_meters) AS avg_nearest_parking_meters
      FROM store_parking_counts
      GROUP BY h3_cell, brand
    )
    SELECT 
      h3_cell,
      brand,
      store_count,
      ROUND(avg_parkings_5km, 1) AS avg_parkings_5km,
      ROUND(avg_parkings_10km, 1) AS avg_parkings_10km,
      ROUND(avg_parkings_15km, 1) AS avg_parkings_15km,
      ROUND(avg_nearest_parking_meters / 1000, 2) AS avg_nearest_parking_km,
      CASE 
        WHEN avg_nearest_parking_meters <= 5000 THEN 'Within 5km'
        WHEN avg_nearest_parking_meters <= 10000 THEN 'Within 10km'
        WHEN avg_nearest_parking_meters <= 20000 THEN 'Within 20km'
        ELSE 'Beyond 20km'
      END AS accessibility_category
    FROM h3_aggregation
    ORDER BY avg_parkings_5km DESC
    """
    
    h3_result = session.sql(query).collect()
    
    if not h3_result:
        st.error(f"No stores found for {selected_brand}")
        st.stop()

def get_distance_color(avg_nearest_km):
    if avg_nearest_km <= 5:
        return [34, 139, 34]
    elif avg_nearest_km <= 10:
        return [255, 215, 0]
    elif avg_nearest_km <= 20:
        return [255, 140, 0]
    else:
        return [220, 20, 60]

h3_data = []
for row in h3_result:
    parking_count_col = f'AVG_PARKINGS_{distance_threshold}KM'
    avg_parkings = row[parking_count_col]
    avg_nearest_km = row['AVG_NEAREST_PARKING_KM']
    
    color = get_distance_color(avg_nearest_km)
    
    h3_data.append({
        'hex_id': row['H3_CELL'],
        'brand': row['BRAND'],
        'store_count': row['STORE_COUNT'],
        'avg_parkings_5km': row['AVG_PARKINGS_5KM'],
        'avg_parkings_10km': row['AVG_PARKINGS_10KM'],
        'avg_parkings_15km': row['AVG_PARKINGS_15KM'],
        'avg_parkings': avg_parkings,
        'avg_nearest_parking_km': avg_nearest_km,
        'accessibility_category': row['ACCESSIBILITY_CATEGORY'],
        'color': color
    })

h3_df = pd.DataFrame(h3_data)

if len(h3_df) == 0:
    st.warning("No hexagons with stores found")
    st.stop()

layers = []

h3_layer = pdk.Layer(
    'H3HexagonLayer',
    h3_df,
    get_hexagon='hex_id',
    get_fill_color='color',
    get_line_color='color',
    pickable=True,
    stroked=False,
    filled=True,
    extruded=False,
    opacity=0.7,
    auto_highlight=True
)
layers.append(h3_layer)

if show_parkings:
    parking_query = """
    SELECT 
      ST_X(ST_CENTROID(geometry)) AS lon,
      ST_Y(ST_CENTROID(geometry)) AS lat
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

center_lat = 51.1657
center_lon = 10.4515

view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=6,
    pitch=0
)

tooltip = {
    "html": f"""
    <b>Stores:</b> {{store_count}}<br/>
    <b>Accessibility:</b> {{accessibility_category}}<br/>
    <b>Avg Parkings ({distance_threshold}km):</b> {{avg_parkings:.1f}}<br/>
    <b>Avg Parkings (5km/10km/15km):</b> {{avg_parkings_5km:.1f}}/{{avg_parkings_10km:.1f}}/{{avg_parkings_15km:.1f}}<br/>
    <b>Avg Nearest:</b> {{avg_nearest_parking_km:.2f}} km
    """,
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

total_hexagons = len(h3_df)
total_stores = h3_df['store_count'].sum()
within_5km = len(h3_df[h3_df['accessibility_category'] == 'Within 5km'])
within_10km = len(h3_df[h3_df['accessibility_category'] == 'Within 10km'])
within_20km = len(h3_df[h3_df['accessibility_category'] == 'Within 20km'])
beyond_20km = len(h3_df[h3_df['accessibility_category'] == 'Beyond 20km'])
avg_parkings = h3_df['avg_parkings'].mean()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Hexagons", f"{total_hexagons:,}")
    st.caption(f"{total_stores:,} stores")

with col2:
    st.metric("Within 5km", f"{within_5km:,}", 
              delta=f"{(within_5km/total_hexagons*100):.1f}%",
              delta_color="normal")

with col3:
    st.metric("Within 10km", f"{within_10km:,}",
              delta=f"{(within_10km/total_hexagons*100):.1f}%",
              delta_color="off")

with col4:
    st.metric("Within 20km", f"{within_20km:,}",
              delta=f"{(within_20km/total_hexagons*100):.1f}%",
              delta_color="off")

with col5:
    st.metric("Beyond 20km", f"{beyond_20km:,}",
              delta=f"{(beyond_20km/total_hexagons*100):.1f}%",
              delta_color="inverse")

col6, col7, col8 = st.columns(3)

with col6:
    st.metric(f"Avg Parkings ({distance_threshold}km)", f"{avg_parkings:.1f}")

with col7:
    worst_hex = h3_df.nsmallest(1, 'avg_parkings')
    if not worst_hex.empty:
        st.metric("Worst Area", f"{worst_hex.iloc[0]['store_count']} stores", 
                  delta=f"{worst_hex.iloc[0]['avg_parkings']:.1f} parkings",
                  delta_color="inverse")

with col8:
    best_hex = h3_df.nlargest(1, 'avg_parkings')
    if not best_hex.empty:
        st.metric("Best Area", f"{best_hex.iloc[0]['store_count']} stores", 
                  delta=f"{best_hex.iloc[0]['avg_parkings']:.1f} parkings",
                  delta_color="normal")

st.divider()

with st.expander("📋 View Detailed Hexagon Data"):
    display_df = h3_df[[
        'store_count',
        'accessibility_category', 
        'avg_parkings_5km', 
        'avg_parkings_10km', 
        'avg_parkings_15km', 
        'avg_nearest_parking_km'
    ]].copy()
    
    display_df.columns = [
        'Store Count',
        'Accessibility',
        'Avg Parkings (5km)',
        'Avg Parkings (10km)',
        'Avg Parkings (15km)',
        'Avg Nearest (km)'
    ]
    
    display_df = display_df.sort_values('Avg Parkings (5km)')
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )
    
    st.caption(f"Showing {len(display_df)} hexagons sorted by parking accessibility (worst to best)")

st.divider()

st.markdown("### 💡 Key Insights")

beyond_20km_pct = (beyond_20km / total_hexagons * 100)
if beyond_20km_pct > 10:
    st.warning(f"⚠️ **{beyond_20km} areas ({beyond_20km_pct:.1f}%)** have parking beyond 20km. Consider alternative delivery strategies for these locations.")
elif beyond_20km_pct > 5:
    st.info(f"ℹ️ **{beyond_20km} areas ({beyond_20km_pct:.1f}%)** have parking beyond 20km. Monitor delivery performance at these locations.")
else:
    st.success(f"✅ Only **{beyond_20km} areas ({beyond_20km_pct:.1f}%)** have parking beyond 20km. Overall logistics infrastructure is good.")

if within_5km + within_10km > total_hexagons * 0.7:
    st.success(f"✅ **{within_5km + within_10km} areas ({(within_5km + within_10km)/total_hexagons*100:.1f}%)** have parking within 10km.")

st.caption("💡 **Tip:** Use H3 resolution to adjust granularity (4=regional, 7=city-level). Distance threshold changes the parking search radius. Toggle parking locations to see facility distribution.")
