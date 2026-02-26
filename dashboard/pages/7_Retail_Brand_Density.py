import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
import json

st.set_page_config(
    page_title="Retail Brand Density - Fleet Analytics",
    page_icon="🏪",
    layout="wide"
)

session = get_active_session()

st.title("🏪 Germany Retail Brand Density Analysis")
st.markdown("Analyze retail store density and brand dominance across Germany using H3 hexagons")

available_brands = ['EDEKA', 'NETTO', 'REWE', 'ALDI', 'LIDL', 'PENNY']

st.sidebar.header("Visualization Controls")

selected_brand = st.sidebar.selectbox(
    "Select Brand",
    options=available_brands,
    index=0,
    help="Choose a retail brand to analyze"
)

h3_resolution = st.sidebar.slider(
    "H3 Resolution",
    min_value=5,
    max_value=9,
    value=7,
    help="Higher resolution = smaller hexagons (7 is recommended for city-level analysis)"
)

st.sidebar.divider()

opacity = st.sidebar.slider(
    "Map Opacity",
    min_value=0.0,
    max_value=1.0,
    value=0.7,
    step=0.1
)

show_stores = st.sidebar.checkbox(
    "Show Individual Stores",
    value=False,
    help="Display individual store locations on the map"
)

generate_button = st.sidebar.button("🗺️ Generate Maps", type="primary", use_container_width=True)

st.sidebar.divider()
st.sidebar.markdown("### Available Brands")
for brand in available_brands:
    st.sidebar.markdown(f"- **{brand}**")

if not generate_button:
    st.info("👈 Select a brand and click 'Generate Maps' to visualize retail density")
    st.markdown("""
    ### About This Analysis
    
    This dashboard provides two types of visualizations:
    
    **Map 1: Brand Density**
    - Shows the concentration of stores for the selected brand
    - Color intensity indicates store count per H3 hexagon
    - Useful for identifying high-density areas for a specific brand
    
    **Map 2: Brand Dominance**
    - Shows which brand is dominant in each area
    - Each hexagon is colored by the brand with most stores
    - Reveals competitive landscape and market share by geography
    """)
    st.stop()

col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Map 1: {selected_brand} Store Density")
    
    with st.spinner(f"Calculating {selected_brand} density..."):
        density_query = f"""
        SELECT 
            H3_POINT_TO_CELL_STRING(geometry, {h3_resolution}) as h3_cell,
            COUNT(*) as store_count,
            AVG(ST_X(geometry)) as center_lon,
            AVG(ST_Y(geometry)) as center_lat
        FROM FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES
        WHERE canonical_name = '{selected_brand}'
        GROUP BY h3_cell
        ORDER BY store_count DESC
        """
        
        density_result = session.sql(density_query).collect()
        
        if not density_result:
            st.warning(f"No stores found for {selected_brand}")
        else:
            density_data = []
            max_count = max([row['STORE_COUNT'] for row in density_result])
            
            for row in density_result:
                normalized = row['STORE_COUNT'] / max_count
                
                if normalized < 0.25:
                    color = [255, 255, 178]
                elif normalized < 0.5:
                    color = [254, 204, 92]
                elif normalized < 0.75:
                    color = [253, 141, 60]
                else:
                    color = [227, 26, 28]
                
                density_data.append({
                    'hex_id': row['H3_CELL'],
                    'color': color,
                    'store_count': row['STORE_COUNT']
                })
            
            density_df = pd.DataFrame(density_data)
            
            center_lat = sum([row['CENTER_LAT'] for row in density_result]) / len(density_result)
            center_lon = sum([row['CENTER_LON'] for row in density_result]) / len(density_result)
            
            layers = []
            
            layers.append(pdk.Layer(
                'H3HexagonLayer',
                density_df,
                get_hexagon='hex_id',
                get_fill_color='color',
                get_line_color=[100, 100, 100],
                line_width_min_pixels=1,
                pickable=True,
                stroked=True,
                filled=True,
                extruded=False,
                opacity=opacity,
                auto_highlight=True
            ))
            
            if show_stores:
                store_query = f"""
                SELECT 
                    ST_X(geometry) as lon,
                    ST_Y(geometry) as lat,
                    store_name
                FROM FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES
                WHERE canonical_name = '{selected_brand}'
                """
                store_result = session.sql(store_query).collect()
                store_df = pd.DataFrame([{
                    'lon': row['LON'],
                    'lat': row['LAT'],
                    'name': row['STORE_NAME']
                } for row in store_result])
                
                layers.append(pdk.Layer(
                    'ScatterplotLayer',
                    store_df,
                    get_position='[lon, lat]',
                    get_fill_color=[255, 140, 0, 180],
                    get_radius=50,
                    pickable=True,
                    filled=True
                ))
            
            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=6,
                pitch=0
            )
            
            tooltip = {
                "html": "<b>Stores:</b> {store_count}<br/><b>H3 Cell:</b> {hex_id}",
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
            
            st.metric(f"Total {selected_brand} Stores", len(density_result) * density_result[0]['STORE_COUNT'] if density_result else 0)
            st.metric("Hexagons with Stores", len(density_result))
            st.metric("Max Stores per Hexagon", max_count)

with col2:
    st.subheader("Map 2: Brand Dominance Analysis")
    
    with st.spinner("Calculating brand dominance..."):
        dominance_query = f"""
        WITH store_h3 AS (
            SELECT 
                H3_POINT_TO_CELL_STRING(geometry, {h3_resolution}) as h3_cell,
                canonical_name,
                COUNT(*) as store_count
            FROM FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES
            GROUP BY h3_cell, canonical_name
        ),
        dominant_brand AS (
            SELECT 
                h3_cell,
                canonical_name as dominant_brand,
                store_count,
                ROW_NUMBER() OVER (PARTITION BY h3_cell ORDER BY store_count DESC) as rank
            FROM store_h3
        ),
        h3_centers AS (
            SELECT 
                H3_POINT_TO_CELL_STRING(geometry, {h3_resolution}) as h3_cell,
                AVG(ST_X(geometry)) as center_lon,
                AVG(ST_Y(geometry)) as center_lat
            FROM FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES
            GROUP BY h3_cell
        )
        SELECT 
            d.h3_cell,
            d.dominant_brand,
            d.store_count,
            c.center_lon,
            c.center_lat
        FROM dominant_brand d
        JOIN h3_centers c ON d.h3_cell = c.h3_cell
        WHERE d.rank = 1
        ORDER BY d.store_count DESC
        """
        
        dominance_result = session.sql(dominance_query).collect()
        
        if not dominance_result:
            st.warning("No dominance data available")
        else:
            brand_colors = {
                'EDEKA': [46, 125, 50],
                'NETTO': [255, 193, 7],
                'REWE': [211, 47, 47],
                'ALDI': [25, 118, 210],
                'LIDL': [251, 140, 0],
                'PENNY': [142, 36, 170]
            }
            
            dominance_data = []
            brand_counts = {}
            
            for row in dominance_result:
                brand = row['DOMINANT_BRAND']
                color = brand_colors.get(brand, [128, 128, 128])
                
                brand_counts[brand] = brand_counts.get(brand, 0) + 1
                
                dominance_data.append({
                    'hex_id': row['H3_CELL'],
                    'color': color,
                    'dominant_brand': brand,
                    'store_count': row['STORE_COUNT']
                })
            
            dominance_df = pd.DataFrame(dominance_data)
            
            center_lat = sum([row['CENTER_LAT'] for row in dominance_result]) / len(dominance_result)
            center_lon = sum([row['CENTER_LON'] for row in dominance_result]) / len(dominance_result)
            
            layer = pdk.Layer(
                'H3HexagonLayer',
                dominance_df,
                get_hexagon='hex_id',
                get_fill_color='color',
                get_line_color=[100, 100, 100],
                line_width_min_pixels=1,
                pickable=True,
                stroked=True,
                filled=True,
                extruded=False,
                opacity=opacity,
                auto_highlight=True
            )
            
            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=6,
                pitch=0
            )
            
            tooltip = {
                "html": "<b>Dominant Brand:</b> {dominant_brand}<br/><b>Stores:</b> {store_count}<br/><b>H3 Cell:</b> {h3_cell}",
                "style": {
                    "backgroundColor": "steelblue",
                    "color": "white",
                    "padding": "10px"
                }
            }
            
            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style='road'
            )
            
            st.pydeck_chart(deck, use_container_width=True)
            
            st.markdown("### Brand Dominance Summary")
            for brand in sorted(brand_counts.keys(), key=lambda x: brand_counts[x], reverse=True):
                st.metric(f"{brand} Dominant Areas", brand_counts[brand])

st.divider()

with st.expander("📊 Brand Legend (Map 2)"):
    legend_cols = st.columns(6)
    for idx, brand in enumerate(available_brands):
        with legend_cols[idx]:
            st.markdown(f"**{brand}**")
            color = {
                'EDEKA': '🟢',
                'NETTO': '🟡',
                'REWE': '🔴',
                'ALDI': '🔵',
                'LIDL': '🟠',
                'PENNY': '🟣'
            }
            st.markdown(f"{color.get(brand, '⚪')} {brand}")

st.caption("💡 **Tip:** Use higher H3 resolution (8-9) for detailed city-level analysis, or lower resolution (5-6) for regional overview.")
