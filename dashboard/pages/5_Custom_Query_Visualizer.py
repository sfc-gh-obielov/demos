import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
import json

st.set_page_config(
    page_title="Custom Query Visualizer - Fleet Analytics",
    page_icon="🔍",
    layout="wide"
)

session = get_active_session()

st.title("🔍 Custom Query Visualizer")
st.markdown("Execute custom SQL queries and visualize geography results with color-coded metrics")

example_query = """WITH routes AS (
    SELECT 
        origin_lon,
        origin_lat,
        dest_lon,
        dest_lat,
        direction,
        OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS(
            'driving-car',
            ARRAY_CONSTRUCT(origin_lon, origin_lat),
            ARRAY_CONSTRUCT(dest_lon, dest_lat)
        ) as route
    FROM (
        VALUES 
            (-88.0133, 44.5133, -87.0646, 45.7452, 'North - Escanaba, MI'),
            (-88.0133, 44.5133, -87.3771, 44.8342, 'Northeast - Sturgeon Bay, WI'),
            (-88.0133, 44.5133, -87.6571, 44.0886, 'East - Manitowoc, WI'),
            (-88.0133, 44.5133, -87.7145, 43.7508, 'Southeast - Sheboygan, WI'),
            (-88.0133, 44.5133, -87.9065, 43.0389, 'South - Milwaukee, WI'),
            (-88.0133, 44.5133, -89.4012, 43.0731, 'Southwest - Madison, WI'),
            (-88.0133, 44.5133, -89.5301, 44.5236, 'West - Stevens Point, WI'),
            (-88.0133, 44.5133, -92.1041, 46.7207, 'Northwest - Superior, WI'),
            (-88.0133, 44.5133, -90.8843, 46.5433, 'North-Northwest - Ashland, WI'),
            (-88.0133, 44.5133, -89.0940, 42.2711, 'South-Southwest - Rockford, IL')
    ) AS t(origin_lon, origin_lat, dest_lon, dest_lat, direction)
),
route_geometries AS (
    SELECT 
        direction,
        ROUND(route:features[0]:properties:summary:distance::NUMBER / 1000, 2) as distance_km,
        ROUND(route:features[0]:properties:summary:duration::NUMBER / 60, 2) as duration_min,
        TO_GEOGRAPHY(route:features[0]:geometry) as geometry
    FROM routes
)
SELECT 
    geometry,
    direction,
    distance_km,
    duration_min
FROM route_geometries"""

st.sidebar.header("Query Input")

load_example = st.sidebar.checkbox("Load Example Query", value=False)

if load_example:
    default_query = example_query
else:
    default_query = ""

sql_query = st.sidebar.text_area(
    "Enter SQL Query",
    value=default_query,
    height=400,
    help="Query must return at least one GEOGRAPHY column and one numeric column for color coding"
)

st.sidebar.divider()

st.sidebar.markdown("### Requirements")
st.sidebar.markdown("""
Your query should return:
- **One GEOGRAPHY column** (e.g., geometry, combined_routes)
- **At least one numeric column** for color coding (e.g., distance_km, duration_min)
- **Optional text columns** for labels and tooltips
""")

if not sql_query.strip():
    st.info("👈 Enter a SQL query in the sidebar that returns geography and numeric columns")
    st.stop()

if st.sidebar.button("🚀 Execute Query", type="primary", use_container_width=True):
    st.session_state.query_executed = True
    st.session_state.sql_query = sql_query
else:
    if 'query_executed' not in st.session_state:
        st.info("👈 Click 'Execute Query' to run your SQL and visualize results")
        st.stop()
    sql_query = st.session_state.get('sql_query', sql_query)

try:
    with st.spinner("Executing query..."):
        result = session.sql(sql_query).collect()
        
        if not result:
            st.warning("Query returned no results")
            st.stop()
        
        df = session.sql(sql_query).to_pandas()
        
        if df.empty:
            st.warning("Query returned empty result set")
            st.stop()
    
    st.success(f"✅ Query executed successfully - {len(df)} rows returned")
    
    geography_columns = []
    numeric_columns = []
    text_columns = []
    
    for col in df.columns:
        col_type = str(df[col].dtype)
        
        if col_type == 'object':
            sample_val = df[col].iloc[0]
            if sample_val and isinstance(sample_val, str):
                try:
                    json.loads(sample_val)
                    geography_columns.append(col)
                except:
                    text_columns.append(col)
            elif sample_val is None:
                text_columns.append(col)
            else:
                text_columns.append(col)
        elif col_type in ['int64', 'float64', 'int32', 'float32']:
            numeric_columns.append(col)
        else:
            text_columns.append(col)
    
    if not geography_columns:
        st.error("❌ No GEOGRAPHY column detected in query results")
        st.info("Make sure your query returns at least one GEOGRAPHY column (use TO_GEOGRAPHY, ST_GEOGRAPHYFROMTEXT, etc.)")
        st.stop()
    
    if not numeric_columns:
        st.warning("⚠️ No numeric columns detected - color coding will not be available")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        selected_geo_col = st.selectbox(
            "Select Geography Column",
            options=geography_columns,
            help="Choose which geography column to visualize"
        )
    
    with col2:
        if numeric_columns:
            selected_metric_col = st.selectbox(
                "Select Metric for Color Coding",
                options=numeric_columns,
                help="Choose which numeric column to use for color gradient"
            )
        else:
            selected_metric_col = None
            st.info("No numeric columns available for color coding")
    
    st.divider()
    
    with st.spinner("Converting geography to GeoJSON..."):
        conversion_query = f"""
        WITH original_data AS (
            {sql_query}
        )
        SELECT 
            ST_ASGEOJSON({selected_geo_col}) as geojson_str,
            *
        FROM original_data
        """
        
        geo_df = session.sql(conversion_query).to_pandas()
    
    if selected_metric_col:
        metric_values = geo_df[selected_metric_col].dropna()
        min_val = metric_values.min()
        max_val = metric_values.max()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(f"Min {selected_metric_col}", f"{min_val:.2f}")
        with col2:
            st.metric(f"Avg {selected_metric_col}", f"{metric_values.mean():.2f}")
        with col3:
            st.metric(f"Max {selected_metric_col}", f"{max_val:.2f}")
    
    def get_color_from_value(value, min_val, max_val):
        """Generate color gradient from green to red based on normalized value"""
        if pd.isna(value):
            return [128, 128, 128, 200]
        
        if max_val == min_val:
            return [255, 215, 0, 200]
        
        normalized = (value - min_val) / (max_val - min_val)
        
        if normalized < 0.25:
            r, g, b = 34, 139, 34
        elif normalized < 0.5:
            r, g, b = 154, 205, 50
        elif normalized < 0.75:
            r, g, b = 255, 165, 0
        else:
            r, g, b = 220, 20, 60
        
        return [r, g, b, 200]
    
    if selected_metric_col:
        geo_df['COLOR'] = geo_df[selected_metric_col].apply(
            lambda x: get_color_from_value(x, min_val, max_val)
        )
    else:
        geo_df['COLOR'] = [[70, 130, 180, 200]] * len(geo_df)
    
    layers = []
    all_coords = []
    
    for idx, row in geo_df.iterrows():
        geojson_str = row['GEOJSON_STR']
        if not geojson_str or pd.isna(geojson_str):
            continue
        
        geojson = json.loads(geojson_str)
        geom_type = geojson['type']
        color = row['COLOR']
        
        tooltip_data = {}
        for col in geo_df.columns:
            if col not in ['GEOJSON_STR', 'COLOR', selected_geo_col]:
                val = row[col]
                if pd.notna(val):
                    tooltip_data[col] = val
        
        if geom_type == 'Point':
            coords = geojson['coordinates']
            all_coords.append(coords)
            
            point_df = pd.DataFrame([{
                'lon': coords[0],
                'lat': coords[1],
                'color': color,
                **tooltip_data
            }])
            
            layers.append(pdk.Layer(
                'ScatterplotLayer',
                point_df,
                get_position='[lon, lat]',
                get_fill_color='color',
                get_radius=200,
                pickable=True,
                opacity=0.8,
                filled=True
            ))
        
        elif geom_type == 'MultiPoint':
            for coords in geojson['coordinates']:
                all_coords.append(coords)
            
            points = [{
                'lon': c[0],
                'lat': c[1],
                'color': color,
                **tooltip_data
            } for c in geojson['coordinates']]
            
            point_df = pd.DataFrame(points)
            
            layers.append(pdk.Layer(
                'ScatterplotLayer',
                point_df,
                get_position='[lon, lat]',
                get_fill_color='color',
                get_radius=200,
                pickable=True,
                opacity=0.8,
                filled=True
            ))
        
        elif geom_type == 'LineString':
            coords = geojson['coordinates']
            for c in coords:
                all_coords.append(c)
            
            line_df = pd.DataFrame([{
                'path': coords,
                'color': color,
                **tooltip_data
            }])
            
            layers.append(pdk.Layer(
                'PathLayer',
                line_df,
                get_path='path',
                get_color='color',
                width_min_pixels=4,
                pickable=True,
                opacity=0.8
            ))
        
        elif geom_type == 'MultiLineString':
            for line_coords in geojson['coordinates']:
                for c in line_coords:
                    all_coords.append(c)
                
                line_df = pd.DataFrame([{
                    'path': line_coords,
                    'color': color,
                    **tooltip_data
                }])
                
                layers.append(pdk.Layer(
                    'PathLayer',
                    line_df,
                    get_path='path',
                    get_color='color',
                    width_min_pixels=4,
                    pickable=True,
                    opacity=0.8
                ))
        
        elif geom_type == 'Polygon':
            polygon_coords = geojson['coordinates']
            for ring in polygon_coords:
                for c in ring:
                    all_coords.append(c)
            
            fill_color = color
            line_color = [color[0], color[1], color[2], 255]
            
            polygon_df = pd.DataFrame([{
                'polygon': polygon_coords,
                'fill_color': fill_color,
                'line_color': line_color,
                **tooltip_data
            }])
            
            layers.append(pdk.Layer(
                'PolygonLayer',
                polygon_df,
                get_polygon='polygon',
                get_fill_color='fill_color',
                get_line_color='line_color',
                line_width_min_pixels=2,
                pickable=True,
                opacity=0.7,
                filled=True,
                stroked=True
            ))
        
        elif geom_type == 'MultiPolygon':
            for polygon_coords in geojson['coordinates']:
                for ring in polygon_coords:
                    for c in ring:
                        all_coords.append(c)
                
                fill_color = color
                line_color = [color[0], color[1], color[2], 255]
                
                polygon_df = pd.DataFrame([{
                    'polygon': polygon_coords,
                    'fill_color': fill_color,
                    'line_color': line_color,
                    **tooltip_data
                }])
                
                layers.append(pdk.Layer(
                    'PolygonLayer',
                    polygon_df,
                    get_polygon='polygon',
                    get_fill_color='fill_color',
                    get_line_color='line_color',
                    line_width_min_pixels=2,
                    pickable=True,
                    opacity=0.7,
                    filled=True,
                    stroked=True
                ))
    
    if all_coords:
        lons = [c[0] for c in all_coords]
        lats = [c[1] for c in all_coords]
        
        center_lon = sum(lons) / len(lons)
        center_lat = sum(lats) / len(lats)
        
        lon_span = max(lons) - min(lons)
        lat_span = max(lats) - min(lats)
        max_span = max(lon_span, lat_span)
        
        if max_span < 0.01:
            zoom = 14
        elif max_span < 0.05:
            zoom = 12
        elif max_span < 0.2:
            zoom = 10
        elif max_span < 1.0:
            zoom = 8
        elif max_span < 5.0:
            zoom = 6
        elif max_span < 15.0:
            zoom = 5
        else:
            zoom = 4
    else:
        center_lon, center_lat = -88.0133, 44.5133
        zoom = 8
    
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom,
        pitch=0,
        bearing=0
    )
    
    tooltip_html = "<b>Geometry Details</b><br/>"
    for col in geo_df.columns:
        if col not in ['GEOJSON_STR', 'COLOR', selected_geo_col]:
            tooltip_html += f"<b>{col}:</b> {{{col}}}<br/>"
    
    tooltip = {
        "html": tooltip_html,
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
        map_style='light'
    )
    
    st.pydeck_chart(deck, use_container_width=True)
    
    if selected_metric_col:
        st.markdown(f"""
        **Color Legend ({selected_metric_col}):**
        🟢 Low ({min_val:.2f} - {min_val + (max_val-min_val)*0.25:.2f}) | 
        🟡 Medium ({min_val + (max_val-min_val)*0.25:.2f} - {min_val + (max_val-min_val)*0.75:.2f}) | 
        🔴 High ({min_val + (max_val-min_val)*0.75:.2f} - {max_val:.2f})
        """)
    
    with st.expander("📊 View Data Table"):
        display_df = df.copy()
        
        for col in display_df.columns:
            if str(display_df[col].dtype) == 'object':
                sample_val = display_df[col].iloc[0] if len(display_df) > 0 else None
                if sample_val and isinstance(sample_val, str):
                    try:
                        json.loads(sample_val)
                        display_df[col] = display_df[col].apply(lambda x: str(x)[:100] + "..." if x and len(str(x)) > 100 else x)
                    except:
                        pass
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    with st.expander("📝 View Query"):
        st.code(sql_query, language="sql")

except Exception as e:
    st.error(f"❌ Error executing query: {str(e)}")
    st.markdown("**Common issues:**")
    st.markdown("- Check SQL syntax is valid")
    st.markdown("- Ensure tables and functions exist in your account")
    st.markdown("- Verify geography columns are properly cast (use TO_GEOGRAPHY or ST_GEOGRAPHYFROMTEXT)")
    st.markdown("- Check that OpenRouteService Native App is installed if using DIRECTIONS function")
    
    with st.expander("💡 View Error Details"):
        st.exception(e)

st.divider()

st.caption("💡 **Tip:** Your query should return at least one GEOGRAPHY column and numeric columns for color coding. Use the example query as a template.")
