import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
import json

st.set_page_config(
    page_title="WKT Visualizer - Fleet Analytics",
    page_icon="🌍",
    layout="wide"
)

session = get_active_session()

st.title("🌍 WKT Visualizer")
st.markdown("Visualize Well-Known Text (WKT) geometries on an interactive map")

example_wkts = {
    "Point": "POINT(-122.4194 37.7749)",
    "LineString": "LINESTRING(-122.4194 37.7749, -122.4089 37.7858, -122.3959 37.7914)",
    "Polygon": "POLYGON((-122.4194 37.7749, -122.4089 37.7858, -122.3959 37.7914, -122.3852 37.7815, -122.4194 37.7749))",
    "MultiPoint": "MULTIPOINT((-122.4194 37.7749), (-122.4089 37.7858), (-122.3959 37.7914))",
    "MultiLineString": "MULTILINESTRING((-122.4194 37.7749, -122.4089 37.7858), (-122.3959 37.7914, -122.3852 37.7815))",
    "MultiPolygon": "MULTIPOLYGON(((-122.52 37.70, -122.50 37.71, -122.49 37.70, -122.52 37.70)), ((-122.45 37.78, -122.43 37.79, -122.42 37.78, -122.45 37.78)))"
}

st.sidebar.header("WKT Input")

example_type = st.sidebar.selectbox(
    "Load Example WKT",
    options=["Custom"] + list(example_wkts.keys()),
    help="Load a sample WKT for testing"
)

if example_type != "Custom":
    default_wkt = example_wkts[example_type]
else:
    default_wkt = ""

wkt_input = st.sidebar.text_area(
    "Enter WKT Geometry",
    value=default_wkt,
    height=150,
    help="Paste your WKT geometry here (e.g., POINT, LINESTRING, POLYGON, MULTIPOLYGON, etc.)"
)

st.sidebar.divider()

color_choice = st.sidebar.color_picker(
    "Geometry Color",
    "#FF6B6B",
    help="Choose the color for your geometry"
)

opacity = st.sidebar.slider(
    "Opacity",
    min_value=0.0,
    max_value=1.0,
    value=0.7,
    step=0.1,
    help="Adjust the transparency of the geometry"
)

st.sidebar.divider()

map_style_options = {
    "Streets (Detailed)": "mapbox://styles/mapbox/streets-v12",
    "Light": "mapbox://styles/mapbox/light-v11",
    "Dark": "mapbox://styles/mapbox/dark-v11",
    "Outdoors": "mapbox://styles/mapbox/outdoors-v12",
    "Satellite": "mapbox://styles/mapbox/satellite-v9",
    "Satellite Streets": "mapbox://styles/mapbox/satellite-streets-v12"
}

selected_style_name = st.sidebar.selectbox(
    "Map Style",
    options=list(map_style_options.keys()),
    index=0,
    help="Choose the basemap style - Streets shows building names and detailed labels"
)

map_style = map_style_options[selected_style_name]

st.sidebar.divider()

st.sidebar.markdown("### Supported WKT Types")
st.sidebar.markdown("""
- **POINT** - Single coordinate
- **LINESTRING** - Connected line
- **POLYGON** - Closed area
- **MULTIPOINT** - Multiple points
- **MULTILINESTRING** - Multiple lines
- **MULTIPOLYGON** - Multiple polygons
- **GEOMETRYCOLLECTION** - Mixed types
""")

if not wkt_input.strip():
    st.info("👈 Enter a WKT geometry in the sidebar to visualize it on the map")
    st.stop()

def hex_to_rgb(hex_color):
    """Convert hex color to RGB list"""
    hex_color = hex_color.lstrip('#')
    return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)]

try:
    with st.spinner("Parsing WKT and converting to GeoJSON..."):
        query = f"""
        SELECT 
            ST_ASGEOJSON(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS geojson,
            ST_XMIN(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS min_lon,
            ST_XMAX(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS max_lon,
            ST_YMIN(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS min_lat,
            ST_YMAX(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS max_lat,
            ST_AREA(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS area_sq_meters,
            ST_LENGTH(ST_GEOGRAPHYFROMTEXT('{wkt_input}')) AS length_meters
        """
        
        result = session.sql(query).collect()
        
        if not result:
            st.error("Failed to parse WKT geometry")
            st.stop()
        
        row = result[0]
        geojson_str = row['GEOJSON']
        min_lon = row['MIN_LON']
        max_lon = row['MAX_LON']
        min_lat = row['MIN_LAT']
        max_lat = row['MAX_LAT']
        area_sq_meters = row['AREA_SQ_METERS']
        length_meters = row['LENGTH_METERS']
        
        geojson = json.loads(geojson_str)
        geometry_type = geojson['type']
        
    st.success(f"✅ Successfully parsed {geometry_type} geometry")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Geometry Type", geometry_type)
    
    with col2:
        if area_sq_meters and area_sq_meters > 0:
            area_sq_km = area_sq_meters / 1_000_000
            st.metric("Area", f"{area_sq_km:.2f} km²")
        else:
            st.metric("Area", "N/A")
    
    with col3:
        if length_meters and length_meters > 0:
            length_km = length_meters / 1000
            st.metric("Length", f"{length_km:.2f} km")
        else:
            st.metric("Length", "N/A")
    
    with col4:
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        st.metric("Center", f"{center_lat:.4f}, {center_lon:.4f}")
    
    st.divider()
    
    rgb_color = hex_to_rgb(color_choice)
    fill_color = rgb_color + [int(opacity * 255)]
    line_color = rgb_color + [255]
    
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2
    
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    max_span = max(lat_span, lon_span)
    
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
    else:
        zoom = 4
    
    layers = []
    
    if geometry_type == "Point":
        coords = geojson['coordinates']
        point_data = pd.DataFrame([{
            'lon': coords[0],
            'lat': coords[1],
            'color': fill_color
        }])
        
        layers.append(pdk.Layer(
            'ScatterplotLayer',
            point_data,
            get_position='[lon, lat]',
            get_fill_color='color',
            get_radius=100,
            pickable=True,
            opacity=opacity,
            filled=True
        ))
    
    elif geometry_type == "MultiPoint":
        points = []
        for coords in geojson['coordinates']:
            points.append({
                'lon': coords[0],
                'lat': coords[1],
                'color': fill_color
            })
        point_data = pd.DataFrame(points)
        
        layers.append(pdk.Layer(
            'ScatterplotLayer',
            point_data,
            get_position='[lon, lat]',
            get_fill_color='color',
            get_radius=100,
            pickable=True,
            opacity=opacity,
            filled=True
        ))
    
    elif geometry_type == "LineString":
        coords = geojson['coordinates']
        line_data = pd.DataFrame([{
            'path': coords,
            'color': line_color
        }])
        
        layers.append(pdk.Layer(
            'PathLayer',
            line_data,
            get_path='path',
            get_color='color',
            width_min_pixels=3,
            pickable=True,
            opacity=opacity
        ))
    
    elif geometry_type == "MultiLineString":
        lines = []
        for line_coords in geojson['coordinates']:
            lines.append({
                'path': line_coords,
                'color': line_color
            })
        line_data = pd.DataFrame(lines)
        
        layers.append(pdk.Layer(
            'PathLayer',
            line_data,
            get_path='path',
            get_color='color',
            width_min_pixels=3,
            pickable=True,
            opacity=opacity
        ))
    
    elif geometry_type == "Polygon":
        polygon_coords = geojson['coordinates']
        polygon_data = pd.DataFrame([{
            'polygon': polygon_coords,
            'fill_color': fill_color,
            'line_color': line_color
        }])
        
        layers.append(pdk.Layer(
            'PolygonLayer',
            polygon_data,
            get_polygon='polygon',
            get_fill_color='fill_color',
            get_line_color='line_color',
            line_width_min_pixels=2,
            pickable=True,
            opacity=opacity,
            filled=True,
            stroked=True
        ))
    
    elif geometry_type == "MultiPolygon":
        polygons = []
        for polygon_coords in geojson['coordinates']:
            polygons.append({
                'polygon': polygon_coords,
                'fill_color': fill_color,
                'line_color': line_color
            })
        polygon_data = pd.DataFrame(polygons)
        
        layers.append(pdk.Layer(
            'PolygonLayer',
            polygon_data,
            get_polygon='polygon',
            get_fill_color='fill_color',
            get_line_color='line_color',
            line_width_min_pixels=2,
            pickable=True,
            opacity=opacity,
            filled=True,
            stroked=True
        ))
    
    elif geometry_type == "GeometryCollection":
        st.warning("GeometryCollection detected - rendering individual geometries")
        
        for geom in geojson['geometries']:
            geom_type = geom['type']
            
            if geom_type == "Point":
                coords = geom['coordinates']
                point_data = pd.DataFrame([{
                    'lon': coords[0],
                    'lat': coords[1],
                    'color': fill_color
                }])
                
                layers.append(pdk.Layer(
                    'ScatterplotLayer',
                    point_data,
                    get_position='[lon, lat]',
                    get_fill_color='color',
                    get_radius=100,
                    pickable=True,
                    opacity=opacity,
                    filled=True
                ))
            
            elif geom_type == "LineString":
                coords = geom['coordinates']
                line_data = pd.DataFrame([{
                    'path': coords,
                    'color': line_color
                }])
                
                layers.append(pdk.Layer(
                    'PathLayer',
                    line_data,
                    get_path='path',
                    get_color='color',
                    width_min_pixels=3,
                    pickable=True,
                    opacity=opacity
                ))
            
            elif geom_type == "Polygon":
                polygon_coords = geom['coordinates']
                polygon_data = pd.DataFrame([{
                    'polygon': polygon_coords,
                    'fill_color': fill_color,
                    'line_color': line_color
                }])
                
                layers.append(pdk.Layer(
                    'PolygonLayer',
                    polygon_data,
                    get_polygon='polygon',
                    get_fill_color='fill_color',
                    get_line_color='line_color',
                    line_width_min_pixels=2,
                    pickable=True,
                    opacity=opacity,
                    filled=True,
                    stroked=True
                ))
    
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom,
        pitch=0,
        bearing=0
    )
    
    tooltip = {
        "html": f"<b>Type:</b> {geometry_type}",
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
        map_style=map_style
    )
    
    st.pydeck_chart(deck, use_container_width=True)
    
    with st.expander("📋 View GeoJSON"):
        st.json(geojson)
    
    with st.expander("📐 Geometry Details"):
        st.markdown(f"""
        **Bounds:**
        - Min Longitude: `{min_lon:.6f}`
        - Max Longitude: `{max_lon:.6f}`
        - Min Latitude: `{min_lat:.6f}`
        - Max Latitude: `{max_lat:.6f}`
        
        **Measurements:**
        - Area: `{area_sq_meters:,.2f}` m² ({area_sq_meters / 1_000_000:.4f} km²)
        - Length: `{length_meters:,.2f}` m ({length_meters / 1000:.4f} km)
        
        **Center Point:**
        - Latitude: `{center_lat:.6f}`
        - Longitude: `{center_lon:.6f}`
        """)
    
    with st.expander("📝 WKT Input"):
        st.code(wkt_input, language="text")

except Exception as e:
    st.error(f"❌ Error processing WKT geometry: {str(e)}")
    st.markdown("**Common issues:**")
    st.markdown("- Check WKT syntax is valid")
    st.markdown("- Ensure coordinates are in longitude, latitude order")
    st.markdown("- Verify polygons are closed (first point = last point)")
    st.markdown("- Check for proper parentheses and comma placement")
    
    with st.expander("💡 View Error Details"):
        st.exception(e)

st.divider()

st.caption("💡 **Tip:** Load example geometries from the sidebar to explore different WKT types, or paste your own WKT to visualize custom geometries.")
