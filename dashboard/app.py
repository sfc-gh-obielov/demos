import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Fleet Analytics Dashboard",
    page_icon="🚗",
    layout="wide"
)

session = get_active_session()

st.title("Fleet Analytics Dashboard")
st.markdown("Analysis of GPS trajectory data from the GeoLife dataset")

st.info("Select a page from the sidebar to explore different analytics")

st.markdown("### Available Pages:")
st.markdown("""
- **Overview** - Summary statistics and distributions
- **Route Comparison** - Compare actual GPS routes with OpenRouteService calculated routes
""")

st.divider()

st.markdown("### Quick Stats")

overview_query = """
SELECT 
    COUNT(*) as total_points,
    COUNT(DISTINCT UID) as total_users,
    COUNT(DISTINCT CONCAT(UID, '-', TID)) as total_trips,
    ROUND(AVG(SPEED), 2) as avg_speed,
    ROUND(MAX(SPEED), 2) as max_speed
FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
"""

overview_df = session.sql(overview_query).to_pandas()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "Total Points",
        f"{overview_df['TOTAL_POINTS'].iloc[0]:,}"
    )

with col2:
    st.metric(
        "Total Users",
        f"{overview_df['TOTAL_USERS'].iloc[0]:,}"
    )

with col3:
    st.metric(
        "Total Trips",
        f"{overview_df['TOTAL_TRIPS'].iloc[0]:,}"
    )

with col4:
    st.metric(
        "Avg Speed",
        f"{overview_df['AVG_SPEED'].iloc[0]} km/h"
    )

with col5:
    st.metric(
        "Max Speed",
        f"{overview_df['MAX_SPEED'].iloc[0]} km/h"
    )
