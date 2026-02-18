import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Overview - Fleet Analytics",
    page_icon="📊",
    layout="wide"
)

session = get_active_session()

st.title("📊 Fleet Analytics Overview")
st.markdown("Detailed analysis of GPS trajectory data")

with st.sidebar:
    st.header("Filters")
    
    users_query = "SELECT DISTINCT UID FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN ORDER BY UID"
    users_df = session.sql(users_query).to_pandas()
    selected_users = st.multiselect(
        "Users",
        options=users_df["UID"].tolist(),
        default=None
    )
    
    modes_query = "SELECT DISTINCT transportation_mode FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN WHERE transportation_mode IS NOT NULL ORDER BY transportation_mode"
    modes_df = session.sql(modes_query).to_pandas()
    selected_modes = st.multiselect(
        "Transportation Modes",
        options=modes_df["TRANSPORTATION_MODE"].tolist(),
        default=None
    )
    
    countries_query = "SELECT DISTINCT country_name FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN WHERE country_name IS NOT NULL ORDER BY country_name"
    countries_df = session.sql(countries_query).to_pandas()
    selected_countries = st.multiselect(
        "Countries",
        options=countries_df["COUNTRY_NAME"].tolist(),
        default=None
    )

where_clauses = []
if selected_users:
    user_list = ", ".join([f"'{u}'" for u in selected_users])
    where_clauses.append(f"UID IN ({user_list})")
if selected_modes:
    mode_list = ", ".join([f"'{m}'" for m in selected_modes])
    where_clauses.append(f"transportation_mode IN ({mode_list})")
if selected_countries:
    country_list = ", ".join([f"'{c}'" for c in selected_countries])
    where_clauses.append(f"country_name IN ({country_list})")

where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""

overview_query = f"""
SELECT 
    COUNT(*) as total_points,
    COUNT(DISTINCT UID) as total_users,
    COUNT(DISTINCT CONCAT(UID, '-', TID)) as total_trips,
    ROUND(AVG(SPEED), 2) as avg_speed,
    ROUND(MAX(SPEED), 2) as max_speed
FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
WHERE 1=1 {where_clause}
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

st.divider()

col1, col2 = st.columns(2)

with col1:
    with st.container():
        st.subheader("Transportation Mode Distribution")
        
        mode_query = f"""
        SELECT 
            transportation_mode,
            COUNT(DISTINCT CONCAT(UID, '-', TID)) as trip_count,
            ROUND(AVG(trip_avg_speed), 2) as avg_speed
        FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
        WHERE transportation_mode IS NOT NULL {where_clause}
        GROUP BY transportation_mode
        ORDER BY trip_count DESC
        """
        
        mode_df = session.sql(mode_query).to_pandas()
        
        if not mode_df.empty:
            chart = alt.Chart(mode_df).mark_bar().encode(
                x=alt.X("TRIP_COUNT:Q", title="Number of Trips"),
                y=alt.Y("TRANSPORTATION_MODE:N", title="Mode", sort="-x"),
                color=alt.Color("TRANSPORTATION_MODE:N", legend=None),
                tooltip=["TRANSPORTATION_MODE", "TRIP_COUNT", "AVG_SPEED"]
            ).properties(height=300)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No data available for selected filters")

with col2:
    with st.container():
        st.subheader("Top Countries by Trips")
        
        country_query = f"""
        SELECT 
            country_name,
            COUNT(DISTINCT CONCAT(UID, '-', TID)) as trip_count,
            COUNT(*) as point_count
        FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
        WHERE country_name IS NOT NULL {where_clause}
        GROUP BY country_name
        ORDER BY trip_count DESC
        LIMIT 10
        """
        
        country_df = session.sql(country_query).to_pandas()
        
        if not country_df.empty:
            chart = alt.Chart(country_df).mark_bar().encode(
                x=alt.X("TRIP_COUNT:Q", title="Number of Trips"),
                y=alt.Y("COUNTRY_NAME:N", title="Country", sort="-x"),
                color=alt.Color("COUNTRY_NAME:N", legend=None),
                tooltip=["COUNTRY_NAME", "TRIP_COUNT", "POINT_COUNT"]
            ).properties(height=300)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No data available for selected filters")

st.divider()

with st.container():
    st.subheader("Speed Distribution by Transportation Mode")
    
    speed_dist_query = f"""
    SELECT 
        transportation_mode,
        ROUND(MIN(trip_avg_speed), 2) as min_speed,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_avg_speed), 2) as p25_speed,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_avg_speed), 2) as median_speed,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_avg_speed), 2) as p75_speed,
        ROUND(MAX(trip_avg_speed), 2) as max_speed
    FROM (
        SELECT DISTINCT UID, TID, transportation_mode, trip_avg_speed
        FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
        WHERE transportation_mode IS NOT NULL {where_clause}
    )
    GROUP BY transportation_mode
    ORDER BY median_speed
    """
    
    speed_dist_df = session.sql(speed_dist_query).to_pandas()
    
    if not speed_dist_df.empty:
        st.dataframe(
            speed_dist_df,
            use_container_width=True
        )
    else:
        st.info("No data available for selected filters")

st.divider()

col3, col4 = st.columns(2)

with col3:
    with st.container():
        st.subheader("Transportation Mode by Country")
        
        mode_country_query = f"""
        SELECT 
            country_name,
            transportation_mode,
            COUNT(DISTINCT CONCAT(UID, '-', TID)) as trip_count
        FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
        WHERE country_name IS NOT NULL 
            AND transportation_mode IS NOT NULL
            {where_clause}
        GROUP BY country_name, transportation_mode
        ORDER BY country_name, trip_count DESC
        """
        
        mode_country_df = session.sql(mode_country_query).to_pandas()
        
        if not mode_country_df.empty:
            top_countries = mode_country_df.groupby("COUNTRY_NAME")["TRIP_COUNT"].sum().nlargest(5).index
            filtered_df = mode_country_df[mode_country_df["COUNTRY_NAME"].isin(top_countries)]
            
            chart = alt.Chart(filtered_df).mark_bar().encode(
                x=alt.X("TRIP_COUNT:Q", title="Number of Trips"),
                y=alt.Y("COUNTRY_NAME:N", title="Country", sort="-x"),
                color=alt.Color("TRANSPORTATION_MODE:N", title="Mode"),
                tooltip=["COUNTRY_NAME", "TRANSPORTATION_MODE", "TRIP_COUNT"]
            ).properties(height=300)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No data available for selected filters")

with col4:
    with st.container():
        st.subheader("Average Speed by Mode and Country")
        
        speed_country_query = f"""
        SELECT 
            country_name,
            transportation_mode,
            ROUND(AVG(trip_avg_speed), 2) as avg_speed,
            COUNT(DISTINCT CONCAT(UID, '-', TID)) as trip_count
        FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
        WHERE country_name IS NOT NULL 
            AND transportation_mode IS NOT NULL
            {where_clause}
        GROUP BY country_name, transportation_mode
        HAVING trip_count >= 5
        ORDER BY avg_speed DESC
        LIMIT 20
        """
        
        speed_country_df = session.sql(speed_country_query).to_pandas()
        
        if not speed_country_df.empty:
            st.dataframe(
                speed_country_df,
                use_container_width=True
            )
        else:
            st.info("No data available for selected filters")

st.divider()

with st.container():
    st.subheader("Sample Trip Data")
    
    sample_query = f"""
    SELECT 
        UID,
        TID,
        transportation_mode,
        country_name,
        COUNT(*) as points,
        trip_avg_speed,
        trip_max_speed,
        trip_median_speed
    FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
    WHERE 1=1 {where_clause}
    GROUP BY UID, TID, transportation_mode, country_name, 
             trip_avg_speed, trip_max_speed, trip_median_speed
    ORDER BY trip_avg_speed DESC
    LIMIT 100
    """
    
    sample_df = session.sql(sample_query).to_pandas()
    
    if not sample_df.empty:
        st.dataframe(
            sample_df,
            use_container_width=True
        )
    else:
        st.info("No data available for selected filters")
