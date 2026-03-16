import os
import logging
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

os.environ['SNOWFLAKE_CONNECTION_NAME'] = 'airpublic'
conn = snowflake.connector.connect(connection_name=os.environ['SNOWFLAKE_CONNECTION_NAME'])
cursor = conn.cursor()

cursor.execute("""
    WITH unique_od AS (
        SELECT DISTINCT ts.ORIGIN_ID, ts.DEST_ID,
            o.LNG AS ORIGIN_LNG, o.LAT AS ORIGIN_LAT,
            d.LNG AS DEST_LNG, d.LAT AS DEST_LAT
        FROM SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.TRIP_SCHEDULE_2W ts
        JOIN SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.GERMANY_DESTINATIONS o ON ts.ORIGIN_ID = o.ID
        JOIN SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.GERMANY_DESTINATIONS d ON ts.DEST_ID = d.ID
        WHERE ts.ORIGIN_ID IS NOT NULL AND ts.DEST_ID IS NOT NULL
    ),
    already_cached AS (
        SELECT ORIGIN_ID, DEST_ID FROM FLEET_DEMOS.ROUTING.ROUTE_CACHE
    )
    SELECT od.ORIGIN_ID, od.DEST_ID, od.ORIGIN_LNG, od.ORIGIN_LAT, od.DEST_LNG, od.DEST_LAT
    FROM unique_od od
    LEFT JOIN already_cached ac ON od.ORIGIN_ID = ac.ORIGIN_ID AND od.DEST_ID = ac.DEST_ID
    WHERE ac.ORIGIN_ID IS NULL
""")
rows = cursor.fetchall()
logger.info(f"Found {len(rows)} OD pairs to route")

BATCH_SIZE = 200
total_inserted = 0
total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i+BATCH_SIZE]
    values_parts = []
    for origin_id, dest_id, o_lng, o_lat, d_lng, d_lat in batch:
        values_parts.append(f"('{origin_id}', '{dest_id}', {o_lng}, {o_lat}, {d_lng}, {d_lat})")
    values_sql = ",\n".join(values_parts)

    sql = f"""
    INSERT INTO FLEET_DEMOS.ROUTING.ROUTE_CACHE
        (ORIGIN_ID, DEST_ID, ORIGIN_LNG, ORIGIN_LAT, DEST_LNG, DEST_LAT,
         STRAIGHT_DISTANCE_M, ROAD_DISTANCE_M, DURATION_SECONDS, ROUTE_LINE, CALCULATED_AT)
    WITH to_route AS (
        SELECT * FROM VALUES
        {values_sql}
        AS t(ORIGIN_ID, DEST_ID, ORIGIN_LNG, ORIGIN_LAT, DEST_LNG, DEST_LAT)
    )
    SELECT
        s.ORIGIN_ID, s.DEST_ID, s.ORIGIN_LNG, s.ORIGIN_LAT, s.DEST_LNG, s.DEST_LAT,
        HAVERSINE(s.ORIGIN_LAT, s.ORIGIN_LNG, s.DEST_LAT, s.DEST_LNG) * 1000,
        r.DISTANCE,
        r.DURATION,
        r.GEOJSON,
        CURRENT_TIMESTAMP()
    FROM to_route s,
    TABLE(OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS_GEO(
        'driving-hgv',
        OBJECT_CONSTRUCT(
            'coordinates', ARRAY_CONSTRUCT(
                ARRAY_CONSTRUCT(s.ORIGIN_LNG, s.ORIGIN_LAT),
                ARRAY_CONSTRUCT(s.DEST_LNG, s.DEST_LAT)
            )
        )::VARIANT
    )) r
    """
    try:
        cursor.execute(sql)
        total_inserted += len(batch)
        batch_num = i // BATCH_SIZE + 1
        logger.info(f"Batch {batch_num}/{total_batches}: +{len(batch)}, total: {total_inserted}/{len(rows)}")
    except Exception as e:
        batch_num = i // BATCH_SIZE + 1
        logger.error(f"Batch {batch_num}/{total_batches} failed: {e}")

cursor.close()
conn.close()
logger.info(f"Done. Total inserted: {total_inserted}")
