#!/usr/bin/env python3
"""
Test script for schedule-aware continuous telemetry generator.

Loads TRIP_SCHEDULE, TRUCK_FLEET, and ROUTE_CACHE from SYNTHETIC_DATASETS.FLEET_INTELLIGENCE,
then generates GPS telemetry that follows the scheduled OD pairs.
"""
import os
import sys
import json
import logging
from datetime import date, datetime, timedelta
from typing import List
import pandas as pd
import numpy as np

os.environ['SNOWFLAKE_CONNECTION_NAME'] = 'airpublic'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.simulate import TruckAssignment, haversine_distance
from src.driver_profiles import DriverProfile, ProfileType, BehaviorSimulator
from src.routing import RouteResult, ORSRouter

SRC_SCHEMA = "SYNTHETIC_DATASETS.FLEET_INTELLIGENCE"
ROUTE_CACHE_TABLE = "FLEET_DEMOS.ROUTING.ROUTE_CACHE"


class ScheduleAwareRouter:
    """Router that uses ROUTE_CACHE from Snowflake (GEOGRAPHY-based routes)."""

    def __init__(self, conn):
        self.conn = conn
        self.cache_hits = 0
        self.cache_misses = 0
        self._cache_dict = {}

    def _load_route(self, origin_id: str, dest_id: str) -> RouteResult:
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT ORIGIN_ID, DEST_ID, ORIGIN_LNG, ORIGIN_LAT, DEST_LNG, DEST_LAT,
                    ROAD_DISTANCE_M / 1000.0 AS ROAD_DISTANCE_KM,
                    DURATION_SECONDS / 60.0 AS ROAD_DURATION_MIN,
                    ST_ASGEOJSON(ROUTE_LINE) AS ROUTE_GEOJSON
                FROM {ROUTE_CACHE_TABLE}
                WHERE ORIGIN_ID = %s AND DEST_ID = %s
                LIMIT 1
            """, (origin_id, dest_id))
            row = cursor.fetchone()
            if row is None:
                return None

            geojson_str = row[8]
            if geojson_str is None:
                return None

            geojson = json.loads(geojson_str)
            coords = [(c[0], c[1]) for c in geojson.get('coordinates', [])]
            if not coords:
                return None

            return RouteResult(
                origin_id=row[0],
                dest_id=row[1],
                origin_coords=(row[2], row[3]),
                dest_coords=(row[4], row[5]),
                distance_km=row[6],
                duration_min=row[7],
                coordinates=coords,
                num_points=len(coords)
            )
        finally:
            cursor.close()

    def get_route(
        self,
        origin_id: str,
        dest_id: str,
        origin_lng: float,
        origin_lat: float,
        dest_lng: float,
        dest_lat: float,
        route_index: int = 0
    ) -> RouteResult:
        key = (origin_id, dest_id)
        if key in self._cache_dict:
            self.cache_hits += 1
            return self._cache_dict[key]

        route = self._load_route(origin_id, dest_id)
        if route:
            self._cache_dict[key] = route
            self.cache_hits += 1
            return route

        self.cache_misses += 1
        distance_km = haversine_distance(origin_lat, origin_lng, dest_lat, dest_lng)
        duration_min = (distance_km / 60) * 60
        num_points = max(10, int(distance_km / 2))
        coords = []
        for i in range(num_points):
            t = i / (num_points - 1)
            coords.append((origin_lng + t * (dest_lng - origin_lng),
                           origin_lat + t * (dest_lat - origin_lat)))

        route = RouteResult(
            origin_id=origin_id, dest_id=dest_id,
            origin_coords=(origin_lng, origin_lat),
            dest_coords=(dest_lng, dest_lat),
            distance_km=distance_km, duration_min=duration_min,
            coordinates=coords, num_points=len(coords)
        )
        self._cache_dict[key] = route
        return route


def load_production_data(conn, start_date: str, end_date: str, num_trucks: int = 10):
    """Load truck fleet, schedule, locations, and rest stops from production schema."""
    logger.info("Loading truck fleet...")
    truck_fleet = pd.read_sql(f"""
        SELECT TRUCK_ID, HOME_BASE_ID, HOME_LNG, HOME_LAT, HOME_CITY,
            TRUCK_TYPE, DRIVER_PROFILE, BASE_SPEED_KMH
        FROM {SRC_SCHEMA}.TRUCK_FLEET
        ORDER BY TRUCK_ID
        LIMIT {num_trucks}
    """, conn)
    logger.info(f"  Loaded {len(truck_fleet)} trucks")

    truck_ids_sql = ",".join([f"'{t}'" for t in truck_fleet['TRUCK_ID'].tolist()])

    logger.info("Loading trip schedule...")
    trip_schedule = pd.read_sql(f"""
        SELECT TRUCK_ID, TRIP_DATE, TRIP_TYPE, ROUTE_VARIATION,
            ORIGIN_ID, DEST_ID, SHIFT_START_TIME, ROUTE_DEVIATION_FACTOR, DRIVER_PROFILE
        FROM {SRC_SCHEMA}.TRIP_SCHEDULE
        WHERE TRUCK_ID IN ({truck_ids_sql})
            AND TRIP_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY TRIP_DATE, TRUCK_ID
    """, conn)
    logger.info(f"  Loaded {len(trip_schedule)} scheduled trips")

    logger.info("Loading all locations (warehouses + destinations)...")
    locations = pd.read_sql(f"""
        SELECT ID, NAME, LNG AS LONGITUDE, LAT AS LATITUDE, CITY
        FROM {SRC_SCHEMA}.GERMANY_DESTINATIONS
    """, conn)
    logger.info(f"  Loaded {len(locations)} locations")

    logger.info("Loading rest stops...")
    rest_stops = pd.read_sql(f"""
        SELECT REST_STOP_ID, NAME, LNG AS LONGITUDE, LAT AS LATITUDE,
            REST_TYPE
        FROM {SRC_SCHEMA}.GERMANY_REST_STOPS
        LIMIT 2000
    """, conn)
    logger.info(f"  Loaded {len(rest_stops)} rest stops")

    return truck_fleet, trip_schedule, locations, rest_stops


def create_trucks_from_fleet(truck_fleet: pd.DataFrame) -> List[TruckAssignment]:
    """Create TruckAssignment objects from TRUCK_FLEET table data."""
    profiles_map = {
        'COMPLIANT': DriverProfile(
            profile_type=ProfileType.COMPLIANT,
            detour_probability=0.05, speeding_probability=0.02,
            hos_violation_probability=0.005, speed_variance=0.05
        ),
        'MILD': DriverProfile(
            profile_type=ProfileType.MILD,
            detour_probability=0.15, speeding_probability=0.12,
            hos_violation_probability=0.03, speed_variance=0.10
        ),
        'OUTLIER': DriverProfile(
            profile_type=ProfileType.OUTLIER,
            detour_probability=0.30, speeding_probability=0.25,
            hos_violation_probability=0.08, speed_variance=0.18
        )
    }

    trucks = []
    for _, row in truck_fleet.iterrows():
        truck_id = row['TRUCK_ID']
        profile_name = row.get('DRIVER_PROFILE', 'COMPLIANT')
        profile = profiles_map.get(profile_name, profiles_map['COMPLIANT'])
        idx = int(truck_id.replace('TRK-', ''))

        trucks.append(TruckAssignment(
            truck_id=truck_id,
            driver_id=f"DRV-{idx:05d}",
            profile=profile,
            home_base_id=row['HOME_BASE_ID'],
            home_coords=(row['HOME_LNG'], row['HOME_LAT']),
            truck_type=row.get('TRUCK_TYPE', 'HGV'),
            base_speed_kmh=row.get('BASE_SPEED_KMH', 75)
        ))

    return trucks


def save_to_snowflake(telemetry_df: pd.DataFrame, table: str):
    """Save telemetry to Snowflake."""
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    conn = snowflake.connector.connect(connection_name='airpublic')
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.execute(f"""
        CREATE TABLE {table} (
            TELEMETRY_ID VARCHAR,
            TRUCK_ID VARCHAR,
            DRIVER_ID VARCHAR,
            TRIP_ID VARCHAR,
            TS TIMESTAMP_NTZ,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            SPEED_KMH FLOAT,
            HEADING_DEG FLOAT,
            POSTED_SPEED_KMH FLOAT,
            STATUS VARCHAR,
            IS_SPEEDING BOOLEAN,
            IS_HOS_VIOLATION BOOLEAN,
            IS_DETOUR BOOLEAN,
            GPS_ACCURACY_M FLOAT,
            LOCATION_ID VARCHAR,
            LOCATION_TYPE VARCHAR,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            GEOMETRY GEOGRAPHY
        )
    """)

    df_out = telemetry_df.rename(columns={
        'timestamp': 'TS',
        'telemetry_id': 'TELEMETRY_ID',
        'truck_id': 'TRUCK_ID',
        'driver_id': 'DRIVER_ID',
        'trip_id': 'TRIP_ID',
        'latitude': 'LATITUDE',
        'longitude': 'LONGITUDE',
        'speed_kmh': 'SPEED_KMH',
        'heading_deg': 'HEADING_DEG',
        'posted_speed_kmh': 'POSTED_SPEED_KMH',
        'status': 'STATUS',
        'is_speeding': 'IS_SPEEDING',
        'is_hos_violation': 'IS_HOS_VIOLATION',
        'is_detour': 'IS_DETOUR',
        'gps_accuracy_m': 'GPS_ACCURACY_M',
        'location_id': 'LOCATION_ID',
        'location_type': 'LOCATION_TYPE'
    })

    keep_cols = ['TELEMETRY_ID', 'TRUCK_ID', 'DRIVER_ID', 'TRIP_ID', 'TS',
                 'LATITUDE', 'LONGITUDE', 'SPEED_KMH', 'HEADING_DEG',
                 'POSTED_SPEED_KMH', 'STATUS', 'IS_SPEEDING',
                 'IS_HOS_VIOLATION', 'IS_DETOUR', 'GPS_ACCURACY_M',
                 'LOCATION_ID', 'LOCATION_TYPE']
    df_out = df_out[[c for c in keep_cols if c in df_out.columns]]

    db = table.split('.')[0]
    schema = table.split('.')[1]
    tbl_name = table.split('.')[2]

    logger.info(f"Uploading {len(df_out)} rows...")
    success, nchunks, nrows, _ = write_pandas(
        conn, df_out, tbl_name,
        database=db, schema=schema,
        auto_create_table=False,
        overwrite=False
    )
    logger.info(f"Uploaded {nrows} rows")

    logger.info("Adding GEOMETRY column from lat/lng...")
    cursor.execute(f"""
        UPDATE {table}
        SET GEOMETRY = ST_MAKEPOINT(LONGITUDE, LATITUDE)
        WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    """)
    logger.info(f"Done. Saved {nrows} rows to {table}")

    cursor.close()
    conn.close()


def validate_gps_vs_schedule(telemetry_df: pd.DataFrame, trip_schedule: pd.DataFrame, locations: pd.DataFrame):
    """Validate that GPS start points match scheduled origins."""
    loc_map = {}
    for _, row in locations.iterrows():
        loc_map[row['ID']] = (row['LONGITUDE'], row['LATITUDE'])

    errors = 0
    checked = 0
    for _, sched_row in trip_schedule.head(20).iterrows():
        truck_id = sched_row['TRUCK_ID']
        trip_date = sched_row['TRIP_DATE']
        if hasattr(trip_date, 'strftime'):
            trip_date_str = trip_date.strftime('%Y%m%d')
        else:
            trip_date_str = str(trip_date).replace('-', '')

        trip_id_pattern = f"{trip_date_str}-{truck_id}-00"
        trip_pts = telemetry_df[telemetry_df['trip_id'] == trip_id_pattern]

        if trip_pts.empty:
            continue

        checked += 1
        moving_pts = trip_pts[trip_pts['status'] == 'MOVING']
        if moving_pts.empty:
            continue

        first_pt = moving_pts.sort_values('timestamp').iloc[0]
        gps_lng, gps_lat = first_pt['longitude'], first_pt['latitude']

        origin_coords = loc_map.get(sched_row['ORIGIN_ID'])
        if origin_coords is None:
            continue

        dist_km = haversine_distance(gps_lat, gps_lng, origin_coords[1], origin_coords[0])
        status = "OK" if dist_km < 5 else "MISMATCH"
        if dist_km >= 5:
            errors += 1
        print(f"  {trip_id_pattern}: GPS start ({gps_lat:.2f},{gps_lng:.2f}) vs "
              f"sched origin ({origin_coords[1]:.2f},{origin_coords[0]:.2f}) = {dist_km:.1f}km [{status}]")

    print(f"\nValidation: {checked - errors}/{checked} trips have GPS within 5km of scheduled origin")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--trucks', type=int, default=5)
    parser.add_argument('--days', type=int, default=3)
    parser.add_argument('--save', action='store_true')
    parser.add_argument('--table', default='SYNTHETIC_DATASETS.FLEET_INTELLIGENCE.FACT_TRUCK_TELEMETRY_TEST')
    args = parser.parse_args()

    print("=" * 70)
    print(f"Schedule-Aware Telemetry Generator - Test Run")
    print(f"{args.trucks} trucks x {args.days} days")
    print("=" * 70)

    import snowflake.connector
    conn = snowflake.connector.connect(connection_name='airpublic')

    start_date = "2025-12-01"
    end_date_dt = date(2025, 12, 1) + timedelta(days=args.days - 1)
    end_date = end_date_dt.strftime('%Y-%m-%d')

    truck_fleet, trip_schedule, locations, rest_stops = load_production_data(
        conn, start_date, end_date, num_trucks=args.trucks
    )

    trucks = create_trucks_from_fleet(truck_fleet)
    logger.info(f"Created {len(trucks)} truck assignments from TRUCK_FLEET")

    router = ScheduleAwareRouter(conn)
    logger.info("Initialized schedule-aware router with ROUTE_CACHE")

    config = {
        'seed': 42,
        'fleet': {
            'num_trucks': args.trucks,
            'weekday_operating_rate': 0.85,
            'weekend_operating_rate': 0.40,
            'trips_per_day': {'min': 1, 'max': 1}
        },
        'distance_distribution': {
            'short_pct': 0.60, 'short_max_km': 100,
            'medium_pct': 0.30, 'medium_max_km': 300,
            'long_pct': 0.10
        },
        'routing': {
            'alternative_route_probability': 0.20,
            'ors': {
                'service': 'OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS',
                'profile': 'driving-hgv',
                'cache_enabled': True
            }
        },
        'telemetry': {
            'ping_interval': {
                'moving': {'target_sec': 30, 'variance_sec': 10},
                'dwell': {'min_sec': 300, 'max_sec': 600}
            },
            'gps_jitter': {
                'typical_m': 10, 'typical_std_m': 5,
                'multipath_probability': 0.02, 'multipath_max_m': 150
            }
        },
        'breaks': {
            'driving_hours_between_breaks': 4.5,
            'mandatory_break_duration_min': 45
        },
        'speeding': {'threshold_factor': 1.08},
        'dwell': {
            'warehouse': {'loading': {'median_min': 30, 'sigma': 0.5, 'max_min': 120}},
            'rest_stop': {'mandatory_break': {'median_min': 45, 'sigma': 0.3, 'max_min': 90}}
        }
    }

    from src.continuous_generator import ContinuousTelemetryGenerator

    behavior = BehaviorSimulator(config, seed=42)

    warehouses = locations.copy()
    warehouses.columns = warehouses.columns.str.lower()
    warehouses = warehouses.rename(columns={'id': 'warehouse_id'})
    destinations = warehouses.copy().rename(columns={'warehouse_id': 'destination_id'})

    generator = ContinuousTelemetryGenerator(
        config=config,
        trucks=trucks,
        router=router,
        behavior=behavior,
        warehouses=warehouses,
        destinations=destinations,
        rest_stops=rest_stops,
        trip_schedule=trip_schedule,
        locations=locations
    )

    start_dt = date(2025, 12, 1)
    end_dt = end_date_dt

    logger.info(f"\nGenerating telemetry from {start_dt} to {end_dt}...")

    all_points = []
    point_count = 0

    for point in generator.generate_continuous(start_dt, end_dt):
        all_points.append({
            'telemetry_id': point.telemetry_id,
            'truck_id': point.truck_id,
            'driver_id': point.driver_id,
            'trip_id': point.trip_id,
            'timestamp': point.timestamp,
            'latitude': point.latitude,
            'longitude': point.longitude,
            'speed_kmh': point.speed_kmh,
            'heading_deg': point.heading_deg,
            'posted_speed_kmh': point.posted_speed_kmh,
            'status': point.status,
            'is_speeding': point.is_speeding,
            'is_hos_violation': point.is_hos_violation,
            'is_detour': point.is_detour,
            'gps_accuracy_m': point.gps_accuracy_m,
            'location_id': point.location_id,
            'location_type': point.location_type
        })
        point_count += 1
        if point_count % 5000 == 0:
            logger.info(f"  Generated {point_count} points...")

    logger.info(f"\nTotal points generated: {len(all_points)}")

    if all_points:
        telemetry_df = pd.DataFrame(all_points)

        print("\n" + "=" * 70)
        print("GENERATION SUMMARY")
        print("=" * 70)
        print(f"\nTotal telemetry points: {len(telemetry_df):,}")
        print(f"Unique trucks: {telemetry_df['truck_id'].nunique()}")
        print(f"Unique trips: {telemetry_df['trip_id'].nunique()}")

        print(f"\nPoints by status:")
        for status, count in telemetry_df['status'].value_counts().items():
            print(f"  {status}: {count:,}")

        print(f"\nRouter stats: {router.cache_hits} cache hits, {router.cache_misses} cache misses")

        print("\n" + "=" * 70)
        print("VALIDATION: GPS start vs scheduled origin")
        print("=" * 70)
        validate_gps_vs_schedule(telemetry_df, trip_schedule, locations)

        if args.save:
            print("\n" + "=" * 70)
            print(f"Saving to {args.table}...")
            save_to_snowflake(telemetry_df, args.table)
            print("=" * 70)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
