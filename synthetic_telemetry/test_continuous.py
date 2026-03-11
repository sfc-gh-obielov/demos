#!/usr/bin/env python3
"""
Test script for continuous telemetry generator.

Generates telemetry for 10 trucks over 1 month with:
- 30-second average ping intervals while driving
- Dwell periods at warehouses and rest stops
- Continuous truck positions (only trip_id changes)
- Overnight stays based on distance from home
"""
import os
import sys
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

class MockRouter:
    """Mock router that uses cached routes from Snowflake."""
    
    def __init__(self, route_cache: pd.DataFrame):
        self.route_cache = route_cache
        self.cache_hits = 0
        self.cache_misses = 0
        self._cache_dict = {}
        
        for _, row in route_cache.iterrows():
            key = (row['ORIGIN_ID'], row['DEST_ID'])
            coords = self._parse_coordinates(row['ROUTE_COORDINATES'])
            self._cache_dict[key] = RouteResult(
                origin_id=row['ORIGIN_ID'],
                dest_id=row['DEST_ID'],
                origin_coords=(row['ORIGIN_LNG'], row['ORIGIN_LAT']),
                dest_coords=(row['DEST_LNG'], row['DEST_LAT']),
                distance_km=row['ROAD_DISTANCE_KM'],
                duration_min=row['ROAD_DURATION_MIN'],
                coordinates=coords,
                num_points=len(coords)
            )
    
    def _parse_coordinates(self, coords_data) -> List[tuple]:
        """Parse coordinates from various formats."""
        if coords_data is None:
            return []
        
        if isinstance(coords_data, list):
            result = []
            for item in coords_data:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    result.append((float(item[0]), float(item[1])))
                elif isinstance(item, str) and ',' in item:
                    parts = item.split(',')
                    if len(parts) >= 2:
                        result.append((float(parts[0]), float(parts[1])))
            return result
        
        if isinstance(coords_data, str):
            result = []
            for pair in coords_data.split(','):
                parts = pair.strip().split()
                if len(parts) >= 2:
                    try:
                        result.append((float(parts[0]), float(parts[1])))
                    except ValueError:
                        continue
            return result
        
        return []
    
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
        """Get route from cache or create synthetic one."""
        key = (origin_id, dest_id)
        
        if key in self._cache_dict:
            self.cache_hits += 1
            return self._cache_dict[key]
        
        self.cache_misses += 1
        
        distance_km = haversine_distance(origin_lat, origin_lng, dest_lat, dest_lng)
        avg_speed_kmh = 60
        duration_min = (distance_km / avg_speed_kmh) * 60
        
        num_points = max(10, int(distance_km / 2))
        coords = []
        for i in range(num_points):
            t = i / (num_points - 1)
            lng = origin_lng + t * (dest_lng - origin_lng)
            lat = origin_lat + t * (dest_lat - origin_lat)
            coords.append((lng, lat))
        
        route = RouteResult(
            origin_id=origin_id,
            dest_id=dest_id,
            origin_coords=(origin_lng, origin_lat),
            dest_coords=(dest_lng, dest_lat),
            distance_km=distance_km,
            duration_min=duration_min,
            coordinates=coords,
            num_points=len(coords)
        )
        
        self._cache_dict[key] = route
        return route


def load_test_data():
    """Load test data from Snowflake."""
    import snowflake.connector
    
    conn = snowflake.connector.connect(connection_name='airpublic')
    
    logger.info("Loading warehouses...")
    warehouses_sql = """
    SELECT 
        ID AS warehouse_id,
        NAME,
        LAT AS latitude,
        LNG AS longitude,
        'WAREHOUSE' AS location_type
    FROM FLEET_DEMOS.ROUTING.GERMANY_DESTINATIONS
    WHERE LOCATION_TYPE = 'WAREHOUSE'
    ORDER BY RANDOM()
    LIMIT 50
    """
    warehouses = pd.read_sql(warehouses_sql, conn)
    logger.info(f"  Loaded {len(warehouses)} warehouses")
    
    logger.info("Loading destinations...")
    destinations_sql = """
    SELECT 
        ID AS destination_id,
        NAME,
        LAT AS latitude,
        LNG AS longitude,
        LOCATION_TYPE AS location_type
    FROM FLEET_DEMOS.ROUTING.GERMANY_DESTINATIONS
    ORDER BY RANDOM()
    LIMIT 200
    """
    destinations = pd.read_sql(destinations_sql, conn)
    logger.info(f"  Loaded {len(destinations)} destinations")
    
    logger.info("Loading rest stops...")
    rest_stops_sql = """
    SELECT 
        REST_STOP_ID AS rest_stop_id,
        NAME,
        LAT AS latitude,
        LNG AS longitude,
        REST_TYPE AS rest_type
    FROM FLEET_DEMOS.ROUTING.GERMANY_REST_STOPS
    LIMIT 500
    """
    rest_stops = pd.read_sql(rest_stops_sql, conn)
    logger.info(f"  Loaded {len(rest_stops)} rest stops")
    
    logger.info("Loading route cache...")
    route_cache_sql = """
    SELECT *
    FROM FLEET_DEMOS.ROUTING.ORS_ROUTE_CACHE
    WHERE ROAD_DISTANCE_KM BETWEEN 20 AND 400
    LIMIT 5000
    """
    route_cache = pd.read_sql(route_cache_sql, conn)
    logger.info(f"  Loaded {len(route_cache)} cached routes")
    
    conn.close()
    return warehouses, destinations, rest_stops, route_cache


def create_test_trucks(warehouses: pd.DataFrame, num_trucks: int = 10) -> List[TruckAssignment]:
    """Create test truck assignments."""
    profiles = {
        'COMPLIANT': DriverProfile(
            profile_type=ProfileType.COMPLIANT,
            detour_probability=0.05,
            speeding_probability=0.02,
            hos_violation_probability=0.005,
            speed_variance=0.05
        ),
        'MILD': DriverProfile(
            profile_type=ProfileType.MILD,
            detour_probability=0.15,
            speeding_probability=0.12,
            hos_violation_probability=0.03,
            speed_variance=0.10
        ),
        'OUTLIER': DriverProfile(
            profile_type=ProfileType.OUTLIER,
            detour_probability=0.30,
            speeding_probability=0.25,
            hos_violation_probability=0.08,
            speed_variance=0.18
        )
    }
    
    profile_weights = [0.92, 0.06, 0.02]
    profile_names = ['COMPLIANT', 'MILD', 'OUTLIER']
    
    trucks = []
    rng = np.random.default_rng(42)
    
    for i in range(num_trucks):
        home_idx = i % len(warehouses)
        home = warehouses.iloc[home_idx]
        
        profile_name = rng.choice(profile_names, p=profile_weights)
        
        trucks.append(TruckAssignment(
            truck_id=f"TRK-{i+1:04d}",
            driver_id=f"DRV-{i+1:04d}",
            profile=profiles[profile_name],
            home_base_id=home['WAREHOUSE_ID'],
            home_coords=(home['LONGITUDE'], home['LATITUDE']),
            truck_type="HGV",
            base_speed_kmh=70
        ))
    
    return trucks


def save_to_snowflake(telemetry_df: pd.DataFrame):
    """Save telemetry data to Snowflake."""
    import snowflake.connector
    
    conn = snowflake.connector.connect(connection_name='airpublic')
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE IF EXISTS FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST")
    
    logger.info(f"Inserting {len(telemetry_df)} telemetry records...")
    
    batch_size = 1000
    total = len(telemetry_df)
    
    for start_idx in range(0, total, batch_size):
        end_idx = min(start_idx + batch_size, total)
        batch = telemetry_df.iloc[start_idx:end_idx]
        
        values_list = []
        for _, row in batch.iterrows():
            ts_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['timestamp']) else 'NULL'
            values = f"""(
                '{row['telemetry_id']}',
                '{row['truck_id']}',
                '{row['driver_id']}',
                '{row['trip_id']}',
                '{ts_str}',
                {row['latitude']},
                {row['longitude']},
                {row['speed_kmh']},
                {row['heading_deg']},
                {row['posted_speed_kmh']},
                '{row['status']}',
                {str(row['is_speeding']).upper()},
                {str(row['is_hos_violation']).upper()},
                {str(row['is_detour']).upper()},
                {row['gps_accuracy_m']},
                NULL,
                NULL
            )"""
            values_list.append(values)
        
        sql = f"""
        INSERT INTO FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST
        (TELEMETRY_ID, TRUCK_ID, DRIVER_ID, TRIP_ID, TS, LATITUDE, LONGITUDE,
         SPEED_KMH, HEADING_DEG, POSTED_SPEED_KMH, STATUS, IS_SPEEDING,
         IS_HOS_VIOLATION, IS_DETOUR, GPS_ACCURACY_M, LOCATION_ID, LOCATION_TYPE)
        VALUES {','.join(values_list)}
        """
        cursor.execute(sql)
        
        logger.info(f"  Inserted {end_idx}/{total} records...")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Successfully saved {total} records to Snowflake")


def main():
    print("=" * 70)
    print("Continuous Telemetry Generator - Test Run")
    print("10 trucks x 7 days = ~25,000 telemetry points expected")
    print("=" * 70)
    
    warehouses, destinations, rest_stops, route_cache = load_test_data()
    
    trucks = create_test_trucks(warehouses, num_trucks=10)
    logger.info(f"Created {len(trucks)} truck assignments")
    
    config = {
        'seed': 42,
        'fleet': {
            'num_trucks': 10,
            'weekday_operating_rate': 0.85,
            'weekend_operating_rate': 0.40,
            'trips_per_day': {'min': 1, 'max': 2}
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
                'typical_m': 10,
                'typical_std_m': 5,
                'multipath_probability': 0.02,
                'multipath_max_m': 150
            }
        },
        'breaks': {
            'driving_hours_between_breaks': 4.5,
            'mandatory_break_duration_min': 45
        },
        'speeding': {
            'threshold_factor': 1.08
        },
        'dwell': {
            'warehouse': {
                'loading': {'median_min': 30, 'sigma': 0.5, 'max_min': 120}
            },
            'rest_stop': {
                'mandatory_break': {'median_min': 45, 'sigma': 0.3, 'max_min': 90}
            }
        }
    }
    
    from src.continuous_generator import ContinuousTelemetryGenerator
    from src.driver_profiles import BehaviorSimulator
    
    behavior = BehaviorSimulator(config, seed=42)
    
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name='airpublic')
    router = ORSRouter(config, connection=conn)
    logger.info("Initialized ORS router with real routing service")
    
    generator = ContinuousTelemetryGenerator(
        config=config,
        trucks=trucks,
        router=router,
        behavior=behavior,
        warehouses=warehouses,
        destinations=destinations,
        rest_stops=rest_stops
    )
    
    start_date = date(2025, 12, 1)
    end_date = date(2025, 12, 3)  # 3 days for quick test
    
    logger.info(f"\nGenerating telemetry from {start_date} to {end_date}...")
    
    all_points = []
    point_count = 0
    
    for point in generator.generate_continuous(start_date, end_date):
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
        
        print(f"\nPoints per truck:")
        for truck_id, count in telemetry_df.groupby('truck_id').size().items():
            print(f"  {truck_id}: {count:,}")
        
        moving_df = telemetry_df[telemetry_df['status'] == 'MOVING']
        if len(moving_df) > 1:
            moving_df = moving_df.sort_values(['truck_id', 'timestamp'])
            intervals = moving_df.groupby('truck_id')['timestamp'].diff().dt.total_seconds()
            valid_intervals = intervals[(intervals > 0) & (intervals < 300)]
            if len(valid_intervals) > 0:
                print(f"\nMoving ping intervals:")
                print(f"  Mean: {valid_intervals.mean():.1f} sec")
                print(f"  Median: {valid_intervals.median():.1f} sec")
                print(f"  Min: {valid_intervals.min():.1f} sec")
                print(f"  Max: {valid_intervals.max():.1f} sec")
        
        speeding_count = telemetry_df['is_speeding'].sum()
        hos_count = telemetry_df['is_hos_violation'].sum()
        print(f"\nViolations:")
        print(f"  Speeding points: {speeding_count:,}")
        print(f"  HOS violation points: {hos_count:,}")
        
        print(f"\nRouter cache stats:")
        if hasattr(router, 'cache') and router.cache:
            stats = router.cache.get_stats()
            print(f"  Cached routes: {stats.get('cached_routes', 0)}")
        else:
            print("  Cache disabled")
        
        print("\n" + "=" * 70)
        print("Saving to Snowflake...")
        save_to_snowflake(telemetry_df)
        print("=" * 70)
    
    print("\nDone!")


if __name__ == "__main__":
    main()
