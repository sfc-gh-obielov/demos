#!/usr/bin/env python3
"""
Standalone test script to generate synthetic telemetry and load to Snowflake.
"""
import os
import sys
import uuid
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Tuple, Optional
import snowflake.connector

os.environ['SNOWFLAKE_CONNECTION_NAME'] = 'default'

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

DATABASE = "FLEET_DEMOS"
SCHEMA = "ROUTING"

@dataclass
class Trip:
    trip_id: str
    truck_id: str
    driver_id: str
    origin_id: str
    origin_name: str
    dest_id: str
    dest_name: str
    planned_start: datetime
    actual_start: datetime
    actual_end: datetime
    distance_km: float
    duration_min: float
    route_coords: List[Tuple[float, float]]
    status: str = "COMPLETED"

def get_connection():
    conn_name = os.environ.get('SNOWFLAKE_CONNECTION_NAME', 'default')
    return snowflake.connector.connect(connection_name=conn_name)

def haversine_distance(lat1, lng1, lat2, lng2):
    lat1_rad, lat2_rad = np.radians(lat1), np.radians(lat2)
    dlat = np.radians(lat2 - lat1)
    dlng = np.radians(lng2 - lng1)
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlng/2)**2
    return 6371 * 2 * np.arcsin(np.sqrt(a))

def coords_to_wkt_linestring(coords):
    if not coords or len(coords) < 2:
        return None
    points = ", ".join([f"{lng} {lat}" for lng, lat in coords])
    return f"LINESTRING({points})"

def fetch_origins_destinations(conn, limit=100):
    print("Fetching warehouses and destinations from Snowflake...")
    
    warehouses_sql = f"""
    SELECT
        ID::VARCHAR AS LOCATION_ID,
        NAMES::VARIANT:primary::VARCHAR AS NAME,
        GEOMETRY::GEOGRAPHY:coordinates[1]::FLOAT AS LAT,
        GEOMETRY::GEOGRAPHY:coordinates[0]::FLOAT AS LNG
    FROM {DATABASE}.{SCHEMA}.GERMANY_DESTINATIONS
    WHERE CATEGORIES::VARCHAR ILIKE '%warehouse%'
    LIMIT 50
    """
    
    destinations_sql = f"""
    SELECT
        ID::VARCHAR AS LOCATION_ID,
        NAMES::VARIANT:primary::VARCHAR AS NAME,
        GEOMETRY::GEOGRAPHY:coordinates[1]::FLOAT AS LAT,
        GEOMETRY::GEOGRAPHY:coordinates[0]::FLOAT AS LNG
    FROM {DATABASE}.{SCHEMA}.GERMANY_DESTINATIONS
    WHERE CATEGORIES::VARCHAR ILIKE '%retail%'
       OR CATEGORIES::VARCHAR ILIKE '%logistics%'
    LIMIT {limit}
    """
    
    cursor = conn.cursor()
    cursor.execute(warehouses_sql)
    warehouses = cursor.fetchall()
    
    cursor.execute(destinations_sql)
    destinations = cursor.fetchall()
    
    print(f"  Loaded {len(warehouses)} warehouses, {len(destinations)} destinations")
    return warehouses, destinations

def fetch_rest_stops(conn, limit=200):
    print("Fetching rest stops...")
    sql = f"""
    SELECT
        ID::VARCHAR AS STOP_ID,
        NAME::VARCHAR AS NAME,
        LAT::FLOAT AS LAT,
        LNG::FLOAT AS LNG
    FROM {DATABASE}.{SCHEMA}.GERMANY_REST_STOPS
    LIMIT {limit}
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    stops = cursor.fetchall()
    print(f"  Loaded {len(stops)} rest stops")
    return stops

def get_ors_route(conn, origin, destination):
    origin_lat, origin_lng = origin[2], origin[3]
    dest_lat, dest_lng = destination[2], destination[3]
    
    sql = f"""
    SELECT
        OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS(
            'driving-hgv',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('lat', {origin_lat}, 'lon', {origin_lng}),
                OBJECT_CONSTRUCT('lat', {dest_lat}, 'lon', {dest_lng})
            )
        ) AS ROUTE
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    
    if row and row[0]:
        import json
        route_data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        if 'routes' in route_data and len(route_data['routes']) > 0:
            route = route_data['routes'][0]
            geometry = route.get('geometry', '')
            distance_m = route['summary'].get('distance', 0)
            duration_s = route['summary'].get('duration', 0)
            
            coords = decode_polyline(geometry)
            return {
                'distance_km': distance_m / 1000,
                'duration_min': duration_s / 60,
                'coordinates': coords
            }
    return None

def decode_polyline(encoded):
    coords = []
    i = 0
    lat = 0
    lng = 0
    while i < len(encoded):
        shift = 0
        result = 0
        while True:
            b = ord(encoded[i]) - 63
            i += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if result & 1 else result >> 1
        lat += dlat
        
        shift = 0
        result = 0
        while True:
            b = ord(encoded[i]) - 63
            i += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if result & 1 else result >> 1
        lng += dlng
        
        coords.append((lng / 1e5, lat / 1e5))
    return coords

def generate_telemetry_for_trip(trip: Trip, driver_profile: str = "COMPLIANT"):
    telemetry_records = []
    violations = []
    
    if not trip.route_coords or len(trip.route_coords) < 2:
        return telemetry_records, violations
    
    profile_params = {
        'COMPLIANT': {'speed_var': 0.05, 'speeding_prob': 0.02},
        'MILD': {'speed_var': 0.10, 'speeding_prob': 0.12},
        'OUTLIER': {'speed_var': 0.18, 'speeding_prob': 0.25}
    }
    params = profile_params.get(driver_profile, profile_params['COMPLIANT'])
    
    total_points = len(trip.route_coords)
    current_time = trip.actual_start
    
    for i, (lng, lat) in enumerate(trip.route_coords):
        route_progress = i / max(1, total_points - 1)
        
        if route_progress < 0.1 or route_progress > 0.9:
            posted_speed = 60.0
        elif trip.distance_km > 100:
            posted_speed = 80.0
        else:
            posted_speed = 70.0
        
        base_speed = posted_speed * (0.85 + random.random() * 0.15)
        speed_variation = np.random.normal(0, params['speed_var'])
        actual_speed = base_speed * (1 + speed_variation)
        
        is_speeding = random.random() < params['speeding_prob']
        if is_speeding:
            actual_speed = posted_speed * (1.08 + random.random() * 0.15)
        
        actual_speed = max(0, min(actual_speed, 130))
        
        if i < total_points - 1:
            next_lng, next_lat = trip.route_coords[i + 1]
            heading = np.degrees(np.arctan2(next_lng - lng, next_lat - lat)) % 360
        else:
            heading = 0
        
        gps_accuracy = np.random.normal(10, 5)
        if random.random() < 0.02:
            gps_accuracy = random.uniform(50, 150)
        
        record = {
            'TELEMETRY_ID': str(uuid.uuid4()),
            'TRUCK_ID': trip.truck_id,
            'DRIVER_ID': trip.driver_id,
            'TRIP_ID': trip.trip_id,
            'TS': current_time,
            'LATITUDE': lat + np.random.normal(0, 0.00005),
            'LONGITUDE': lng + np.random.normal(0, 0.00005),
            'SPEED_KMH': round(actual_speed, 1),
            'HEADING_DEG': round(heading, 1),
            'POSTED_SPEED_KMH': posted_speed,
            'STATUS': 'MOVING' if actual_speed > 5 else 'STOPPED',
            'IS_SPEEDING': actual_speed > posted_speed * 1.08,
            'IS_HOS_VIOLATION': False,
            'IS_DETOUR': False,
            'GPS_ACCURACY_M': round(gps_accuracy, 1),
            'LOCATION_ID': None,
            'LOCATION_TYPE': None
        }
        telemetry_records.append(record)
        
        if record['IS_SPEEDING']:
            violations.append({
                'VIOLATION_ID': str(uuid.uuid4()),
                'TRIP_ID': trip.trip_id,
                'TRUCK_ID': trip.truck_id,
                'DRIVER_ID': trip.driver_id,
                'TS': current_time,
                'LATITUDE': lat,
                'LONGITUDE': lng,
                'VIOLATION_TYPE': 'SPEEDING',
                'ACTUAL_VALUE': actual_speed,
                'THRESHOLD_VALUE': posted_speed * 1.08,
                'SEVERITY': 'SEVERE' if actual_speed > posted_speed * 1.20 else 'MINOR'
            })
        
        ping_interval = random.randint(20, 90)
        current_time += timedelta(seconds=ping_interval)
    
    return telemetry_records, violations

def select_destination_by_distance(origin, destinations):
    origin_lat, origin_lng = origin[2], origin[3]
    
    short_dist, med_dist, long_dist = [], [], []
    for dest in destinations:
        dist = haversine_distance(origin_lat, origin_lng, dest[2], dest[3])
        if dist < 100:
            short_dist.append((dest, dist))
        elif dist < 300:
            med_dist.append((dest, dist))
        else:
            long_dist.append((dest, dist))
    
    r = random.random()
    if r < 0.60 and short_dist:
        return random.choice(short_dist)[0]
    elif r < 0.90 and med_dist:
        return random.choice(med_dist)[0]
    elif long_dist:
        return random.choice(long_dist)[0]
    elif med_dist:
        return random.choice(med_dist)[0]
    elif short_dist:
        return random.choice(short_dist)[0]
    return random.choice(destinations)

def load_telemetry_to_snowflake(conn, telemetry_df):
    print(f"Loading {len(telemetry_df)} telemetry records to Snowflake...")
    
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {DATABASE}.{SCHEMA}.FACT_TRUCK_TELEMETRY_TEST")
    
    for _, row in telemetry_df.iterrows():
        sql = f"""
        INSERT INTO {DATABASE}.{SCHEMA}.FACT_TRUCK_TELEMETRY_TEST
        (TELEMETRY_ID, TRUCK_ID, DRIVER_ID, TRIP_ID, TS, LATITUDE, LONGITUDE,
         SPEED_KMH, HEADING_DEG, POSTED_SPEED_KMH, STATUS, IS_SPEEDING,
         IS_HOS_VIOLATION, IS_DETOUR, GPS_ACCURACY_M, LOCATION_ID, LOCATION_TYPE)
        VALUES (
            '{row['TELEMETRY_ID']}',
            '{row['TRUCK_ID']}',
            '{row['DRIVER_ID']}',
            '{row['TRIP_ID']}',
            '{row['TS'].strftime('%Y-%m-%d %H:%M:%S')}',
            {row['LATITUDE']},
            {row['LONGITUDE']},
            {row['SPEED_KMH']},
            {row['HEADING_DEG']},
            {row['POSTED_SPEED_KMH']},
            '{row['STATUS']}',
            {str(row['IS_SPEEDING']).upper()},
            {str(row['IS_HOS_VIOLATION']).upper()},
            {str(row['IS_DETOUR']).upper()},
            {row['GPS_ACCURACY_M']},
            NULL,
            NULL
        )
        """
        cursor.execute(sql)
    
    conn.commit()
    print(f"  Loaded {len(telemetry_df)} telemetry records")

def load_trips_to_snowflake(conn, trips: List[Trip]):
    print(f"Loading {len(trips)} trips to Snowflake...")
    
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {DATABASE}.{SCHEMA}.FACT_TRIP_TEST")
    
    for trip in trips:
        wkt = coords_to_wkt_linestring(trip.route_coords)
        geog_expr = f"TRY_TO_GEOGRAPHY('{wkt}')" if wkt else "NULL"
        
        sql = f"""
        INSERT INTO {DATABASE}.{SCHEMA}.FACT_TRIP_TEST
        (TRIP_ID, TRUCK_ID, DRIVER_ID, ORIGIN_ID, ORIGIN_NAME, DESTINATION_ID,
         DESTINATION_NAME, PLANNED_START, ACTUAL_START, ACTUAL_END,
         DISTANCE_KM, DURATION_MIN, ROUTE_GEOG, STATUS)
        VALUES (
            '{trip.trip_id}',
            '{trip.truck_id}',
            '{trip.driver_id}',
            '{trip.origin_id}',
            '{trip.origin_name.replace("'", "''")}',
            '{trip.dest_id}',
            '{trip.dest_name.replace("'", "''")}',
            '{trip.planned_start.strftime('%Y-%m-%d %H:%M:%S')}',
            '{trip.actual_start.strftime('%Y-%m-%d %H:%M:%S')}',
            '{trip.actual_end.strftime('%Y-%m-%d %H:%M:%S')}',
            {trip.distance_km},
            {trip.duration_min},
            {geog_expr},
            '{trip.status}'
        )
        """
        cursor.execute(sql)
    
    conn.commit()
    print(f"  Loaded {len(trips)} trips")

def load_violations_to_snowflake(conn, violations_df):
    if violations_df.empty:
        print("No violations to load")
        return
    
    print(f"Loading {len(violations_df)} violations to Snowflake...")
    
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {DATABASE}.{SCHEMA}.FACT_VIOLATION_TEST")
    
    for _, row in violations_df.iterrows():
        sql = f"""
        INSERT INTO {DATABASE}.{SCHEMA}.FACT_VIOLATION_TEST
        (VIOLATION_ID, TRIP_ID, TRUCK_ID, DRIVER_ID, TS, LATITUDE, LONGITUDE,
         VIOLATION_TYPE, ACTUAL_VALUE, THRESHOLD_VALUE, SEVERITY)
        VALUES (
            '{row['VIOLATION_ID']}',
            '{row['TRIP_ID']}',
            '{row['TRUCK_ID']}',
            '{row['DRIVER_ID']}',
            '{row['TS'].strftime('%Y-%m-%d %H:%M:%S')}',
            {row['LATITUDE']},
            {row['LONGITUDE']},
            '{row['VIOLATION_TYPE']}',
            {row['ACTUAL_VALUE']},
            {row['THRESHOLD_VALUE']},
            '{row['SEVERITY']}'
        )
        """
        cursor.execute(sql)
    
    conn.commit()
    print(f"  Loaded {len(violations_df)} violations")

def main():
    print("=" * 60)
    print("Synthetic Telemetry Generator - Test Run")
    print("=" * 60)
    
    conn = get_connection()
    print("Connected to Snowflake")
    
    warehouses, destinations = fetch_origins_destinations(conn)
    rest_stops = fetch_rest_stops(conn)
    
    if not warehouses or not destinations:
        print("ERROR: No warehouses or destinations found")
        return
    
    NUM_TRIPS = 5
    NUM_TRUCKS = 3
    
    truck_ids = [f"TRK-{str(i).zfill(4)}" for i in range(1, NUM_TRUCKS + 1)]
    driver_profiles = ['COMPLIANT'] * 2 + ['MILD']
    
    all_telemetry = []
    all_violations = []
    all_trips = []
    
    base_date = datetime(2025, 12, 1, 6, 0, 0)
    
    print(f"\nGenerating {NUM_TRIPS} trips...")
    
    for trip_num in range(NUM_TRIPS):
        truck_idx = trip_num % NUM_TRUCKS
        truck_id = truck_ids[truck_idx]
        driver_id = f"DRV-{truck_id[-4:]}"
        profile = driver_profiles[truck_idx]
        
        origin = random.choice(warehouses)
        destination = select_destination_by_distance(origin, destinations)
        
        print(f"\n  Trip {trip_num + 1}: {origin[1][:30]}... -> {destination[1][:30]}...")
        print(f"    Truck: {truck_id}, Driver Profile: {profile}")
        
        route = get_ors_route(conn, origin, destination)
        
        if route:
            print(f"    Route: {route['distance_km']:.1f} km, {route['duration_min']:.0f} min")
            
            planned_start = base_date + timedelta(hours=trip_num * 3)
            actual_start = planned_start + timedelta(minutes=random.randint(-10, 15))
            actual_end = actual_start + timedelta(minutes=route['duration_min'])
            
            trip = Trip(
                trip_id=f"TRIP-{trip_num + 1:06d}",
                truck_id=truck_id,
                driver_id=driver_id,
                origin_id=origin[0],
                origin_name=origin[1] or "Unknown",
                dest_id=destination[0],
                dest_name=destination[1] or "Unknown",
                planned_start=planned_start,
                actual_start=actual_start,
                actual_end=actual_end,
                distance_km=route['distance_km'],
                duration_min=route['duration_min'],
                route_coords=route['coordinates']
            )
            all_trips.append(trip)
            
            telemetry, violations = generate_telemetry_for_trip(trip, profile)
            all_telemetry.extend(telemetry)
            all_violations.extend(violations)
            
            print(f"    Generated {len(telemetry)} telemetry points, {len(violations)} violations")
        else:
            print(f"    WARN: Could not get route, skipping")
    
    print(f"\n{'=' * 60}")
    print(f"SUMMARY:")
    print(f"  Total Trips: {len(all_trips)}")
    print(f"  Total Telemetry Records: {len(all_telemetry)}")
    print(f"  Total Violations: {len(all_violations)}")
    print(f"{'=' * 60}")
    
    if all_telemetry:
        telemetry_df = pd.DataFrame(all_telemetry)
        load_telemetry_to_snowflake(conn, telemetry_df)
    
    if all_trips:
        load_trips_to_snowflake(conn, all_trips)
    
    if all_violations:
        violations_df = pd.DataFrame(all_violations)
        load_violations_to_snowflake(conn, violations_df)
    
    print("\nVerifying loaded data...")
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.FACT_TRUCK_TELEMETRY_TEST")
    telem_count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.FACT_TRIP_TEST")
    trip_count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.FACT_VIOLATION_TEST")
    viol_count = cursor.fetchone()[0]
    
    print(f"  FACT_TRUCK_TELEMETRY_TEST: {telem_count} rows")
    print(f"  FACT_TRIP_TEST: {trip_count} rows")
    print(f"  FACT_VIOLATION_TEST: {viol_count} rows")
    
    print("\nDone!")
    conn.close()

if __name__ == "__main__":
    main()
