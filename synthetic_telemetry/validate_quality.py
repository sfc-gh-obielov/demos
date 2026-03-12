#!/usr/bin/env python3
import os, sys
os.environ['SNOWFLAKE_CONNECTION_NAME'] = 'airpublic'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import date
from src.simulate import haversine_distance

from test_continuous import (
    load_production_data, create_trucks_from_fleet, ScheduleAwareRouter
)
from src.continuous_generator import ContinuousTelemetryGenerator
from src.driver_profiles import BehaviorSimulator

import snowflake.connector
conn = snowflake.connector.connect(connection_name='airpublic')

truck_fleet, trip_schedule, locations, rest_stops = load_production_data(
    conn, '2025-12-01', '2025-12-01', num_trucks=5
)
trucks = create_trucks_from_fleet(truck_fleet)
router = ScheduleAwareRouter(conn)

config = {
    'seed': 42,
    'fleet': {'num_trucks': 5, 'weekday_operating_rate': 0.85,
              'weekend_operating_rate': 0.40, 'trips_per_day': {'min': 1, 'max': 1}},
    'distance_distribution': {'short_pct': 0.60, 'short_max_km': 100,
                              'medium_pct': 0.30, 'medium_max_km': 300, 'long_pct': 0.10},
    'routing': {'alternative_route_probability': 0.20,
                'ors': {'service': 'OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS',
                        'profile': 'driving-hgv', 'cache_enabled': True}},
    'telemetry': {'ping_interval': {'moving': {'target_sec': 30, 'variance_sec': 10},
                                     'dwell': {'min_sec': 300, 'max_sec': 600}},
                  'gps_jitter': {'typical_m': 10, 'typical_std_m': 5,
                                 'multipath_probability': 0.02, 'multipath_max_m': 150}},
    'breaks': {'driving_hours_between_breaks': 4.5, 'mandatory_break_duration_min': 45},
    'speeding': {'threshold_factor': 1.08},
    'dwell': {'warehouse': {'loading': {'median_min': 30, 'sigma': 0.5, 'max_min': 120}},
              'rest_stop': {'mandatory_break': {'median_min': 45, 'sigma': 0.3, 'max_min': 90}}}
}

behavior = BehaviorSimulator(config, seed=42)
warehouses = locations.copy()
warehouses.columns = warehouses.columns.str.lower()
warehouses = warehouses.rename(columns={'id': 'warehouse_id'})
destinations = warehouses.copy().rename(columns={'warehouse_id': 'destination_id'})

generator = ContinuousTelemetryGenerator(
    config=config, trucks=trucks, router=router, behavior=behavior,
    warehouses=warehouses, destinations=destinations,
    rest_stops=rest_stops, trip_schedule=trip_schedule, locations=locations
)

points = []
for p in generator.generate_continuous(date(2025, 12, 1), date(2025, 12, 1)):
    points.append({
        'truck_id': p.truck_id, 'ts': p.timestamp,
        'lat': p.latitude, 'lng': p.longitude,
        'status': p.status, 'trip_id': p.trip_id,
        'speed': p.speed_kmh, 'is_speeding': p.is_speeding,
        'posted_speed': p.posted_speed_kmh
    })

df = pd.DataFrame(points)
print(f"\nTotal points: {len(df)}")
print(f"Points by status:\n{df['status'].value_counts().to_string()}")

print("\n=== TIMESTAMP GAP CHECK ===")
for tid in sorted(df['truck_id'].unique()):
    tdf = df[df['truck_id'] == tid].sort_values('ts')
    gaps = tdf['ts'].diff().dt.total_seconds().dropna()
    max_gap = gaps.max()
    mean_gap = gaps.mean()
    over_60 = (gaps > 65).sum()
    print(f"{tid}: max={max_gap:.0f}s avg={mean_gap:.1f}s over65s={over_60} pts={len(tdf)}")

print("\n=== TELEPORTATION CHECK ===")
for tid in sorted(df['truck_id'].unique()):
    tdf = df[df['truck_id'] == tid].sort_values('ts').reset_index(drop=True)
    max_jump = 0
    teleports = 0
    for i in range(1, len(tdf)):
        d = haversine_distance(tdf.loc[i-1, 'lat'], tdf.loc[i-1, 'lng'],
                               tdf.loc[i, 'lat'], tdf.loc[i, 'lng'])
        dt = (tdf.loc[i, 'ts'] - tdf.loc[i-1, 'ts']).total_seconds()
        if d > max_jump:
            max_jump = d
        if d > 2.0 and dt < 120:
            teleports += 1
    print(f"{tid}: max_jump={max_jump:.2f}km teleports(>2km/<120s)={teleports}")

print("\n=== SPEEDING CHECK ===")
moving = df[df['status'] == 'MOVING']
if len(moving) > 0:
    speeding_pts = moving[moving['is_speeding'] == True]
    print(f"Moving points: {len(moving)}")
    print(f"Speeding points: {len(speeding_pts)} ({100*len(speeding_pts)/len(moving):.1f}%)")
    print(f"Avg speed: {moving['speed'].mean():.1f} km/h")
    print(f"Max speed: {moving['speed'].max():.1f} km/h")

conn.close()
print("\nDone!")
