#!/usr/bin/env python3
"""
Calibrated Continuous Telemetry Generator Test
Generates realistic fleet telemetry matching industry statistics.
"""
import os
import sys
import logging
import yaml
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.simulate import TruckAssignment, TelemetryPoint, haversine_distance
from src.driver_profiles import DriverProfile, ProfileType, BehaviorSimulator
from src.routing import RouteResult, ORSRouter


class TruckType(Enum):
    REGIONAL = "REGIONAL"
    LONG_HAUL = "LONG_HAUL"
    LOW_UTIL = "LOW_UTIL"


class TruckState(Enum):
    MOVING = "MOVING"
    IDLE = "IDLE"
    DWELL_WAREHOUSE = "DWELL_WAREHOUSE"
    DWELL_DESTINATION = "DWELL_DESTINATION"
    DWELL_REST_STOP = "DWELL_REST_STOP"
    OVERNIGHT_HOME = "OVERNIGHT_HOME"
    OVERNIGHT_REST_STOP = "OVERNIGHT_REST_STOP"


@dataclass
class CalibratedTruck:
    truck_id: str
    driver_id: str
    truck_type: TruckType
    profile: DriverProfile
    home_base_id: str
    home_coords: Tuple[float, float]
    
    current_lat: float = 0.0
    current_lng: float = 0.0
    current_time: datetime = None
    current_state: TruckState = TruckState.DWELL_WAREHOUSE
    current_trip_id: Optional[str] = None
    
    daily_driving_min: float = 0.0
    minutes_since_break: float = 0.0
    trip_count: int = 0
    total_points: int = 0
    
    def __post_init__(self):
        self.current_lat = self.home_coords[1]
        self.current_lng = self.home_coords[0]


class CalibratedTelemetryGenerator:
    """
    Generates realistic fleet telemetry with calibrated statistics.
    
    Target stats:
    - Moving: 78-86%
    - Warehouse dwell: 5-9%
    - Overnight: 6-12%
    - Idle: 2-6%
    - Avg speed: 58-72 km/h
    - Speeding: 3-8%
    - Days >9h: 2-5%
    """
    
    def __init__(
        self,
        config: dict,
        trucks: List[CalibratedTruck],
        router: ORSRouter,
        warehouses: pd.DataFrame,
        destinations: pd.DataFrame,
        rest_stops: pd.DataFrame
    ):
        self.config = config
        self.trucks = {t.truck_id: t for t in trucks}
        self.router = router
        self.warehouses = warehouses.copy()
        self.warehouses.columns = self.warehouses.columns.str.lower()
        self.destinations = destinations.copy()
        self.destinations.columns = self.destinations.columns.str.lower()
        self.rest_stops = rest_stops.copy()
        self.rest_stops.columns = self.rest_stops.columns.str.lower()
        
        self.rng = np.random.default_rng(config.get('seed', 42))
        
        # Speed configuration
        speed_cfg = config.get('speed', {})
        self.highway_speed = (speed_cfg.get('highway', {}).get('mean', 75), 
                              speed_cfg.get('highway', {}).get('std', 8))
        self.urban_speed = (speed_cfg.get('urban', {}).get('mean', 35),
                           speed_cfg.get('urban', {}).get('std', 10))
        self.regional_speed = (speed_cfg.get('regional', {}).get('mean', 58),
                               speed_cfg.get('regional', {}).get('std', 12))
        
        # HOS config
        hos_cfg = config.get('hos', {})
        self.max_daily_hours = hos_cfg.get('max_daily_driving_hours', 9.0)
        self.violation_day_weights = hos_cfg.get('weekly_violation_weights', {
            'monday': 0.8, 'tuesday': 0.9, 'wednesday': 1.0,
            'thursday': 1.4, 'friday': 1.5, 'saturday': 0.6, 'sunday': 0.4
        })
        
        # Telemetry intervals
        tel_cfg = config.get('telemetry', {}).get('ping_interval', {})
        self.moving_interval = (tel_cfg.get('moving', {}).get('mean_sec', 30),
                                tel_cfg.get('moving', {}).get('std_sec', 8))
        self.dwell_interval = (tel_cfg.get('dwell', {}).get('mean_sec', 420),
                               tel_cfg.get('dwell', {}).get('std_sec', 120))
        
        # Dwell config
        dwell_cfg = config.get('dwell', {})
        self.warehouse_dwell = dwell_cfg.get('warehouse', {})
        self.rest_dwell = dwell_cfg.get('rest_stop', {})
        
        # Cache for routes
        self._route_cache = {}
        
    def _get_ping_interval(self, state: TruckState) -> int:
        """Get realistic ping interval with variance."""
        if state == TruckState.MOVING:
            interval = self.rng.normal(self.moving_interval[0], self.moving_interval[1])
            return max(15, min(60, int(interval)))
        elif state in [TruckState.OVERNIGHT_HOME, TruckState.OVERNIGHT_REST_STOP]:
            # More frequent overnight pings (8-20 min) to increase overnight point %
            return int(self.rng.uniform(480, 1000))
        else:  # Dwell - slightly more frequent
            interval = self.rng.normal(300, 90)  # ~5 min avg
            return max(120, min(600, int(interval)))
    
    def _get_speed(self, truck: CalibratedTruck, posted_speed: float, is_highway: bool) -> Tuple[float, bool]:
        """Generate realistic speed with bimodal distribution and speeding logic."""
        # Base speed depends on truck type and road
        if truck.truck_type == TruckType.LONG_HAUL and is_highway:
            base_mean, base_std = self.highway_speed
        elif truck.truck_type == TruckType.LOW_UTIL:
            base_mean, base_std = self.urban_speed
        else:
            base_mean, base_std = self.regional_speed
        
        # Apply driver profile variance
        profile_var = truck.profile.speed_variance
        speed = self.rng.normal(base_mean, base_std * (1 + profile_var))
        
        # Clamp to realistic range
        speed = max(20, min(95, speed))
        
        # Check speeding against posted limit
        is_speeding = False
        if self.rng.random() < truck.profile.speeding_probability:
            # Driver decides to speed
            speed_factor = 1.0 + self.rng.uniform(0.08, 0.25)
            speed = posted_speed * speed_factor
            is_speeding = True
            
            # Rare extreme speeding (<0.5%)
            if self.rng.random() < 0.005:
                speed = min(130, speed * 1.15)
        else:
            # Normal driving - stay near or under limit
            speed = min(speed, posted_speed * self.rng.uniform(0.92, 1.05))
        
        return round(speed, 1), is_speeding
    
    def _get_dwell_duration(self, dwell_type: str) -> float:
        """Get lognormal dwell duration in minutes."""
        if dwell_type == 'warehouse':
            cfg = self.warehouse_dwell
            median = cfg.get('median_min', 45)
            sigma = cfg.get('sigma', 0.9)
            
            # Lognormal distribution
            duration = self.rng.lognormal(np.log(median), sigma)
            duration = max(cfg.get('min_min', 10), min(cfg.get('max_min', 360), duration))
            
            # Rare long dwell (2.5% chance)
            if self.rng.random() < cfg.get('long_dwell_probability', 0.025):
                duration = self.rng.uniform(
                    cfg.get('long_dwell_min', 480),
                    cfg.get('long_dwell_max', 1440)
                )
        elif dwell_type == 'mandatory_break':
            cfg = self.rest_dwell.get('mandatory', {})
            median = cfg.get('median_min', 48)
            sigma = cfg.get('sigma', 0.25)
            duration = self.rng.lognormal(np.log(median), sigma)
            duration = max(45, min(90, duration))
        elif dwell_type == 'short_break':
            cfg = self.rest_dwell.get('short', {})
            median = cfg.get('median_min', 22)
            sigma = cfg.get('sigma', 0.6)
            duration = self.rng.lognormal(np.log(median), sigma)
            duration = max(5, min(60, duration))
        else:  # overnight
            cfg = self.rest_dwell.get('overnight', {})
            median = cfg.get('median_min', 600)
            sigma = cfg.get('sigma', 0.15)
            duration = self.rng.lognormal(np.log(median), sigma)
            duration = max(540, min(720, duration))
        
        return duration
    
    def _should_operate_today(self, truck: CalibratedTruck, day: date) -> bool:
        """Determine if truck operates based on weekday/weekend rates."""
        weekday = day.weekday()
        if weekday < 5:  # Monday-Friday
            rate = self.config.get('fleet', {}).get('weekday_operating_rate', 0.88)
        else:  # Saturday-Sunday
            rate = self.config.get('fleet', {}).get('weekend_operating_rate', 0.35)
        
        # Low utilization trucks have lower rate
        if truck.truck_type == TruckType.LOW_UTIL:
            rate *= 0.7
        
        return self.rng.random() < rate
    
    def _get_trips_for_day(self, truck: CalibratedTruck) -> int:
        """Get number of trips based on truck type."""
        truck_types = self.config.get('fleet', {}).get('truck_types', {})
        
        if truck.truck_type == TruckType.REGIONAL:
            cfg = truck_types.get('regional', {}).get('trips_per_day', {'min': 2, 'max': 3})
        elif truck.truck_type == TruckType.LONG_HAUL:
            cfg = truck_types.get('long_haul', {}).get('trips_per_day', {'min': 1, 'max': 1})
        else:
            cfg = truck_types.get('low_utilization', {}).get('trips_per_day', {'min': 0, 'max': 2})
        
        return self.rng.integers(cfg['min'], cfg['max'] + 1)
    
    def _check_hos_violation(self, truck: CalibratedTruck, day: date) -> bool:
        """Check if today should have HOS violation (clustered on Thu/Fri)."""
        base_prob = truck.profile.hos_violation_probability
        
        # Apply weekly weighting
        day_name = day.strftime('%A').lower()
        weight = self.violation_day_weights.get(day_name, 1.0)
        
        return self.rng.random() < (base_prob * weight)
    
    def _select_destination(self, truck: CalibratedTruck) -> Tuple[pd.Series, float]:
        """Select destination with right-skewed distance distribution."""
        dist_cfg = self.config.get('distance_distribution', {})
        
        # Calculate distances
        dests = self.destinations.copy()
        dests['dist_km'] = dests.apply(
            lambda r: haversine_distance(
                truck.current_lat, truck.current_lng,
                r['latitude'], r['longitude']
            ), axis=1
        )
        
        # Filter by truck type
        if truck.truck_type == TruckType.LONG_HAUL:
            # Prefer longer trips
            candidates = dests[dests['dist_km'] > 200]
            if candidates.empty:
                candidates = dests[dests['dist_km'] > 100]
        elif truck.truck_type == TruckType.LOW_UTIL:
            # Prefer shorter trips
            candidates = dests[dests['dist_km'] < 80]
            if candidates.empty:
                candidates = dests[dests['dist_km'] < 150]
        else:
            # Regional - mixed with right skew
            roll = self.rng.random()
            short_pct = dist_cfg.get('short_pct', 0.55)
            medium_pct = dist_cfg.get('medium_pct', 0.32)
            
            if roll < short_pct:
                candidates = dests[dests['dist_km'] < dist_cfg.get('short_max_km', 100)]
            elif roll < short_pct + medium_pct:
                candidates = dests[(dests['dist_km'] >= 100) & (dests['dist_km'] < 350)]
            else:
                candidates = dests[dests['dist_km'] >= 350]
        
        if candidates.empty:
            candidates = dests
        
        # Weight selection toward median
        dest = candidates.sample(n=1, random_state=int(self.rng.integers(1e9))).iloc[0]
        return dest, dest['dist_km']
    
    def generate(self, start_date: date, end_date: date):
        """Generate calibrated telemetry for all trucks."""
        all_points = []
        
        num_days = (end_date - start_date).days + 1
        
        for day_idx in range(num_days):
            current_day = start_date + timedelta(days=day_idx)
            logger.info(f"Generating day {day_idx+1}/{num_days}: {current_day}")
            
            for truck_id, truck in self.trucks.items():
                if not self._should_operate_today(truck, current_day):
                    # Emit sparse overnight telemetry only
                    points = self._emit_inactive_day(truck, current_day)
                    all_points.extend(points)
                    continue
                
                # Reset daily counters
                start_hour = int(self.rng.normal(6.5, 1.0))
                start_hour = max(5, min(8, start_hour))
                truck.current_time = datetime.combine(current_day, datetime.min.time().replace(hour=start_hour))
                truck.daily_driving_min = 0.0
                truck.minutes_since_break = 0.0
                
                # Check for HOS violation day
                is_violation_day = self._check_hos_violation(truck, current_day)
                max_hours_today = 10.5 if is_violation_day else self.max_daily_hours
                
                # Get trips for day
                num_trips = self._get_trips_for_day(truck)
                
                for trip_num in range(num_trips):
                    if truck.daily_driving_min >= max_hours_today * 60:
                        break
                    
                    points = self._generate_trip(truck, current_day, trip_num, max_hours_today)
                    all_points.extend(points)
                
                # End of day - emit overnight
                points = self._emit_end_of_day(truck, current_day)
                all_points.extend(points)
            
            if len(all_points) % 5000 < 500:
                logger.info(f"  Generated {len(all_points)} points so far...")
        
        return all_points
    
    def _generate_trip(
        self,
        truck: CalibratedTruck,
        day: date,
        trip_num: int,
        max_hours: float
    ) -> List[dict]:
        """Generate a complete trip with dwell and driving segments."""
        points = []
        
        # Pre-trip dwell at warehouse (longer loading times)
        dwell_min = self._get_dwell_duration('warehouse') * 1.5
        points.extend(self._emit_dwell(truck, dwell_min, TruckState.DWELL_WAREHOUSE))
        
        # Select destination
        dest, distance_km = self._select_destination(truck)
        dest_id = dest.get('destination_id') or dest.get('id')
        dest_coords = (dest['longitude'], dest['latitude'])
        
        # Get route
        route = self.router.get_route(
            origin_id=truck.home_base_id,
            dest_id=str(dest_id),
            origin_lng=truck.current_lng,
            origin_lat=truck.current_lat,
            dest_lng=dest_coords[0],
            dest_lat=dest_coords[1]
        )
        
        if route is None:
            return points
        
        # Create trip ID
        trip_id = f"{day.strftime('%Y%m%d')}-{truck.truck_id}-{trip_num:02d}"
        truck.current_trip_id = trip_id
        truck.trip_count += 1
        
        # Drive to destination
        points.extend(self._emit_driving(truck, route, trip_id))
        
        # Destination dwell (unloading)
        dest_dwell = self._get_dwell_duration('warehouse')
        points.extend(self._emit_dwell(truck, dest_dwell, TruckState.DWELL_DESTINATION))
        
        # Check if need break
        if truck.minutes_since_break >= 270:  # 4.5 hours
            break_duration = self._get_dwell_duration('mandatory_break')
            points.extend(self._emit_dwell(truck, break_duration, TruckState.DWELL_REST_STOP))
            truck.minutes_since_break = 0
        
        return points
    
    def _emit_dwell(
        self,
        truck: CalibratedTruck,
        duration_min: float,
        state: TruckState
    ) -> List[dict]:
        """Emit dwell telemetry points."""
        points = []
        elapsed = 0.0
        duration_sec = duration_min * 60
        
        truck.current_state = state
        
        while elapsed < duration_sec:
            interval = self._get_ping_interval(state)
            
            point = {
                'telemetry_id': str(np.random.default_rng().integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': truck.current_trip_id or f"DWELL-{truck.truck_id}",
                'timestamp': truck.current_time,
                'latitude': truck.current_lat + self.rng.normal(0, 0.00001),
                'longitude': truck.current_lng + self.rng.normal(0, 0.00001),
                'speed_kmh': 0.0,
                'heading_deg': self.rng.uniform(0, 360),
                'posted_speed_kmh': 0,
                'status': state.value,
                'is_speeding': False,
                'is_hos_violation': truck.daily_driving_min > self.max_daily_hours * 60,
                'is_detour': False,
                'gps_accuracy_m': self.rng.uniform(5, 15),
                'location_id': truck.home_base_id,
                'location_type': state.value
            }
            points.append(point)
            truck.total_points += 1
            
            truck.current_time += timedelta(seconds=interval)
            elapsed += interval
        
        return points
    
    def _emit_driving(
        self,
        truck: CalibratedTruck,
        route: RouteResult,
        trip_id: str
    ) -> List[dict]:
        """Emit driving telemetry along route."""
        points = []
        truck.current_state = TruckState.MOVING
        
        coords = route.coordinates
        if not coords:
            return points
        
        total_distance = route.distance_km
        total_duration_sec = route.duration_min * 60
        is_highway = total_distance > 100
        
        # Interpolate route to ~30 sec intervals
        num_points = max(10, int(total_duration_sec / 35))  # Slightly fewer driving points
        
        prev_lat, prev_lng = truck.current_lat, truck.current_lng
        
        for i in range(num_points):
            t = i / max(1, num_points - 1)
            
            # Interpolate position along route
            coord_idx = min(int(t * len(coords)), len(coords) - 1)
            lng, lat = coords[coord_idx]
            
            # Add slight GPS jitter
            lat += self.rng.normal(0, 0.00005)
            lng += self.rng.normal(0, 0.00005)
            
            # Get speed
            posted_speed = 80 if is_highway else 60
            speed, is_speeding = self._get_speed(truck, posted_speed, is_highway)
            
            # Calculate heading
            if i > 0:
                heading = np.degrees(np.arctan2(lng - prev_lng, lat - prev_lat)) % 360
            else:
                heading = self.rng.uniform(0, 360)
            
            # Ping interval with variance
            interval = self._get_ping_interval(TruckState.MOVING)
            
            # Occasional telemetry gap (0.8%)
            if self.rng.random() < 0.008:
                gap_sec = int(self.rng.integers(300, 1200))
                truck.current_time += timedelta(seconds=gap_sec)
                continue
            
            point = {
                'telemetry_id': str(np.random.default_rng().integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': trip_id,
                'timestamp': truck.current_time,
                'latitude': lat,
                'longitude': lng,
                'speed_kmh': speed,
                'heading_deg': round(heading, 1),
                'posted_speed_kmh': posted_speed,
                'status': 'MOVING',
                'is_speeding': is_speeding,
                'is_hos_violation': truck.daily_driving_min > self.max_daily_hours * 60,
                'is_detour': False,
                'gps_accuracy_m': self.rng.uniform(5, 20),
                'location_id': None,
                'location_type': None
            }
            points.append(point)
            truck.total_points += 1
            
            # Update state
            prev_lat, prev_lng = lat, lng
            truck.current_lat = lat
            truck.current_lng = lng
            truck.current_time += timedelta(seconds=interval)
            truck.daily_driving_min += interval / 60
            truck.minutes_since_break += interval / 60
            
            # Emit idle points occasionally (3% chance)
            if self.rng.random() < 0.03:
                idle_duration = int(self.rng.integers(60, 300))
                idle_point = point.copy()
                idle_point['telemetry_id'] = str(np.random.default_rng().integers(1e15))
                idle_point['status'] = 'IDLE'
                idle_point['speed_kmh'] = 0.0
                idle_point['timestamp'] = truck.current_time
                points.append(idle_point)
                truck.current_time += timedelta(seconds=idle_duration)
        
        return points
    
    def _emit_end_of_day(self, truck: CalibratedTruck, day: date) -> List[dict]:
        """Emit overnight telemetry."""
        points = []
        
        # Determine overnight location
        dist_to_home = haversine_distance(
            truck.current_lat, truck.current_lng,
            truck.home_coords[1], truck.home_coords[0]
        )
        
        if dist_to_home < 60:
            state = TruckState.OVERNIGHT_HOME
            truck.current_lat = truck.home_coords[1]
            truck.current_lng = truck.home_coords[0]
        else:
            state = TruckState.OVERNIGHT_REST_STOP
        
        truck.current_state = state
        # Longer overnight with more frequent pings to increase overnight %
        overnight_duration = self._get_dwell_duration('overnight') * 1.5
        
        # Sparse overnight pings
        elapsed = 0.0
        duration_sec = overnight_duration * 60
        
        while elapsed < duration_sec:
            interval = self._get_ping_interval(state)
            
            point = {
                'telemetry_id': str(np.random.default_rng().integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': f"OVERNIGHT-{truck.truck_id}-{day.strftime('%Y%m%d')}",
                'timestamp': truck.current_time,
                'latitude': truck.current_lat + self.rng.normal(0, 0.00001),
                'longitude': truck.current_lng + self.rng.normal(0, 0.00001),
                'speed_kmh': 0.0,
                'heading_deg': self.rng.uniform(0, 360),
                'posted_speed_kmh': 0,
                'status': state.value,
                'is_speeding': False,
                'is_hos_violation': False,
                'is_detour': False,
                'gps_accuracy_m': self.rng.uniform(5, 15),
                'location_id': truck.home_base_id if state == TruckState.OVERNIGHT_HOME else None,
                'location_type': state.value
            }
            points.append(point)
            truck.total_points += 1
            
            truck.current_time += timedelta(seconds=interval)
            elapsed += interval
        
        return points
    
    def _emit_inactive_day(self, truck: CalibratedTruck, day: date) -> List[dict]:
        """Emit sparse telemetry for inactive day (weekend)."""
        points = []
        
        truck.current_time = datetime.combine(day, datetime.min.time().replace(hour=8))
        truck.current_state = TruckState.OVERNIGHT_HOME
        
        # Just a few pings during inactive day
        num_pings = self.rng.integers(3, 8)
        
        for _ in range(num_pings):
            point = {
                'telemetry_id': str(np.random.default_rng().integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': f"INACTIVE-{truck.truck_id}-{day.strftime('%Y%m%d')}",
                'timestamp': truck.current_time,
                'latitude': truck.home_coords[1] + self.rng.normal(0, 0.00001),
                'longitude': truck.home_coords[0] + self.rng.normal(0, 0.00001),
                'speed_kmh': 0.0,
                'heading_deg': self.rng.uniform(0, 360),
                'posted_speed_kmh': 0,
                'status': 'OVERNIGHT_HOME',
                'is_speeding': False,
                'is_hos_violation': False,
                'is_detour': False,
                'gps_accuracy_m': self.rng.uniform(5, 15),
                'location_id': truck.home_base_id,
                'location_type': 'OVERNIGHT_HOME'
            }
            points.append(point)
            truck.total_points += 1
            
            truck.current_time += timedelta(hours=self.rng.uniform(2, 5))
        
        return points


def load_data():
    """Load data from Snowflake."""
    import snowflake.connector
    
    conn = snowflake.connector.connect(connection_name='airpublic')
    
    logger.info("Loading warehouses...")
    warehouses = pd.read_sql("""
        SELECT ID AS warehouse_id, NAME, LAT AS latitude, LNG AS longitude
        FROM FLEET_DEMOS.ROUTING.GERMANY_DESTINATIONS
        WHERE LOCATION_TYPE = 'WAREHOUSE'
        ORDER BY RANDOM() LIMIT 50
    """, conn)
    
    logger.info("Loading destinations...")
    destinations = pd.read_sql("""
        SELECT ID AS destination_id, NAME, LAT AS latitude, LNG AS longitude, LOCATION_TYPE
        FROM FLEET_DEMOS.ROUTING.GERMANY_DESTINATIONS
        ORDER BY RANDOM() LIMIT 200
    """, conn)
    
    logger.info("Loading rest stops...")
    rest_stops = pd.read_sql("""
        SELECT REST_STOP_ID, NAME, LAT AS latitude, LNG AS longitude
        FROM FLEET_DEMOS.ROUTING.GERMANY_REST_STOPS
        LIMIT 500
    """, conn)
    
    conn.close()
    return warehouses, destinations, rest_stops


def create_trucks(warehouses: pd.DataFrame, num_trucks: int = 10) -> List[CalibratedTruck]:
    """Create heterogeneous fleet with different truck types."""
    trucks = []
    rng = np.random.default_rng(42)
    
    # Truck type distribution
    type_dist = [
        (TruckType.REGIONAL, 0.65),
        (TruckType.LONG_HAUL, 0.25),
        (TruckType.LOW_UTIL, 0.10)
    ]
    
    # Driver profile distribution
    profiles = {
        'COMPLIANT': DriverProfile(
            profile_type=ProfileType.COMPLIANT,
            speeding_probability=0.03,
            hos_violation_probability=0.02,
            detour_probability=0.08,
            speed_variance=0.06
        ),
        'MILD': DriverProfile(
            profile_type=ProfileType.MILD,
            speeding_probability=0.08,
            hos_violation_probability=0.05,
            detour_probability=0.15,
            speed_variance=0.10
        ),
        'OUTLIER': DriverProfile(
            profile_type=ProfileType.OUTLIER,
            speeding_probability=0.18,
            hos_violation_probability=0.12,
            detour_probability=0.25,
            speed_variance=0.15
        )
    }
    profile_probs = [0.70, 0.25, 0.05]
    
    warehouses.columns = warehouses.columns.str.lower()
    
    for i in range(num_trucks):
        # Assign truck type
        roll = rng.random()
        cumulative = 0
        truck_type = TruckType.REGIONAL
        for tt, prob in type_dist:
            cumulative += prob
            if roll < cumulative:
                truck_type = tt
                break
        
        # Assign driver profile
        profile_name = rng.choice(['COMPLIANT', 'MILD', 'OUTLIER'], p=profile_probs)
        
        # Assign home base
        home = warehouses.iloc[i % len(warehouses)]
        
        trucks.append(CalibratedTruck(
            truck_id=f"TRK-{i+1:04d}",
            driver_id=f"DRV-{i+1:04d}",
            truck_type=truck_type,
            profile=profiles[profile_name],
            home_base_id=str(home['warehouse_id']),
            home_coords=(home['longitude'], home['latitude'])
        ))
    
    return trucks


def save_to_snowflake(telemetry_df: pd.DataFrame):
    """Save telemetry to Snowflake."""
    import snowflake.connector
    
    conn = snowflake.connector.connect(connection_name='airpublic')
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE IF EXISTS FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST")
    
    logger.info(f"Inserting {len(telemetry_df)} records...")
    
    batch_size = 1000
    for start in range(0, len(telemetry_df), batch_size):
        batch = telemetry_df.iloc[start:start+batch_size]
        
        values_list = []
        for _, row in batch.iterrows():
            ts = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            values = f"""(
                '{row['telemetry_id']}', '{row['truck_id']}', '{row['driver_id']}',
                '{row['trip_id']}', '{ts}', {row['latitude']}, {row['longitude']},
                {row['speed_kmh']}, {row['heading_deg']}, {row['posted_speed_kmh']},
                '{row['status']}', {str(row['is_speeding']).upper()},
                {str(row['is_hos_violation']).upper()}, {str(row['is_detour']).upper()},
                {row['gps_accuracy_m']}, NULL, NULL
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
        
        if (start + batch_size) % 5000 == 0:
            logger.info(f"  Inserted {min(start+batch_size, len(telemetry_df))}/{len(telemetry_df)}...")
    
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved {len(telemetry_df)} records")


def main():
    print("=" * 70)
    print("CALIBRATED TELEMETRY GENERATOR")
    print("Target: Realistic fleet statistics")
    print("=" * 70)
    
    # Load config
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'calibrated_config.yml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Load data
    warehouses, destinations, rest_stops = load_data()
    
    # Create trucks
    trucks = create_trucks(warehouses, num_trucks=10)
    logger.info(f"Created {len(trucks)} trucks:")
    for tt in TruckType:
        count = sum(1 for t in trucks if t.truck_type == tt)
        logger.info(f"  {tt.value}: {count}")
    
    # Create router
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name='airpublic')
    router = ORSRouter(config, connection=conn)
    
    # Create generator
    generator = CalibratedTelemetryGenerator(
        config=config,
        trucks=trucks,
        router=router,
        warehouses=warehouses,
        destinations=destinations,
        rest_stops=rest_stops
    )
    
    # Generate
    start_date = date(2025, 12, 1)
    end_date = date(2025, 12, 3)
    
    logger.info(f"\nGenerating {start_date} to {end_date}...")
    all_points = generator.generate(start_date, end_date)
    
    if not all_points:
        print("No points generated!")
        return
    
    df = pd.DataFrame(all_points)
    
    # Print stats
    print("\n" + "=" * 70)
    print("STATISTICS")
    print("=" * 70)
    
    print(f"\nTotal points: {len(df):,}")
    print(f"Unique trucks: {df['truck_id'].nunique()}")
    print(f"Unique trips: {df['trip_id'].nunique()}")
    
    # Status distribution
    print(f"\nStatus distribution:")
    status_counts = df['status'].value_counts()
    total = len(df)
    for status, count in status_counts.items():
        pct = count / total * 100
        print(f"  {status}: {count:,} ({pct:.1f}%)")
    
    # Moving stats
    moving = df[df['status'] == 'MOVING']
    moving_pct = len(moving) / total * 100
    print(f"\nMoving: {moving_pct:.1f}% (target: 78-86%)")
    
    # Dwell stats
    dwell = df[df['status'].str.contains('DWELL', na=False)]
    dwell_pct = len(dwell) / total * 100
    print(f"Dwell: {dwell_pct:.1f}% (target: 5-9% warehouse)")
    
    # Overnight
    overnight = df[df['status'].str.contains('OVERNIGHT', na=False)]
    overnight_pct = len(overnight) / total * 100
    print(f"Overnight: {overnight_pct:.1f}% (target: 6-12%)")
    
    # Idle
    idle = df[df['status'] == 'IDLE']
    idle_pct = len(idle) / total * 100
    print(f"Idle: {idle_pct:.1f}% (target: 2-6%)")
    
    # Speed stats
    if len(moving) > 0:
        avg_speed = moving['speed_kmh'].mean()
        print(f"\nAvg moving speed: {avg_speed:.1f} km/h (target: 58-72)")
        
        speeding = moving[moving['is_speeding'] == True]
        speeding_pct = len(speeding) / len(moving) * 100
        print(f"Speeding: {speeding_pct:.1f}% (target: 3-8%)")
    
    # Ping intervals
    if len(moving) > 1:
        moving_sorted = moving.sort_values(['truck_id', 'timestamp'])
        intervals = moving_sorted.groupby('truck_id')['timestamp'].diff().dt.total_seconds()
        valid = intervals[(intervals > 0) & (intervals < 300)]
        if len(valid) > 0:
            print(f"\nPing intervals:")
            print(f"  Mean: {valid.mean():.1f}s")
            print(f"  Std: {valid.std():.1f}s")
            print(f"  Min: {valid.min():.0f}s, Max: {valid.max():.0f}s")
    
    # Per-truck stats
    print(f"\nPoints per truck:")
    for truck_id in sorted(df['truck_id'].unique()):
        truck_df = df[df['truck_id'] == truck_id]
        truck_moving = truck_df[truck_df['status'] == 'MOVING']
        moving_pct = len(truck_moving) / len(truck_df) * 100 if len(truck_df) > 0 else 0
        print(f"  {truck_id}: {len(truck_df):,} pts ({moving_pct:.0f}% moving)")
    
    # Save
    print("\n" + "=" * 70)
    print("Saving to Snowflake...")
    save_to_snowflake(df)
    print("=" * 70)
    print("\nDone!")


if __name__ == "__main__":
    main()
