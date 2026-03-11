#!/usr/bin/env python3
"""
Forensic-Grade Synthetic Telemetry Generator
Generates telemetry indistinguishable from production data.

Key forensic features:
- Geographic speed consistency (urban/rural corridors)
- Persistent driver behavioral signatures
- Warehouse-specific dwell patterns
- Congestion windows (peak hours)
- Fat-tailed trip distributions
- Weekday behavioral shape
- Device artifacts (duplicates, out-of-order, multipath)
- Stop micro-movement
- Correlated anomalies
"""
import os
import sys
import logging
import yaml
import hashlib
from datetime import date, datetime, timedelta, time
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.simulate import TruckAssignment, TelemetryPoint, haversine_distance
from src.driver_profiles import DriverProfile, ProfileType
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


class AreaType(Enum):
    URBAN = "URBAN"
    SUBURBAN = "SUBURBAN"
    RURAL = "RURAL"
    HIGHWAY = "HIGHWAY"


# German metro areas for geographic consistency
GERMAN_METROS = {
    'berlin': {'center': (13.405, 52.52), 'radius_km': 35},
    'munich': {'center': (11.582, 48.135), 'radius_km': 30},
    'hamburg': {'center': (9.993, 53.551), 'radius_km': 25},
    'frankfurt': {'center': (8.682, 50.110), 'radius_km': 25},
    'cologne': {'center': (6.960, 50.938), 'radius_km': 20},
    'dusseldorf': {'center': (6.773, 51.228), 'radius_km': 18},
    'stuttgart': {'center': (9.183, 48.783), 'radius_km': 20},
    'dortmund': {'center': (7.466, 51.514), 'radius_km': 15},
    'essen': {'center': (7.012, 51.458), 'radius_km': 15},
    'leipzig': {'center': (12.373, 51.340), 'radius_km': 18},
    'bremen': {'center': (8.807, 53.075), 'radius_km': 15},
    'hannover': {'center': (9.732, 52.375), 'radius_km': 18},
    'nuremberg': {'center': (11.078, 49.454), 'radius_km': 15},
}


@dataclass
class DriverBehavior:
    """Persistent driver behavioral signature."""
    driver_id: str
    
    # Base tendencies (persistent across simulation)
    speeding_tendency: float = 0.0      # -1 to 1, affects daily speeding probability
    dwell_tendency: float = 0.0         # -1 to 1, affects dwell durations
    hos_compliance: float = 0.0         # -1 to 1, affects HOS violation probability
    route_preference: float = 0.0       # -1 to 1, optimal vs alternative routes
    
    # Rolling state (autocorrelated day-to-day)
    recent_speeding_rate: float = 0.0
    recent_dwell_avg: float = 0.0
    consecutive_long_days: int = 0
    
    def update_after_day(self, speeding_events: int, total_driving_points: int, 
                         avg_dwell: float, driving_hours: float):
        """Update behavioral state with autocorrelation."""
        # Update speeding rate with decay
        if total_driving_points > 0:
            today_rate = speeding_events / total_driving_points
            self.recent_speeding_rate = 0.7 * self.recent_speeding_rate + 0.3 * today_rate
        
        # Update dwell tendency
        self.recent_dwell_avg = 0.8 * self.recent_dwell_avg + 0.2 * avg_dwell
        
        # Track consecutive long days
        if driving_hours > 8.5:
            self.consecutive_long_days += 1
        else:
            self.consecutive_long_days = max(0, self.consecutive_long_days - 1)


@dataclass
class WarehouseProfile:
    """Warehouse-specific dwell characteristics."""
    warehouse_id: str
    name: str
    lat: float
    lng: float
    
    # Dwell characteristics
    base_dwell_median: float = 45.0     # minutes
    dwell_sigma: float = 0.9
    is_slow_facility: bool = False      # Some warehouses are consistently slow
    congestion_days: Set[int] = field(default_factory=set)  # Day-of-month with congestion
    
    def get_dwell_duration(self, rng: np.random.Generator, day: date) -> float:
        """Get dwell duration considering facility characteristics."""
        median = self.base_dwell_median
        sigma = self.dwell_sigma
        
        # Slow facilities add 30-50% to median
        if self.is_slow_facility:
            median *= rng.uniform(1.3, 1.5)
        
        # Congestion days add 50-100%
        if day.day in self.congestion_days:
            median *= rng.uniform(1.5, 2.0)
        
        duration = rng.lognormal(np.log(median), sigma)
        return max(10, min(480, duration))


@dataclass
class ForensicTruck:
    """Truck with forensic-grade state tracking."""
    truck_id: str
    driver_id: str
    truck_type: TruckType
    profile: DriverProfile
    behavior: DriverBehavior
    home_base_id: str
    home_coords: Tuple[float, float]
    
    # Utilization tier (for Pareto distribution)
    utilization_tier: float = 1.0       # 0.5 = low, 1.0 = normal, 1.5 = high
    
    # State
    current_lat: float = 0.0
    current_lng: float = 0.0
    current_time: datetime = None
    current_state: TruckState = TruckState.DWELL_WAREHOUSE
    current_trip_id: Optional[str] = None
    
    daily_driving_min: float = 0.0
    minutes_since_break: float = 0.0
    trip_count: int = 0
    total_points: int = 0
    total_distance_km: float = 0.0
    
    # Daily tracking for behavioral updates
    daily_speeding_events: int = 0
    daily_driving_points: int = 0
    daily_dwell_durations: List[float] = field(default_factory=list)
    
    def __post_init__(self):
        self.current_lat = self.home_coords[1]
        self.current_lng = self.home_coords[0]
    
    def reset_daily_tracking(self):
        self.daily_speeding_events = 0
        self.daily_driving_points = 0
        self.daily_dwell_durations = []


class ForensicTelemetryGenerator:
    """
    Generates forensic-grade synthetic telemetry.
    
    Implements all next-level improvements for production-indistinguishable data.
    """
    
    def __init__(
        self,
        config: dict,
        trucks: List[ForensicTruck],
        router: ORSRouter,
        warehouses: pd.DataFrame,
        destinations: pd.DataFrame,
        rest_stops: pd.DataFrame,
        hgv_parkings: pd.DataFrame = None
    ):
        self.config = config
        self.trucks = {t.truck_id: t for t in trucks}
        self.router = router
        
        # Normalize column names
        self.warehouses = warehouses.copy()
        self.warehouses.columns = self.warehouses.columns.str.lower()
        self.destinations = destinations.copy()
        self.destinations.columns = self.destinations.columns.str.lower()
        self.rest_stops = rest_stops.copy()
        self.rest_stops.columns = self.rest_stops.columns.str.lower()
        
        # HGV parkings (primary stop locations)
        if hgv_parkings is not None:
            self.hgv_parkings = hgv_parkings.copy()
            self.hgv_parkings.columns = self.hgv_parkings.columns.str.lower()
        else:
            self.hgv_parkings = pd.DataFrame()
        
        self.rng = np.random.default_rng(config.get('seed', 42))
        
        # Initialize warehouse profiles
        self._init_warehouse_profiles()
        
        # Speed profiles by area type
        self.speed_profiles = {
            AreaType.URBAN: {'mean': 32, 'std': 8, 'posted': 50},
            AreaType.SUBURBAN: {'mean': 48, 'std': 10, 'posted': 60},
            AreaType.RURAL: {'mean': 65, 'std': 12, 'posted': 70},
            AreaType.HIGHWAY: {'mean': 76, 'std': 6, 'posted': 80},
        }
        
        # Weekday patterns
        self.weekday_patterns = {
            0: {'activity': 0.85, 'speeding_mult': 0.9, 'hos_pressure': 0.8},   # Monday
            1: {'activity': 1.0, 'speeding_mult': 1.0, 'hos_pressure': 1.0},    # Tuesday
            2: {'activity': 1.0, 'speeding_mult': 1.0, 'hos_pressure': 1.0},    # Wednesday
            3: {'activity': 1.05, 'speeding_mult': 1.1, 'hos_pressure': 1.2},   # Thursday
            4: {'activity': 0.95, 'speeding_mult': 1.15, 'hos_pressure': 1.3},  # Friday
            5: {'activity': 0.35, 'speeding_mult': 0.8, 'hos_pressure': 0.5},   # Saturday
            6: {'activity': 0.25, 'speeding_mult': 0.7, 'hos_pressure': 0.4},   # Sunday
        }
        
        # Congestion windows (peak hours)
        self.congestion_windows = [
            (time(7, 0), time(9, 30)),   # Morning rush
            (time(16, 0), time(18, 30)), # Evening rush
        ]
        
        # Device artifact probabilities
        self.device_artifacts = {
            'duplicate_timestamp': 0.0008,
            'out_of_order': 0.0005,
            'multipath_cluster': 0.002,
            'timestamp_jitter_sec': 2,
        }
        
    def _init_warehouse_profiles(self):
        """Initialize warehouse-specific profiles."""
        self.warehouse_profiles: Dict[str, WarehouseProfile] = {}
        
        for _, row in self.warehouses.iterrows():
            wh_id = str(row.get('warehouse_id') or row.get('id'))
            
            # Randomly designate 15% as slow facilities
            is_slow = self.rng.random() < 0.15
            
            # Generate 1-2 congestion days per month
            congestion_days = set(self.rng.choice(range(1, 29), size=self.rng.integers(1, 3), replace=False))
            
            self.warehouse_profiles[wh_id] = WarehouseProfile(
                warehouse_id=wh_id,
                name=row.get('name', ''),
                lat=row['latitude'],
                lng=row['longitude'],
                base_dwell_median=self.rng.uniform(35, 60),
                dwell_sigma=self.rng.uniform(0.7, 1.1),
                is_slow_facility=is_slow,
                congestion_days=congestion_days
            )
    
    def _find_nearest_parking(
        self, 
        lat: float, 
        lng: float, 
        max_distance_km: float = 50
    ) -> Tuple[Optional[str], Optional[float], Optional[float], str]:
        """Find nearest parking location (80% HGV parking, 20% rest stop).
        
        Returns: (parking_id, parking_lat, parking_lng, parking_type)
        """
        # Decide parking type: 80% HGV, 20% rest stop
        use_hgv = self.rng.random() < 0.80 and not self.hgv_parkings.empty
        
        if use_hgv:
            parkings = self.hgv_parkings
            type_name = 'HGV_PARKING'
        else:
            parkings = self.rest_stops
            type_name = 'REST_STOP'
        
        if parkings.empty:
            return None, None, None, 'ROADSIDE'
        
        # Calculate distances
        parkings = parkings.copy()
        parkings['dist_km'] = parkings.apply(
            lambda r: haversine_distance(lat, lng, r['latitude'], r['longitude']),
            axis=1
        )
        
        # Filter by max distance
        nearby = parkings[parkings['dist_km'] <= max_distance_km]
        
        if nearby.empty:
            # Fallback: get closest regardless of distance
            nearest = parkings.nsmallest(1, 'dist_km').iloc[0]
        else:
            # Pick from top 5 nearest (adds some randomness)
            top_nearby = nearby.nsmallest(5, 'dist_km')
            nearest = top_nearby.sample(n=1, random_state=int(self.rng.integers(1e9))).iloc[0]
        
        parking_id = str(nearest.get('parking_id') or nearest.get('rest_stop_id') or 'unknown')
        return parking_id, nearest['latitude'], nearest['longitude'], type_name
    
    def _get_area_type(self, lat: float, lng: float) -> AreaType:
        """Determine area type based on proximity to metro areas."""
        for metro, info in GERMAN_METROS.items():
            center_lng, center_lat = info['center']
            dist = haversine_distance(lat, lng, center_lat, center_lng)
            
            if dist < info['radius_km'] * 0.4:
                return AreaType.URBAN
            elif dist < info['radius_km']:
                return AreaType.SUBURBAN
        
        return AreaType.RURAL
    
    def _is_congestion_time(self, current_time: datetime) -> bool:
        """Check if current time is within congestion window."""
        t = current_time.time()
        for start, end in self.congestion_windows:
            if start <= t <= end:
                return True
        return False
    
    def _get_speed(
        self,
        truck: ForensicTruck,
        area_type: AreaType,
        current_time: datetime,
        is_highway: bool
    ) -> Tuple[float, bool]:
        """Generate geographically and temporally consistent speed."""
        
        if is_highway:
            area_type = AreaType.HIGHWAY
        
        profile = self.speed_profiles[area_type]
        base_mean = profile['mean']
        base_std = profile['std']
        posted = profile['posted']
        
        # Apply congestion slowdown in metros during peak hours
        if area_type in [AreaType.URBAN, AreaType.SUBURBAN] and self._is_congestion_time(current_time):
            base_mean *= 0.65
            base_std *= 1.3
        
        # Apply driver behavior tendency
        behavior_adj = 1.0 + (truck.behavior.speeding_tendency * 0.1)
        
        # Apply weekday pattern
        weekday = current_time.weekday()
        weekday_mult = self.weekday_patterns[weekday]['speeding_mult']
        
        # Generate speed
        speed = self.rng.normal(base_mean * behavior_adj, base_std)
        speed = max(15, min(95, speed))
        
        # Speeding logic with persistent behavior
        is_speeding = False
        base_speeding_prob = truck.profile.speeding_probability
        
        # Autocorrelation: more likely to speed if recently speeding
        speeding_prob = base_speeding_prob * (1 + truck.behavior.recent_speeding_rate)
        speeding_prob *= weekday_mult
        
        # More speeding in late-day hours (correlated anomaly)
        hour = current_time.hour
        if 15 <= hour <= 19:
            speeding_prob *= 1.2
        
        if self.rng.random() < speeding_prob:
            speed_factor = 1.0 + self.rng.uniform(0.08, 0.22)
            speed = posted * speed_factor
            is_speeding = True
            truck.daily_speeding_events += 1
        
        return round(speed, 1), is_speeding
    
    def _get_dwell_duration(
        self,
        truck: ForensicTruck,
        dwell_type: str,
        warehouse_id: Optional[str],
        day: date
    ) -> float:
        """Get dwell duration with warehouse-specific and driver-persistent characteristics."""
        
        if dwell_type == 'warehouse' and warehouse_id and warehouse_id in self.warehouse_profiles:
            wh_profile = self.warehouse_profiles[warehouse_id]
            base_duration = wh_profile.get_dwell_duration(self.rng, day)
        else:
            # Default dwell
            if dwell_type in ['warehouse', 'destination']:
                base_duration = self.rng.lognormal(np.log(45), 0.9)
            elif dwell_type == 'mandatory_break':
                base_duration = self.rng.lognormal(np.log(48), 0.25)
            elif dwell_type == 'short_break':
                base_duration = self.rng.lognormal(np.log(22), 0.6)
            else:  # overnight
                base_duration = self.rng.lognormal(np.log(600), 0.15)
        
        # Apply driver dwell tendency (persistent)
        dwell_factor = 1.0 + (truck.behavior.dwell_tendency * 0.2)
        duration = base_duration * dwell_factor
        
        # Rare extreme long dwell (1-3% tail)
        if dwell_type in ['warehouse', 'destination'] and self.rng.random() < 0.025:
            duration = self.rng.uniform(480, 1440)
        
        truck.daily_dwell_durations.append(duration)
        return duration
    
    def _get_ping_interval(self, state: TruckState, area_type: AreaType) -> int:
        """Get ping interval with area-based gap probability."""
        if state == TruckState.MOVING:
            interval = int(self.rng.normal(30, 8))
            interval = max(15, min(55, interval))
            
            # Rural areas have more telemetry gaps
            gap_prob = 0.008 if area_type == AreaType.RURAL else 0.004
            if self.rng.random() < gap_prob:
                interval += int(self.rng.uniform(180, 600))
            
            return interval
            
        elif state in [TruckState.OVERNIGHT_HOME, TruckState.OVERNIGHT_REST_STOP]:
            # Sparser overnight pings
            return int(self.rng.uniform(600, 1200))
        else:
            # Dwell pings - tuned for 5-9% dwell ratio
            return max(120, min(400, int(self.rng.normal(200, 60))))
    
    def _should_operate_today(self, truck: ForensicTruck, day: date) -> bool:
        """Determine operation with utilization tier consideration."""
        weekday = day.weekday()
        base_rate = self.weekday_patterns[weekday]['activity']
        
        # Apply utilization tier
        rate = base_rate * truck.utilization_tier
        
        # Low-util trucks even lower
        if truck.truck_type == TruckType.LOW_UTIL:
            rate *= 0.6
        
        return self.rng.random() < min(0.98, rate)
    
    def _get_trips_for_day(self, truck: ForensicTruck, day: date) -> int:
        """Get trip count with utilization tier and weekday pattern."""
        weekday = day.weekday()
        
        if truck.truck_type == TruckType.REGIONAL:
            base_min, base_max = 2, 3
        elif truck.truck_type == TruckType.LONG_HAUL:
            base_min, base_max = 1, 1
        else:
            base_min, base_max = 0, 2
        
        # High utilization trucks do more trips
        if truck.utilization_tier > 1.2:
            base_max += 1
        elif truck.utilization_tier < 0.8:
            base_max = max(base_min, base_max - 1)
        
        # Weekend: fewer trips
        if weekday >= 5:
            base_max = max(base_min, base_max - 1)
        
        return self.rng.integers(base_min, base_max + 1)
    
    def _select_destination(
        self,
        truck: ForensicTruck,
        include_repositioning: bool = True
    ) -> Tuple[pd.Series, float, str]:
        """Select destination with fat-tailed distance AND directional diversity."""
        dests = self.destinations.copy()
        
        # Calculate distance and bearing to each destination
        dests['dist_km'] = dests.apply(
            lambda r: haversine_distance(
                truck.current_lat, truck.current_lng,
                r['latitude'], r['longitude']
            ), axis=1
        )
        
        # Calculate bearing (direction) for diversity
        dests['bearing'] = dests.apply(
            lambda r: np.degrees(np.arctan2(
                r['longitude'] - truck.current_lng,
                r['latitude'] - truck.current_lat
            )) % 360, axis=1
        )
        
        # Assign directional quadrant (N, E, S, W)
        dests['quadrant'] = pd.cut(
            dests['bearing'], 
            bins=[0, 90, 180, 270, 360],
            labels=['NE', 'SE', 'SW', 'NW'],
            include_lowest=True
        )
        
        # Repositioning trips (3-5% chance)
        if include_repositioning and self.rng.random() < 0.04:
            candidates = dests[dests['dist_km'] < 15]
            if not candidates.empty:
                dest = candidates.sample(n=1, random_state=int(self.rng.integers(1e9))).iloc[0]
                return dest, dest['dist_km'], 'REPOSITIONING'
        
        # First, select target direction (ensure diversity)
        target_quadrant = self.rng.choice(['NE', 'SE', 'SW', 'NW'])
        
        # Fat-tailed distribution with directional constraint
        if truck.truck_type == TruckType.LONG_HAUL:
            roll = self.rng.random()
            if roll < 0.4:
                dist_filter = (dests['dist_km'] >= 150) & (dests['dist_km'] < 350)
            elif roll < 0.9:
                dist_filter = (dests['dist_km'] >= 350) & (dests['dist_km'] < 600)
            else:
                dist_filter = dests['dist_km'] >= 600
        elif truck.truck_type == TruckType.LOW_UTIL:
            dist_filter = dests['dist_km'] < 100
        else:
            roll = self.rng.random()
            if roll < 0.50:
                dist_filter = dests['dist_km'] < 100
            elif roll < 0.80:
                dist_filter = (dests['dist_km'] >= 100) & (dests['dist_km'] < 250)
            elif roll < 0.92:
                dist_filter = (dests['dist_km'] >= 250) & (dests['dist_km'] < 450)
            else:
                dist_filter = dests['dist_km'] >= 450
        
        # Try to get candidates in target direction first
        candidates = dests[dist_filter & (dests['quadrant'] == target_quadrant)]
        
        # If no candidates in that direction, try any direction with right distance
        if candidates.empty:
            candidates = dests[dist_filter]
        
        # Final fallback
        if candidates.empty:
            candidates = dests
        
        dest = candidates.sample(n=1, random_state=int(self.rng.integers(1e9))).iloc[0]
        return dest, dest['dist_km'], 'DELIVERY'
    
    def _find_nearby_pois(
        self,
        lat: float,
        lng: float,
        radius_km: float = 15
    ) -> pd.DataFrame:
        """Find nearby POIs for realistic detour waypoints."""
        all_pois = pd.concat([
            self.rest_stops[['latitude', 'longitude']].assign(poi_type='REST_STOP'),
            self.destinations[['latitude', 'longitude']].assign(poi_type='DESTINATION')
        ], ignore_index=True)
        
        all_pois['dist_km'] = all_pois.apply(
            lambda r: haversine_distance(lat, lng, r['latitude'], r['longitude']),
            axis=1
        )
        
        nearby = all_pois[all_pois['dist_km'] <= radius_km].sort_values('dist_km')
        return nearby.head(5)
    
    def _select_detour_waypoint(
        self,
        origin_lat: float,
        origin_lng: float,
        dest_lat: float,
        dest_lng: float,
        max_deviation_km: float = 30
    ) -> Tuple[float, float, str]:
        """Select a waypoint for route deviation.
        
        Strategy: Find a point roughly perpendicular to the direct path,
        within max_deviation_km, using nearby POIs or random offset.
        """
        mid_lat = (origin_lat + dest_lat) / 2
        mid_lng = (origin_lng + dest_lng) / 2
        
        dx = dest_lng - origin_lng
        dy = dest_lat - origin_lat
        perp_dx, perp_dy = -dy, dx
        
        magnitude = np.sqrt(perp_dx**2 + perp_dy**2)
        if magnitude > 0:
            deviation_km = self.rng.uniform(8, max_deviation_km)
            scale = (deviation_km / 111) / magnitude
            direction = self.rng.choice([-1, 1])
            waypoint_lat = mid_lat + perp_dy * scale * direction
            waypoint_lng = mid_lng + perp_dx * scale * direction
        else:
            waypoint_lat = mid_lat + self.rng.uniform(-0.15, 0.15)
            waypoint_lng = mid_lng + self.rng.uniform(-0.15, 0.15)
        
        nearby_pois = self._find_nearby_pois(waypoint_lat, waypoint_lng, radius_km=12)
        if not nearby_pois.empty:
            poi = nearby_pois.iloc[0]
            return poi['latitude'], poi['longitude'], f"DETOUR_{poi['poi_type']}"
        
        return waypoint_lat, waypoint_lng, 'DETOUR_RANDOM'
    
    def _emit_micro_movement(
        self,
        truck: ForensicTruck,
        duration_min: float,
        state: TruckState,
        location_id: Optional[str]
    ) -> List[dict]:
        """Emit dwell telemetry with realistic micro-movement at actual parking locations."""
        points = []
        elapsed = 0.0
        duration_sec = duration_min * 60
        
        area_type = self._get_area_type(truck.current_lat, truck.current_lng)
        truck.current_state = state
        
        # For rest stops/breaks, snap to nearest parking location
        actual_location_id = location_id
        actual_location_type = state.value
        
        if state in [TruckState.DWELL_REST_STOP, TruckState.OVERNIGHT_REST_STOP] and location_id is None:
            parking_id, parking_lat, parking_lng, parking_type = self._find_nearest_parking(
                truck.current_lat, truck.current_lng
            )
            if parking_lat is not None:
                truck.current_lat = parking_lat
                truck.current_lng = parking_lng
                actual_location_id = parking_id
                actual_location_type = parking_type
        
        # Base position with drift
        base_lat = truck.current_lat
        base_lng = truck.current_lng
        drift_lat = 0.0
        drift_lng = 0.0
        
        while elapsed < duration_sec:
            interval = self._get_ping_interval(state, area_type)
            
            # Micro-movement: simulate low-speed creep (1-5 km/h equivalent drift)
            if self.rng.random() < 0.3:  # 30% of pings show micro-movement
                drift_lat += self.rng.normal(0, 0.00003)
                drift_lng += self.rng.normal(0, 0.00003)
                micro_speed = self.rng.uniform(1, 5)
            else:
                micro_speed = 0.0
            
            # Clamp drift to realistic range
            drift_lat = np.clip(drift_lat, -0.0002, 0.0002)
            drift_lng = np.clip(drift_lng, -0.0002, 0.0002)
            
            lat = base_lat + drift_lat + self.rng.normal(0, 0.00001)
            lng = base_lng + drift_lng + self.rng.normal(0, 0.00001)
            
            # Timestamp with jitter
            jitter_sec = self.rng.integers(-2, 3)
            ts = truck.current_time + timedelta(seconds=int(jitter_sec))
            
            point = {
                'telemetry_id': str(self.rng.integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': truck.current_trip_id or f"DWELL-{truck.truck_id}",
                'timestamp': ts,
                'latitude': lat,
                'longitude': lng,
                'speed_kmh': micro_speed,
                'heading_deg': self.rng.uniform(0, 360),
                'posted_speed_kmh': 0,
                'status': state.value,
                'is_speeding': False,
                'is_hos_violation': truck.daily_driving_min > 540,
                'is_detour': False,
                'gps_accuracy_m': self.rng.uniform(5, 15),
                'location_id': actual_location_id,
                'location_type': actual_location_type
            }
            points.append(point)
            truck.total_points += 1
            
            truck.current_time += timedelta(seconds=interval)
            elapsed += interval
        
        return points
    
    def _emit_driving(
        self,
        truck: ForensicTruck,
        route: RouteResult,
        trip_id: str,
        trip_type: str,
        is_detour: bool = False
    ) -> List[dict]:
        """Emit driving telemetry with geographic consistency."""
        points = []
        truck.current_state = TruckState.MOVING
        
        coords = route.coordinates
        if not coords:
            return points
        
        total_distance = route.distance_km
        total_duration_sec = route.duration_min * 60
        
        num_points = max(10, int(total_duration_sec / 35))
        segment_distance = total_distance / num_points
        
        prev_lat, prev_lng = truck.current_lat, truck.current_lng
        prev_area = self._get_area_type(prev_lat, prev_lng)
        
        # Track for refueling stops
        distance_since_fuel = self.rng.uniform(0, 300)
        
        for i in range(num_points):
            t = i / max(1, num_points - 1)
            coord_idx = min(int(t * len(coords)), len(coords) - 1)
            lng, lat = coords[coord_idx]
            
            # Determine area type for geographic consistency
            area_type = self._get_area_type(lat, lng)
            is_highway = total_distance > 100 and area_type == AreaType.RURAL
            
            # Get geographically consistent speed
            speed, is_speeding = self._get_speed(truck, area_type, truck.current_time, is_highway)
            
            # Heading calculation
            if i > 0:
                heading = np.degrees(np.arctan2(lng - prev_lng, lat - prev_lat)) % 360
            else:
                heading = self.rng.uniform(0, 360)
            
            interval = self._get_ping_interval(TruckState.MOVING, area_type)
            
            # GPS multipath near metros/warehouses
            gps_accuracy = self.rng.uniform(5, 15)
            if area_type == AreaType.URBAN and self.rng.random() < self.device_artifacts['multipath_cluster']:
                lat += self.rng.uniform(-0.001, 0.001)
                lng += self.rng.uniform(-0.001, 0.001)
                gps_accuracy = self.rng.uniform(50, 150)
            
            # Timestamp with jitter
            jitter = self.rng.integers(-self.device_artifacts['timestamp_jitter_sec'],
                                       self.device_artifacts['timestamp_jitter_sec'] + 1)
            ts = truck.current_time + timedelta(seconds=int(jitter))
            
            point = {
                'telemetry_id': str(self.rng.integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': trip_id,
                'timestamp': ts,
                'latitude': lat + self.rng.normal(0, 0.00003),
                'longitude': lng + self.rng.normal(0, 0.00003),
                'speed_kmh': speed,
                'heading_deg': round(heading, 1),
                'posted_speed_kmh': self.speed_profiles[area_type]['posted'],
                'status': 'MOVING',
                'is_speeding': is_speeding,
                'is_hos_violation': truck.daily_driving_min > 540,
                'is_detour': is_detour,
                'gps_accuracy_m': gps_accuracy,
                'location_id': None,
                'location_type': None
            }
            points.append(point)
            truck.total_points += 1
            truck.daily_driving_points += 1
            
            # Device artifacts: duplicate timestamp
            if self.rng.random() < self.device_artifacts['duplicate_timestamp']:
                dup_point = point.copy()
                dup_point['telemetry_id'] = str(self.rng.integers(1e15))
                dup_point['speed_kmh'] = speed + self.rng.uniform(-2, 2)
                points.append(dup_point)
            
            # Update state
            prev_lat, prev_lng = lat, lng
            prev_area = area_type
            truck.current_lat = lat
            truck.current_lng = lng
            truck.current_time += timedelta(seconds=interval)
            truck.daily_driving_min += interval / 60
            truck.minutes_since_break += interval / 60
            truck.total_distance_km += segment_distance
            distance_since_fuel += segment_distance
            
            # Idle events (traffic, etc.)
            if self.rng.random() < 0.025:
                idle_duration = int(self.rng.integers(30, 180))
                idle_point = point.copy()
                idle_point['telemetry_id'] = str(self.rng.integers(1e15))
                idle_point['status'] = 'IDLE'
                idle_point['speed_kmh'] = 0.0
                idle_point['timestamp'] = truck.current_time
                points.append(idle_point)
                truck.current_time += timedelta(seconds=idle_duration)
            
            # Refueling stop (~every 400-600km)
            if distance_since_fuel > self.rng.uniform(400, 600):
                fuel_duration = self.rng.uniform(15, 35)
                points.extend(self._emit_micro_movement(
                    truck, fuel_duration, TruckState.DWELL_REST_STOP, None
                ))
                distance_since_fuel = 0
                truck.minutes_since_break = 0
        
        return points
    
    def _should_abort_trip(self, truck: ForensicTruck) -> bool:
        """Occasionally abort trips (operational noise)."""
        # 2% chance of abort for long-haul, 1% for others
        abort_prob = 0.02 if truck.truck_type == TruckType.LONG_HAUL else 0.01
        return self.rng.random() < abort_prob
    
    def generate(self, start_date: date, end_date: date, save_callback=None, truncate_first: bool = True) -> int:
        """Generate forensic-grade telemetry with incremental saves.
        
        Args:
            save_callback: Function to call with daily points (for incremental save)
            truncate_first: Whether to truncate table before first insert
            
        Returns:
            Total points generated
        """
        total_points = 0
        num_days = (end_date - start_date).days + 1
        first_save = True
        
        for day_idx in range(num_days):
            current_day = start_date + timedelta(days=day_idx)
            weekday = current_day.weekday()
            logger.info(f"Generating day {day_idx+1}/{num_days}: {current_day} ({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][weekday]})")
            
            day_points = []
            
            for truck_id, truck in self.trucks.items():
                truck.reset_daily_tracking()
                
                if not self._should_operate_today(truck, current_day):
                    points = self._emit_inactive_day(truck, current_day)
                    day_points.extend(points)
                    continue
                
                # Start time with variance
                start_hour = int(self.rng.normal(6.5, 0.8))
                start_hour = max(5, min(8, start_hour))
                start_min = int(self.rng.uniform(0, 45))
                truck.current_time = datetime.combine(current_day, time(start_hour, start_min))
                truck.daily_driving_min = 0.0
                truck.minutes_since_break = 0.0
                
                # HOS violation day (correlated with long-haul and Thu/Fri)
                hos_prob = truck.profile.hos_violation_probability
                hos_prob *= self.weekday_patterns[weekday]['hos_pressure']
                if truck.truck_type == TruckType.LONG_HAUL:
                    hos_prob *= 1.5
                if truck.behavior.consecutive_long_days > 1:
                    hos_prob *= 1.3
                
                is_violation_day = self.rng.random() < hos_prob
                max_hours = 10.2 if is_violation_day else 9.0
                
                num_trips = self._get_trips_for_day(truck, current_day)
                
                for trip_num in range(num_trips):
                    if truck.daily_driving_min >= max_hours * 60:
                        break
                    
                    # Check for aborted trip
                    if self._should_abort_trip(truck):
                        points = self._emit_aborted_trip(truck, current_day, trip_num)
                        day_points.extend(points)
                        continue
                    
                    points = self._generate_trip(truck, current_day, trip_num, max_hours)
                    day_points.extend(points)
                
                # End of day
                points = self._emit_end_of_day(truck, current_day)
                day_points.extend(points)
                
                # Update behavioral state
                avg_dwell = np.mean(truck.daily_dwell_durations) if truck.daily_dwell_durations else 45
                truck.behavior.update_after_day(
                    truck.daily_speeding_events,
                    truck.daily_driving_points,
                    avg_dwell,
                    truck.daily_driving_min / 60
                )
            
            # Apply device artifacts to day's data
            self._inject_out_of_order(day_points)
            
            # Save incrementally
            if save_callback and day_points:
                should_truncate = truncate_first and first_save
                save_callback(day_points, truncate=should_truncate)
                first_save = False
                
            total_points += len(day_points)
            logger.info(f"  Day complete: {len(day_points):,} points (total: {total_points:,})")
        
        return total_points
    
    def _generate_trip(
        self,
        truck: ForensicTruck,
        day: date,
        trip_num: int,
        max_hours: float
    ) -> List[dict]:
        """Generate a delivery trip with unified trip_id.
        
        A trip encompasses: origin dwell → drive → dest dwell
        All phases share the same trip_id.
        May include route deviation based on driver profile.
        """
        points = []
        
        trip_id = f"{day.strftime('%Y%m%d')}-{truck.truck_id}-{trip_num:02d}"
        truck.current_trip_id = trip_id
        truck.trip_count += 1
        
        wh_id = truck.home_base_id
        
        dwell_min = self._get_dwell_duration(truck, 'warehouse', wh_id, day) * 0.7
        points.extend(self._emit_micro_movement(truck, dwell_min, TruckState.DWELL_WAREHOUSE, wh_id))
        
        dest, distance_km, trip_type = self._select_destination(truck)
        dest_id = str(dest.get('destination_id') or dest.get('id'))
        dest_coords = (dest['longitude'], dest['latitude'])
        
        should_detour = self.rng.random() < truck.profile.detour_probability
        
        if should_detour and distance_km > 50:
            wp_lat, wp_lng, detour_type = self._select_detour_waypoint(
                truck.current_lat, truck.current_lng,
                dest_coords[1], dest_coords[0]
            )
            
            route1 = self.router.get_route(
                origin_id=wh_id,
                dest_id=f"DETOUR-{trip_id}",
                origin_lng=truck.current_lng,
                origin_lat=truck.current_lat,
                dest_lng=wp_lng,
                dest_lat=wp_lat
            )
            
            if route1:
                points.extend(self._emit_driving(truck, route1, trip_id, trip_type, is_detour=True))
                
                route2 = self.router.get_route(
                    origin_id=f"DETOUR-{trip_id}",
                    dest_id=dest_id,
                    origin_lng=wp_lng,
                    origin_lat=wp_lat,
                    dest_lng=dest_coords[0],
                    dest_lat=dest_coords[1]
                )
                
                if route2:
                    points.extend(self._emit_driving(truck, route2, trip_id, trip_type, is_detour=False))
                else:
                    route_direct = self.router.get_route(
                        origin_id=f"DETOUR-{trip_id}",
                        dest_id=dest_id,
                        origin_lng=truck.current_lng,
                        origin_lat=truck.current_lat,
                        dest_lng=dest_coords[0],
                        dest_lat=dest_coords[1]
                    )
                    if route_direct:
                        points.extend(self._emit_driving(truck, route_direct, trip_id, trip_type))
            else:
                route = self.router.get_route(
                    origin_id=wh_id,
                    dest_id=dest_id,
                    origin_lng=truck.current_lng,
                    origin_lat=truck.current_lat,
                    dest_lng=dest_coords[0],
                    dest_lat=dest_coords[1]
                )
                if route:
                    points.extend(self._emit_driving(truck, route, trip_id, trip_type))
        else:
            route = self.router.get_route(
                origin_id=wh_id,
                dest_id=dest_id,
                origin_lng=truck.current_lng,
                origin_lat=truck.current_lat,
                dest_lng=dest_coords[0],
                dest_lat=dest_coords[1]
            )
            
            if route is None:
                return points
            
            points.extend(self._emit_driving(truck, route, trip_id, trip_type))
        
        dest_dwell = self._get_dwell_duration(truck, 'destination', None, day) * 0.5
        points.extend(self._emit_micro_movement(truck, dest_dwell, TruckState.DWELL_DESTINATION, dest_id))
        
        if truck.minutes_since_break >= 270:
            break_duration = self._get_dwell_duration(truck, 'mandatory_break', None, day)
            points.extend(self._emit_micro_movement(truck, break_duration, TruckState.DWELL_REST_STOP, None))
            truck.minutes_since_break = 0
        
        elif self.rng.random() < 0.25:
            short_break = self._get_dwell_duration(truck, 'short_break', None, day)
            points.extend(self._emit_micro_movement(truck, short_break, TruckState.DWELL_REST_STOP, None))
        
        return points
    
    def _emit_aborted_trip(
        self,
        truck: ForensicTruck,
        day: date,
        trip_num: int
    ) -> List[dict]:
        """Emit an aborted trip (operational noise)."""
        points = []
        
        trip_id = f"{day.strftime('%Y%m%d')}-{truck.truck_id}-{trip_num:02d}-ABORT"
        truck.current_trip_id = trip_id
        
        # Short dwell
        dwell = self.rng.uniform(10, 25)
        points.extend(self._emit_micro_movement(truck, dwell, TruckState.DWELL_WAREHOUSE, truck.home_base_id))
        
        # Short movement (5-20 km)
        short_dist = self.rng.uniform(5, 20)
        short_duration = short_dist / 40 * 60  # ~40 km/h avg
        
        for _ in range(int(short_duration / 0.5)):
            truck.current_lat += self.rng.normal(0, 0.002)
            truck.current_lng += self.rng.normal(0, 0.002)
            
            point = {
                'telemetry_id': str(self.rng.integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': trip_id,
                'timestamp': truck.current_time,
                'latitude': truck.current_lat,
                'longitude': truck.current_lng,
                'speed_kmh': self.rng.uniform(25, 50),
                'heading_deg': self.rng.uniform(0, 360),
                'posted_speed_kmh': 50,
                'status': 'MOVING',
                'is_speeding': False,
                'is_hos_violation': False,
                'is_detour': False,
                'gps_accuracy_m': self.rng.uniform(5, 15),
                'location_id': None,
                'location_type': None
            }
            points.append(point)
            truck.current_time += timedelta(seconds=30)
        
        # Long dwell (aborted - waiting)
        abort_dwell = self.rng.uniform(60, 180)
        points.extend(self._emit_micro_movement(truck, abort_dwell, TruckState.DWELL_REST_STOP, None))
        
        return points
    
    def _emit_end_of_day(self, truck: ForensicTruck, day: date) -> List[dict]:
        """Emit overnight telemetry."""
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
        
        overnight_duration = self._get_dwell_duration(truck, 'overnight', None, day)
        return self._emit_micro_movement(truck, overnight_duration, state, 
                                         truck.home_base_id if state == TruckState.OVERNIGHT_HOME else None)
    
    def _emit_inactive_day(self, truck: ForensicTruck, day: date) -> List[dict]:
        """Emit sparse telemetry for inactive day."""
        points = []
        truck.current_time = datetime.combine(day, time(8, 0))
        truck.current_state = TruckState.OVERNIGHT_HOME
        
        num_pings = int(self.rng.integers(3, 7))
        
        for _ in range(num_pings):
            point = {
                'telemetry_id': str(self.rng.integers(1e15)),
                'truck_id': truck.truck_id,
                'driver_id': truck.driver_id,
                'trip_id': f"INACTIVE-{truck.truck_id}-{day.strftime('%Y%m%d')}",
                'timestamp': truck.current_time,
                'latitude': truck.home_coords[1] + self.rng.normal(0, 0.00002),
                'longitude': truck.home_coords[0] + self.rng.normal(0, 0.00002),
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
            truck.current_time += timedelta(hours=float(self.rng.uniform(2, 5)))
        
        return points
    
    def _inject_out_of_order(self, points: List[dict]):
        """Inject occasional out-of-order events."""
        if len(points) < 100:
            return
        
        num_swaps = int(len(points) * self.device_artifacts['out_of_order'])
        
        for _ in range(num_swaps):
            idx = self.rng.integers(10, len(points) - 10)
            offset = self.rng.integers(2, 6)
            
            # Swap timestamps
            if idx + offset < len(points):
                points[idx]['timestamp'], points[idx + offset]['timestamp'] = \
                    points[idx + offset]['timestamp'], points[idx]['timestamp']


def load_data():
    """Load data from Snowflake with geographic diversity."""
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name='airpublic')
    
    logger.info("Loading warehouses...")
    warehouses = pd.read_sql("""
        SELECT ID AS warehouse_id, NAME, LAT AS latitude, LNG AS longitude, CITY
        FROM FLEET_DEMOS.ROUTING.GERMANY_DESTINATIONS
        WHERE LOCATION_TYPE = 'WAREHOUSE'
        ORDER BY RANDOM()
        LIMIT 200
    """, conn)
    
    logger.info("Loading destinations...")
    destinations = pd.read_sql("""
        SELECT ID AS destination_id, NAME, LAT AS latitude, LNG AS longitude, LOCATION_TYPE, CITY
        FROM FLEET_DEMOS.ROUTING.GERMANY_DESTINATIONS 
        ORDER BY RANDOM()
        LIMIT 1000
    """, conn)
    
    logger.info("Loading rest stops...")
    rest_stops = pd.read_sql("""
        SELECT REST_STOP_ID AS parking_id, NAME, LAT AS latitude, LNG AS longitude, 'REST_STOP' AS parking_type
        FROM FLEET_DEMOS.ROUTING.GERMANY_REST_STOPS
    """, conn)
    
    logger.info("Loading HGV parkings...")
    hgv_parkings = pd.read_sql("""
        SELECT 
            'HGV-' || ROW_NUMBER() OVER (ORDER BY ST_Y(ST_CENTROID(GEOMETRY))) AS parking_id,
            'HGV Parking' AS name,
            ST_Y(ST_CENTROID(GEOMETRY)) AS latitude,
            ST_X(ST_CENTROID(GEOMETRY)) AS longitude,
            'HGV_PARKING' AS parking_type
        FROM FLEET_DEMOS.ROUTING.GERMANY_HGV_PARKINGS
    """, conn)
    
    conn.close()
    return warehouses, destinations, rest_stops, hgv_parkings


def create_trucks(warehouses: pd.DataFrame, num_trucks: int = 10) -> List[ForensicTruck]:
    """Create fleet with Pareto utilization distribution and diverse home bases."""
    trucks = []
    rng = np.random.default_rng(42)
    
    warehouses.columns = warehouses.columns.str.lower()
    
    # Shuffle warehouses for diverse assignment
    shuffled_warehouses = warehouses.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Generate Pareto-distributed utilization tiers
    # Bottom 10% low, middle 70% normal, top 20% high
    utilization_tiers = []
    for i in range(num_trucks):
        roll = rng.random()
        if roll < 0.10:
            tier = rng.uniform(0.5, 0.7)   # Low utilization
        elif roll < 0.80:
            tier = rng.uniform(0.85, 1.15) # Normal
        else:
            tier = rng.uniform(1.3, 1.6)   # High utilization
        utilization_tiers.append(tier)
    
    # Truck types
    type_dist = [(TruckType.REGIONAL, 0.65), (TruckType.LONG_HAUL, 0.25), (TruckType.LOW_UTIL, 0.10)]
    
    # Driver profiles
    profiles = {
        'COMPLIANT': DriverProfile(profile_type=ProfileType.COMPLIANT, speeding_probability=0.03,
                                   hos_violation_probability=0.02, detour_probability=0.08, speed_variance=0.06),
        'MILD': DriverProfile(profile_type=ProfileType.MILD, speeding_probability=0.08,
                              hos_violation_probability=0.05, detour_probability=0.15, speed_variance=0.10),
        'OUTLIER': DriverProfile(profile_type=ProfileType.OUTLIER, speeding_probability=0.18,
                                 hos_violation_probability=0.12, detour_probability=0.25, speed_variance=0.15)
    }
    
    for i in range(num_trucks):
        # Truck type
        roll = rng.random()
        cumulative = 0
        truck_type = TruckType.REGIONAL
        for tt, prob in type_dist:
            cumulative += prob
            if roll < cumulative:
                truck_type = tt
                break
        
        # Driver profile
        profile_name = rng.choice(['COMPLIANT', 'MILD', 'OUTLIER'], p=[0.70, 0.25, 0.05])
        
        # Persistent behavioral signature
        behavior = DriverBehavior(
            driver_id=f"DRV-{i+1:04d}",
            speeding_tendency=rng.uniform(-0.3, 0.3),
            dwell_tendency=rng.uniform(-0.2, 0.2),
            hos_compliance=rng.uniform(-0.2, 0.2),
            route_preference=rng.uniform(-0.3, 0.3)
        )
        
        home = shuffled_warehouses.iloc[i % len(shuffled_warehouses)]
        
        trucks.append(ForensicTruck(
            truck_id=f"TRK-{i+1:04d}",
            driver_id=f"DRV-{i+1:04d}",
            truck_type=truck_type,
            profile=profiles[profile_name],
            behavior=behavior,
            home_base_id=str(home['warehouse_id']),
            home_coords=(home['longitude'], home['latitude']),
            utilization_tier=utilization_tiers[i]
        ))
    
    return trucks


def save_to_snowflake(df: pd.DataFrame):
    """Save to Snowflake."""
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name='airpublic')
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE IF EXISTS FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST")
    
    logger.info(f"Inserting {len(df):,} records...")
    
    batch_size = 5000
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start+batch_size]
        values = []
        for _, row in batch.iterrows():
            ts = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            loc_id = f"'{row['location_id']}'" if row['location_id'] else 'NULL'
            loc_type = f"'{row['location_type']}'" if row['location_type'] else 'NULL'
            v = f"('{row['telemetry_id']}','{row['truck_id']}','{row['driver_id']}','{row['trip_id']}','{ts}',{row['latitude']},{row['longitude']},{row['speed_kmh']},{row['heading_deg']},{row['posted_speed_kmh']},'{row['status']}',{str(row['is_speeding']).upper()},{str(row['is_hos_violation']).upper()},{str(row['is_detour']).upper()},{row['gps_accuracy_m']},{loc_id},{loc_type})"
            values.append(v)
        
        sql = f"INSERT INTO FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST (TELEMETRY_ID,TRUCK_ID,DRIVER_ID,TRIP_ID,TS,LATITUDE,LONGITUDE,SPEED_KMH,HEADING_DEG,POSTED_SPEED_KMH,STATUS,IS_SPEEDING,IS_HOS_VIOLATION,IS_DETOUR,GPS_ACCURACY_M,LOCATION_ID,LOCATION_TYPE) VALUES {','.join(values)}"
        cursor.execute(sql)
        
        if (start + batch_size) % 50000 == 0:
            logger.info(f"  Inserted {min(start+batch_size, len(df)):,}/{len(df):,}...")
    
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved {len(df):,} records")


class IncrementalSaver:
    """Handles incremental saves to Snowflake."""
    
    def __init__(self, connection_name: str = 'airpublic'):
        self.connection_name = connection_name
        self.conn = None
        self.cursor = None
        self.total_inserted = 0
        
    def __enter__(self):
        import snowflake.connector
        self.conn = snowflake.connector.connect(connection_name=self.connection_name)
        self.cursor = self.conn.cursor()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def save(self, points: list, truncate: bool = False):
        """Save points to Snowflake."""
        if not points:
            return
            
        if truncate:
            self.cursor.execute("TRUNCATE TABLE IF EXISTS FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST")
            logger.info("Truncated table")
        
        df = pd.DataFrame(points)
        batch_size = 5000
        
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start+batch_size]
            values = []
            for _, row in batch.iterrows():
                ts = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                loc_id = f"'{row['location_id']}'" if row['location_id'] else 'NULL'
                loc_type = f"'{row['location_type']}'" if row['location_type'] else 'NULL'
                v = f"('{row['telemetry_id']}','{row['truck_id']}','{row['driver_id']}','{row['trip_id']}','{ts}',{row['latitude']},{row['longitude']},{row['speed_kmh']},{row['heading_deg']},{row['posted_speed_kmh']},'{row['status']}',{str(row['is_speeding']).upper()},{str(row['is_hos_violation']).upper()},{str(row['is_detour']).upper()},{row['gps_accuracy_m']},{loc_id},{loc_type})"
                values.append(v)
            
            sql = f"INSERT INTO FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST (TELEMETRY_ID,TRUCK_ID,DRIVER_ID,TRIP_ID,TS,LATITUDE,LONGITUDE,SPEED_KMH,HEADING_DEG,POSTED_SPEED_KMH,STATUS,IS_SPEEDING,IS_HOS_VIOLATION,IS_DETOUR,GPS_ACCURACY_M,LOCATION_ID,LOCATION_TYPE) VALUES {','.join(values)}"
            self.cursor.execute(sql)
        
        self.conn.commit()
        self.total_inserted += len(df)
        logger.info(f"  Saved {len(df):,} points (total in DB: {self.total_inserted:,})")


def main():
    print("=" * 70)
    print("FORENSIC-GRADE TELEMETRY GENERATOR")
    print("Production-indistinguishable synthetic data")
    print("=" * 70)
    
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'calibrated_config.yml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    warehouses, destinations, rest_stops, hgv_parkings = load_data()
    trucks = create_trucks(warehouses, num_trucks=500)
    
    logger.info(f"Created {len(trucks)} trucks:")
    for tt in TruckType:
        count = sum(1 for t in trucks if t.truck_type == tt)
        logger.info(f"  {tt.value}: {count}")
    
    # Utilization distribution
    tiers = [t.utilization_tier for t in trucks]
    logger.info(f"Utilization tiers: min={min(tiers):.2f}, max={max(tiers):.2f}, mean={np.mean(tiers):.2f}")
    
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name='airpublic')
    router = ORSRouter(config, connection=conn)
    
    generator = ForensicTelemetryGenerator(
        config=config, trucks=trucks, router=router,
        warehouses=warehouses, destinations=destinations, 
        rest_stops=rest_stops, hgv_parkings=hgv_parkings
    )
    
    start_date = date(2025, 12, 27)  # Batch 2: regenerate remaining broken days
    end_date = date(2025, 12, 31)
    
    logger.info(f"\nGenerating {start_date} to {end_date}...")
    
    # Use incremental saver - writes after each day
    with IncrementalSaver() as saver:
        total_points = generator.generate(
            start_date, end_date, 
            save_callback=saver.save,
            truncate_first=False
        )
    
    if total_points == 0:
        print("No points generated!")
        return
    
    # Query final stats from Snowflake
    print("\n" + "=" * 70)
    print("GENERATION COMPLETE")
    print("=" * 70)
    print(f"\nTotal points generated: {total_points:,}")
    
    # Get stats from DB
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST")
    db_count = cursor.fetchone()[0]
    print(f"Records in database: {db_count:,}")
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("Data saved to FLEET_DEMOS.ROUTING.FACT_TRUCK_TELEMETRY_TEST")
    print("=" * 70)
    print("\nDone!")


if __name__ == "__main__":
    main()
