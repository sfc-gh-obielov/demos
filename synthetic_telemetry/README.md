# Synthetic Truck Telemetry Generator

Generates realistic GPS telemetry data for HGV trucks operating in Germany. Uses real POI locations from Overture Maps, road-following routes from OpenRouteService (ORS), and realistic driver behavior profiles.

## Features

- **Real POI Data**: Warehouses, retail stores, and rest stops from Overture Maps
- **Road-Following Routes**: HGV-optimized routes via ORS (not straight lines)
- **Realistic Driver Behavior**: 
  - 92% COMPLIANT, 6% MILD, 2% OUTLIER profiles
  - Event-level anomalies (speeding, HOS violations, detours)
- **EU Regulations**: 9h max driving, 45min mandatory breaks after 4.5h
- **Variable Telemetry**: 
  - Ping intervals: 20-90s moving, 5-20min stopped
  - GPS jitter: 3-15m typical, rare 50-200m spikes
  - Occasional telemetry gaps (1% of segments)
- **Memory-Efficient**: Daily chunked processing with Parquet staging
- **Deterministic**: Seeded RNG for reproducible results

## Directory Structure

```
synthetic_telemetry/
├── config/
│   └── config.yml          # All configurable parameters
├── src/
│   ├── overture.py         # POI loader (Snowflake + fallback)
│   ├── routing.py          # ORS integration with caching
│   ├── driver_profiles.py  # Behavior mixture model
│   ├── simulate.py         # Trip and telemetry generation
│   ├── snowflake_io.py     # DDL, Parquet, COPY INTO
│   └── qa.py               # Validation queries
├── output/                 # Parquet files (generated)
├── cache/                  # Route cache (SQLite)
├── main.py                 # CLI entry point
└── README.md
```

## Prerequisites

```bash
pip install snowflake-connector-python pandas numpy pyarrow pyyaml python-dateutil
```

## Usage

### 1. Setup Snowflake Tables

```bash
python main.py setup --config config/config.yml
```

Creates:
- `DIM_WAREHOUSE` - Warehouse locations
- `DIM_STOP` - Rest stop locations
- `DIM_TRUCK` - Truck fleet with driver profiles
- `DIM_DRIVER` - Driver profile details
- `FACT_TRIP` - Trip metadata
- `FACT_TRUCK_TELEMETRY` - GPS telemetry (clustered by date, truck)
- `FACT_VIOLATION` - Speeding and HOS violation records

### 2. Generate Telemetry

```bash
# Generate to Parquet only
python main.py generate --config config/config.yml

# Generate and load to Snowflake
python main.py generate --config config/config.yml --load
```

### 3. Run QA Validation

```bash
python main.py qa --config config/config.yml --output qa_results.csv
```

Validates:
- Row counts within expected ranges
- Temporal coverage (3 months, >95% days)
- Spatial bounds (Germany bbox)
- Speeding rate (2-15%)
- HOS violation rate (0.5-5%)
- Detour rate (5-35%)
- Route quality (avg gap <2km between points)
- Null rates (<1% for critical columns)

## Configuration

All parameters are configurable in `config/config.yml`:

```yaml
seed: 42                    # Deterministic RNG seed

fleet:
  num_trucks: 500
  weekday_operating_rate: 0.85
  weekend_operating_rate: 0.40

driver_profiles:
  COMPLIANT:
    proportion: 0.92
    speeding_probability: 0.02
  MILD:
    proportion: 0.06
    speeding_probability: 0.12
  OUTLIER:
    proportion: 0.02
    speeding_probability: 0.25

telemetry:
  ping_interval:
    moving: {min_sec: 20, max_sec: 90}
    stopped: {min_sec: 300, max_sec: 1200}
  gps_jitter:
    typical_m: 10
    multipath_probability: 0.02
```

## Data Quality Notes

- Routes follow actual roads (not straight lines)
- Average point gap ~500m indicates road-following
- Speeding flags based on HGV posted limits (80km/h motorway)
- HOS violations only for OUTLIER profiles exceeding 9h/day
- Rest stops inserted along route corridors (30km buffer)

## Output Schema

### FACT_TRUCK_TELEMETRY

| Column | Type | Description |
|--------|------|-------------|
| TELEMETRY_ID | VARCHAR | Unique ping ID |
| TRUCK_ID | VARCHAR | Truck identifier |
| TRIP_ID | VARCHAR | Trip identifier |
| TS | TIMESTAMP_NTZ | Ping timestamp |
| LATITUDE | FLOAT | GPS latitude |
| LONGITUDE | FLOAT | GPS longitude |
| SPEED_KMH | FLOAT | Speed in km/h |
| POSTED_SPEED_KMH | FLOAT | Posted speed limit |
| STATUS | VARCHAR | MOVING, DWELL_WAREHOUSE, DWELL_STOP |
| IS_SPEEDING | BOOLEAN | Speed > posted * 1.08 |
| IS_HOS_VIOLATION | BOOLEAN | >9h driving today |
| IS_DETOUR | BOOLEAN | Alternative route taken |

## Snowflake Dependencies

- **ORS Native App**: `OPENROUTESERVICE_NATIVE_APP.CORE.DIRECTIONS`
- **Overture Maps**: Access to POI tables (or uses fallback generators)
- **Connection**: Uses `SNOWFLAKE_CONNECTION_NAME` env var or config

## Extending

To add new regions:
1. Update `region.bbox` in config
2. Load new POI data to Snowflake
3. Adjust speed limits in `routing.posted_speeds`

To modify driver behavior:
1. Add/modify profiles in `driver_profiles` config section
2. Adjust probabilities for desired anomaly rates
