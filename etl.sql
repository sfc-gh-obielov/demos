-- ============================================================
-- Fleet Demos ETL
-- ============================================================
-- Purpose: ETL queries for fleet demo tables
-- Database: FLEET_DEMOS.ROUTING
-- ============================================================

-- ============================================================
-- Country Reference Table
-- ============================================================
-- Purpose: Map ISO country codes to English names
-- Target: FLEET_DEMOS.ROUTING.COUNTRY_REFERENCE
-- Usage: Join with Overture Maps DIVISION_AREA for English country names
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.COUNTRY_REFERENCE (
    iso_code VARCHAR(2) PRIMARY KEY,
    english_name VARCHAR(100),
    official_name VARCHAR(200)
) AS
SELECT * FROM VALUES
    ('AF', 'Afghanistan', 'Islamic Republic of Afghanistan'),
    ('AL', 'Albania', 'Republic of Albania'),
    ('DZ', 'Algeria', 'People''s Democratic Republic of Algeria'),
    ('AD', 'Andorra', 'Principality of Andorra'),
    ('AO', 'Angola', 'Republic of Angola'),
    ('AG', 'Antigua and Barbuda', 'Antigua and Barbuda'),
    ('AR', 'Argentina', 'Argentine Republic'),
    ('AM', 'Armenia', 'Republic of Armenia'),
    ('AU', 'Australia', 'Commonwealth of Australia'),
    ('AT', 'Austria', 'Republic of Austria'),
    ('AZ', 'Azerbaijan', 'Republic of Azerbaijan'),
    ('BS', 'Bahamas', 'Commonwealth of the Bahamas'),
    ('BH', 'Bahrain', 'Kingdom of Bahrain'),
    ('BD', 'Bangladesh', 'People''s Republic of Bangladesh'),
    ('BB', 'Barbados', 'Barbados'),
    ('BY', 'Belarus', 'Republic of Belarus'),
    ('BE', 'Belgium', 'Kingdom of Belgium'),
    ('BZ', 'Belize', 'Belize'),
    ('BJ', 'Benin', 'Republic of Benin'),
    ('BT', 'Bhutan', 'Kingdom of Bhutan'),
    ('BO', 'Bolivia', 'Plurinational State of Bolivia'),
    ('BA', 'Bosnia and Herzegovina', 'Bosnia and Herzegovina'),
    ('BW', 'Botswana', 'Republic of Botswana'),
    ('BR', 'Brazil', 'Federative Republic of Brazil'),
    ('BN', 'Brunei', 'Brunei Darussalam'),
    ('BG', 'Bulgaria', 'Republic of Bulgaria'),
    ('BF', 'Burkina Faso', 'Burkina Faso'),
    ('BI', 'Burundi', 'Republic of Burundi'),
    ('KH', 'Cambodia', 'Kingdom of Cambodia'),
    ('CM', 'Cameroon', 'Republic of Cameroon'),
    ('CA', 'Canada', 'Canada'),
    ('CV', 'Cape Verde', 'Republic of Cabo Verde'),
    ('CF', 'Central African Republic', 'Central African Republic'),
    ('TD', 'Chad', 'Republic of Chad'),
    ('CL', 'Chile', 'Republic of Chile'),
    ('CN', 'China', 'People''s Republic of China'),
    ('CO', 'Colombia', 'Republic of Colombia'),
    ('KM', 'Comoros', 'Union of the Comoros'),
    ('CG', 'Congo', 'Republic of the Congo'),
    ('CD', 'Democratic Republic of the Congo', 'Democratic Republic of the Congo'),
    ('CR', 'Costa Rica', 'Republic of Costa Rica'),
    ('CI', 'Ivory Coast', 'Republic of Côte d''Ivoire'),
    ('HR', 'Croatia', 'Republic of Croatia'),
    ('CU', 'Cuba', 'Republic of Cuba'),
    ('CY', 'Cyprus', 'Republic of Cyprus'),
    ('CZ', 'Czech Republic', 'Czech Republic'),
    ('DK', 'Denmark', 'Kingdom of Denmark'),
    ('DJ', 'Djibouti', 'Republic of Djibouti'),
    ('DM', 'Dominica', 'Commonwealth of Dominica'),
    ('DO', 'Dominican Republic', 'Dominican Republic'),
    ('EC', 'Ecuador', 'Republic of Ecuador'),
    ('EG', 'Egypt', 'Arab Republic of Egypt'),
    ('SV', 'El Salvador', 'Republic of El Salvador'),
    ('GQ', 'Equatorial Guinea', 'Republic of Equatorial Guinea'),
    ('ER', 'Eritrea', 'State of Eritrea'),
    ('EE', 'Estonia', 'Republic of Estonia'),
    ('ET', 'Ethiopia', 'Federal Democratic Republic of Ethiopia'),
    ('FJ', 'Fiji', 'Republic of Fiji'),
    ('FI', 'Finland', 'Republic of Finland'),
    ('FR', 'France', 'French Republic'),
    ('GA', 'Gabon', 'Gabonese Republic'),
    ('GM', 'Gambia', 'Republic of the Gambia'),
    ('GE', 'Georgia', 'Georgia'),
    ('DE', 'Germany', 'Federal Republic of Germany'),
    ('GH', 'Ghana', 'Republic of Ghana'),
    ('GR', 'Greece', 'Hellenic Republic'),
    ('GD', 'Grenada', 'Grenada'),
    ('GT', 'Guatemala', 'Republic of Guatemala'),
    ('GN', 'Guinea', 'Republic of Guinea'),
    ('GW', 'Guinea-Bissau', 'Republic of Guinea-Bissau'),
    ('GY', 'Guyana', 'Co-operative Republic of Guyana'),
    ('HT', 'Haiti', 'Republic of Haiti'),
    ('HN', 'Honduras', 'Republic of Honduras'),
    ('HU', 'Hungary', 'Hungary'),
    ('IS', 'Iceland', 'Republic of Iceland'),
    ('IN', 'India', 'Republic of India'),
    ('ID', 'Indonesia', 'Republic of Indonesia'),
    ('IR', 'Iran', 'Islamic Republic of Iran'),
    ('IQ', 'Iraq', 'Republic of Iraq'),
    ('IE', 'Ireland', 'Ireland'),
    ('IL', 'Israel', 'State of Israel'),
    ('IT', 'Italy', 'Italian Republic'),
    ('JM', 'Jamaica', 'Jamaica'),
    ('JP', 'Japan', 'Japan'),
    ('JO', 'Jordan', 'Hashemite Kingdom of Jordan'),
    ('KZ', 'Kazakhstan', 'Republic of Kazakhstan'),
    ('KE', 'Kenya', 'Republic of Kenya'),
    ('KI', 'Kiribati', 'Republic of Kiribati'),
    ('KP', 'North Korea', 'Democratic People''s Republic of Korea'),
    ('KR', 'South Korea', 'Republic of Korea'),
    ('KW', 'Kuwait', 'State of Kuwait'),
    ('KG', 'Kyrgyzstan', 'Kyrgyz Republic'),
    ('LA', 'Laos', 'Lao People''s Democratic Republic'),
    ('LV', 'Latvia', 'Republic of Latvia'),
    ('LB', 'Lebanon', 'Lebanese Republic'),
    ('LS', 'Lesotho', 'Kingdom of Lesotho'),
    ('LR', 'Liberia', 'Republic of Liberia'),
    ('LY', 'Libya', 'State of Libya'),
    ('LI', 'Liechtenstein', 'Principality of Liechtenstein'),
    ('LT', 'Lithuania', 'Republic of Lithuania'),
    ('LU', 'Luxembourg', 'Grand Duchy of Luxembourg'),
    ('MK', 'North Macedonia', 'Republic of North Macedonia'),
    ('MG', 'Madagascar', 'Republic of Madagascar'),
    ('MW', 'Malawi', 'Republic of Malawi'),
    ('MY', 'Malaysia', 'Malaysia'),
    ('MV', 'Maldives', 'Republic of Maldives'),
    ('ML', 'Mali', 'Republic of Mali'),
    ('MT', 'Malta', 'Republic of Malta'),
    ('MH', 'Marshall Islands', 'Republic of the Marshall Islands'),
    ('MR', 'Mauritania', 'Islamic Republic of Mauritania'),
    ('MU', 'Mauritius', 'Republic of Mauritius'),
    ('MX', 'Mexico', 'United Mexican States'),
    ('FM', 'Micronesia', 'Federated States of Micronesia'),
    ('MD', 'Moldova', 'Republic of Moldova'),
    ('MC', 'Monaco', 'Principality of Monaco'),
    ('MN', 'Mongolia', 'Mongolia'),
    ('ME', 'Montenegro', 'Montenegro'),
    ('MA', 'Morocco', 'Kingdom of Morocco'),
    ('MZ', 'Mozambique', 'Republic of Mozambique'),
    ('MM', 'Myanmar', 'Republic of the Union of Myanmar'),
    ('NA', 'Namibia', 'Republic of Namibia'),
    ('NR', 'Nauru', 'Republic of Nauru'),
    ('NP', 'Nepal', 'Federal Democratic Republic of Nepal'),
    ('NL', 'Netherlands', 'Kingdom of the Netherlands'),
    ('NZ', 'New Zealand', 'New Zealand'),
    ('NI', 'Nicaragua', 'Republic of Nicaragua'),
    ('NE', 'Niger', 'Republic of Niger'),
    ('NG', 'Nigeria', 'Federal Republic of Nigeria'),
    ('NO', 'Norway', 'Kingdom of Norway'),
    ('OM', 'Oman', 'Sultanate of Oman'),
    ('PK', 'Pakistan', 'Islamic Republic of Pakistan'),
    ('PW', 'Palau', 'Republic of Palau'),
    ('PS', 'Palestine', 'State of Palestine'),
    ('PA', 'Panama', 'Republic of Panama'),
    ('PG', 'Papua New Guinea', 'Independent State of Papua New Guinea'),
    ('PY', 'Paraguay', 'Republic of Paraguay'),
    ('PE', 'Peru', 'Republic of Peru'),
    ('PH', 'Philippines', 'Republic of the Philippines'),
    ('PL', 'Poland', 'Republic of Poland'),
    ('PT', 'Portugal', 'Portuguese Republic'),
    ('QA', 'Qatar', 'State of Qatar'),
    ('RO', 'Romania', 'Romania'),
    ('RU', 'Russia', 'Russian Federation'),
    ('RW', 'Rwanda', 'Republic of Rwanda'),
    ('KN', 'Saint Kitts and Nevis', 'Saint Kitts and Nevis'),
    ('LC', 'Saint Lucia', 'Saint Lucia'),
    ('VC', 'Saint Vincent and the Grenadines', 'Saint Vincent and the Grenadines'),
    ('WS', 'Samoa', 'Independent State of Samoa'),
    ('SM', 'San Marino', 'Republic of San Marino'),
    ('ST', 'Sao Tome and Principe', 'Democratic Republic of São Tomé and Príncipe'),
    ('SA', 'Saudi Arabia', 'Kingdom of Saudi Arabia'),
    ('SN', 'Senegal', 'Republic of Senegal'),
    ('RS', 'Serbia', 'Republic of Serbia'),
    ('SC', 'Seychelles', 'Republic of Seychelles'),
    ('SL', 'Sierra Leone', 'Republic of Sierra Leone'),
    ('SG', 'Singapore', 'Republic of Singapore'),
    ('SK', 'Slovakia', 'Slovak Republic'),
    ('SI', 'Slovenia', 'Republic of Slovenia'),
    ('SB', 'Solomon Islands', 'Solomon Islands'),
    ('SO', 'Somalia', 'Federal Republic of Somalia'),
    ('ZA', 'South Africa', 'Republic of South Africa'),
    ('SS', 'South Sudan', 'Republic of South Sudan'),
    ('ES', 'Spain', 'Kingdom of Spain'),
    ('LK', 'Sri Lanka', 'Democratic Socialist Republic of Sri Lanka'),
    ('SD', 'Sudan', 'Republic of the Sudan'),
    ('SR', 'Suriname', 'Republic of Suriname'),
    ('SZ', 'Eswatini', 'Kingdom of Eswatini'),
    ('SE', 'Sweden', 'Kingdom of Sweden'),
    ('CH', 'Switzerland', 'Swiss Confederation'),
    ('SY', 'Syria', 'Syrian Arab Republic'),
    ('TW', 'Taiwan', 'Taiwan'),
    ('TJ', 'Tajikistan', 'Republic of Tajikistan'),
    ('TZ', 'Tanzania', 'United Republic of Tanzania'),
    ('TH', 'Thailand', 'Kingdom of Thailand'),
    ('TL', 'Timor-Leste', 'Democratic Republic of Timor-Leste'),
    ('TG', 'Togo', 'Togolese Republic'),
    ('TO', 'Tonga', 'Kingdom of Tonga'),
    ('TT', 'Trinidad and Tobago', 'Republic of Trinidad and Tobago'),
    ('TN', 'Tunisia', 'Republic of Tunisia'),
    ('TR', 'Turkey', 'Republic of Turkey'),
    ('TM', 'Turkmenistan', 'Turkmenistan'),
    ('TV', 'Tuvalu', 'Tuvalu'),
    ('UG', 'Uganda', 'Republic of Uganda'),
    ('UA', 'Ukraine', 'Ukraine'),
    ('AE', 'United Arab Emirates', 'United Arab Emirates'),
    ('GB', 'United Kingdom', 'United Kingdom of Great Britain and Northern Ireland'),
    ('US', 'United States', 'United States of America'),
    ('UY', 'Uruguay', 'Oriental Republic of Uruguay'),
    ('UZ', 'Uzbekistan', 'Republic of Uzbekistan'),
    ('VU', 'Vanuatu', 'Republic of Vanuatu'),
    ('VA', 'Vatican City', 'Vatican City State'),
    ('VE', 'Venezuela', 'Bolivarian Republic of Venezuela'),
    ('VN', 'Vietnam', 'Socialist Republic of Vietnam'),
    ('YE', 'Yemen', 'Republic of Yemen'),
    ('ZM', 'Zambia', 'Republic of Zambia'),
    ('ZW', 'Zimbabwe', 'Republic of Zimbabwe'),
    ('XK', 'Kosovo', 'Republic of Kosovo'),
    ('AQ', 'Antarctica', 'Antarctica')
AS t(iso_code, english_name, official_name);


-- ============================================================
-- Countries Table with Geometries
-- ============================================================
-- Purpose: Country boundaries with English names and geometries
-- Source: OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA
-- Target: FLEET_DEMOS.ROUTING.COUNTRIES
-- 
-- Features:
--   - Country geometries (boundaries) from Overture Maps
--   - English names from COUNTRY_REFERENCE
--   - ISO country codes for joining
--   - Local/official names from Overture Maps
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.COUNTRIES AS
SELECT 
    d.id,
    d.COUNTRY as iso_code,
    c.english_name,
    c.official_name,
    d.names:primary::STRING as local_name,
    d.geometry,
    d.bbox,
    d.subtype,
    d.class,
    d.region
FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA d
LEFT JOIN FLEET_DEMOS.ROUTING.COUNTRY_REFERENCE c
    ON d.COUNTRY = c.iso_code
WHERE d.SUBTYPE = 'country'
AND d.class = 'land';


-- ============================================================
-- GEOLIFE Clean Routes Table (Step 1: Cleaning)
-- ============================================================
-- Purpose: Clean GPS trajectory data by removing entire trips with speed outliers
-- Source: AIR.PUBLIC.GEOLIFE (24,876,977 GPS trajectory points)
-- Target: FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
-- 
-- Cleaning Strategy:
--   - Calculate speed between consecutive GPS points
--   - Identify trips (UID/TID) containing ANY point with speed > 200 km/h
--   - Exclude entire trips that have outliers (maintains trip continuity)
--
-- Results:
--   - Original: 24.9M points, 18,670 trips
--   - Cleaned: 14.1M points, 15,163 trips (56.8% retention)
--   - Removed: 3,507 trips with outliers (18.8%)
--
-- Method:
--   - Uses LAG window function with ST_ASWKT serialization
--   - ST_DISTANCE calculates meters between consecutive points
--   - TIMESTAMPDIFF calculates seconds between consecutive points
--   - Speed conversion: meters/second * 3.6 = km/h
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN AS
WITH speed_calculated AS (
    SELECT 
        UID,
        TID,
        LAT,
        LNG,
        ZERO_COL,
        ALT,
        DAYNO,
        EVENT_DATE,
        EVENT_TIME,
        EVENT_TIMESTAMP,
        GEOMETRY,
        COALESCE(
            TIMESTAMPDIFF(
                SECOND,
                LAG(EVENT_TIMESTAMP) OVER (PARTITION BY UID, TID ORDER BY EVENT_TIMESTAMP),
                EVENT_TIMESTAMP
            ),
            0
        ) AS TIME_LAG,
        CASE 
            WHEN LAG(ST_ASWKT(GEOMETRY)) OVER (PARTITION BY UID, TID ORDER BY EVENT_TIMESTAMP) IS NULL 
            THEN 0
            WHEN TIMESTAMPDIFF(
                SECOND,
                LAG(EVENT_TIMESTAMP) OVER (PARTITION BY UID, TID ORDER BY EVENT_TIMESTAMP),
                EVENT_TIMESTAMP
            ) = 0 THEN 0
            ELSE (ST_DISTANCE(
                GEOMETRY,
                ST_GEOGRAPHYFROMWKT(
                    LAG(ST_ASWKT(GEOMETRY)) OVER (PARTITION BY UID, TID ORDER BY EVENT_TIMESTAMP)
                )
            ) / TIMESTAMPDIFF(
                SECOND,
                LAG(EVENT_TIMESTAMP) OVER (PARTITION BY UID, TID ORDER BY EVENT_TIMESTAMP),
                EVENT_TIMESTAMP
            )) * 3.6
        END AS SPEED
    FROM AIR.PUBLIC.GEOLIFE
),
trips_with_outliers AS (
    SELECT DISTINCT UID, TID
    FROM speed_calculated
    WHERE SPEED > 200
)
SELECT 
    s.UID,
    s.TID,
    s.LAT,
    s.LNG,
    s.ZERO_COL,
    s.ALT,
    s.DAYNO,
    s.EVENT_DATE,
    s.EVENT_TIME,
    s.EVENT_TIMESTAMP,
    s.GEOMETRY,
    s.TIME_LAG,
    s.SPEED
FROM speed_calculated s
WHERE NOT EXISTS (
    SELECT 1 
    FROM trips_with_outliers t 
    WHERE t.UID = s.UID AND t.TID = s.TID
)
ORDER BY s.UID, s.TID, s.EVENT_TIMESTAMP;


-- ============================================================
-- GEOLIFE Clean Routes Table (Step 2: Country Enrichment)
-- ============================================================
-- Purpose: Enrich clean GPS data with country information
-- Source: FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
-- Target: FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN (replaces existing)
-- 
-- Enrichment Strategy:
--   - Use ST_WITHIN geospatial join to match GPS points with country boundaries
--   - Add country_code (ISO 2-letter code)
--   - Add country_name (English name)
--
-- Method:
--   - LEFT JOIN with COUNTRIES table using ST_WITHIN
--   - Points outside country boundaries will have NULL country values
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN AS
SELECT 
    g.UID,
    g.TID,
    g.LAT,
    g.LNG,
    g.ZERO_COL,
    g.ALT,
    g.DAYNO,
    g.EVENT_DATE,
    g.EVENT_TIME,
    g.EVENT_TIMESTAMP,
    g.GEOMETRY,
    g.TIME_LAG,
    g.SPEED,
    c.iso_code as country_code,
    c.english_name as country_name
FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN g
LEFT JOIN FLEET_DEMOS.ROUTING.COUNTRIES c
    ON ST_WITHIN(g.GEOMETRY, c.geometry)
ORDER BY g.UID, g.TID, g.EVENT_TIMESTAMP;


-- ============================================================
-- GEOLIFE Clean Routes Table (Step 3: Transportation Mode Classification)
-- ============================================================
-- Purpose: Classify trips by transportation mode using rule-based approach
-- Source: FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
-- Target: FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN (replaces existing)
-- 
-- Classification Strategy:
--   - Calculate trip-level speed statistics (avg, max, median, p75)
--   - Apply hierarchical rules based on GeoLife research:
--     * Stationary: avg_speed < 1 km/h
--     * Walking: avg_speed 1-6 km/h, max_speed < 15 km/h
--     * Running: avg_speed 6-12 km/h, max_speed < 20 km/h
--     * Cycling: avg_speed 8-25 km/h, max_speed < 45 km/h
--     * Driving (urban): avg_speed 15-50 km/h, median < 60 km/h
--     * Driving (highway): avg_speed > 40 km/h, median >= 60 km/h
--     * Train/Airplane: avg_speed > 80 km/h
--
-- Method:
--   - CTE calculates trip statistics and applies classification rules
--   - LEFT JOIN to add transportation_mode to each GPS point
--
-- References:
--   - GeoLife GPS Trajectory Dataset (Microsoft Research)
--   - 73 users with manual labels: walk, bike, bus, car, subway, train, airplane
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN AS
WITH trip_statistics AS (
    SELECT 
        UID,
        TID,
        COUNT(*) as point_count,
        ROUND(AVG(SPEED), 2) as avg_speed,
        ROUND(MAX(SPEED), 2) as max_speed,
        ROUND(MIN(SPEED), 2) as min_speed,
        ROUND(STDDEV(SPEED), 2) as stddev_speed,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SPEED), 2) as median_speed,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY SPEED), 2) as p75_speed,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY SPEED), 2) as p25_speed
    FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN
    WHERE SPEED >= 0
    GROUP BY UID, TID
),
trip_classification AS (
    SELECT 
        UID,
        TID,
        point_count,
        avg_speed,
        max_speed,
        median_speed,
        CASE
            WHEN avg_speed < 1 THEN 'stationary'
            WHEN avg_speed >= 1 AND avg_speed < 6 AND max_speed < 15 THEN 'walking'
            WHEN avg_speed >= 6 AND avg_speed < 12 AND max_speed < 20 THEN 'running'
            WHEN avg_speed >= 8 AND avg_speed < 25 AND max_speed < 45 THEN 'cycling'
            WHEN avg_speed >= 80 OR (avg_speed > 60 AND median_speed > 80) THEN 'train_airplane'
            WHEN avg_speed >= 40 AND median_speed >= 60 THEN 'driving_highway'
            WHEN avg_speed >= 15 AND avg_speed < 80 THEN 'driving_urban'
            ELSE 'unknown'
        END AS transportation_mode
    FROM trip_statistics
)
SELECT 
    g.UID,
    g.TID,
    g.LAT,
    g.LNG,
    g.ZERO_COL,
    g.ALT,
    g.DAYNO,
    g.EVENT_DATE,
    g.EVENT_TIME,
    g.EVENT_TIMESTAMP,
    g.GEOMETRY,
    g.TIME_LAG,
    g.SPEED,
    g.country_code,
    g.country_name,
    tc.transportation_mode,
    tc.avg_speed as trip_avg_speed,
    tc.max_speed as trip_max_speed,
    tc.median_speed as trip_median_speed
FROM FLEET_DEMOS.ROUTING.GEOLIFE_CLEAN g
LEFT JOIN trip_classification tc
    ON g.UID = tc.UID AND g.TID = tc.TID
ORDER BY g.UID, g.TID, g.EVENT_TIMESTAMP;


-- ============================================================
-- HGV Parking Locations from Overture Maps (Worldwide)
-- ============================================================
-- Purpose: Extract HGV (Heavy Goods Vehicle) designated parking locations worldwide
-- Source: OVERTURE_MAPS__BASE.CARTO.INFRASTRUCTURE
-- Target: FLEET_DEMOS.ROUTING.HGV_PARKINGS
-- 
-- Data Strategy:
--   - Filter for transit parking infrastructure
--   - Extract locations with HGV tag = 'yes' or 'designated'
--   - Join with country boundaries for country attribution
--   - Only include polygon geometries (exclude points/lines)
--
-- Source Tags Structure:
--   - Uses LATERAL FLATTEN to parse nested key_value array
--   - Extracts 'hgv' tag value from OSM source_tags
--
-- Output Schema:
--   - english_name: Country name
--   - geometry: Parking location polygon
--
-- Use Cases:
--   - Fleet routing optimization
--   - Rest area planning
--   - Truck parking availability analysis
--   - Cross-border logistics planning
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.HGV_PARKINGS AS
SELECT 
    c.english_name, 
    l.geometry 
FROM OVERTURE_MAPS__BASE.CARTO.INFRASTRUCTURE l
INNER JOIN FLEET_DEMOS.ROUTING.COUNTRIES c
    ON ST_INTERSECTS(c.geometry, l.geometry)
, LATERAL FLATTEN(input => l.source_tags:key_value) hgv_tag
WHERE l.class ILIKE 'parking%'
    AND l.subtype ILIKE 'transit'
    AND ST_ASGEOJSON(l.geometry):type::string ILIKE 'POLYGON'
    AND hgv_tag.value:key::string = 'hgv'
    AND hgv_tag.value:value::string IN ('yes', 'designated');


-- ============================================================
-- Germany Retail Stores from Overture Maps
-- ============================================================
-- Purpose: Extract major retail store locations in Germany
-- Source: OVERTURE_MAPS__PLACES.CARTO.PLACE
-- Target: FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES
-- 
-- Brands Included:
--   - REWE (supermarket chain)
--   - LIDL (discount supermarket)
--   - ALDI (discount supermarket)
--   - NETTO (discount supermarket)
--   - PENNY (discount supermarket)
--   - EDEKA (supermarket chain)
--
-- Category Filtering:
--   - Only includes retail/grocery categories
--   - Excludes restaurants, bars, services, medical facilities
--   - Categories: grocery_store, supermarket, convenience_store,
--     discount_store, international_grocery_store, specialty_grocery_store,
--     health_food_store, wholesale_store, department_store
--
-- Output Schema:
--   - id: Overture Maps place ID
--   - store_name: Primary store name (e.g., "REWE City Markt Berlin")
--   - category: Store category (from Overture Maps)
--   - geometry: Point location
--   - addresses: Store addresses
--   - canonical_name: Normalized brand name (e.g., "REWE")
--
-- Use Cases:
--   - Last-mile delivery optimization
--   - Retail supply chain routing
--   - Store coverage analysis by brand
--   - Delivery time window planning
-- ============================================================

CREATE OR REPLACE TABLE FLEET_DEMOS.ROUTING.GERMANY_RETAIL_STORES AS
SELECT 
    p.id,
    p.names:primary::string as store_name,
    p.categories:primary::string as category,
    p.geometry,
    p.addresses,
    CASE
        WHEN p.names:primary::string ILIKE '%REWE%' THEN 'REWE'
        WHEN p.names:primary::string ILIKE '%LIDL%' THEN 'LIDL'
        WHEN p.names:primary::string ILIKE '%ALDI%' THEN 'ALDI'
        WHEN p.names:primary::string ILIKE '%NETTO%' THEN 'NETTO'
        WHEN p.names:primary::string ILIKE '%PENNY%' THEN 'PENNY'
        WHEN p.names:primary::string ILIKE '%EDEKA%' THEN 'EDEKA'
    END as canonical_name
FROM OVERTURE_MAPS__PLACES.CARTO.PLACE p
INNER JOIN FLEET_DEMOS.ROUTING.COUNTRIES c
    ON ST_INTERSECTS(c.geometry, p.geometry)
WHERE c.english_name = 'Germany'
    AND (
        p.names:primary::string ILIKE '%REWE%'
        OR p.names:primary::string ILIKE '%LIDL%'
        OR p.names:primary::string ILIKE '%ALDI%'
        OR p.names:primary::string ILIKE '%NETTO%'
        OR p.names:primary::string ILIKE '%PENNY%'
        OR p.names:primary::string ILIKE '%EDEKA%'
    )
    AND p.categories:primary::string IN (
        'grocery_store',
        'supermarket',
        'convenience_store',
        'discount_store',
        'international_grocery_store',
        'specialty_grocery_store',
        'health_food_store',
        'wholesale_store',
        'department_store'
    );
