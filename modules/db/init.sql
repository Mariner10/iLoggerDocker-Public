-- 1. Enable Extensions
-- These must run first.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. Create the Table (Using FLOAT instead of DOUBLE PRECISION)
CREATE TABLE IF NOT EXISTS device_readings (
    id BIGSERIAL, 
    device_id VARCHAR(255) NOT NULL,
    device_name VARCHAR(255) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    location GEOMETRY(Point, 4326), 
    battery_level INTEGER,
    discharge_rate FLOAT,          
    battery_status VARCHAR(50),
    low_power_mode BOOLEAN,
    positionType VARCHAR(25),
    speed_mps FLOAT,               
    course FLOAT,                  
    is_moving BOOLEAN,
    trip_id VARCHAR(255),
    move_delta FLOAT,              
    time_diff FLOAT,               
    timezone VARCHAR(50),
    distance_from_cluster_center FLOAT 
);

CREATE TABLE IF NOT EXISTS camera_recordings (
    id BIGSERIAL PRIMARY KEY,
    camera_name TEXT NOT NULL,
    video_path TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Convert to TimescaleDB Hypertable
-- We use a 'DO' block or perform a check to ensure we don't try to convert it twice
-- simpler method: use 'if_not_exists => TRUE' which is supported in newer Timescale versions
SELECT create_hypertable(
    'device_readings', 
    'recorded_at', 
    chunk_time_interval => INTERVAL '30 days', 
    if_not_exists => TRUE
);

-- 4. Create Indices
-- Using IF NOT EXISTS prevents errors on re-runs
CREATE INDEX IF NOT EXISTS idx_device_readings_location ON device_readings USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_device_readings_device_id ON device_readings (device_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_device_readings_trip_id ON device_readings (trip_id, recorded_at)
    WHERE trip_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_camera_recordings_time ON camera_recordings USING BRIN (recorded_at);

-- PresenceEngine tables
CREATE TABLE IF NOT EXISTS known_places (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255),
    label VARCHAR(255),
    auto_label VARCHAR(255),
    centroid GEOMETRY(Point, 4326),
    radius_m FLOAT,
    total_dwell_hours FLOAT,
    visit_count INT,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    typical_arrival TEXT,
    typical_departure TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_places_geo ON known_places USING GIST(centroid);
CREATE INDEX IF NOT EXISTS idx_places_device ON known_places(device_id);

CREATE TABLE IF NOT EXISTS place_transitions (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255),
    origin_place_id INT REFERENCES known_places(id) ON DELETE CASCADE,
    dest_place_id INT REFERENCES known_places(id) ON DELETE CASCADE,
    day_of_week INT,
    hour_bucket INT,
    trip_count INT,
    avg_duration_min FLOAT,
    avg_depart_minute INT,
    stddev_depart_minute FLOAT,
    probability FLOAT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trip_templates (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255),
    origin_place_id INT REFERENCES known_places(id) ON DELETE CASCADE,
    dest_place_id INT REFERENCES known_places(id) ON DELETE CASCADE,
    typical_route_geom GEOMETRY(LineString, 4326),
    avg_duration_sec INT,
    avg_distance_km FLOAT,
    sample_count INT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS routines (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255),
    name VARCHAR(255),
    origin_place_id INT REFERENCES known_places(id) ON DELETE CASCADE,
    dest_place_id INT REFERENCES known_places(id) ON DELETE CASCADE,
    days_of_week INT[],
    typical_depart_minute INT,
    depart_stddev_min FLOAT,
    typical_duration_min FLOAT,
    occurrence_count INT,
    confidence FLOAT,
    active BOOLEAN DEFAULT true,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);