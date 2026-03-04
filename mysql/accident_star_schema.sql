CREATE DATABASE accident_intelligence;
USE accident_intelligence;

CREATE TABLE dim_location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    location_name VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);

CREATE TABLE dim_time (
    time_id INT AUTO_INCREMENT PRIMARY KEY,
    day_of_week INT,
    day_name VARCHAR(20),
    hour_of_day INT
);

CREATE TABLE dim_infrastructure (
    infrastructure_id INT AUTO_INCREMENT PRIMARY KEY,
    road_type VARCHAR(50),
    lane_count INT,
    signal_status VARCHAR(50),
    enforcement_level VARCHAR(50),
    speed_limit_bucket VARCHAR(50),
    blackspot_bucket VARCHAR(50)
);

CREATE TABLE dim_environment (
    environment_id INT AUTO_INCREMENT PRIMARY KEY,
    weather VARCHAR(50),
    lighting VARCHAR(50),
    speed_bucket VARCHAR(50),
    traffic_bucket VARCHAR(50)
);

CREATE TABLE fact_accident (
    accident_id INT AUTO_INCREMENT PRIMARY KEY,
    location_id INT,
    time_id INT,
    infrastructure_id INT,
    environment_id INT,
    severity VARCHAR(50),
    cause VARCHAR(100),
    vehicles_involved INT,
    veh_count_at_accident INT,
    blackspot_score DECIMAL(5,3),
    avg_speed_kmph DECIMAL(6,2),
    speed_limit_kmph INT,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (infrastructure_id) REFERENCES dim_infrastructure(infrastructure_id),
    FOREIGN KEY (environment_id) REFERENCES dim_environment(environment_id)
);

CREATE TABLE staging_accident (
    location_name VARCHAR(100),
    road_type VARCHAR(50),
    lane_count INT,
    speed_limit_kmph INT,
    signal_status VARCHAR(50),
    enforcement_level VARCHAR(50),
    blackspot_score DECIMAL(5,3),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    day_of_week INT,
    hour_of_day INT,
    lighting VARCHAR(50),
    weather VARCHAR(50),
    avg_speed_kmph DECIMAL(6,2),
    severity VARCHAR(50),
    vehicles_involved INT,
    cause VARCHAR(100),
    veh_count_at_accident INT,
    speed_bucket VARCHAR(50),
    traffic_bucket VARCHAR(50),
    blackspot_bucket VARCHAR(50),
    speed_limit_bucket VARCHAR(50),
    day_name VARCHAR(20)
);

SELECT * FROM staging_accident;

INSERT INTO dim_location (location_name, latitude, longitude)
SELECT DISTINCT location_name, latitude, longitude
FROM staging_accident;

INSERT INTO dim_time (day_of_week, day_name, hour_of_day)
SELECT DISTINCT day_of_week, day_name, hour_of_day
FROM staging_accident;

INSERT INTO dim_infrastructure
(road_type, lane_count, signal_status, enforcement_level,
 speed_limit_bucket, blackspot_bucket)
SELECT DISTINCT
road_type, lane_count, signal_status, enforcement_level,
speed_limit_bucket, blackspot_bucket
FROM staging_accident;

INSERT INTO dim_environment
(weather, lighting, speed_bucket, traffic_bucket)
SELECT DISTINCT
weather, lighting, speed_bucket, traffic_bucket
FROM staging_accident;

INSERT INTO fact_accident
(location_id, time_id, infrastructure_id, environment_id,
 severity, cause, vehicles_involved, veh_count_at_accident,
 blackspot_score, avg_speed_kmph, speed_limit_kmph)

SELECT
l.location_id,
t.time_id,
i.infrastructure_id,
e.environment_id,
s.severity,
s.cause,
s.vehicles_involved,
s.veh_count_at_accident,
s.blackspot_score,
s.avg_speed_kmph,
s.speed_limit_kmph

FROM staging_accident s
JOIN dim_location l
  ON s.location_name = l.location_name
JOIN dim_time t
  ON s.day_of_week = t.day_of_week
 AND s.hour_of_day = t.hour_of_day
JOIN dim_infrastructure i
  ON s.road_type = i.road_type
 AND s.lane_count = i.lane_count
 AND s.signal_status = i.signal_status
 AND s.enforcement_level = i.enforcement_level
 AND s.speed_limit_bucket = i.speed_limit_bucket
 AND s.blackspot_bucket = i.blackspot_bucket
JOIN dim_environment e
  ON s.weather = e.weather
 AND s.lighting = e.lighting
 AND s.speed_bucket = e.speed_bucket
 AND s.traffic_bucket = e.traffic_bucket;

select * from fact_accident;
