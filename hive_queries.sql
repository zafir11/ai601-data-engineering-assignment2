CREATE EXTERNAL TABLE IF NOT EXISTS raw_user_logs (
    user_id INT,
    content_id INT,
    action STRING,
    timestamp STRING,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/raw/logs/';

ALTER TABLE raw_user_logs ADD PARTITION (year=2023, month=9, day=1) LOCATION '/raw/logs/2023/09/01';
CREATE EXTERNAL TABLE IF NOT EXISTS raw_content_metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/raw/metadata/';
CREATE TABLE fact_user_actions (
    user_id INT,
    content_id INT,
    action STRING,
    timestamp TIMESTAMP,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;
CREATE TABLE dim_content (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
STORED AS PARQUET;
INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT user_id, content_id, action, 
       CAST(timestamp AS TIMESTAMP), device, region, session_id,
       year(CAST(timestamp AS TIMESTAMP)), month(CAST(timestamp AS TIMESTAMP)), day(CAST(timestamp AS TIMESTAMP))
FROM raw_user_logs;
INSERT OVERWRITE TABLE dim_content
SELECT content_id, title, category, length, artist FROM raw_content_metadata;
SELECT year, month, region, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
GROUP BY year, month, region
ORDER BY year, month, active_users DESC;
SELECT c.category, COUNT(*) AS play_count
FROM fact_user_actions f
JOIN dim_content c ON f.content_id = c.content_id
WHERE action = 'play'
GROUP BY c.category
ORDER BY play_count DESC
LIMIT 5;
SELECT year, WEEKOFYEAR(timestamp) AS week, 
       AVG(UNIX_TIMESTAMP(MAX(timestamp)) - UNIX_TIMESTAMP(MIN(timestamp))) AS avg_session_length
FROM fact_user_actions
GROUP BY year, WEEKOFYEAR(timestamp)
ORDER BY year, week;
