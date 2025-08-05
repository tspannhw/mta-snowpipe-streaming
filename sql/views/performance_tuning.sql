-- ============================================================================
-- Snowpipe Streaming Performance Tuning and Optimization
-- MTA Real-time Transit Data Pipeline
-- ============================================================================

-- Use appropriate role and warehouse
USE ROLE SYSADMIN;
USE WAREHOUSE STREAMING_WH;
USE DATABASE DEMO;
USE SCHEMA DEMO;

-- ============================================================================
-- 1. WAREHOUSE OPTIMIZATION
-- ============================================================================

-- Optimize the streaming warehouse for high throughput
ALTER WAREHOUSE STREAMING_WH SET
    WAREHOUSE_SIZE = 'LARGE'              -- Increased size for better performance
    AUTO_SUSPEND = 60                     -- Quick suspend to save costs
    AUTO_RESUME = TRUE                    -- Auto-resume for streaming
    MIN_CLUSTER_COUNT = 2                 -- Minimum clusters for availability
    MAX_CLUSTER_COUNT = 6                 -- Scale up for peak loads
    SCALING_POLICY = 'STANDARD'           -- Standard scaling for consistent performance
    RESOURCE_MONITOR = 'STREAMING_MONITOR'; -- Resource monitoring

-- Create a dedicated warehouse for analytics queries
CREATE OR REPLACE WAREHOUSE ANALYTICS_WH WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY'
    COMMENT = 'Warehouse for analytical queries on MTA data';

-- ============================================================================
-- 2. TABLE OPTIMIZATION
-- ============================================================================

-- Add performance-optimized clustering keys
ALTER TABLE DEMO.DEMO.ICYMTA CLUSTER BY (DATE, LINEREF, VEHICLEREF);

-- Create search optimization for frequently queried columns
ALTER TABLE DEMO.DEMO.ICYMTA ADD SEARCH OPTIMIZATION ON EQUALITY(VEHICLEREF, LINEREF, STOPPOINTREF);
ALTER TABLE DEMO.DEMO.ICYMTA ADD SEARCH OPTIMIZATION ON SUBSTRING(STOPPOINTNAME, DESTINATIONNAME);
ALTER TABLE DEMO.DEMO.ICYMTA ADD SEARCH OPTIMIZATION ON GEO(VEHICLELOCATIONLATITUDE, VEHICLELOCATIONLONGITUDE);

-- ============================================================================
-- 3. MATERIALIZED VIEWS FOR PERFORMANCE
-- ============================================================================

-- Real-time vehicle positions view
CREATE OR REPLACE SECURE MATERIALIZED VIEW MV_REALTIME_VEHICLE_POSITIONS
    CLUSTER BY (LINEREF, RECORDEDATTIME)
AS
SELECT 
    VEHICLEREF,
    LINEREF,
    PUBLISHEDLINENAME,
    VEHICLELOCATIONLATITUDE,
    VEHICLELOCATIONLONGITUDE,
    BEARING,
    RECORDEDATTIME,
    PROGRESSSTATUS,
    DESTINATIONNAME,
    DISTANCEFROMSTOP,
    NUMBEROFSTOPSAWAY,
    DATE
FROM DEMO.DEMO.ICYMTA
WHERE MONITORED = TRUE
  AND VEHICLELOCATIONLATITUDE IS NOT NULL
  AND VEHICLELOCATIONLONGITUDE IS NOT NULL
  AND RECORDEDATTIME >= DATEADD('HOUR', -2, CURRENT_TIMESTAMP()); -- Last 2 hours

-- Aggregated line performance metrics
CREATE OR REPLACE SECURE MATERIALIZED VIEW MV_LINE_PERFORMANCE_METRICS
    CLUSTER BY (DATE, LINEREF)
AS
SELECT 
    DATE,
    LINEREF,
    PUBLISHEDLINENAME,
    COUNT(*) AS total_updates,
    COUNT(DISTINCT VEHICLEREF) AS active_vehicles,
    AVG(DISTANCEFROMSTOP) AS avg_distance_from_stop,
    COUNT(CASE WHEN PROGRESSSTATUS = 'onTime' THEN 1 END) AS on_time_count,
    COUNT(CASE WHEN PROGRESSSTATUS = 'delayed' THEN 1 END) AS delayed_count,
    AVG(ESTIMATEDPASSENGERCOUNT) AS avg_passenger_count,
    MIN(RECORDEDATTIME) AS first_update,
    MAX(RECORDEDATTIME) AS last_update
FROM DEMO.DEMO.ICYMTA
WHERE DATE >= DATEADD('DAY', -7, CURRENT_DATE()) -- Last 7 days
GROUP BY DATE, LINEREF, PUBLISHEDLINENAME;

-- Stop performance analytics
CREATE OR REPLACE SECURE MATERIALIZED VIEW MV_STOP_ANALYTICS
    CLUSTER BY (DATE, STOPPOINTREF)
AS
SELECT 
    DATE,
    STOPPOINTREF,
    STOPPOINTNAME,
    COUNT(*) AS total_arrivals,
    COUNT(DISTINCT VEHICLEREF) AS unique_vehicles,
    COUNT(DISTINCT LINEREF) AS lines_served,
    AVG(CASE 
        WHEN EXPECTEDDEPARTURETIME IS NOT NULL AND RECORDEDATTIME IS NOT NULL 
        THEN DATEDIFF('minute', RECORDEDATTIME, EXPECTEDDEPARTURETIME)
    END) AS avg_dwell_time_minutes,
    SUM(ESTIMATEDPASSENGERCOUNT) AS total_passenger_count
FROM DEMO.DEMO.ICYMTA
WHERE DATE >= DATEADD('DAY', -30, CURRENT_DATE()) -- Last 30 days
  AND STOPPOINTREF IS NOT NULL
GROUP BY DATE, STOPPOINTREF, STOPPOINTNAME;

-- ============================================================================
-- 4. PERFORMANCE INDEXES AND OPTIMIZATION
-- ============================================================================

-- Enable query acceleration for complex analytical queries
ALTER ACCOUNT SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;

-- Create optimized views for common query patterns
CREATE OR REPLACE VIEW V_CURRENT_VEHICLE_STATUS AS
SELECT 
    v.*,
    ROW_NUMBER() OVER (PARTITION BY VEHICLEREF ORDER BY RECORDEDATTIME DESC) as rn
FROM DEMO.DEMO.ICYMTA v
WHERE DATE = CURRENT_DATE()
QUALIFY rn = 1; -- Latest status for each vehicle

-- Line status summary view
CREATE OR REPLACE VIEW V_LINE_STATUS_SUMMARY AS
SELECT 
    LINEREF,
    PUBLISHEDLINENAME,
    COUNT(DISTINCT VEHICLEREF) AS active_vehicles,
    COUNT(*) AS total_updates,
    MAX(RECORDEDATTIME) AS last_update,
    AVG(CASE WHEN PROGRESSSTATUS = 'onTime' THEN 1.0 ELSE 0.0 END) * 100 AS on_time_percentage,
    COUNT(CASE WHEN DISTANCEFROMSTOP < 100 THEN 1 END) AS vehicles_at_stops
FROM V_CURRENT_VEHICLE_STATUS
GROUP BY LINEREF, PUBLISHEDLINENAME
ORDER BY LINEREF;

-- ============================================================================
-- 5. STREAMING OPTIMIZATION PARAMETERS
-- ============================================================================

-- Set optimal parameters for Iceberg streaming
ALTER TABLE DEMO.DEMO.ICYMTA SET CHANGE_TRACKING = TRUE;
ALTER TABLE DEMO.DEMO.ICYMTA SET MAX_DATA_EXTENSION_TIME_IN_DAYS = 7;

-- Enable result caching for repeated queries
ALTER ACCOUNT SET USE_CACHED_RESULT = TRUE;

-- Optimize for streaming workloads
ALTER WAREHOUSE STREAMING_WH SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
ALTER WAREHOUSE STREAMING_WH SET STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 300;

-- ============================================================================
-- 6. PERFORMANCE MONITORING QUERIES
-- ============================================================================

-- Query to monitor streaming performance
CREATE OR REPLACE VIEW V_STREAMING_PERFORMANCE AS
SELECT 
    DATE_TRUNC('MINUTE', INGESTION_TIME) AS minute_bucket,
    COUNT(*) AS records_per_minute,
    COUNT(DISTINCT VEHICLEREF) AS unique_vehicles_per_minute,
    AVG(DATEDIFF('second', RECORDEDATTIME, INGESTION_TIME)) AS avg_latency_seconds,
    MAX(INGESTION_TIME) - MIN(INGESTION_TIME) AS processing_window_seconds
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('MINUTE', INGESTION_TIME)
ORDER BY minute_bucket DESC;

-- Resource utilization monitoring
CREATE OR REPLACE VIEW V_RESOURCE_UTILIZATION AS
SELECT 
    WAREHOUSE_NAME,
    AVG(AVG_RUNNING) AS avg_running_queries,
    AVG(AVG_QUEUED_LOAD) AS avg_queued_load,
    SUM(CREDITS_USED) AS total_credits_used,
    DATE_TRUNC('HOUR', START_TIME) AS hour_bucket
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE WAREHOUSE_NAME IN ('STREAMING_WH', 'ANALYTICS_WH')
  AND START_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME, DATE_TRUNC('HOUR', START_TIME)
ORDER BY hour_bucket DESC, WAREHOUSE_NAME;

-- ============================================================================
-- 7. AUTOMATED OPTIMIZATION TASKS
-- ============================================================================

-- Task to analyze and optimize table clustering
CREATE OR REPLACE TASK TASK_OPTIMIZE_CLUSTERING
    WAREHOUSE = STREAMING_WH
    SCHEDULE = 'USING CRON 0 3 * * * UTC'  -- Daily at 3 AM UTC
AS
    CALL SYSTEM$CLUSTERING_INFORMATION('DEMO.DEMO.ICYMTA', '(DATE, LINEREF, VEHICLEREF)');

-- Task to refresh materialized views
CREATE OR REPLACE TASK TASK_REFRESH_MV
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 4 * * * UTC'  -- Daily at 4 AM UTC
AS
BEGIN
    ALTER MATERIALIZED VIEW MV_REALTIME_VEHICLE_POSITIONS REFRESH;
    ALTER MATERIALIZED VIEW MV_LINE_PERFORMANCE_METRICS REFRESH;
    ALTER MATERIALIZED VIEW MV_STOP_ANALYTICS REFRESH;
END;

-- Task to collect statistics
CREATE OR REPLACE TASK TASK_UPDATE_STATISTICS
    WAREHOUSE = STREAMING_WH
    SCHEDULE = 'USING CRON 0 5 * * * UTC'  -- Daily at 5 AM UTC
AS
    ANALYZE TABLE DEMO.DEMO.ICYMTA COMPUTE STATISTICS;

-- Start all optimization tasks
ALTER TASK TASK_OPTIMIZE_CLUSTERING RESUME;
ALTER TASK TASK_REFRESH_MV RESUME;
ALTER TASK TASK_UPDATE_STATISTICS RESUME;

-- ============================================================================
-- 8. PERFORMANCE TESTING QUERIES
-- ============================================================================

-- Test query performance for common patterns
SELECT 'Real-time vehicle lookup performance test' AS test_name;
SELECT COUNT(*) FROM DEMO.DEMO.ICYMTA 
WHERE VEHICLEREF = 'MTA_1234' 
  AND DATE = CURRENT_DATE();

SELECT 'Line performance query test' AS test_name;
SELECT * FROM V_LINE_STATUS_SUMMARY 
WHERE LINEREF = '4';

SELECT 'Geospatial query performance test' AS test_name;
SELECT COUNT(*) FROM DEMO.DEMO.ICYMTA
WHERE VEHICLELOCATIONLATITUDE BETWEEN 40.7 AND 40.8
  AND VEHICLELOCATIONLONGITUDE BETWEEN -74.0 AND -73.9
  AND DATE = CURRENT_DATE();

-- ============================================================================
-- Performance Optimization Complete
-- ============================================================================

SELECT 'Performance optimization setup completed successfully!' AS STATUS,
       'Clustering, materialized views, and monitoring configured' AS DETAILS;