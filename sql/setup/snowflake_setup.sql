-- ============================================================================
-- Snowpipe Streaming High-Performance Architecture Setup for Iceberg Table
-- MTA Real-time Transit Data Pipeline
-- ============================================================================

-- Enable Iceberg Streaming at account level (requires ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET ENABLE_ICEBERG_STREAMING = TRUE;

-- Switch to appropriate role for setup
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DEMO;
USE SCHEMA DEMO;

-- ============================================================================
-- 1. CREATE ENHANCED ICEBERG TABLE WITH CLUSTERING AND PARTITIONING
-- ============================================================================

-- Drop and recreate the table with performance optimizations
DROP TABLE IF EXISTS DEMO.DEMO.ICYMTA;

CREATE OR REPLACE ICEBERG TABLE DEMO.DEMO.ICYMTA (
    -- Primary identifiers
    STOPPOINTREF STRING,
    VEHICLEREF STRING,
    
    -- Journey and timing data
    PROGRESSRATE STRING,
    EXPECTEDDEPARTURETIME TIMESTAMP_NTZ,
    EXPECTEDARRIVALTIME TIMESTAMP_NTZ, 
    ORIGINAIMEDDEPARTURETIME TIMESTAMP_NTZ,
    AIMEDARRIVALTIME TIMESTAMP_NTZ,
    RECORDEDATTIME TIMESTAMP_NTZ,
    
    -- Stop and journey information
    STOPPOINT STRING,
    VISITNUMBER INTEGER,
    DATAFRAMEREF STRING,
    STOPPOINTNAME STRING,
    JOURNEYPATTERNREF STRING,
    DATEDVEHICLEJOURNEYREF STRING,
    
    -- Situation references
    SITUATIONSIMPLEREF1 STRING,
    SITUATIONSIMPLEREF2 STRING,
    SITUATIONSIMPLEREF3 STRING,
    SITUATIONSIMPLEREF4 STRING,
    SITUATIONSIMPLEREF5 STRING,
    
    -- Vehicle and route information
    BEARING FLOAT,
    OPERATORREF STRING,
    DESTINATIONNAME STRING,
    BLOCKREF STRING,
    LINEREF STRING,
    DIRECTIONREF STRING,
    PUBLISHEDLINENAME STRING,
    DESTINATIONREF STRING,
    ORIGINREF STRING,
    
    -- Location data with privacy tags
    VEHICLELOCATIONLONGITUDE FLOAT WITH TAG (
        SNOWFLAKE.CORE.PRIVACY_CATEGORY='QUASI_IDENTIFIER', 
        SNOWFLAKE.CORE.SEMANTIC_CATEGORY='LONGITUDE'
    ),
    VEHICLELOCATIONLATITUDE FLOAT WITH TAG (
        SNOWFLAKE.CORE.PRIVACY_CATEGORY='QUASI_IDENTIFIER', 
        SNOWFLAKE.CORE.SEMANTIC_CATEGORY='LATITUDE'
    ),
    
    -- Status and proximity information
    ARRIVALPROXIMITYTEXT STRING,
    DISTANCEFROMSTOP FLOAT,
    NUMBEROFSTOPSAWAY INTEGER,
    PROGRESSSTATUS STRING,
    MONITORED BOOLEAN,
    
    -- Passenger information
    ESTIMATEDPASSENGERCAPACITY INTEGER,
    ESTIMATEDPASSENGERCOUNT INTEGER,
    
    -- Metadata
    DATE DATE,
    TS TIMESTAMP_NTZ,
    UUID STRING,
    
    -- Add streaming metadata columns
    INGESTION_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_SYSTEM STRING DEFAULT 'MTA_REALTIME',
    PROCESSING_STATUS STRING DEFAULT 'PROCESSED'
)
EXTERNAL_VOLUME = 'TRANSCOM_TSPANNICEBERG_EXTVOL'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'mta/'
-- Cluster by commonly queried columns for performance
CLUSTER BY (DATE, LINEREF, VEHICLEREF);

-- ============================================================================
-- 2. CREATE STREAM FOR CHANGE TRACKING
-- ============================================================================

CREATE OR REPLACE STREAM ICYMTA_STREAM ON TABLE DEMO.DEMO.ICYMTA;

-- ============================================================================
-- 3. CREATE PERFORMANCE OPTIMIZATION OBJECTS
-- ============================================================================

-- Create a warehouse specifically for streaming workloads
CREATE OR REPLACE WAREHOUSE STREAMING_WH WITH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for Snowpipe Streaming operations';

-- ============================================================================
-- 4. CREATE MONITORING VIEWS
-- ============================================================================

-- Real-time streaming metrics view
CREATE OR REPLACE VIEW V_STREAMING_METRICS AS
SELECT 
    DATE_TRUNC('MINUTE', INGESTION_TIME) AS ingestion_minute,
    COUNT(*) AS records_per_minute,
    COUNT(DISTINCT VEHICLEREF) AS unique_vehicles,
    COUNT(DISTINCT LINEREF) AS unique_lines,
    AVG(DISTANCEFROMSTOP) AS avg_distance_from_stop,
    MAX(INGESTION_TIME) AS latest_ingestion
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('MINUTE', INGESTION_TIME)
ORDER BY ingestion_minute DESC;

-- Data quality monitoring view
CREATE OR REPLACE VIEW V_DATA_QUALITY_METRICS AS
SELECT 
    DATE_TRUNC('HOUR', INGESTION_TIME) AS ingestion_hour,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 1 END) AS missing_latitude,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 1 END) AS missing_longitude,
    COUNT(CASE WHEN VEHICLEREF IS NULL THEN 1 END) AS missing_vehicle_ref,
    COUNT(CASE WHEN LINEREF IS NULL THEN 1 END) AS missing_line_ref,
    (missing_latitude + missing_longitude + missing_vehicle_ref + missing_line_ref) * 100.0 / total_records AS quality_score_pct
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', INGESTION_TIME)
ORDER BY ingestion_hour DESC;

-- ============================================================================
-- 5. CREATE STORED PROCEDURES FOR MAINTENANCE
-- ============================================================================

-- Procedure to optimize table performance
CREATE OR REPLACE PROCEDURE SP_OPTIMIZE_ICEBERG_TABLE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Optimize the Iceberg table
    ALTER TABLE DEMO.DEMO.ICYMTA RECLUSTER;
    
    -- Update table statistics
    ANALYZE TABLE DEMO.DEMO.ICYMTA COMPUTE STATISTICS;
    
    RETURN 'Table optimization completed successfully';
END;
$$;

-- ============================================================================
-- 6. CREATE ALERTS AND TASKS
-- ============================================================================

-- Task to run table optimization daily
CREATE OR REPLACE TASK DAILY_ICEBERG_OPTIMIZATION
    WAREHOUSE = STREAMING_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- Daily at 2 AM UTC
AS
    CALL SP_OPTIMIZE_ICEBERG_TABLE();

-- Start the task
ALTER TASK DAILY_ICEBERG_OPTIMIZATION RESUME;

-- ============================================================================
-- 7. GRANT APPROPRIATE PERMISSIONS
-- ============================================================================

-- Grant permissions for streaming operations
GRANT USAGE ON WAREHOUSE STREAMING_WH TO ROLE PUBLIC;
GRANT ALL ON TABLE DEMO.DEMO.ICYMTA TO ROLE PUBLIC;
GRANT ALL ON STREAM ICYMTA_STREAM TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_STREAMING_METRICS TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_DATA_QUALITY_METRICS TO ROLE PUBLIC;

-- ============================================================================
-- Setup Complete
-- ============================================================================

SELECT 'Snowpipe Streaming High-Performance Architecture setup completed successfully!' AS STATUS;