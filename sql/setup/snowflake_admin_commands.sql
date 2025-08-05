-- ============================================================================
-- SNOWFLAKE ADMINISTRATOR COMMANDS
-- Commands to enable Iceberg tables for MTA Snowpipe Streaming
-- ============================================================================

-- Step 1: Connect as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Step 2: Enable Iceberg tables at account level
ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;

-- Step 3: Verify the setting
SHOW PARAMETERS LIKE 'ENABLE_ICEBERG_TABLES' IN ACCOUNT;

-- Step 4: (Optional) Grant ACCOUNTADMIN role to kafkaguy user for future admin tasks
-- GRANT ROLE ACCOUNTADMIN TO USER kafkaguy;

-- Step 5: Verify Iceberg table functionality
-- Test creating an Iceberg table (replace 'YOUR_EXTERNAL_VOLUME' with actual volume name)
/*
CREATE OR REPLACE ICEBERG TABLE DEMO.DEMO.TEST_ICEBERG (
    id INTEGER,
    name STRING,
    created_at TIMESTAMP
)
EXTERNAL_VOLUME = 'YOUR_EXTERNAL_VOLUME'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'test/';
*/

-- Step 6: Verify the existing ICYMTA table can use streaming
DESC TABLE DEMO.DEMO.ICYMTA;

-- ============================================================================
-- EXPECTED RESULTS:
-- ============================================================================
-- After running ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE:
-- 
-- SHOW PARAMETERS should return:
-- | key                      | value | level   |
-- |--------------------------|-------|---------|
-- | ENABLE_ICEBERG_TABLES    | true  | ACCOUNT |
-- 
-- Your MTA Snowpipe Streaming application should then work with:
-- - Iceberg table streaming enabled
-- - High-performance data ingestion
-- - Real-time data processing
-- ============================================================================