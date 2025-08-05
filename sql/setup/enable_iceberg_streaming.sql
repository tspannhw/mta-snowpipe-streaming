-- Enable Iceberg Streaming for Snowpipe
-- Run this as ACCOUNTADMIN role

USE ROLE ACCOUNTADMIN;

-- Enable Iceberg Tables at Account Level (this is the correct parameter name)
ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;

-- Enable Streaming ingestion
ALTER ACCOUNT SET ENABLE_STREAMING_INGESTION = TRUE;

-- Enable Snowpipe Streaming for Iceberg (if available)
-- ALTER ACCOUNT SET SNOWPIPE_STREAMING_ENABLE_ICEBERG = TRUE;

-- Verify settings
SHOW PARAMETERS LIKE '%ICEBERG%' IN ACCOUNT;
SHOW PARAMETERS LIKE '%STREAMING%' IN ACCOUNT;

-- Grant necessary privileges to your user
GRANT USAGE ON WAREHOUSE INGEST TO USER kafkaguy;
GRANT ALL ON DATABASE DEMO TO USER kafkaguy;
GRANT ALL ON SCHEMA DEMO.DEMO TO USER kafkaguy;
GRANT ALL ON TABLE DEMO.DEMO.ICYMTA TO USER kafkaguy;

-- Verify the ICYMTA table is properly configured for streaming
USE DATABASE DEMO;
USE SCHEMA DEMO;
DESC TABLE ICYMTA;

-- Check table properties
SHOW TABLES LIKE 'ICYMTA';