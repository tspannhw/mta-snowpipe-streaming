-- ============================================================================
-- FIX HAVERSINE FUNCTION ERRORS IN SNOWFLAKE VIEWS
-- Problem: "Numeric value '' is not recognized" 
-- Solution: Properly handle empty strings and convert to numeric values
-- ============================================================================

-- 1. Fix V_STREAMING_METRICS with proper coordinate handling
CREATE OR REPLACE VIEW V_STREAMING_METRICS AS
SELECT 
    DATE_TRUNC('MINUTE', INGESTION_TIME) AS ingestion_minute,
    COUNT(*) AS records_per_minute,
    COUNT(DISTINCT VEHICLEREF) AS unique_vehicles,
    COUNT(DISTINCT LINEREF) AS unique_lines,
    
    -- Handle numeric conversions with proper NULL handling
    AVG(
        CASE 
            WHEN DISTANCEFROMSTOP = '' OR DISTANCEFROMSTOP IS NULL THEN NULL
            ELSE TRY_CAST(DISTANCEFROMSTOP AS DOUBLE)
        END
    ) AS avg_distance_from_stop,
    
    -- Count valid coordinates (non-empty, numeric)
    COUNT(
        CASE 
            WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL 
                 AND VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
                 AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NOT NULL
                 AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NOT NULL
            THEN 1 
        END
    ) AS vehicles_with_valid_coords,
    
    -- Average coordinates (only for valid values)
    AVG(
        CASE 
            WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL
            THEN TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
        END
    ) AS avg_latitude,
    
    AVG(
        CASE 
            WHEN VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
            THEN TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE)
        END
    ) AS avg_longitude,
    
    MAX(INGESTION_TIME) AS latest_ingestion
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('MINUTE', INGESTION_TIME)
ORDER BY ingestion_minute DESC;

-- 2. Create a fixed VW_MTANEARBY view with proper Haversine function
CREATE OR REPLACE VIEW VW_MTANEARBY AS
WITH valid_coordinates AS (
    SELECT 
        *,
        -- Convert string coordinates to numeric, handling empty strings
        CASE 
            WHEN VEHICLELOCATIONLATITUDE = '' OR VEHICLELOCATIONLATITUDE IS NULL THEN NULL
            ELSE TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
        END AS lat_numeric,
        CASE 
            WHEN VEHICLELOCATIONLONGITUDE = '' OR VEHICLELOCATIONLONGITUDE IS NULL THEN NULL
            ELSE TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE)
        END AS lon_numeric
    FROM DEMO.DEMO.ICYMTA
    WHERE INGESTION_TIME >= DATEADD('HOUR', -2, CURRENT_TIMESTAMP())
),
nearby_calculations AS (
    SELECT 
        a.*,
        b.VEHICLEREF AS nearby_vehicle,
        b.LINEREF AS nearby_line,
        b.lat_numeric AS nearby_lat,
        b.lon_numeric AS nearby_lon,
        
        -- Safe Haversine calculation (only for valid coordinates)
        CASE 
            WHEN a.lat_numeric IS NOT NULL AND a.lon_numeric IS NOT NULL 
                 AND b.lat_numeric IS NOT NULL AND b.lon_numeric IS NOT NULL
                 AND a.VEHICLEREF != b.VEHICLEREF  -- Don't compare vehicle to itself
            THEN HAVERSINE(a.lat_numeric, a.lon_numeric, b.lat_numeric, b.lon_numeric)
            ELSE NULL
        END AS distance_km
    FROM valid_coordinates a
    CROSS JOIN valid_coordinates b
    WHERE a.lat_numeric IS NOT NULL 
      AND a.lon_numeric IS NOT NULL
      AND b.lat_numeric IS NOT NULL 
      AND b.lon_numeric IS NOT NULL
      AND a.VEHICLEREF != b.VEHICLEREF
)
SELECT 
    VEHICLEREF,
    LINEREF,
    STOPPOINTREF,
    PUBLISHEDLINENAME,
    lat_numeric AS vehicle_latitude,
    lon_numeric AS vehicle_longitude,
    RECORDEDATTIME,
    nearby_vehicle,
    nearby_line,
    nearby_lat,
    nearby_lon,
    distance_km,
    RANK() OVER (PARTITION BY VEHICLEREF ORDER BY distance_km ASC) AS proximity_rank
FROM nearby_calculations
WHERE distance_km IS NOT NULL 
  AND distance_km <= 5.0  -- Within 5km
ORDER BY VEHICLEREF, distance_km;

-- 3. Create a data quality view to identify coordinate issues
CREATE OR REPLACE VIEW V_COORDINATE_QUALITY AS
SELECT 
    DATE_TRUNC('HOUR', INGESTION_TIME) AS ingestion_hour,
    COUNT(*) AS total_records,
    
    -- Count empty string coordinates
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE = '' THEN 1 END) AS empty_latitude_count,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE = '' THEN 1 END) AS empty_longitude_count,
    
    -- Count NULL coordinates  
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 1 END) AS null_latitude_count,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 1 END) AS null_longitude_count,
    
    -- Count invalid numeric coordinates
    COUNT(CASE 
        WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL 
             AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NULL 
        THEN 1 
    END) AS invalid_latitude_count,
    
    COUNT(CASE 
        WHEN VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL 
             AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NULL 
        THEN 1 
    END) AS invalid_longitude_count,
    
    -- Count valid coordinates
    COUNT(CASE 
        WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL
             AND VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
             AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NOT NULL
             AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NOT NULL
        THEN 1 
    END) AS valid_coordinate_count,
    
    -- Calculate quality percentages
    ROUND(valid_coordinate_count * 100.0 / total_records, 2) AS valid_coord_percentage,
    ROUND((empty_latitude_count + empty_longitude_count) * 100.0 / (total_records * 2), 2) AS empty_coord_percentage
    
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', INGESTION_TIME)
ORDER BY ingestion_hour DESC;

-- 4. Create a utility function to safely convert coordinates
CREATE OR REPLACE FUNCTION SAFE_COORDINATE_CAST(coord_string STRING)
RETURNS DOUBLE
LANGUAGE SQL
AS
$$
    CASE 
        WHEN coord_string = '' OR coord_string IS NULL THEN NULL
        ELSE TRY_CAST(coord_string AS DOUBLE)
    END
$$;

-- 5. Create a helper view with clean coordinates for easy querying
CREATE OR REPLACE VIEW V_CLEAN_COORDINATES AS
SELECT 
    *,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) AS clean_latitude,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) AS clean_longitude,
    
    -- Validate coordinate ranges (NYC area: Lat ~40.4-40.9, Lon ~-74.3 to -73.7)
    CASE 
        WHEN SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) BETWEEN 40.0 AND 41.0
             AND SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) BETWEEN -75.0 AND -73.0
        THEN TRUE
        ELSE FALSE
    END AS coordinates_valid_range,
    
    -- Distance from Manhattan center (roughly 40.7589, -73.9851)
    CASE 
        WHEN SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) IS NOT NULL 
             AND SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) IS NOT NULL
        THEN HAVERSINE(
            SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE), 
            SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE),
            40.7589, 
            -73.9851
        )
        ELSE NULL
    END AS distance_from_manhattan_km
    
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP());

-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON VIEW V_STREAMING_METRICS TO ROLE PUBLIC;
GRANT SELECT ON VIEW VW_MTANEARBY TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_COORDINATE_QUALITY TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_CLEAN_COORDINATES TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION SAFE_COORDINATE_CAST(STRING) TO ROLE PUBLIC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check coordinate data quality
SELECT * FROM V_COORDINATE_QUALITY LIMIT 10;

-- Test the fixed views
SELECT * FROM V_STREAMING_METRICS LIMIT 5;
SELECT * FROM VW_MTANEARBY LIMIT 5;

-- Show sample coordinate conversions
SELECT 
    VEHICLEREF,
    VEHICLELOCATIONLATITUDE AS original_lat,
    VEHICLELOCATIONLONGITUDE AS original_lon,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) AS clean_lat,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) AS clean_lon,
    coordinates_valid_range
FROM V_CLEAN_COORDINATES 
WHERE VEHICLELOCATIONLATITUDE != '' 
LIMIT 10;