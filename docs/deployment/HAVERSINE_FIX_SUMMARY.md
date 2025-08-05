# 🔧 Haversine Function Error Fix

## Problem Description

**Error**: `Numeric value '' is not recognized` in SQL views `V_STREAMING_METRICS` and `VW_MTANEARBY`

**Root Cause**: The Haversine function in Snowflake was receiving empty strings (`''`) instead of NULL values for latitude and longitude coordinates, causing SQL execution failures.

## Technical Analysis

### Issue Details
1. **Data Source**: Kafka messages contain coordinate fields `VehicleLocationLatitude` and `VehicleLocationLongitude`
2. **Mapping Problem**: The streaming application was converting missing/empty coordinate values to empty strings (`''`) instead of NULL
3. **SQL Impact**: Haversine function expects numeric values but received empty strings
4. **Affected Views**: Views using geographic calculations failed with numeric conversion errors

### Data Flow Issue
```
Kafka Message → Python App → Snowflake Table → SQL Views
    ↓              ↓              ↓             ↓
Missing coords → Empty string → FLOAT field → Haversine ERROR
```

## Solutions Implemented

### 1. 🐍 Python Application Fix

**File**: `mta_snowpipe_streaming.py`

**Changes**:
- Added `_safe_coordinate()` method to handle coordinate conversion safely
- Updated coordinate field mapping to use NULL instead of empty strings
- Applied fix to `VEHICLELOCATIONLATITUDE`, `VEHICLELOCATIONLONGITUDE`, and `DISTANCEFROMSTOP`

**Key Code**:
```python
def _safe_coordinate(self, coord_value):
    """Safely convert coordinate value to string or None for database insertion"""
    if coord_value is None:
        return None
    
    coord_str = str(coord_value).strip()
    
    # Return None for empty strings or invalid values
    if not coord_str or coord_str == '' or coord_str.lower() in ['none', 'null', 'nan']:
        return None
    
    # Validate it's a valid number
    try:
        float(coord_str)
        return coord_str
    except (ValueError, TypeError):
        return None
```

### 2. 📊 SQL Views Fix

**File**: `fix_haversine_views.sql`

**New/Updated Views**:

#### V_STREAMING_METRICS (Fixed)
```sql
-- Safe coordinate handling with TRY_CAST
AVG(
    CASE 
        WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL
        THEN TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
    END
) AS avg_latitude
```

#### VW_MTANEARBY (New)
```sql
-- Safe Haversine calculations
CASE 
    WHEN a.lat_numeric IS NOT NULL AND a.lon_numeric IS NOT NULL 
         AND b.lat_numeric IS NOT NULL AND b.lon_numeric IS NOT NULL
    THEN HAVERSINE(a.lat_numeric, a.lon_numeric, b.lat_numeric, b.lon_numeric)
    ELSE NULL
END AS distance_km
```

#### V_COORDINATE_QUALITY (New)
- Monitors data quality issues
- Tracks empty strings, NULL values, invalid formats
- Calculates quality percentages

#### SAFE_COORDINATE_CAST Function (New)
```sql
CREATE OR REPLACE FUNCTION SAFE_COORDINATE_CAST(coord_string STRING)
RETURNS DOUBLE
LANGUAGE SQL
AS
$$
    CASE 
        WHEN coord_string = '' OR coord_string IS NULL THEN NULL
        ELSE TRY_CAST(coord_string AS DOUBLE)
    END
$$
```

### 3. 🔍 Diagnostic Tools

**Files**: 
- `debug_coordinate_issues.py` - Analyzes coordinate data quality
- `run_haversine_fix.py` - Applies SQL fixes automatically

## Deployment Instructions

### Step 1: Update Application
```bash
# Rebuild and deploy the fixed streaming application
docker-compose down
docker-compose build --no-cache snowpipe-streaming
docker-compose up -d
```

### Step 2: Apply SQL Fixes
```bash
# Run the SQL fix script
docker-compose exec snowpipe-streaming python run_haversine_fix.py
```

### Step 3: Verify Fixes
```bash
# Check coordinate data quality
docker-compose exec snowpipe-streaming python debug_coordinate_issues.py
```

## Verification Queries

### Check Data Quality
```sql
SELECT * FROM V_COORDINATE_QUALITY 
ORDER BY ingestion_hour DESC 
LIMIT 5;
```

### Test Haversine Function
```sql
SELECT 
    VEHICLEREF,
    clean_latitude,
    clean_longitude,
    distance_from_manhattan_km
FROM V_CLEAN_COORDINATES 
WHERE coordinates_valid_range = TRUE
LIMIT 10;
```

### Verify Views Working
```sql
-- Test streaming metrics
SELECT * FROM V_STREAMING_METRICS LIMIT 3;

-- Test nearby vehicles
SELECT * FROM VW_MTANEARBY LIMIT 3;
```

## Expected Outcomes

### ✅ Before Fix
- ❌ `V_STREAMING_METRICS` fails with "Numeric value '' is not recognized"
- ❌ `VW_MTANEARBY` fails with coordinate errors
- ❌ Haversine functions crash on empty strings

### ✅ After Fix
- ✅ All views execute successfully
- ✅ Haversine functions work with clean coordinates
- ✅ NULL values properly handled
- ✅ Data quality monitoring available
- ✅ Future coordinate issues prevented

## Data Quality Improvements

### Coordinate Validation
- **Empty String Detection**: Converts `''` to NULL
- **Format Validation**: Ensures numeric values only
- **Range Validation**: NYC area coordinate bounds (40.0-41.0, -75.0 to -73.0)
- **Quality Metrics**: Real-time monitoring of coordinate data quality

### Error Prevention
- **Application Level**: Safe coordinate conversion in Python
- **Database Level**: TRY_CAST and NULL handling in SQL
- **View Level**: Defensive programming with CASE statements
- **Monitoring Level**: Quality tracking and alerting

## Files Created/Modified

### New Files
- ✅ `fix_haversine_views.sql` - Complete SQL fix script
- ✅ `debug_coordinate_issues.py` - Diagnostic tool
- ✅ `run_haversine_fix.py` - Deployment script
- ✅ `HAVERSINE_FIX_SUMMARY.md` - This documentation

### Modified Files
- ✅ `mta_snowpipe_streaming.py` - Safe coordinate handling
- ✅ `.gitignore` - Security improvements (related)

## Performance Impact

### Positive Impacts
- ✅ **Eliminates SQL Errors**: Views now execute successfully
- ✅ **Improved Data Quality**: NULL handling vs empty strings
- ✅ **Better Monitoring**: Quality metrics for troubleshooting

### Considerations
- ⚠️ **Additional Validation**: Slight overhead for coordinate checking
- ⚠️ **View Complexity**: More defensive SQL code
- ✅ **Overall Net Positive**: Reliability over marginal performance cost

## Future Recommendations

### Data Ingestion
1. **Upstream Validation**: Validate coordinates at Kafka producer level
2. **Schema Evolution**: Consider strict coordinate typing
3. **Data Pipeline**: Add coordinate validation pipeline stage

### Monitoring
1. **Quality Alerts**: Set up alerts for coordinate quality degradation
2. **Geographic Bounds**: Monitor for coordinates outside expected ranges
3. **Haversine Usage**: Track geographic calculation performance

### Optimization
1. **Indexing**: Add geographic indexes for coordinate queries
2. **Clustering**: Cluster tables by geographic regions
3. **Caching**: Cache frequently calculated distances

---

## 🎯 Summary

This fix resolves the "Numeric value '' is not recognized" error by:

1. **Preventing the issue** at the application level (NULL instead of empty strings)
2. **Fixing existing views** with defensive SQL programming
3. **Adding monitoring** to detect future coordinate issues
4. **Providing tools** for diagnosis and maintenance

The solution ensures reliable geographic calculations while maintaining data quality and system performance.