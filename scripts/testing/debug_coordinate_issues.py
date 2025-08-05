#!/usr/bin/env python3
"""
Debug and fix coordinate issues causing Haversine function errors
Fixes: "Numeric value '' is not recognized" in SQL views
"""

import os
import sys
sys.path.append('/app')

from mta_snowpipe_streaming import ConfigManager
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend

def load_private_key(private_key_path, passphrase=None):
    """Load private key for Snowflake authentication"""
    with open(private_key_path, 'rb') as pem_in:
        pemlines = pem_in.read()
        if passphrase:
            passphrase = passphrase.encode()
        private_key_obj = load_pem_private_key(pemlines, passphrase, default_backend())
    
    private_key_text = private_key_obj.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()
    ).decode('utf-8')
    
    return private_key_text

def main():
    try:
        # Use the same config manager as the main app
        config = ConfigManager('/app/config.yaml')
        
        # Get Snowflake connection parameters
        account = config.get('snowflake', 'account')
        user = config.get('snowflake', 'user')
        warehouse = config.get('snowflake', 'warehouse')
        database = config.get('snowflake', 'database')
        schema = config.get('snowflake', 'schema')
        role = config.get('snowflake', 'role')
        
        # Load private key
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        if not private_key_path:
            print("❌ ERROR: SNOWFLAKE_PRIVATE_KEY_PATH environment variable not set")
            return
            
        private_key = load_private_key(private_key_path)
        
        # Create session
        session = Session.builder.configs({
            'account': account,
            'user': user,
            'private_key': private_key,
            'warehouse': warehouse,
            'database': database,
            'schema': schema,
            'role': role
        }).create()
        
        print("✅ Connected to Snowflake successfully!")
        print(f"📊 Database: {database}.{schema}")
        
        # 1. Check coordinate data quality
        print("\n🔍 CHECKING COORDINATE DATA QUALITY...")
        print("=" * 50)
        
        coord_quality_query = """
        SELECT 
            COUNT(*) AS total_records,
            COUNT(CASE WHEN VEHICLELOCATIONLATITUDE = '' THEN 1 END) AS empty_latitude,
            COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE = '' THEN 1 END) AS empty_longitude,
            COUNT(CASE WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 1 END) AS null_latitude,
            COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 1 END) AS null_longitude,
            COUNT(CASE 
                WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL 
                     AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NULL 
                THEN 1 
            END) AS invalid_latitude,
            COUNT(CASE 
                WHEN VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL 
                     AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NULL 
                THEN 1 
            END) AS invalid_longitude
        FROM DEMO.DEMO.ICYMTA
        WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
        """
        
        result = session.sql(coord_quality_query).collect()
        if result:
            row = result[0]
            print(f"📈 Total records (last hour): {row['TOTAL_RECORDS']}")
            print(f"🔸 Empty latitude strings: {row['EMPTY_LATITUDE']}")
            print(f"🔸 Empty longitude strings: {row['EMPTY_LONGITUDE']}")
            print(f"🔸 NULL latitudes: {row['NULL_LATITUDE']}")
            print(f"🔸 NULL longitudes: {row['NULL_LONGITUDE']}")
            print(f"❌ Invalid latitude formats: {row['INVALID_LATITUDE']}")
            print(f"❌ Invalid longitude formats: {row['INVALID_LONGITUDE']}")
            
            # Calculate percentages
            total = row['TOTAL_RECORDS']
            if total > 0:
                empty_pct = (row['EMPTY_LATITUDE'] + row['EMPTY_LONGITUDE']) * 100.0 / (total * 2)
                print(f"📊 Empty coordinate percentage: {empty_pct:.2f}%")
        
        # 2. Show sample problematic records
        print("\n🔍 SAMPLE PROBLEMATIC RECORDS...")
        print("=" * 50)
        
        sample_query = """
        SELECT 
            VEHICLEREF,
            VEHICLELOCATIONLATITUDE,
            VEHICLELOCATIONLONGITUDE,
            CASE 
                WHEN VEHICLELOCATIONLATITUDE = '' THEN 'EMPTY_STRING'
                WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 'NULL'
                WHEN TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NULL THEN 'INVALID_NUMERIC'
                ELSE 'VALID'
            END AS lat_status,
            CASE 
                WHEN VEHICLELOCATIONLONGITUDE = '' THEN 'EMPTY_STRING'
                WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 'NULL'
                WHEN TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NULL THEN 'INVALID_NUMERIC'
                ELSE 'VALID'
            END AS lon_status
        FROM DEMO.DEMO.ICYMTA
        WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
          AND (VEHICLELOCATIONLATITUDE = '' OR VEHICLELOCATIONLONGITUDE = '' 
               OR TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NULL 
               OR TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NULL)
        LIMIT 10
        """
        
        result = session.sql(sample_query).collect()
        for row in result:
            print(f"🚌 Vehicle {row['VEHICLEREF']}: "
                  f"Lat='{row['VEHICLELOCATIONLATITUDE']}' ({row['LAT_STATUS']}) "
                  f"Lon='{row['VEHICLELOCATIONLONGITUDE']}' ({row['LON_STATUS']})")
        
        # 3. Test Haversine with current data
        print("\n🧪 TESTING HAVERSINE FUNCTION...")
        print("=" * 50)
        
        haversine_test_query = """
        WITH test_coords AS (
            SELECT 
                VEHICLEREF,
                VEHICLELOCATIONLATITUDE,
                VEHICLELOCATIONLONGITUDE,
                CASE 
                    WHEN VEHICLELOCATIONLATITUDE = '' OR VEHICLELOCATIONLATITUDE IS NULL THEN NULL
                    ELSE TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
                END AS clean_lat,
                CASE 
                    WHEN VEHICLELOCATIONLONGITUDE = '' OR VEHICLELOCATIONLONGITUDE IS NULL THEN NULL
                    ELSE TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE)
                END AS clean_lon
            FROM DEMO.DEMO.ICYMTA
            WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
            LIMIT 5
        )
        SELECT 
            VEHICLEREF,
            clean_lat,
            clean_lon,
            CASE 
                WHEN clean_lat IS NOT NULL AND clean_lon IS NOT NULL
                THEN HAVERSINE(clean_lat, clean_lon, 40.7589, -73.9851)  -- Distance from Manhattan
                ELSE NULL
            END AS distance_from_manhattan_km
        FROM test_coords
        """
        
        try:
            result = session.sql(haversine_test_query).collect()
            print("✅ Haversine function working with cleaned coordinates!")
            for row in result:
                if row['DISTANCE_FROM_MANHATTAN_KM'] is not None:
                    print(f"🚌 Vehicle {row['VEHICLEREF']}: {row['DISTANCE_FROM_MANHATTAN_KM']:.2f} km from Manhattan")
                else:
                    print(f"🚌 Vehicle {row['VEHICLEREF']}: Invalid coordinates")
        except Exception as e:
            print(f"❌ Haversine test failed: {e}")
        
        # 4. Check existing views for issues
        print("\n🔍 CHECKING EXISTING VIEWS...")
        print("=" * 50)
        
        view_check_queries = [
            "SHOW VIEWS LIKE 'V_STREAMING_METRICS'",
            "SHOW VIEWS LIKE 'VW_MTANEARBY'",
            "SHOW VIEWS LIKE '%STREAMING%'",
            "SHOW VIEWS LIKE '%MTA%'"
        ]
        
        for query in view_check_queries:
            try:
                result = session.sql(query).collect()
                if result:
                    print(f"📋 Found views: {query}")
                    for row in result:
                        print(f"   - {row['name'] if 'name' in row else row}")
            except Exception as e:
                print(f"⚠️  Query failed: {query} - {e}")
        
        print("\n🔧 RECOMMENDED ACTIONS:")
        print("=" * 50)
        print("1. Run the fix_haversine_views.sql script to create safe views")
        print("2. Update existing views to use TRY_CAST() for coordinate conversion")
        print("3. Handle empty strings ('') before passing to Haversine function")
        print("4. Consider updating the streaming application to insert NULL instead of empty strings")
        
        session.close()
        
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    main()