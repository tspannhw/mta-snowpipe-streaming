#!/usr/bin/env python3
"""
Run the Haversine fix SQL script to resolve coordinate issues
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
        
        # Read and execute the fix SQL script
        print("\n🔧 Applying Haversine function fixes...")
        print("=" * 50)
        
        # Read SQL file
        with open('/app/fix_haversine_views.sql', 'r') as f:
            sql_content = f.read()
        
        # Split into individual statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
        
        success_count = 0
        error_count = 0
        
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue
                
            print(f"\n📝 Executing statement {i}/{len(statements)}...")
            try:
                # Skip comments and empty statements
                if statement.startswith('--') or len(statement.strip()) < 10:
                    continue
                    
                result = session.sql(statement).collect()
                print(f"✅ Statement {i} executed successfully")
                success_count += 1
                
                # If it's a SELECT statement, show some results
                if statement.strip().upper().startswith('SELECT') and result:
                    print(f"   📋 Returned {len(result)} rows")
                    if len(result) <= 3:
                        for row in result:
                            print(f"   - {dict(row)}")
                
            except Exception as e:
                print(f"❌ Statement {i} failed: {e}")
                error_count += 1
                # Print the statement that failed for debugging
                print(f"   Statement: {statement[:100]}...")
        
        print(f"\n📊 SUMMARY:")
        print(f"✅ Successful statements: {success_count}")
        print(f"❌ Failed statements: {error_count}")
        
        if error_count == 0:
            print(f"\n🎉 All Haversine fixes applied successfully!")
            print(f"🔧 Views created/updated:")
            print(f"   - V_STREAMING_METRICS (fixed coordinate handling)")
            print(f"   - VW_MTANEARBY (safe Haversine calculations)")
            print(f"   - V_COORDINATE_QUALITY (data quality monitoring)")
            print(f"   - V_CLEAN_COORDINATES (cleaned coordinate view)")
            print(f"   - SAFE_COORDINATE_CAST (utility function)")
        else:
            print(f"\n⚠️  Some statements failed. Check the errors above.")
        
        session.close()
        
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    main()