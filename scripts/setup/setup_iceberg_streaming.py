#!/usr/bin/env python3
"""
Setup Iceberg Streaming for Snowflake
This script enables the necessary parameters and validates the configuration.
"""

import json
import sys
from snowflake.snowpark import Session
from pathlib import Path

def load_profile():
    """Load Snowflake profile from profile.json"""
    try:
        with open('profile.json', 'r') as f:
            profile = json.load(f)
        print("✅ Profile loaded successfully")
        return profile
    except FileNotFoundError:
        print("❌ profile.json not found. Please create it first.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in profile.json: {e}")
        sys.exit(1)

def create_session(profile):
    """Create Snowflake session with profile settings"""
    try:
        # Extract connection parameters
        connection_params = {
            'account': profile['account'],
            'user': profile['user'],
            'authenticator': profile.get('authenticator', 'SNOWFLAKE_JWT'),
            'private_key_path': profile['private_key_path'],
            'warehouse': profile['warehouse'],
            'database': profile['database'],
            'schema': profile['schema'],
            'role': profile['role']
        }
        
        # Add optional parameters
        if 'connection_timeout' in profile:
            connection_params['connection_timeout'] = profile['connection_timeout']
        if 'network_timeout' in profile:
            connection_params['network_timeout'] = profile['network_timeout']
        
        session = Session.builder.configs(connection_params).create()
        print("✅ Snowflake session created successfully")
        return session
        
    except Exception as e:
        print(f"❌ Failed to create session: {e}")
        sys.exit(1)

def enable_iceberg_parameters(session):
    """Enable Iceberg streaming parameters"""
    print("\n🔧 Enabling Iceberg Parameters...")
    print("=" * 50)
    
    commands = [
        "USE ROLE ACCOUNTADMIN",
        "ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE",
        "ALTER ACCOUNT SET ENABLE_STREAMING_INGESTION = TRUE"
    ]
    
    for cmd in commands:
        try:
            print(f"🔄 Executing: {cmd}")
            session.sql(cmd).collect()
            print(f"✅ Success: {cmd}")
        except Exception as e:
            print(f"⚠️  Warning: {cmd} - {e}")
    
    print("\n✅ Iceberg parameters enabled")

def verify_configuration(session):
    """Verify Iceberg configuration"""
    print("\n🔍 Verifying Configuration...")
    print("=" * 50)
    
    # Check Iceberg parameters
    try:
        result = session.sql("SHOW PARAMETERS LIKE '%ICEBERG%' IN ACCOUNT").collect()
        if result:
            for row in result:
                print(f"📊 {row['key']} = {row['value']}")
        else:
            print("⚠️  No Iceberg parameters found")
    except Exception as e:
        print(f"❌ Error checking Iceberg parameters: {e}")
    
    # Check streaming parameters
    try:
        result = session.sql("SHOW PARAMETERS LIKE '%STREAMING%' IN ACCOUNT").collect()
        if result:
            for row in result:
                print(f"📊 {row['key']} = {row['value']}")
        else:
            print("⚠️  No streaming parameters found")
    except Exception as e:
        print(f"❌ Error checking streaming parameters: {e}")

def verify_table_access(session, profile):
    """Verify access to ICYMTA table"""
    print("\n🔍 Verifying Table Access...")
    print("=" * 50)
    
    try:
        # Switch to target database/schema
        session.sql(f"USE DATABASE {profile['database']}").collect()
        session.sql(f"USE SCHEMA {profile['schema']}").collect()
        
        # Check table structure
        result = session.sql("DESC TABLE ICYMTA").collect()
        print(f"✅ ICYMTA table found with {len(result)} columns")
        
        # Check row count
        count_result = session.sql("SELECT COUNT(*) as row_count FROM ICYMTA").collect()
        row_count = count_result[0]['ROW_COUNT']
        print(f"📊 Table has {row_count} rows")
        
        # Show table properties
        props_result = session.sql("SHOW TABLES LIKE 'ICYMTA'").collect()
        if props_result:
            table_info = props_result[0]
            print(f"📋 Table type: {table_info.get('kind', 'Unknown')}")
            print(f"📋 Created: {table_info.get('created_on', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error accessing ICYMTA table: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 Snowflake Iceberg Streaming Setup")
    print("=" * 50)
    
    # Load profile
    profile = load_profile()
    
    # Create session
    session = create_session(profile)
    
    try:
        # Enable parameters
        enable_iceberg_parameters(session)
        
        # Verify configuration
        verify_configuration(session)
        
        # Verify table access
        table_ok = verify_table_access(session, profile)
        
        print("\n🎉 Setup Complete!")
        print("=" * 50)
        
        if table_ok:
            print("✅ Iceberg streaming is ready for MTA data")
            print("✅ You can now run: docker-compose up snowpipe-streaming")
        else:
            print("⚠️  Table access issues - check permissions")
            
    finally:
        session.close()
        print("🔌 Session closed")

if __name__ == "__main__":
    main()