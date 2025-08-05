#!/usr/bin/env python3
"""
Check Iceberg-related parameters in Snowflake
Find the correct way to enable Iceberg streaming
"""

import os
import sys
from pathlib import Path

try:
    from snowflake.snowpark import Session
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    sys.exit(1)

def load_env_file():
    """Load environment variables from .env file"""
    env_file = Path('.env')
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

def connect_to_snowflake():
    """Connect to Snowflake using key-pair authentication"""
    print("🔗 Connecting to Snowflake...")
    
    # Load configuration
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
    
    # Load private key
    with open(private_key_path, 'rb') as f:
        private_key_content = f.read()
    
    if private_key_passphrase:
        private_key = load_pem_private_key(
            private_key_content,
            password=private_key_passphrase.encode('utf-8')
        )
    else:
        private_key = load_pem_private_key(
            private_key_content,
            password=None
        )
    
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    # Create connection
    connection_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'private_key': private_key_der,
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'STREAMING_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'DEMO'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DEMO'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
    }
    
    try:
        session = Session.builder.configs(connection_params).create()
        print("✅ Connected successfully")
        return session
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def check_iceberg_parameters(session):
    """Check for Iceberg-related parameters"""
    print("\n🔍 Checking Iceberg-related Parameters")
    print("=" * 50)
    
    # Search patterns for Iceberg-related parameters
    search_patterns = [
        'ICEBERG',
        'STREAMING',
        'APACHE',
        'TABLE_FORMAT',
        'OPEN_TABLE_FORMAT'
    ]
    
    for pattern in search_patterns:
        print(f"\n🔎 Searching for parameters containing '{pattern}':")
        try:
            # Check account-level parameters
            result = session.sql(f"SHOW PARAMETERS LIKE '%{pattern}%' IN ACCOUNT").collect()
            if result:
                print(f"  📊 ACCOUNT level:")
                for row in result:
                    print(f"    • {row['key']} = {row['value']} (level: {row.get('level', 'unknown')})")
            else:
                print(f"    No account-level parameters found for '{pattern}'")
            
            # Check session-level parameters
            result = session.sql(f"SHOW PARAMETERS LIKE '%{pattern}%' IN SESSION").collect()
            if result:
                print(f"  📊 SESSION level:")
                for row in result:
                    print(f"    • {row['key']} = {row['value']}")
            
        except Exception as e:
            print(f"    ⚠️  Error searching for '{pattern}': {e}")

def check_iceberg_features(session):
    """Check if Iceberg features are available"""
    print("\n🧪 Testing Iceberg Feature Availability")
    print("=" * 50)
    
    # Test 1: Try to create an Iceberg table (syntax check)
    print("1. 🔧 Testing Iceberg table creation syntax...")
    try:
        # This will fail if Iceberg is not supported, but we can check the error
        session.sql("""
            CREATE OR REPLACE ICEBERG TABLE TEST_ICEBERG_SYNTAX_CHECK (
                id INTEGER,
                name STRING
            )
            EXTERNAL_VOLUME = 'nonexistent_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 'test/'
        """).collect()
        print("   ✅ Iceberg table syntax is supported")
    except Exception as e:
        error_msg = str(e)
        if "ICEBERG" in error_msg.upper() and "not supported" in error_msg.lower():
            print(f"   ❌ Iceberg tables not supported: {error_msg}")
        elif "EXTERNAL_VOLUME" in error_msg or "does not exist" in error_msg:
            print("   ✅ Iceberg table syntax is supported (volume doesn't exist)")
        else:
            print(f"   ⚠️  Error: {error_msg}")
    
    # Test 2: Check account edition and region
    print("\n2. 📋 Checking account information...")
    try:
        result = session.sql("""
            SELECT 
                CURRENT_ACCOUNT() as account,
                CURRENT_REGION() as region,
                CURRENT_VERSION() as version
        """).collect()
        
        if result:
            row = result[0]
            print(f"   Account: {row['ACCOUNT']}")
            print(f"   Region: {row['REGION']}")
            print(f"   Version: {row['VERSION']}")
    except Exception as e:
        print(f"   ⚠️  Error getting account info: {e}")

def suggest_solutions(session):
    """Suggest solutions based on findings"""
    print("\n💡 Suggested Solutions")
    print("=" * 50)
    
    print("1. 🔄 Try alternative parameter names:")
    alt_commands = [
        "ALTER ACCOUNT SET ENABLE_APACHE_ICEBERG_TABLES = TRUE;",
        "ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;",
        "ALTER ACCOUNT SET APACHE_ICEBERG_ENABLED = TRUE;",
        "ALTER ACCOUNT SET ICEBERG_ENABLED = TRUE;"
    ]
    
    for cmd in alt_commands:
        print(f"   • {cmd}")
    
    print("\n2. 🎯 Check with different role:")
    print("   • Try using ACCOUNTADMIN role")
    print("   • Some parameters require specific privileges")
    
    print("\n3. 📞 Contact Snowflake Support:")
    print("   • Iceberg streaming might need to be enabled by Snowflake")
    print("   • Feature might not be available in your region/edition")
    
    print("\n4. 🔍 Check Snowflake documentation:")
    print("   • Search for 'Apache Iceberg' in Snowflake docs")
    print("   • Look for account-level parameters for Iceberg")
    
    # Test some alternative commands
    print("\n🧪 Testing alternative enable commands:")
    alt_params = [
        "ENABLE_APACHE_ICEBERG_TABLES",
        "ENABLE_ICEBERG_TABLES", 
        "APACHE_ICEBERG_ENABLED",
        "ICEBERG_ENABLED"
    ]
    
    for param in alt_params:
        try:
            print(f"\n   Testing: ALTER ACCOUNT SET {param} = TRUE;")
            session.sql(f"ALTER ACCOUNT SET {param} = TRUE").collect()
            print(f"   ✅ SUCCESS: {param} parameter accepted!")
            
            # Verify the setting
            result = session.sql(f"SHOW PARAMETERS LIKE '{param}' IN ACCOUNT").collect()
            if result:
                for row in result:
                    print(f"   📊 {row['key']} = {row['value']}")
            break
            
        except Exception as e:
            error_msg = str(e)
            if "invalid property" in error_msg.lower():
                print(f"   ❌ Invalid: {param}")
            elif "insufficient privileges" in error_msg.lower():
                print(f"   🔐 Insufficient privileges for: {param}")
            else:
                print(f"   ⚠️  Error with {param}: {error_msg}")

def main():
    """Main function"""
    print("❄️  Snowflake Iceberg Parameter Checker")
    print("=" * 60)
    
    load_env_file()
    
    # Connect to Snowflake
    session = connect_to_snowflake()
    if not session:
        print("❌ Cannot proceed without Snowflake connection")
        return
    
    try:
        # Check for Iceberg parameters
        check_iceberg_parameters(session)
        
        # Test Iceberg features
        check_iceberg_features(session)
        
        # Suggest solutions
        suggest_solutions(session)
        
    finally:
        session.close()
        print("\n🔌 Session closed")

if __name__ == "__main__":
    main()