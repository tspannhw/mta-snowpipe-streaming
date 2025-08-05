#!/usr/bin/env python3
"""
Enable Iceberg tables with ACCOUNTADMIN role
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

def connect_as_accountadmin():
    """Connect to Snowflake as ACCOUNTADMIN"""
    print("🔗 Connecting to Snowflake as ACCOUNTADMIN...")
    
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
    
    # Create connection with ACCOUNTADMIN role
    connection_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'private_key': private_key_der,
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'STREAMING_WH'),
        'role': 'ACCOUNTADMIN',  # Force ACCOUNTADMIN role
    }
    
    try:
        session = Session.builder.configs(connection_params).create()
        
        # Verify role
        result = session.sql("SELECT CURRENT_ROLE()").collect()
        current_role = result[0][0] if result else "UNKNOWN"
        
        print(f"✅ Connected as role: {current_role}")
        return session, current_role
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None, None

def enable_iceberg_tables(session):
    """Enable Iceberg tables at account level"""
    print("\n🔧 Enabling Iceberg Tables")
    print("=" * 40)
    
    try:
        # Enable Iceberg tables
        print("Running: ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;")
        session.sql("ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE").collect()
        print("✅ ENABLE_ICEBERG_TABLES set to TRUE")
        
        # Verify the setting
        print("\nVerifying setting...")
        result = session.sql("SHOW PARAMETERS LIKE 'ENABLE_ICEBERG_TABLES' IN ACCOUNT").collect()
        
        if result:
            for row in result:
                print(f"📊 {row['key']} = {row['value']} (level: {row.get('level', 'account')})")
        else:
            print("⚠️  Parameter not found in SHOW PARAMETERS output")
            
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Failed to enable Iceberg tables: {error_msg}")
        
        if "insufficient privileges" in error_msg.lower():
            print("💡 Your user doesn't have ACCOUNTADMIN privileges")
            print("   Ask your Snowflake administrator to run this command:")
            print("   ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;")
        elif "invalid property" in error_msg.lower():
            print("💡 ENABLE_ICEBERG_TABLES parameter not available")
            print("   This might mean:")
            print("   • Feature not available in your Snowflake edition")
            print("   • Feature needs to be enabled by Snowflake Support")
        
        return False

def test_iceberg_functionality(session):
    """Test Iceberg table creation"""
    print("\n🧪 Testing Iceberg Table Functionality")
    print("=" * 40)
    
    try:
        # First check if we have an external volume
        print("1. Checking for external volumes...")
        volumes_result = session.sql("SHOW EXTERNAL VOLUMES").collect()
        
        if volumes_result:
            volume_name = volumes_result[0]['name']
            print(f"   ✅ Found external volume: {volume_name}")
            
            # Try to create a test Iceberg table
            test_table_sql = f"""
                CREATE OR REPLACE ICEBERG TABLE DEMO.DEMO.TEST_ICEBERG (
                    id INTEGER,
                    name STRING,
                    created_at TIMESTAMP
                )
                EXTERNAL_VOLUME = '{volume_name}'
                CATALOG = 'SNOWFLAKE'
                BASE_LOCATION = 'test_iceberg/'
            """
            
            print("2. Creating test Iceberg table...")
            session.sql(test_table_sql).collect()
            print("   ✅ Test Iceberg table created successfully!")
            
            # Insert test data
            print("3. Testing data insertion...")
            session.sql("""
                INSERT INTO DEMO.DEMO.TEST_ICEBERG VALUES 
                (1, 'Test Record', CURRENT_TIMESTAMP())
            """).collect()
            print("   ✅ Data inserted successfully!")
            
            # Query data
            print("4. Testing data query...")
            result = session.sql("SELECT * FROM DEMO.DEMO.TEST_ICEBERG").collect()
            if result:
                print(f"   ✅ Data retrieved: {len(result)} row(s)")
                for row in result:
                    print(f"      ID: {row['ID']}, Name: {row['NAME']}")
            
            # Clean up
            print("5. Cleaning up test table...")
            session.sql("DROP TABLE IF EXISTS DEMO.DEMO.TEST_ICEBERG").collect()
            print("   ✅ Test table dropped")
            
            return True
            
        else:
            print("   ⚠️  No external volumes found")
            print("   💡 You need to create an external volume first:")
            print("      CREATE EXTERNAL VOLUME my_iceberg_volume")
            print("      STORAGE_LOCATIONS = (('s3://your-bucket/path/'))")
            print("      STORAGE_INTEGRATION = your_storage_integration;")
            return False
            
    except Exception as e:
        print(f"❌ Iceberg test failed: {e}")
        return False

def main():
    """Main function"""
    print("❄️  Snowflake Iceberg Enabler")
    print("=" * 50)
    
    load_env_file()
    
    # Connect as ACCOUNTADMIN
    session, role = connect_as_accountadmin()
    if not session:
        print("\n💡 If connection failed due to role issues:")
        print("   1. Your user might not have ACCOUNTADMIN role granted")
        print("   2. Ask your Snowflake admin to grant ACCOUNTADMIN role")
        print("   3. Or ask them to run: ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;")
        return
    
    try:
        if role != 'ACCOUNTADMIN':
            print(f"⚠️  Connected as {role}, not ACCOUNTADMIN")
            print("   Some operations might fail due to insufficient privileges")
        
        # Enable Iceberg tables
        iceberg_enabled = enable_iceberg_tables(session)
        
        if iceberg_enabled:
            # Test Iceberg functionality
            test_iceberg_functionality(session)
            
            print("\n🎉 SUCCESS!")
            print("=" * 30)
            print("✅ Iceberg tables are now enabled")
            print("✅ Your ICYMTA table can now use Iceberg streaming")
            print("✅ Run your Snowpipe streaming application!")
        else:
            print("\n📋 Manual Steps Required:")
            print("=" * 30)
            print("Ask your Snowflake administrator to run:")
            print("ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;")
        
    finally:
        session.close()
        print("\n🔌 Session closed")

if __name__ == "__main__":
    main()