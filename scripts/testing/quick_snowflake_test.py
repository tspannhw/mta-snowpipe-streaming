#!/usr/bin/env python3
"""
Quick Snowflake Connectivity Test
Simple test for key-pair authentication
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
    print("Install with: pip install snowflake-snowpark-python cryptography")
    sys.exit(1)

def load_env_file():
    """Load environment variables from .env file if it exists"""
    env_file = Path('.env')
    if env_file.exists():
        print("🔧 Loading .env file...")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    else:
        print("⚠️  No .env file found. Using system environment variables.")

def test_snowflake_connection():
    """Test Snowflake connection with key-pair authentication"""
    print("🚀 Quick Snowflake Connectivity Test")
    print("=" * 50)
    
    # Load environment
    load_env_file()
    
    # Get required environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
    
    # Optional parameters with defaults
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'STREAMING_WH')
    database = os.getenv('SNOWFLAKE_DATABASE', 'DEMO')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'DEMO')
    role = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
    
    # Validate required parameters
    if not account:
        print("❌ SNOWFLAKE_ACCOUNT environment variable is required")
        return False
    
    if not user:
        print("❌ SNOWFLAKE_USER environment variable is required")
        return False
    
    if not private_key_path:
        print("❌ SNOWFLAKE_PRIVATE_KEY_PATH environment variable is required")
        return False
    
    # Check if private key file exists
    key_file = Path(private_key_path)
    if not key_file.exists():
        print(f"❌ Private key file not found: {private_key_path}")
        return False
    
    print(f"📋 Configuration:")
    print(f"  Account: {account}")
    print(f"  User: {user}")
    print(f"  Warehouse: {warehouse}")
    print(f"  Database: {database}")
    print(f"  Schema: {schema}")
    print(f"  Role: {role}")
    print(f"  Private Key: {private_key_path}")
    print("-" * 50)
    
    try:
        # Load private key
        print("🔑 Loading private key...")
        with open(private_key_path, 'rb') as f:
            private_key_content = f.read()
        
        # Parse private key with optional passphrase
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
        
        # Convert to DER format for Snowflake
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        print("✅ Private key loaded successfully")
        
        # Create connection parameters
        connection_params = {
            'account': account,
            'user': user,
            'private_key': private_key_der,
            'warehouse': warehouse,
            'database': database,
            'schema': schema,
            'role': role,
        }
        
        # Test connection
        print("🔗 Testing Snowflake connection...")
        session = Session.builder.configs(connection_params).create()
        
        # Test basic query
        result = session.sql("""
            SELECT 
                CURRENT_USER() as user,
                CURRENT_ROLE() as role,
                CURRENT_WAREHOUSE() as warehouse,
                CURRENT_DATABASE() as database,
                CURRENT_SCHEMA() as schema,
                CURRENT_TIMESTAMP() as timestamp
        """).collect()
        
        if result:
            row = result[0]
            print("✅ Connection successful!")
            print(f"   Connected as: {row['USER']}")
            print(f"   Active role: {row['ROLE']}")
            print(f"   Using warehouse: {row['WAREHOUSE']}")
            print(f"   Current database: {row['DATABASE']}")
            print(f"   Current schema: {row['SCHEMA']}")
            print(f"   Server time: {row['TIMESTAMP']}")
            
            # Test ICYMTA table if exists
            try:
                table_result = session.sql(f"DESCRIBE TABLE {database}.{schema}.ICYMTA").collect()
                if table_result:
                    print(f"✅ ICYMTA table found with {len(table_result)} columns")
                    
                    # Get row count
                    count_result = session.sql(f"SELECT COUNT(*) as row_count FROM {database}.{schema}.ICYMTA").collect()
                    if count_result:
                        row_count = count_result[0]['ROW_COUNT']
                        print(f"   Table contains {row_count:,} rows")
                        
            except Exception as e:
                if "does not exist" in str(e).lower():
                    print(f"⚠️  ICYMTA table not found in {database}.{schema}")
                else:
                    print(f"⚠️  Could not access ICYMTA table: {e}")
            
            # Close session
            session.close()
            print("🔌 Session closed")
            
            print("\n🎉 Snowflake connectivity test PASSED!")
            return True
        else:
            print("❌ Query returned no results")
            session.close()
            return False
            
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Connection failed: {error_msg}")
        
        # Provide helpful suggestions based on error type
        if "Incorrect username or password" in error_msg:
            print("\n💡 Suggestions:")
            print("   • The public key may not be associated with your user account")
            print(f"   • Run in Snowflake: ALTER USER {user} SET RSA_PUBLIC_KEY='<your_public_key>';")
            print("   • Verify the user exists and has proper permissions")
            print("   • Check that the private key matches the registered public key")
        elif "does not exist" in error_msg.lower():
            print("\n💡 Suggestions:")
            print("   • Check if the account, warehouse, database, or schema exists")
            print("   • Verify the account identifier is correct")
        elif "network" in error_msg.lower() or "timeout" in error_msg.lower():
            print("\n💡 Suggestions:")
            print("   • Check network connectivity and firewall settings")
            print("   • Verify you can reach the Snowflake endpoint")
        elif "private key" in error_msg.lower() or "key" in error_msg.lower():
            print("\n💡 Suggestions:")
            print("   • Verify the private key file format (should be PKCS#8 .p8)")
            print("   • Check if the private key passphrase is correct")
            print("   • Ensure the private key file has proper permissions (600)")
        
        return False

def main():
    """Main function"""
    try:
        success = test_snowflake_connection()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()