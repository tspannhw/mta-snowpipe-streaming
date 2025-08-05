#!/usr/bin/env python3
"""
Test different Snowflake account identifier formats
Helps identify the correct account format if JWT token is invalid
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

def test_account_format(account_format):
    """Test a specific account format"""
    print(f"\n🧪 Testing account format: {account_format}")
    print("-" * 50)
    
    # Load private key
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
    
    try:
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
        
        # Test connection with this account format
        connection_params = {
            'account': account_format,
            'user': os.getenv('SNOWFLAKE_USER'),
            'private_key': private_key_der,
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'STREAMING_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'DEMO'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DEMO'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
        }
        
        session = Session.builder.configs(connection_params).create()
        result = session.sql("SELECT CURRENT_USER(), CURRENT_ACCOUNT()").collect()
        
        if result:
            row = result[0]
            print(f"✅ SUCCESS with account format: {account_format}")
            print(f"   Connected user: {row[0]}")
            print(f"   Actual account: {row[1]}")
            session.close()
            return True, account_format
        
    except Exception as e:
        error_msg = str(e)
        if "JWT token is invalid" in error_msg:
            print(f"🔑 JWT token invalid (public key issue): {account_format}")
        elif "Incorrect username or password" in error_msg:
            print(f"🔐 Auth failed: {account_format}")
        elif "does not exist" in error_msg.lower():
            print(f"❌ Account not found: {account_format}")
        elif "network" in error_msg.lower() or "timeout" in error_msg.lower():
            print(f"🌐 Network error: {account_format}")
        else:
            print(f"❌ Other error: {account_format} - {error_msg}")
    
    return False, None

def main():
    """Test different account identifier formats"""
    print("🏢 Snowflake Account Format Tester")
    print("=" * 50)
    
    load_env_file()
    
    original_account = os.getenv('SNOWFLAKE_ACCOUNT')
    if not original_account:
        print("❌ SNOWFLAKE_ACCOUNT not set")
        return
    
    print(f"Original account: {original_account}")
    
    # Generate different account format variations
    account_formats = [
        original_account,  # Original format
        original_account.upper(),  # Uppercase
        original_account.lower(),  # Lowercase
    ]
    
    # Remove .snowflakecomputing.com if present
    if '.snowflakecomputing.com' in original_account:
        base_account = original_account.replace('.snowflakecomputing.com', '')
        account_formats.extend([
            base_account,
            base_account.upper(),
            base_account.lower()
        ])
    
    # Remove duplicates while preserving order
    account_formats = list(dict.fromkeys(account_formats))
    
    print(f"\nTesting {len(account_formats)} account format variations...")
    
    successful_formats = []
    jwt_invalid_formats = []
    
    for account_format in account_formats:
        success, working_format = test_account_format(account_format)
        if success:
            successful_formats.append(working_format)
        elif "JWT token invalid" in str(test_account_format(account_format)):
            jwt_invalid_formats.append(account_format)
    
    print("\n" + "=" * 60)
    print("📊 RESULTS SUMMARY")
    print("=" * 60)
    
    if successful_formats:
        print(f"✅ WORKING FORMATS:")
        for fmt in successful_formats:
            print(f"   • {fmt}")
        print(f"\n🎯 Use this account format: {successful_formats[0]}")
    
    elif jwt_invalid_formats:
        print(f"🔑 JWT TOKEN INVALID (Public key not registered):")
        for fmt in jwt_invalid_formats:
            print(f"   • {fmt}")
        print(f"\n💡 The account format is correct, but you need to register your public key!")
        print(f"   Run: ALTER USER {os.getenv('SNOWFLAKE_USER')} SET RSA_PUBLIC_KEY='...'")
    
    else:
        print("❌ NO WORKING FORMATS FOUND")
        print("💡 Check:")
        print("   • Account identifier spelling")
        print("   • Network connectivity")
        print("   • User exists in the account")

if __name__ == "__main__":
    main()