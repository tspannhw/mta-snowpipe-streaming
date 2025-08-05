#!/usr/bin/env python3
"""
Extract Public Key from Private Key for Snowflake Registration
"""

import os
import sys
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

def extract_public_key():
    """Extract public key from private key file"""
    print("🔑 Snowflake Public Key Extractor")
    print("=" * 50)
    
    # Load environment
    env_file = Path('.env')
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
    user = os.getenv('SNOWFLAKE_USER')
    
    if not private_key_path:
        print("❌ SNOWFLAKE_PRIVATE_KEY_PATH environment variable is required")
        return False
    
    if not user:
        print("❌ SNOWFLAKE_USER environment variable is required")
        return False
    
    # Check if private key file exists
    key_file = Path(private_key_path)
    if not key_file.exists():
        print(f"❌ Private key file not found: {private_key_path}")
        return False
    
    print(f"📁 Private Key File: {private_key_path}")
    print(f"👤 Snowflake User: {user}")
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
            print("✅ Private key loaded with passphrase")
        else:
            private_key = load_pem_private_key(
                private_key_content,
                password=None
            )
            print("✅ Private key loaded without passphrase")
        
        # Extract public key
        public_key = private_key.public_key()
        
        # Serialize public key to PEM format
        public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        # Convert to the format Snowflake expects (remove headers and newlines)
        public_key_str = public_key_pem.decode('utf-8')
        public_key_clean = public_key_str.replace('-----BEGIN PUBLIC KEY-----\n', '').replace('\n-----END PUBLIC KEY-----\n', '').replace('\n', '')
        
        print("\n🎯 SUCCESS! Public key extracted:")
        print("=" * 60)
        print(public_key_clean)
        print("=" * 60)
        
        # Save to file
        output_file = f"snowflake_public_key_{user}.txt"
        with open(output_file, 'w') as f:
            f.write(public_key_clean)
        
        print(f"\n💾 Public key saved to: {output_file}")
        
        # Provide Snowflake SQL command
        print(f"\n📋 Run this command in Snowflake to register the public key:")
        print("-" * 60)
        print(f"ALTER USER {user} SET RSA_PUBLIC_KEY='{public_key_clean}';")
        print("-" * 60)
        
        # Additional verification steps
        print(f"\n🔍 To verify the key is set correctly, run this in Snowflake:")
        print(f"DESC USER {user};")
        print("Look for the RSA_PUBLIC_KEY property in the output.")
        
        print(f"\n✅ Next steps:")
        print(f"1. Copy the ALTER USER command above")
        print(f"2. Run it in your Snowflake account (as ACCOUNTADMIN or user with appropriate privileges)")
        print(f"3. Re-run the connectivity test: python quick_snowflake_test.py")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to extract public key: {e}")
        return False

def main():
    """Main function"""
    try:
        success = extract_public_key()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()