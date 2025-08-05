#!/usr/bin/env python3
"""
Debug Snowflake Key-Pair Authentication Issues
Comprehensive troubleshooting for JWT token invalid errors
"""

import os
import sys
import re
import base64
import json
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

try:
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    import jwt
    import requests
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    print("Install with: pip install cryptography PyJWT requests")
    sys.exit(1)

class SnowflakeAuthDebugger:
    def __init__(self):
        self.config = {}
        self.private_key = None
        self.public_key = None
        
    def load_configuration(self):
        """Load configuration from environment"""
        print("🔧 Loading Configuration")
        print("=" * 50)
        
        # Load from .env file if it exists
        env_file = Path('.env')
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value
        
        self.config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'private_key_path': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'),
            'private_key_passphrase': os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
        }
        
        for key, value in self.config.items():
            if key != 'private_key_passphrase':
                print(f"  {key}: {value}")
            elif value:
                print(f"  {key}: ***HIDDEN***")
        
        print("-" * 50)
        
        # Validate required fields
        if not self.config['account'] or not self.config['user'] or not self.config['private_key_path']:
            print("❌ Missing required configuration")
            return False
        
        return True
    
    def analyze_account_identifier(self):
        """Analyze the account identifier format"""
        print("🏢 Analyzing Account Identifier")
        print("=" * 50)
        
        account = self.config['account']
        print(f"Raw account: {account}")
        
        # Check for common account formats
        patterns = {
            'Legacy format': r'^[A-Z0-9]+$',
            'Organization format': r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$',
            'Full URL': r'^.*\.snowflakecomputing\.com$',
            'Region format': r'^[A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+$'
        }
        
        matched_format = None
        for format_name, pattern in patterns.items():
            if re.match(pattern, account):
                matched_format = format_name
                print(f"✅ Account format: {format_name}")
                break
        
        if not matched_format:
            print(f"⚠️  Unknown account format: {account}")
        
        # Suggest normalized format
        if '.snowflakecomputing.com' in account:
            normalized = account.replace('.snowflakecomputing.com', '')
            print(f"💡 Try using: {normalized}")
            self.config['account_normalized'] = normalized
        else:
            self.config['account_normalized'] = account
        
        print("-" * 50)
        return True
    
    def load_and_validate_keys(self):
        """Load and validate private/public key pair"""
        print("🔑 Loading and Validating Keys")
        print("=" * 50)
        
        private_key_path = self.config['private_key_path']
        
        # Check file existence and permissions
        key_file = Path(private_key_path)
        if not key_file.exists():
            print(f"❌ Private key file not found: {private_key_path}")
            return False
        
        file_stat = key_file.stat()
        file_mode = oct(file_stat.st_mode)[-3:]
        print(f"📁 File: {private_key_path}")
        print(f"🔒 Permissions: {file_mode}")
        
        if file_mode != '600':
            print(f"⚠️  Recommended permissions: 600 (current: {file_mode})")
            print(f"   Run: chmod 600 {private_key_path}")
        
        # Load private key
        try:
            with open(private_key_path, 'rb') as f:
                private_key_content = f.read()
            
            passphrase = self.config['private_key_passphrase']
            if passphrase:
                self.private_key = load_pem_private_key(
                    private_key_content,
                    password=passphrase.encode('utf-8')
                )
                print("✅ Private key loaded with passphrase")
            else:
                self.private_key = load_pem_private_key(
                    private_key_content,
                    password=None
                )
                print("✅ Private key loaded without passphrase")
            
            # Extract public key
            self.public_key = self.private_key.public_key()
            
            # Validate key properties
            if isinstance(self.private_key, rsa.RSAPrivateKey):
                key_size = self.private_key.key_size
                print(f"🔢 Key type: RSA")
                print(f"🔢 Key size: {key_size} bits")
                
                if key_size < 2048:
                    print(f"⚠️  Warning: Key size {key_size} is below recommended 2048 bits")
            else:
                print(f"❌ Unsupported key type: {type(self.private_key)}")
                return False
            
            print("-" * 50)
            return True
            
        except Exception as e:
            print(f"❌ Failed to load private key: {e}")
            return False
    
    def generate_and_display_public_key(self):
        """Generate public key in Snowflake format"""
        print("📋 Public Key for Snowflake Registration")
        print("=" * 50)
        
        # Serialize public key to PEM format
        public_key_pem = self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        # Convert to Snowflake format (remove headers and newlines)
        public_key_str = public_key_pem.decode('utf-8')
        public_key_clean = public_key_str.replace('-----BEGIN PUBLIC KEY-----\n', '').replace('\n-----END PUBLIC KEY-----\n', '').replace('\n', '')
        
        print("Public key (Snowflake format):")
        print(public_key_clean)
        print()
        
        # Generate fingerprint for verification
        fingerprint = hashlib.sha256(public_key_pem).hexdigest()[:16]
        print(f"Key fingerprint: {fingerprint}")
        
        # Save to file
        output_file = f"public_key_{self.config['user']}.txt"
        with open(output_file, 'w') as f:
            f.write(public_key_clean)
        print(f"💾 Saved to: {output_file}")
        
        # SQL command
        print(f"\n📝 Run this in Snowflake:")
        print(f"ALTER USER {self.config['user']} SET RSA_PUBLIC_KEY='{public_key_clean}';")
        
        print("-" * 50)
        return public_key_clean
    
    def test_jwt_generation(self):
        """Test JWT token generation"""
        print("🎫 Testing JWT Token Generation")
        print("=" * 50)
        
        try:
            # Prepare JWT claims
            user = self.config['user'].upper()
            account = self.config['account_normalized'].upper()
            
            iat = int(time.time())
            exp = iat + 3600  # 1 hour expiration
            
            # JWT header
            header = {
                'typ': 'JWT',
                'alg': 'RS256'
            }
            
            # JWT payload (claims)
            payload = {
                'iss': f"{account}.{user}.SHA256:{self._get_public_key_fingerprint()}",
                'sub': f"{account}.{user}",
                'iat': iat,
                'exp': exp
            }
            
            print(f"JWT Header: {json.dumps(header, indent=2)}")
            print(f"JWT Payload: {json.dumps(payload, indent=2)}")
            
            # Convert private key to PEM format for PyJWT
            private_key_pem = self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            # Generate JWT token
            token = jwt.encode(payload, private_key_pem, algorithm='RS256', headers=header)
            
            print(f"\n🎫 Generated JWT Token:")
            print(f"Length: {len(token)} characters")
            print(f"Token: {token[:50]}...{token[-50:]}")
            
            # Decode and verify token structure
            try:
                decoded_header = jwt.get_unverified_header(token)
                decoded_payload = jwt.decode(token, options={"verify_signature": False})
                
                print(f"\n✅ JWT Structure Valid:")
                print(f"  Header: {decoded_header}")
                print(f"  Payload: {decoded_payload}")
                
            except Exception as e:
                print(f"❌ JWT structure invalid: {e}")
                return False
            
            print("-" * 50)
            return True
            
        except Exception as e:
            print(f"❌ JWT generation failed: {e}")
            return False
    
    def _get_public_key_fingerprint(self):
        """Get SHA256 fingerprint of public key"""
        public_key_der = self.public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return hashlib.sha256(public_key_der).hexdigest().upper()
    
    def test_account_connectivity(self):
        """Test basic connectivity to Snowflake account"""
        print("🌐 Testing Account Connectivity")
        print("=" * 50)
        
        account = self.config['account_normalized']
        
        # Test different URL formats
        urls_to_test = [
            f"https://{account}.snowflakecomputing.com",
            f"https://{account.lower()}.snowflakecomputing.com",
        ]
        
        for url in urls_to_test:
            try:
                print(f"Testing: {url}")
                response = requests.get(f"{url}/healthcheck", timeout=10)
                print(f"  Status: {response.status_code}")
                if response.status_code == 200:
                    print(f"  ✅ Account endpoint reachable")
                else:
                    print(f"  ⚠️  Unexpected status code")
            except requests.exceptions.RequestException as e:
                print(f"  ❌ Connection failed: {e}")
        
        print("-" * 50)
    
    def provide_troubleshooting_steps(self):
        """Provide comprehensive troubleshooting steps"""
        print("🔧 Troubleshooting Steps")
        print("=" * 50)
        
        user = self.config['user']
        
        print("1. 🔑 Verify Public Key Registration:")
        print(f"   Run in Snowflake: DESC USER {user};")
        print(f"   Look for 'RSA_PUBLIC_KEY' property")
        print()
        
        print("2. 🔄 Re-register Public Key:")
        print(f"   Run the ALTER USER command from above")
        print(f"   Ensure you have ACCOUNTADMIN or user management privileges")
        print()
        
        print("3. ✅ Verify User Exists and is Active:")
        print(f"   Run in Snowflake: SHOW USERS LIKE '{user}';")
        print()
        
        print("4. 🎯 Check User Permissions:")
        print(f"   Run in Snowflake: SHOW GRANTS TO USER {user};")
        print()
        
        print("5. 🔐 Test with Different Role:")
        print(f"   Try setting SNOWFLAKE_ROLE=ACCOUNTADMIN")
        print()
        
        print("6. 📊 Check Account Parameters:")
        print(f"   Run in Snowflake: SHOW PARAMETERS LIKE '%RSA%' IN ACCOUNT;")
        print()
        
        print("7. 🔍 Common Issues:")
        print("   • Public key not registered or incorrect")
        print("   • User doesn't exist or is disabled")
        print("   • Account identifier format incorrect")
        print("   • Network/firewall blocking connection")
        print("   • Private key file corrupted or wrong format")
        
        print("-" * 50)
    
    def run_debug_session(self):
        """Run complete debugging session"""
        print("🐛 Snowflake Key-Pair Authentication Debugger")
        print("=" * 60)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("=" * 60)
        
        success_steps = 0
        total_steps = 6
        
        try:
            # Step 1: Load configuration
            if self.load_configuration():
                success_steps += 1
            else:
                return False
            
            # Step 2: Analyze account identifier
            if self.analyze_account_identifier():
                success_steps += 1
            
            # Step 3: Load and validate keys
            if self.load_and_validate_keys():
                success_steps += 1
            else:
                return False
            
            # Step 4: Generate public key
            if self.generate_and_display_public_key():
                success_steps += 1
            
            # Step 5: Test JWT generation
            if self.test_jwt_generation():
                success_steps += 1
            
            # Step 6: Test connectivity
            self.test_account_connectivity()
            success_steps += 1
            
            # Provide troubleshooting steps
            self.provide_troubleshooting_steps()
            
            print(f"\n📊 Debug Summary: {success_steps}/{total_steps} steps completed successfully")
            
            if success_steps == total_steps:
                print("✅ All validation steps passed. The issue is likely:")
                print("   1. Public key not registered in Snowflake")
                print("   2. User doesn't have proper permissions")
                print("   3. Account/user configuration mismatch")
            
            return success_steps >= 4
            
        except Exception as e:
            print(f"💥 Debug session failed: {e}")
            return False

def main():
    """Main function"""
    debugger = SnowflakeAuthDebugger()
    
    try:
        success = debugger.run_debug_session()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Debug session interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()