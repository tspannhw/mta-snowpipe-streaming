#!/usr/bin/env python3
import os
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

# Load private key
private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
with open(private_key_path, 'rb') as f:
    private_key = load_pem_private_key(f.read(), password=None)

private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Test connection
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
    result = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()").collect()
    print(f"✅ Connected as: {result[0][0]} with role {result[0][1]} using warehouse {result[0][2]}")
    session.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")