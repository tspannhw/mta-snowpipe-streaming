#!/usr/bin/env python3
"""
Check the exact schema of the ICYMTA table to fix column mapping
"""

import os
import yaml
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
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Get Snowflake connection parameters
    account = config['snowflake']['account']
    user = config['snowflake']['user']
    warehouse = config['snowflake']['warehouse']
    database = config['snowflake']['database']
    schema = config['snowflake']['schema']
    role = config['snowflake']['role']
    
    # Load private key
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
    
    if not private_key_path:
        print("❌ SNOWFLAKE_PRIVATE_KEY_PATH environment variable not set")
        return
    
    private_key = load_private_key(private_key_path, private_key_passphrase)
    
    # Create Snowflake connection
    connection_params = {
        'account': account,
        'user': user,
        'warehouse': warehouse,
        'database': database,
        'schema': schema,
        'role': role,
        'private_key': private_key
    }
    
    print(f"Connecting to Snowflake: {account}")
    session = Session.builder.configs(connection_params).create()
    
    # Get table schema
    table_name = "DEMO.DEMO.ICYMTA"
    print(f"\n📋 Table Schema for {table_name}:")
    print("=" * 60)
    
    result = session.sql(f"DESCRIBE TABLE {table_name}").collect()
    
    print(f"Total columns: {len(result)}")
    print("\nColumn order and types:")
    for i, row in enumerate(result, 1):
        column_name = row[0]
        data_type = row[1]
        nullable = row[2]
        print(f"{i:2d}. {column_name:<30} {data_type:<20} {nullable}")
    
    # Show sample data to understand the current state
    print(f"\n📊 Sample Data from {table_name}:")
    print("=" * 60)
    try:
        sample_result = session.sql(f"SELECT * FROM {table_name} LIMIT 3").collect()
        if sample_result:
            # Get column names
            columns = [row[0] for row in result]
            print("Columns:", columns[:5], "...")
            
            for i, row in enumerate(sample_result):
                print(f"\nRow {i + 1}:")
                for j, value in enumerate(row):
                    if j < 10:  # Show first 10 columns
                        print(f"  {columns[j]}: {value}")
        else:
            print("No data found in table")
    except Exception as e:
        print(f"Error querying sample data: {e}")
    
    session.close()
    print("\n✅ Schema check complete!")

if __name__ == "__main__":
    main()