#!/usr/bin/env python3
"""
Snowflake Connectivity Test Script
Tests key-pair authentication and basic functionality
"""

import os
import sys
import yaml
import json
from datetime import datetime
from pathlib import Path

try:
    from snowflake.snowpark import Session
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    print("Install with: pip install snowflake-snowpark-python cryptography")
    sys.exit(1)

class SnowflakeConnectivityTest:
    def __init__(self):
        self.config = {}
        self.session = None
        self.test_results = []
    
    def log_test(self, test_name, success, message, details=None):
        """Log test results"""
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}: {message}")
        
        result = {
            "test": test_name,
            "success": success,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "details": details
        }
        self.test_results.append(result)
        
        if details and not success:
            for key, value in details.items():
                print(f"    {key}: {value}")
    
    def load_configuration(self):
        """Load Snowflake configuration from environment and config files"""
        print("🔧 Loading Snowflake Configuration")
        print("=" * 50)
        
        # Try to load from .env file if it exists
        env_file = Path('.env')
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value
        
        # Load from config.yaml if it exists
        config_file = Path('config.yaml')
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    yaml_config = yaml.safe_load(f) or {}
                    if 'snowflake' in yaml_config:
                        self.config.update(yaml_config['snowflake'])
            except Exception as e:
                print(f"⚠️  Warning: Could not load config.yaml: {e}")
        
        # Override with environment variables (priority)
        env_config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'STREAMING_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'DEMO'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DEMO'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
            'private_key_path': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'),
            'private_key_passphrase': os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
        }
        
        # Update config with non-None values
        for key, value in env_config.items():
            if value is not None:
                self.config[key] = value
        
        # Display configuration
        config_display = {k: v for k, v in self.config.items() if k != 'private_key_passphrase'}
        for key, value in config_display.items():
            print(f"  {key}: {value}")
        
        if self.config.get('private_key_passphrase'):
            print(f"  private_key_passphrase: ***HIDDEN***")
        
        print("-" * 50)
        
        return self.validate_configuration()
    
    def validate_configuration(self):
        """Validate required configuration parameters"""
        required_fields = ['account', 'user', 'private_key_path']
        missing_fields = []
        
        for field in required_fields:
            if not self.config.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            self.log_test(
                "Configuration Validation",
                False,
                f"Missing required fields: {', '.join(missing_fields)}",
                {
                    "missing_fields": missing_fields,
                    "required_env_vars": [
                        "SNOWFLAKE_ACCOUNT",
                        "SNOWFLAKE_USER",
                        "SNOWFLAKE_PRIVATE_KEY_PATH"
                    ]
                }
            )
            return False
        
        self.log_test("Configuration Validation", True, "All required fields present")
        return True
    
    def test_private_key_file(self):
        """Test private key file accessibility and format"""
        private_key_path = self.config.get('private_key_path')
        
        if not private_key_path:
            self.log_test("Private Key File", False, "No private key path specified")
            return False, None
        
        # Check if file exists
        key_file = Path(private_key_path)
        if not key_file.exists():
            self.log_test(
                "Private Key File",
                False,
                f"Private key file not found: {private_key_path}",
                {"suggestion": "Check the file path and ensure the file exists"}
            )
            return False, None
        
        # Check file permissions
        file_stat = key_file.stat()
        file_mode = oct(file_stat.st_mode)[-3:]
        
        if file_mode != '600':
            print(f"⚠️  Warning: Private key file permissions are {file_mode}, recommended: 600")
            print(f"    Run: chmod 600 {private_key_path}")
        
        # Try to load the private key
        try:
            with open(private_key_path, 'rb') as key_file:
                private_key_content = key_file.read()
            
            passphrase = self.config.get('private_key_passphrase')
            if passphrase:
                private_key = load_pem_private_key(
                    private_key_content,
                    password=passphrase.encode('utf-8')
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
            
            self.log_test("Private Key File", True, "Private key loaded and parsed successfully")
            return True, private_key_der
            
        except Exception as e:
            self.log_test(
                "Private Key File",
                False,
                f"Failed to load private key: {str(e)}",
                {
                    "error_type": type(e).__name__,
                    "suggestion": "Verify the key format is PKCS#8 (.p8) and passphrase is correct"
                }
            )
            return False, None
    
    def test_snowflake_connection(self, private_key_der):
        """Test Snowflake connection with key-pair authentication"""
        connection_params = {
            'account': self.config['account'],
            'user': self.config['user'],
            'private_key': private_key_der,
            'warehouse': self.config.get('warehouse', 'STREAMING_WH'),
            'database': self.config.get('database', 'DEMO'),
            'schema': self.config.get('schema', 'DEMO'),
            'role': self.config.get('role', 'SYSADMIN'),
        }
        
        try:
            print("🔗 Creating Snowflake session...")
            self.session = Session.builder.configs(connection_params).create()
            
            self.log_test("Snowflake Connection", True, "Session created successfully")
            return True
            
        except Exception as e:
            error_msg = str(e)
            suggestions = []
            
            if "Incorrect username or password" in error_msg:
                suggestions.extend([
                    "The public key may not be associated with the user",
                    f"Run in Snowflake: ALTER USER {self.config['user']} SET RSA_PUBLIC_KEY='<your_public_key>';",
                    "Verify the user exists and has proper permissions"
                ])
            elif "does not exist" in error_msg.lower():
                suggestions.append("Check if the account, warehouse, database, or schema exists")
            elif "network" in error_msg.lower() or "timeout" in error_msg.lower():
                suggestions.append("Check network connectivity and firewall settings")
            
            self.log_test(
                "Snowflake Connection",
                False,
                f"Connection failed: {error_msg}",
                {"suggestions": suggestions}
            )
            return False
    
    def test_basic_queries(self):
        """Test basic Snowflake queries"""
        if not self.session:
            self.log_test("Basic Queries", False, "No active session")
            return False
        
        try:
            # Test current context
            result = self.session.sql("""
                SELECT 
                    CURRENT_VERSION() as version,
                    CURRENT_USER() as user,
                    CURRENT_ROLE() as role,
                    CURRENT_WAREHOUSE() as warehouse,
                    CURRENT_DATABASE() as database,
                    CURRENT_SCHEMA() as schema,
                    CURRENT_TIMESTAMP() as timestamp
            """).collect()
            
            if result:
                row = result[0]
                self.log_test(
                    "Basic Queries",
                    True,
                    "Successfully executed context queries",
                    {
                        "snowflake_version": row['VERSION'],
                        "connected_user": row['USER'],
                        "active_role": row['ROLE'],
                        "active_warehouse": row['WAREHOUSE'],
                        "active_database": row['DATABASE'],
                        "active_schema": row['SCHEMA'],
                        "server_time": str(row['TIMESTAMP'])
                    }
                )
                return True
            else:
                self.log_test("Basic Queries", False, "Query returned no results")
                return False
                
        except Exception as e:
            self.log_test("Basic Queries", False, f"Query execution failed: {str(e)}")
            return False
    
    def test_iceberg_streaming_config(self):
        """Test Iceberg streaming configuration"""
        if not self.session:
            self.log_test("Iceberg Streaming Config", False, "No active session")
            return False
        
        try:
            result = self.session.sql(
                "SHOW PARAMETERS LIKE 'ENABLE_ICEBERG_TABLES' IN ACCOUNT"
            ).collect()
            
            if result:
                setting = result[0]
                enabled = setting['value'].upper() == 'TRUE'
                
                self.log_test(
                    "Iceberg Streaming Config",
                    enabled,
                    f"ENABLE_ICEBERG_TABLES = {setting['value']}",
                    {
                        "parameter": setting['key'],
                        "value": setting['value'],
                        "level": setting['level'] if 'level' in setting else 'unknown'
                    }
                )
                
                if not enabled:
                    print("    💡 To enable: ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;")
                
                return enabled
            else:
                self.log_test(
                    "Iceberg Streaming Config",
                    False,
                    "ENABLE_ICEBERG_TABLES parameter not found",
                    {"suggestion": "Contact your Snowflake administrator to enable Iceberg streaming"}
                )
                return False
                
        except Exception as e:
            self.log_test(
                "Iceberg Streaming Config",
                False,
                f"Failed to check Iceberg streaming config: {str(e)}"
            )
            return False
    
    def test_icymta_table_access(self):
        """Test access to the ICYMTA table"""
        if not self.session:
            self.log_test("ICYMTA Table Access", False, "No active session")
            return False
        
        database = self.config.get('database', 'DEMO')
        schema = self.config.get('schema', 'DEMO')
        table_name = f"{database}.{schema}.ICYMTA"
        
        try:
            # Test if table exists
            result = self.session.sql(f"DESCRIBE TABLE {table_name}").collect()
            
            if result:
                column_count = len(result)
                self.log_test(
                    "ICYMTA Table Access",
                    True,
                    f"Table exists with {column_count} columns",
                    {
                        "table_name": table_name,
                        "column_count": column_count,
                        "columns": [row['name'] for row in result[:5]]  # First 5 columns
                    }
                )
                
                # Test row count
                try:
                    count_result = self.session.sql(f"SELECT COUNT(*) as row_count FROM {table_name}").collect()
                    if count_result:
                        row_count = count_result[0]['ROW_COUNT']
                        print(f"    📊 Table has {row_count:,} rows")
                except Exception as e:
                    print(f"    ⚠️  Could not get row count: {e}")
                
                return True
            else:
                self.log_test("ICYMTA Table Access", False, f"Table {table_name} not found")
                return False
                
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                self.log_test(
                    "ICYMTA Table Access",
                    False,
                    f"Table {table_name} does not exist",
                    {"suggestion": "Run the snowflake_setup.sql script to create the table"}
                )
            else:
                self.log_test(
                    "ICYMTA Table Access",
                    False,
                    f"Failed to access table: {error_msg}"
                )
            return False
    
    def test_write_permissions(self):
        """Test write permissions by creating a temporary table"""
        if not self.session:
            self.log_test("Write Permissions", False, "No active session")
            return False
        
        database = self.config.get('database', 'DEMO')
        schema = self.config.get('schema', 'DEMO')
        test_table = f"{database}.{schema}.SNOWPIPE_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create test table
            self.session.sql(f"""
                CREATE OR REPLACE TEMPORARY TABLE {test_table} (
                    test_id INTEGER,
                    test_message STRING,
                    test_timestamp TIMESTAMP
                )
            """).collect()
            
            # Insert test data
            self.session.sql(f"""
                INSERT INTO {test_table} VALUES 
                (1, 'Snowpipe connectivity test', CURRENT_TIMESTAMP())
            """).collect()
            
            # Query test data
            result = self.session.sql(f"SELECT * FROM {test_table}").collect()
            
            if result and len(result) == 1:
                self.log_test(
                    "Write Permissions",
                    True,
                    "Successfully created table, inserted data, and queried results",
                    {"test_table": test_table}
                )
                
                # Clean up
                self.session.sql(f"DROP TABLE IF EXISTS {test_table}").collect()
                return True
            else:
                self.log_test("Write Permissions", False, "Test data not found after insert")
                return False
                
        except Exception as e:
            self.log_test(
                "Write Permissions",
                False,
                f"Write test failed: {str(e)}",
                {"suggestion": "Check if the user has CREATE and INSERT permissions"}
            )
            return False
    
    def cleanup(self):
        """Clean up resources"""
        if self.session:
            try:
                self.session.close()
                print("🔌 Session closed")
            except Exception as e:
                print(f"⚠️  Error closing session: {e}")
    
    def generate_report(self):
        """Generate a summary report"""
        print("\n" + "=" * 60)
        print("📋 SNOWFLAKE CONNECTIVITY TEST REPORT")
        print("=" * 60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['success'])
        failed_tests = total_tests - passed_tests
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests} ✅")
        print(f"Failed: {failed_tests} ❌")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print(f"\n❌ Failed Tests:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  • {result['test']}: {result['message']}")
        
        # Save detailed report
        report_file = f"snowflake_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(report_file, 'w') as f:
                json.dump({
                    'summary': {
                        'total_tests': total_tests,
                        'passed': passed_tests,
                        'failed': failed_tests,
                        'success_rate': (passed_tests/total_tests)*100
                    },
                    'configuration': {k: v for k, v in self.config.items() if k != 'private_key_passphrase'},
                    'test_results': self.test_results
                }, f, indent=2, default=str)
            print(f"\n📄 Detailed report saved: {report_file}")
        except Exception as e:
            print(f"⚠️  Could not save report: {e}")
        
        return failed_tests == 0
    
    def run_all_tests(self):
        """Run all connectivity tests"""
        print("🚀 Starting Snowflake Connectivity Tests")
        print("=" * 60)
        
        try:
            # 1. Load and validate configuration
            if not self.load_configuration():
                return False
            
            # 2. Test private key file
            key_valid, private_key_der = self.test_private_key_file()
            if not key_valid:
                return False
            
            # 3. Test Snowflake connection
            if not self.test_snowflake_connection(private_key_der):
                return False
            
            # 4. Test basic queries
            if not self.test_basic_queries():
                return False
            
            # 5. Test Iceberg streaming configuration
            self.test_iceberg_streaming_config()
            
            # 6. Test ICYMTA table access
            self.test_icymta_table_access()
            
            # 7. Test write permissions
            self.test_write_permissions()
            
            return True
            
        except KeyboardInterrupt:
            print("\n⚠️  Tests interrupted by user")
            return False
        except Exception as e:
            print(f"\n❌ Unexpected error during testing: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """Main function"""
    test_runner = SnowflakeConnectivityTest()
    
    try:
        success = test_runner.run_all_tests()
        final_success = test_runner.generate_report()
        
        if final_success:
            print("\n🎉 All critical tests passed! Snowflake connectivity is working properly.")
            sys.exit(0)
        else:
            print("\n⚠️  Some tests failed. Check the report above for details.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()