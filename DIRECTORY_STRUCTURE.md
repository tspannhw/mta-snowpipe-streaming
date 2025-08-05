# 📁 MTA Snowpipe Streaming - Directory Structure

This document provides an overview of the organized project structure for the MTA Snowpipe Streaming High-Performance Architecture.

## 📂 **Root Directory**

```
mta/
├── 🐳 Core Application Files
│   ├── mta_snowpipe_streaming.py      # Main streaming application
│   ├── config.yaml                    # Application configuration
│   ├── requirements.txt               # Python dependencies
│   ├── docker-compose.yml             # Docker orchestration
│   ├── Dockerfile                     # Container definition
│   └── README.md                      # Project documentation
│
├── 📁 dashboard/                      # Real-time dashboard
│   ├── app.py                         # Flask dashboard app
│   ├── templates/                     # HTML templates
│   └── Dockerfile                     # Dashboard container
│
├── 📁 scripts/                       # Organized scripts by purpose
├── 📁 docs/                          # Documentation by category
├── 📁 images/                        # Documentation Screenshots
├── 📁 sql/                           # SQL files by function
├── 📁 data/                          # Data storage
├── 📁 logs/                          # Application logs
└── 📁 monitoring/                    # Monitoring configs
```

## 🔧 **Scripts Directory (`scripts/`)**

### **`scripts/setup/`** - Initial Setup & Configuration
```
setup/
├── enable_iceberg_with_accountadmin.py    # Enable Iceberg tables in Snowflake
└── setup_iceberg_streaming.py             # Configure Iceberg streaming parameters
```

### **`scripts/testing/`** - Testing & Verification
```
testing/
├── check_iceberg_parameters.py            # Verify Iceberg configuration
├── check_table_schema.py                  # Validate table schema
├── debug_coordinate_issues.py             # Debug coordinate data issues
├── debug_snowflake_auth.py                # Debug authentication issues
├── quick_snowflake_test.py               # Fast Snowflake connectivity test
├── quick_snowflake_test2.py              # Alternative connectivity test
├── snowtest.py                           # Comprehensive Snowflake test
├── test_account_formats.py               # Test account format variations
├── test_kafka_connection.py              # Verify Kafka connectivity
└── test_snowflake_connection.py          # Full Snowflake connection test
```

### **`scripts/utilities/`** - Helper Scripts
```
utilities/
├── architecture_diagram.py               # Generate architecture diagrams
├── extract_public_key.py                # Extract public key from private key
└── run_haversine_fix.py                 # Apply Haversine function fixes
```

### **`scripts/deployment/`** - Deployment Scripts
```
deployment/
└── deploy.sh                            # Main deployment automation script
```

## 📚 **Documentation Directory (`docs/`)**

### **`docs/architecture/`** - Architecture & Design
```
architecture/
├── mta_data_flow_diagram.pdf             # Data flow visualization
├── mta_data_flow_diagram.png             # Data flow diagram (PNG)
├── snowpipe_streaming_architecture.pdf   # Architecture documentation
└── snowpipe_streaming_architecture.png   # Architecture diagram (PNG)
```

### **`docs/deployment/`** - Deployment & Operations
```
deployment/
├── DEPLOY_UPDATES.md                     # Deployment update notes
├── DEPLOYMENT_NOTES.md                   # Deployment instructions
├── HAVERSINE_FIX_SUMMARY.md             # Haversine error fix documentation
├── env_template_keypair.txt              # Environment template for key-pair auth
└── snowflake_verification_checklist.md   # Snowflake setup checklist
```

### **`docs/kafka/`** - Kafka Integration
```
kafka/
├── KAFKA_MIGRATION_SUMMARY.md           # Kafka migration summary
└── KAFKA_SETUP.md                       # Kafka configuration guide
```

### **`docs/snowflake/`** - Snowflake Configuration
```
snowflake/
├── profile.json                          # Snowflake profile configuration
```

## 🗄️ **SQL Directory (`sql/`)**

### **`sql/setup/`** - Initial Database Setup
```
setup/
├── enable_iceberg_streaming.sql          # Enable Iceberg streaming features
├── snowflake_admin_commands.sql          # Administrative SQL commands
└── snowflake_setup.sql                   # Main database setup script
```

### **`sql/views/`** - Database Views & Queries
```
views/
└── performance_tuning.sql                # Performance optimization queries
```

### **`sql/fixes/`** - Bug Fixes & Patches
```
fixes/
└── fix_haversine_views.sql              # Fix Haversine function errors
```

## 🏗️ **Infrastructure Directories**

### **`data/`** - Data Storage
- Local data files and backups
- Temporary processing files

### **`logs/`** - Application Logs
- Application runtime logs
- Error logs and debugging output
- Failed record storage

### **`monitoring/`** - Monitoring Configuration
- Legacy monitoring configuration files
- Alerting and metrics setup

## 🎯 **Directory Organization Benefits**

### **📁 Clear Separation of Concerns**
- **Scripts**: Organized by purpose (setup, testing, utilities, deployment)
- **Documentation**: Categorized by topic (architecture, deployment, integrations)
- **SQL**: Grouped by function (setup, views, fixes)

### **🔍 Easy Navigation**
- **Purpose-driven structure**: Find files by their intended use
- **Logical grouping**: Related files stored together
- **Scalable organization**: Easy to add new files in appropriate categories

### **🚀 Development Workflow**
- **Setup**: Use `scripts/setup/` for initial configuration
- **Testing**: Run scripts from `scripts/testing/` for verification
- **Deployment**: Execute `scripts/deployment/deploy.sh` for production
- **Troubleshooting**: Reference `docs/` for guidance and use `scripts/utilities/` for fixes

### **📖 Documentation Access**
- **Architecture**: `docs/architecture/` for system design
- **Operations**: `docs/deployment/` for deployment guides
- **Integration**: `docs/kafka/` and `docs/snowflake/` for specific configurations

## 🔄 **Maintenance & Updates**

- **Add new scripts**: Place in appropriate `scripts/` subdirectory
- **Document changes**: Update relevant files in `docs/`
- **SQL modifications**: Organize by purpose in `sql/` subdirectories
- **Version control**: All directories included in `.gitignore` configuration

This organized structure provides a professional, maintainable codebase that scales with project complexity and team collaboration needs.