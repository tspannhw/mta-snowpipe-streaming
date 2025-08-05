#!/bin/bash

# ============================================================================
# Snowpipe Streaming High-Performance Architecture Deployment Script
# MTA Real-time Transit Data Pipeline
# ============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

# Functions
print_banner() {
    echo -e "${BLUE}"
    echo "============================================================================"
    echo "  Snowpipe Streaming High-Performance Architecture Deployment"
    echo "  MTA Real-time Transit Data Pipeline with Kafka Integration"
    echo "============================================================================"
    echo -e "${NC}"
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Docker is installed and running
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Check if Docker Compose is available
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    
    # Set the compose command
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    else
        COMPOSE_CMD="docker compose"
    fi
    
    log_info "Prerequisites check completed successfully"
}

check_environment() {
    log_info "Checking environment configuration..."
    
    if [[ ! -f "$ENV_FILE" ]]; then
        log_warn "Environment file not found. Creating template..."
        create_env_template
        log_error "Please configure the .env file with your credentials and run the script again."
        exit 1
    fi
    
    # Source the environment file
    source "$ENV_FILE"
    
    # Check data source configuration
    DATA_SOURCE="${DEFAULT_DATA_SOURCE:-kafka}"
    log_info "Data source configured as: $DATA_SOURCE"
    
    # Check required Snowflake variables (always required)
    snowflake_vars=("SNOWFLAKE_ACCOUNT" "SNOWFLAKE_USER" "SNOWFLAKE_PRIVATE_KEY_PATH")
    for var in "${snowflake_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log_error "Required Snowflake variable $var is not set in .env file"
            if [[ "$var" == "SNOWFLAKE_PRIVATE_KEY_PATH" ]]; then
                log_error "Key-pair authentication is required. Password authentication is not supported."
                log_error "Please set SNOWFLAKE_PRIVATE_KEY_PATH to your .p8 private key file path"
            fi
            exit 1
        fi
    done
    
    # Validate private key file exists and is readable
    if [[ ! -f "$SNOWFLAKE_PRIVATE_KEY_PATH" ]]; then
        log_error "Private key file not found: $SNOWFLAKE_PRIVATE_KEY_PATH"
        log_error "Please ensure the private key file exists and the path is correct"
        exit 1
    fi
    
    if [[ ! -r "$SNOWFLAKE_PRIVATE_KEY_PATH" ]]; then
        log_error "Private key file is not readable: $SNOWFLAKE_PRIVATE_KEY_PATH"
        log_error "Please check file permissions (should be 600 or 400)"
        exit 1
    fi
    
    log_info "Snowflake key-pair authentication validated successfully"
    
    # Check data source specific variables
    if [[ "$DATA_SOURCE" == "kafka" ]]; then
        log_info "Validating Kafka configuration..."
        kafka_vars=("KAFKA_BOOTSTRAP_SERVERS" "KAFKA_SASL_USERNAME" "KAFKA_SASL_PASSWORD")
        for var in "${kafka_vars[@]}"; do
            if [[ -z "${!var:-}" ]]; then
                log_error "Required Kafka variable $var is not set in .env file"
                log_error "For Kafka data source, please configure all Kafka authentication variables"
                exit 1
            fi
        done
        log_info "Kafka configuration validated successfully"
    else
        log_error "Invalid DEFAULT_DATA_SOURCE: $DATA_SOURCE. Must be 'kafka'"
        exit 1
    fi
    
    log_info "Environment configuration check completed"
}

create_env_template() {
    cat > "$ENV_FILE" << 'EOF'
# ============================================================================
# Environment Configuration for Snowpipe Streaming with Kafka
# ============================================================================

# Snowflake Configuration (REQUIRED - Key-pair Authentication Only)
SNOWFLAKE_ACCOUNT=your_account.region.aws
SNOWFLAKE_USER=your_username
# Key-pair authentication (REQUIRED - no password support)
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/snowflake_private_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=  # Optional - only if private key is encrypted
SNOWFLAKE_WAREHOUSE=STREAMING_WH
SNOWFLAKE_DATABASE=DEMO
SNOWFLAKE_SCHEMA=DEMO
SNOWFLAKE_ROLE=PUBLIC

# Data Source Configuration
DEFAULT_DATA_SOURCE=kafka  # Options: kafka, mta_api

# Kafka Configuration (PRIMARY SOURCE - REQUIRED for kafka mode)
KAFKA_BOOTSTRAP_SERVERS=your-kafka-broker:9092
KAFKA_SASL_USERNAME=your_kafka_username
KAFKA_SASL_PASSWORD=your_kafka_password
KAFKA_TOPIC=icymta
KAFKA_CONSUMER_GROUP=snowpipe-streaming-consumer

# Application Performance Settings
PARALLEL_CHANNELS=4
BATCH_SIZE=1000
FLUSH_INTERVAL_SECONDS=10
MAX_MEMORY_GB=4
MAX_CPU_CORES=2

# Monitoring Configuration
HEALTH_CHECK_PORT=8001
ENABLE_HEALTH_CHECKS=true

# Optional: Advanced Kafka Settings
# KAFKA_SESSION_TIMEOUT_MS=30000
# KAFKA_HEARTBEAT_INTERVAL_MS=3000
# KAFKA_MAX_POLL_RECORDS=500
# KAFKA_AUTO_OFFSET_RESET=latest

# Optional: SSL Certificate Paths (if required)
# KAFKA_SSL_CAFILE=/path/to/ca-cert.pem
# KAFKA_SSL_CERTFILE=/path/to/client-cert.pem
# KAFKA_SSL_KEYFILE=/path/to/client-key.pem

# Optional: Alerting Configuration
ALERT_EMAIL=alerts@yourdomain.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK
EOF
    
    log_info "Environment template created at $ENV_FILE"
    echo ""
    log_warn "IMPORTANT: Configure the following based on your data source:"
    echo "  • For Kafka: Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD"
    echo "  • For MTA API: Set MTA_API_KEY and change DEFAULT_DATA_SOURCE=mta_api"
    echo "  • For Snowflake: Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH"
}

setup_directories() {
    log_info "Setting up required directories..."
    
    # Create necessary directories
    mkdir -p logs
    mkdir -p data
    mkdir -p monitoring/grafana/dashboards
    mkdir -p monitoring/grafana/datasources
    
    # Set proper permissions
    chmod 755 logs data
    chmod -R 755 monitoring
    
    log_info "Directories setup completed"
}

run_snowflake_setup() {
    log_info "Running Snowflake setup scripts..."
    
    # Check if snowsql is available for running setup scripts
    if command -v snowsql &> /dev/null; then
        log_info "Running Snowflake initialization script..."
        
        # Note: In production, you would run these with proper authentication
        log_warn "Please run the following SQL scripts manually in Snowflake:"
        echo "  1. snowflake_setup.sql"
        echo "  2. performance_tuning.sql"
    else
        log_warn "SnowSQL not found. Please run the Snowflake setup scripts manually:"
        echo "  1. snowflake_setup.sql"
        echo "  2. performance_tuning.sql"
    fi
}

build_and_deploy() {
    log_info "Building and deploying the application..."
    
    # Build the Docker image
    log_info "Building Docker image..."
    $COMPOSE_CMD build --no-cache
    
    # Start the services
    log_info "Starting services..."
    $COMPOSE_CMD up -d
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    check_service_health
}

check_service_health() {
    log_info "Checking service health..."
    
    services=("snowpipe-streaming" "dashboard")
    
    for service in "${services[@]}"; do
        if $COMPOSE_CMD ps "$service" | grep -q "Up"; then
            log_info "$service is running"
        else
            log_error "$service is not running properly"
            $COMPOSE_CMD logs "$service"
        fi
    done
    
    # Check application health endpoint
    log_info "Checking application health endpoint..."
    log_info "Waiting for services to initialize..."
    sleep 30
    
    if curl -s -f http://localhost:8001/health > /dev/null; then
        log_info "Application health check passed"
        
        # Check data source specific health
        DATA_SOURCE="${DEFAULT_DATA_SOURCE:-kafka}"
        if [[ "$DATA_SOURCE" == "kafka" ]]; then
            log_info "Checking Kafka integration..."
            # Try to get health info with Kafka details
            health_response=$(curl -s http://localhost:8001/health 2>/dev/null || echo '{}')
            if echo "$health_response" | grep -q "kafka" 2>/dev/null; then
                log_info "Kafka integration health check included"
            fi
        fi
    else
        log_warn "Application health check failed - service may still be starting"
    fi
    
    # Check dashboard health endpoint
    log_info "Checking dashboard health endpoint..."
    log_info "Waiting for dashboard to initialize..."
    sleep 15
    
    if curl -s -f http://localhost:8080/api/metrics > /dev/null; then
        log_info "Dashboard health check passed - real-time dashboard is ready"
    else
        log_warn "Dashboard health check failed - dashboard may still be starting"
    fi
}

show_access_info() {
    echo -e "${BLUE}"
    echo "============================================================================"
    echo "  Deployment Completed Successfully!"
    echo "============================================================================"
    echo -e "${NC}"
    
    # Show data source information
    DATA_SOURCE="${DEFAULT_DATA_SOURCE:-kafka}"
    echo "Data Source Configuration:"
    echo "  • Primary Source:     $DATA_SOURCE"
    if [[ "$DATA_SOURCE" == "kafka" ]]; then
        echo "  • Kafka Topic:        ${KAFKA_TOPIC:-icymta}"
        echo "  • Consumer Group:     ${KAFKA_CONSUMER_GROUP:-snowpipe-streaming-consumer}"
    fi
    echo ""
    
    echo "Service Access URLs:"
    echo "  • 📊 Real-time Dashboard: http://localhost:8080"
    echo "  • 🏥 Application Health:  http://localhost:8001/health"
    echo ""
    echo "Monitoring Commands:"
    echo "  • View all logs:      $COMPOSE_CMD logs -f"
    echo "  • View app logs:      $COMPOSE_CMD logs -f snowpipe-streaming"
    echo "  • View dashboard:     $COMPOSE_CMD logs -f dashboard"
    echo "  • Check status:       $COMPOSE_CMD ps"
    echo "  • Stop services:      $COMPOSE_CMD down"
    echo "  • Restart services:   $COMPOSE_CMD restart"
    echo "  • Test Kafka:         ./deploy.sh test-kafka"

    
    if [[ "$DATA_SOURCE" == "kafka" ]]; then
        echo ""
        log_info "Kafka Integration Active - Monitor consumer lag and message processing"
    fi
}

test_kafka_connection() {
    log_info "Testing Kafka connection..."
    
    # Check if environment is loaded
    if [[ ! -f "$ENV_FILE" ]]; then
        log_error "Environment file not found. Please run setup first."
        exit 1
    fi
    
    # Source environment
    source "$ENV_FILE"
    
    # Check if Kafka is configured
    if [[ "${DEFAULT_DATA_SOURCE:-kafka}" != "kafka" ]]; then
        log_warn "Data source is not configured for Kafka (current: ${DEFAULT_DATA_SOURCE:-unknown})"
        log_info "To test Kafka, set DEFAULT_DATA_SOURCE=kafka in .env file"
        exit 1
    fi
    
    # Export environment variables for the test script
    export KAFKA_BOOTSTRAP_SERVERS KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD KAFKA_TOPIC
    
    # Run the Kafka connection test
    if [[ -f "test_kafka_connection.py" ]]; then
        log_info "Running Kafka connection test..."
        if python test_kafka_connection.py; then
            log_info "Kafka connection test passed!"
        else
            log_error "Kafka connection test failed!"
            echo ""
            log_info "Troubleshooting steps:"
            echo "  1. Verify Kafka credentials in .env file"
            echo "  2. Check network connectivity to Kafka brokers"
            echo "  3. Ensure the icymta topic exists and has data"
            echo "  4. Verify SASL/SSL configuration"
            exit 1
        fi
    else
        log_error "test_kafka_connection.py not found"
        log_info "Please ensure the test script is in the current directory"
        exit 1
    fi
}

cleanup() {
    log_info "Cleaning up previous deployment..."
    
    # Stop and remove existing containers
    $COMPOSE_CMD down -v --remove-orphans
    
    # Remove old images (optional)
    if [[ "${CLEAN_IMAGES:-false}" == "true" ]]; then
        docker image prune -f
    fi
    
    log_info "Cleanup completed"
}

main() {
    print_banner
    
    # Parse command line arguments
    case "${1:-deploy}" in
        "deploy")
            check_prerequisites
            check_environment
            setup_directories
            build_and_deploy
            show_access_info
            ;;
        "setup")
            check_prerequisites
            check_environment
            setup_directories
            run_snowflake_setup
            ;;
        "start")
            check_prerequisites
            $COMPOSE_CMD up -d
            check_service_health
            show_access_info
            ;;
        "stop")
            check_prerequisites
            $COMPOSE_CMD down
            log_info "Services stopped"
            ;;
        "restart")
            check_prerequisites
            $COMPOSE_CMD restart
            check_service_health
            log_info "Services restarted"
            ;;
        "logs")
            check_prerequisites
            $COMPOSE_CMD logs -f "${2:-snowpipe-streaming}"
            ;;
        "status")
            check_prerequisites
            $COMPOSE_CMD ps
            ;;
        "cleanup")
            check_prerequisites
            cleanup
            ;;
        "health")
            check_prerequisites
            check_service_health
            ;;
        "test-kafka")
            test_kafka_connection
            ;;
        *)
            echo "Usage: $0 {deploy|setup|start|stop|restart|logs|status|cleanup|health|test-kafka}"
            echo ""
            echo "Commands:"
            echo "  deploy     - Full deployment (build, start, configure)"
            echo "  setup      - Setup directories and run Snowflake scripts"
            echo "  start      - Start all services"
            echo "  stop       - Stop all services"
            echo "  restart    - Restart all services"
            echo "  logs       - View service logs (default: snowpipe-streaming)"
            echo "  status     - Show service status"
            echo "  cleanup    - Stop and remove all containers"
            echo "  health     - Check service health"
            echo "  test-kafka - Test Kafka connection and authentication"
            echo ""
            echo "Data Source Configuration:"
            echo "  Set DEFAULT_DATA_SOURCE=kafka (default) or DEFAULT_DATA_SOURCE=mta_api"
            echo "  Configure corresponding authentication in .env file"
            exit 1
            ;;
    esac
}

# Trap to handle interruption
trap 'echo -e "\n${RED}Deployment interrupted${NC}"; exit 1' INT

# Run main function
main "$@"