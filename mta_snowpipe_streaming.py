#!/usr/bin/env python3
"""
============================================================================
Snowpipe Streaming High-Performance Architecture for MTA Real-time Data
============================================================================

This module implements a high-performance Snowpipe Streaming client for 
ingesting MTA real-time transit data into Snowflake Managed Iceberg tables.

Features:
- Real-time data ingestion from MTA APIs
- High-performance parallel streaming channels
- Comprehensive error handling and retry logic
- Data validation and quality checks
- Performance monitoring and metrics
- Automatic table optimization
"""

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import signal

import pandas as pd
import yaml
from snowflake.snowpark import Session
from snowflake.connector import connect as snowflake_connect
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
import requests
import structlog
import psutil
from prometheus_client import start_http_server, Counter, Histogram, Gauge
from confluent_kafka import Consumer, KafkaError, KafkaException
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

# Configure structured logging
logger = structlog.get_logger()

# Prometheus metrics
RECORDS_PROCESSED = Counter('snowpipe_records_processed_total', 'Total records processed')
RECORDS_FAILED = Counter('snowpipe_records_failed_total', 'Total records failed')
INGESTION_LATENCY = Histogram('snowpipe_ingestion_latency_seconds', 'Ingestion latency')
ACTIVE_CHANNELS = Gauge('snowpipe_active_channels', 'Number of active streaming channels')
MEMORY_USAGE = Gauge('snowpipe_memory_usage_mb', 'Memory usage in MB')


@dataclass
class MTARecord:
    """Data class representing an MTA real-time record"""
    stoppointref: Optional[str] = None
    vehicleref: Optional[str] = None
    progressrate: Optional[str] = None
    expecteddeparturetime: Optional[str] = None
    expectedarrivaltime: Optional[str] = None
    stoppoint: Optional[str] = None
    visitnumber: Optional[str] = None
    dataframeref: Optional[str] = None
    stoppointname: Optional[str] = None
    situationsimpleref1: Optional[str] = None
    situationsimpleref2: Optional[str] = None
    situationsimpleref3: Optional[str] = None
    situationsimpleref4: Optional[str] = None
    situationsimpleref5: Optional[str] = None
    bearing: Optional[str] = None
    originaimeddeparturetime: Optional[str] = None
    journeypatternref: Optional[str] = None
    recordedattime: Optional[str] = None
    operatorref: Optional[str] = None
    destinationname: Optional[str] = None
    blockref: Optional[str] = None
    lineref: Optional[str] = None
    vehiclelocationlongitude: Optional[str] = None
    directionref: Optional[str] = None
    arrivalproximitytext: Optional[str] = None
    distancefromstop: Optional[str] = None
    estimatedpassengercapacity: Optional[str] = None
    aimedarrivaltime: Optional[str] = None
    publishedlinename: Optional[str] = None
    datedvehiclejourneyref: Optional[str] = None
    date: Optional[str] = None
    monitored: Optional[str] = None
    progressstatus: Optional[str] = None
    destinationref: Optional[str] = None
    estimatedpassengercount: Optional[str] = None
    vehiclelocationlatitude: Optional[str] = None
    originref: Optional[str] = None
    numberofstopsaway: Optional[str] = None
    ts: Optional[str] = None
    uuid: Optional[str] = None


class ConfigManager:
    """Configuration manager for the streaming application"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self._load_environment_variables()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Configuration file {self.config_path} not found")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise
    
    def _load_environment_variables(self):
        """Load sensitive configuration from environment variables"""
        env_mappings = {
            'SNOWFLAKE_ACCOUNT': ['snowflake', 'account'],
            'SNOWFLAKE_USER': ['snowflake', 'user'],
            # SNOWFLAKE_PASSWORD removed - only key-pair authentication supported
            'SNOWFLAKE_PRIVATE_KEY_PATH': ['snowflake', 'private_key_path'],
            #'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': ['snowflake', 'private_key_passphrase'],
            'MTA_API_KEY': ['data_source', 'mta_api', 'api_key'],
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                self._set_nested_config(config_path, value)
    
    def _set_nested_config(self, path: List[str], value: Any):
        """Set nested configuration value"""
        current = self.config
        for key in path[:-1]:
            current = current.setdefault(key, {})
        current[path[-1]] = value
    
    def _load_private_key(self, private_key_path: str, passphrase: Optional[str] = None) -> bytes:
        """Load and parse private key from file"""
        try:
            with open(private_key_path, 'rb') as key_file:
                private_key_content = key_file.read()
            
            # Parse the private key
            if passphrase:
                private_key = load_pem_private_key(
                    private_key_content,
                    password=passphrase.encode('utf-8'),
                )
            else:
                private_key = load_pem_private_key(
                    private_key_content,
                    password=None,
                )
            
            # Serialize to DER format for Snowflake
            private_key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            logger.info("Private key loaded successfully")
            return private_key_der
            
        except FileNotFoundError:
            logger.error(f"Private key file not found: {private_key_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading private key: {e}")
            raise
    
    def get(self, *keys: str, default: Any = None) -> Any:
        """Get nested configuration value"""
        current = self.config
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default


class DataValidator:
    """Data validation and quality checks for MTA records"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.validation_enabled = config.get('processing', 'validation', 'enable_schema_validation', default=True)
        # Use actual field names from MTA Kafka messages (PascalCase)
        self.required_fields = ['VehicleRef', 'LineRef', 'RecordedAtTime']
    
    def validate_record(self, record: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate a single record and return validation result and errors"""
        errors = []
        
        if not self.validation_enabled:
            return True, errors
        
        # Check required fields
        for field in self.required_fields:
            if not record.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate coordinates (check both PascalCase and lowercase)
        lat = record.get('VehicleLocationLatitude') or record.get('vehiclelocationlatitude')
        lon = record.get('VehicleLocationLongitude') or record.get('vehiclelocationlongitude')
        
        if lat and lon:
            try:
                lat_float = float(lat)
                lon_float = float(lon)
                
                # NYC coordinate bounds check
                if not (40.4 <= lat_float <= 41.0):
                    errors.append(f"Latitude out of NYC bounds: {lat_float}")
                if not (-74.5 <= lon_float <= -73.5):
                    errors.append(f"Longitude out of NYC bounds: {lon_float}")
                    
            except (ValueError, TypeError):
                errors.append("Invalid coordinate format")
        
        # Validate timestamp (check both PascalCase and lowercase)
        timestamp_field = record.get('RecordedAtTime') or record.get('recordedattime')
        if timestamp_field:
            try:
                datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                errors.append("Invalid timestamp format")
        
        is_valid = len(errors) == 0
        return is_valid, errors


class KafkaDataSource:
    """Kafka data source for MTA real-time data"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.consumer = None
        self.running = False
        self.topics = config.get('data_source', 'kafka', 'topics', default=['icymta'])
        
        # Setup Kafka consumer configuration (ONLY essential valid confluent-kafka properties)
        self.kafka_config = {
            'bootstrap.servers': ','.join(config.get('data_source', 'kafka', 'bootstrap_servers', default=['localhost:9092'])),
            'group.id': config.get('data_source', 'kafka', 'consumer_group', default='snowpipe-streaming-consumer'),
            'auto.offset.reset': config.get('data_source', 'kafka', 'auto_offset_reset', default='latest'),
            'enable.auto.commit': config.get('data_source', 'kafka', 'enable_auto_commit', default=True),
            'session.timeout.ms': config.get('data_source', 'kafka', 'session_timeout_ms', default=30000),
        }
        
        # Add SASL_SSL configuration
        security_protocol = config.get('data_source', 'kafka', 'security_protocol')
        if security_protocol == 'SASL_SSL':
            self.kafka_config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': config.get('data_source', 'kafka', 'sasl_mechanism', default='PLAIN'),
                'sasl.username': config.get('data_source', 'kafka', 'sasl_username'),
                'sasl.password': config.get('data_source', 'kafka', 'sasl_password')
            })
            
            # Optional SSL certificate files
            ssl_cafile = config.get('data_source', 'kafka', 'ssl_cafile')
            ssl_certfile = config.get('data_source', 'kafka', 'ssl_certfile')
            ssl_keyfile = config.get('data_source', 'kafka', 'ssl_keyfile')
            
            if ssl_cafile:
                self.kafka_config['ssl.ca.location'] = ssl_cafile
            if ssl_certfile:
                self.kafka_config['ssl.certificate.location'] = ssl_certfile
            if ssl_keyfile:
                self.kafka_config['ssl.key.location'] = ssl_keyfile
        
        logger.info(f"Kafka consumer configured for topics: {self.topics}")
        logger.info(f"Kafka bootstrap servers: {self.kafka_config['bootstrap.servers']}")
        logger.info(f"Security protocol: {self.kafka_config.get('security.protocol', 'PLAINTEXT')}")
    
    async def initialize(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe(self.topics)
            logger.info(f"Kafka consumer initialized and subscribed to topics: {self.topics}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    async def fetch_realtime_data(self) -> List[Dict[str, Any]]:
        """Fetch real-time data from Kafka"""
        if not self.consumer:
            await self.initialize()
        
        records = []
        timeout = 1.0  # 1 second timeout for polling
        
        try:
            # Poll for messages
            while len(records) < 500:  # Max records per poll batch
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    break  # No more messages available
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition reached
                        logger.debug(f"Reached end of partition {msg.partition()} for topic {msg.topic()}")
                        break
                    else:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                        break
                
                # Process the message
                try:
                    # Decode the message value
                    message_value = msg.value().decode('utf-8') if msg.value() else None
                    if message_value:
                        # Parse JSON message
                        parsed_data = json.loads(message_value)
                        
                        # Handle both single objects and arrays
                        if isinstance(parsed_data, list):
                            # If it's a list, process each item
                            for item in parsed_data:
                                if isinstance(item, dict):
                                    record = item.copy()
                                    # Add Kafka metadata
                                    record['_kafka_topic'] = msg.topic()
                                    record['_kafka_partition'] = msg.partition()
                                    record['_kafka_offset'] = msg.offset()
                                    
                                    # Safely extract timestamp (returns tuple: timestamp_type, timestamp_value)
                                    timestamp_tuple = msg.timestamp()
                                    record['_kafka_timestamp'] = timestamp_tuple[1] if timestamp_tuple and len(timestamp_tuple) > 1 and timestamp_tuple[1] else None
                                    
                                    # Ensure required fields
                                    record = self._enrich_record(record)
                                    records.append(record)
                        elif isinstance(parsed_data, dict):
                            # If it's a single object
                            record = parsed_data.copy()
                            # Add Kafka metadata
                            record['_kafka_topic'] = msg.topic()
                            record['_kafka_partition'] = msg.partition()
                            record['_kafka_offset'] = msg.offset()
                            
                            # Safely extract timestamp (returns tuple: timestamp_type, timestamp_value)
                            timestamp_tuple = msg.timestamp()
                            record['_kafka_timestamp'] = timestamp_tuple[1] if timestamp_tuple and len(timestamp_tuple) > 1 and timestamp_tuple[1] else None
                            
                            # Ensure required fields
                            record = self._enrich_record(record)
                            records.append(record)
                        else:
                            logger.warning(f"Unexpected JSON structure: {type(parsed_data)}. Expected dict or list of dicts.")
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                
                # Short timeout for subsequent polls
                timeout = 0.1
        
        except KafkaException as e:
            logger.error(f"Kafka exception during polling: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during Kafka polling: {e}")
        
        if records:
            logger.debug(f"Fetched {len(records)} records from Kafka")
        
        return records
    
    def _enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich Kafka record with required fields"""
        current_time = datetime.now(timezone.utc).isoformat()
        
        # Ensure UUID if not present
        if 'uuid' not in record or not record['uuid']:
            record['uuid'] = str(uuid.uuid4())
        
        # Ensure timestamp fields
        if 'ts' not in record or not record['ts']:
            record['ts'] = current_time
        
        if 'date' not in record or not record['date']:
            record['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Handle both legacy (lowercase) and actual MTA field names (PascalCase)
        if 'RecordedAtTime' not in record or not record['RecordedAtTime']:
            if 'recordedattime' not in record or not record['recordedattime']:
                record['RecordedAtTime'] = current_time
        
        return record
    
    def close(self):
        """Close the Kafka consumer"""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

class SnowpipeStreamingClient:
    """High-performance Snowpipe Streaming client"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.validator = DataValidator(config)
        
        # Initialize data source based on configuration
        default_source = config.get('data_source', 'default_source', default='kafka')
        self.data_source = KafkaDataSource(config)
        logger.info("Using Kafka as primary data source")

        self.session = None
        self.ingest_manager = None
        self.running = False
        self.channels = []  # Will store streaming channel objects
        
        # Real streaming configuration
        self.table_name = None
        self.batch_insert_size = self.config.get('streaming', 'batch_insert_size', default=100)
        self.insert_buffer = []  # Buffer for real-time inserts
        
        # Performance tracking
        self.records_processed = 0
        self.records_failed = 0
        self.last_flush_time = time.time()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
    
    async def initialize(self):
        """Initialize Snowflake connections and streaming channels"""
        logger.info("Initializing Snowpipe Streaming client...")
        
        # Create Snowflake session
        connection_params = {
            'account': self.config.get('snowflake', 'account'),
            'user': self.config.get('snowflake', 'user'),
            'warehouse': self.config.get('snowflake', 'warehouse'),
            'database': self.config.get('snowflake', 'database'),
            'schema': self.config.get('snowflake', 'schema'),
            'role': self.config.get('snowflake', 'role'),
        }
        
        # ENFORCE key-pair authentication ONLY (no password fallback)
        private_key_path = self.config.get('snowflake', 'private_key_path')
        if not private_key_path:
            raise ValueError(
                "SNOWFLAKE_PRIVATE_KEY_PATH environment variable is required. "
                "Password authentication is not supported. Please ensure your .env file contains: "
                "SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/private_key.p8"
            )
        
        logger.info(f"Using key-pair authentication for Snowflake with key: {private_key_path}")
        try:
            private_key_passphrase = self.config.get('snowflake', 'private_key_passphrase')
            private_key_der = self.config._load_private_key(private_key_path, private_key_passphrase)
            connection_params['private_key'] = private_key_der
            logger.info("✅ Private key loaded successfully for Snowflake authentication")
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to load private key from {private_key_path}")
            logger.error(f"❌ Error details: {e}")
            logger.error(f"❌ Verify that:")
            logger.error(f"   1. File exists: {private_key_path}")
            logger.error(f"   2. File is readable (permissions)")
            logger.error(f"   3. File contains valid PEM private key")
            logger.error(f"   4. Passphrase is correct (if required)")
            raise ValueError(f"Failed to load private key for Snowflake authentication: {e}")
        
        try:
            self.session = Session.builder.configs(connection_params).create()
            logger.info("Snowflake session created successfully")
            
            # Store connection params for session refresh
            self.connection_params = connection_params
            self.session_created_time = time.time()
            
            # Initialize real streaming table
            await self._initialize_streaming_table()
            
            # Initialize data source
            if hasattr(self.data_source, 'initialize'):
                await self.data_source.initialize()
            
            # Initialize streaming channels
            await self._initialize_streaming_channels()
            
        except Exception as e:
            logger.error(f"Failed to initialize Snowflake connection: {e}")
            raise
    
    async def _initialize_streaming_table(self):
        """Initialize the real streaming table for direct inserts"""
        try:
            # Get table configuration
            database = self.config.get('snowflake', 'database')
            schema = self.config.get('snowflake', 'schema')
            table_name = self.config.get('streaming', 'table_name')
            
            # Create the fully qualified table name
            self.table_name = f"{database}.{schema}.{table_name}"
            
            # Verify the table exists and is accessible
            result = self.session.sql(f"DESCRIBE TABLE {self.table_name}").collect()
            logger.info(f"✅ Real streaming table initialized: {self.table_name}")
            logger.info(f"   Table has {len(result)} columns")
            
            # Log table structure for debugging
            for row in result[:5]:  # Show first 5 columns
                logger.debug(f"   Column: {row[0]} ({row[1]})")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize streaming table: {e}")
            logger.error(f"   Please ensure table {self.table_name} exists and is accessible")
            raise
    
    async def _initialize_streaming_channels(self):
        """Initialize multiple parallel streaming channels for high performance"""
        num_channels = self.config.get('streaming', 'parallel_channels', default=4)
        channel_name_base = self.config.get('streaming', 'channel_name', default='MTA_REALTIME_CHANNEL')
        
        for i in range(num_channels):
            channel_name = f"{channel_name_base}_{i}"
            try:
                # Create streaming channel (pseudo-code - actual implementation depends on Snowflake SDK)
                channel = await self._create_streaming_channel(channel_name)
                self.channels.append(channel)
                logger.info(f"Created streaming channel: {channel_name}")
            except Exception as e:
                logger.error(f"Failed to create streaming channel {channel_name}: {e}")
        
        ACTIVE_CHANNELS.set(len(self.channels))
    
    async def _create_streaming_channel(self, channel_name: str):
        """Create a real streaming channel for direct table inserts"""
        try:
            # Create a logical channel for batch management
            channel = {
                'name': channel_name,
                'table': self.table_name,
                'buffer': [],
                'last_insert': time.time(),
                'records_inserted': 0
            }
            
            logger.info(f"✅ Real streaming channel created: {channel_name} -> {self.table_name}")
            return channel
            
        except Exception as e:
            logger.error(f"❌ Failed to create streaming channel {channel_name}: {e}")
            raise
    
    async def start_streaming(self):
        """Start the main streaming loop"""
        logger.info("Starting Snowpipe Streaming...")
        self.running = True
        
        # Start health check server
        await self._start_health_server()
        
        # Start periodic session refresh task (refresh every 2 hours to prevent expiration)
        asyncio.create_task(self._periodic_session_refresh())
        
        # Main streaming loop
        # For Kafka, we poll continuously; for API, we use the configured interval
        default_source = self.config.get('data_source', 'default_source', default='kafka')
        if default_source == 'kafka':
            poll_interval = 1  # Continuous polling for Kafka
        else:
            poll_interval = self.config.get('data_source', 'mta_api', 'poll_interval_seconds', default=30)
        
        while self.running:
            try:
                start_time = time.time()
                
                # Fetch data from MTA APIs
                records = await self.data_source.fetch_realtime_data()
                
                if records:
                    # Process and ingest records
                    await self._process_and_ingest(records)
                
                # Update metrics
                MEMORY_USAGE.set(psutil.Process().memory_info().rss / 1024 / 1024)
                
                # Calculate sleep time to maintain polling interval
                elapsed = time.time() - start_time
                sleep_time = max(0, poll_interval - elapsed)
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
            except Exception as e:
                logger.error(f"Error in streaming loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _process_and_ingest(self, records: List[Dict[str, Any]]):
        """Process and ingest records into Snowflake"""
        batch_size = self.config.get('streaming', 'batch_size', default=1000)
        
        # Split records into batches for parallel processing
        batches = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
        
        tasks = []
        for i, batch in enumerate(batches):
            channel_index = i % len(self.channels)
            task = asyncio.create_task(
                self._ingest_batch(batch, self.channels[channel_index])
            )
            tasks.append(task)
        
        # Wait for all batches to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle results and update metrics
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Batch ingestion failed: {result}")
                RECORDS_FAILED.inc()
    
    async def _ingest_batch(self, batch: List[Dict[str, Any]], channel: Dict[str, Any]):
        """Ingest a batch of records into a specific channel"""
        valid_records = []
        
        # Validate and transform records
        for record in batch:
            is_valid, errors = self.validator.validate_record(record)
            
            if is_valid:
                # Transform record
                transformed_record = self._transform_record(record)
                valid_records.append(transformed_record)
            else:
                logger.warning(f"Invalid record: {errors}")
                if self.config.get('processing', 'validation', 'log_validation_errors'):
                    self._log_failed_record(record, errors)
        
        if valid_records:
            max_retries = 2
            retry_count = 0
            
            while retry_count <= max_retries:
                try:
                    # Real streaming ingestion using Snowpipe Streaming API
                    await self._real_streaming_ingest(valid_records, channel)
                    
                    self.records_processed += len(valid_records)
                    RECORDS_PROCESSED.inc(len(valid_records))
                    
                    logger.info(f"Streamed {len(valid_records)} records to channel {channel['name']}")
                    
                    # Success - break out of retry loop
                    break
                    
                except Exception as e:
                    error_str = str(e)
                    logger.error(f"Failed to ingest batch: {e}")
                    
                    # Check if this is an authentication error
                    if self._is_auth_error(error_str) and retry_count < max_retries:
                        logger.warning(f"Authentication error in batch ingestion, attempting session refresh (attempt {retry_count + 1}/{max_retries + 1})")
                        try:
                            await self._refresh_snowflake_session()
                            retry_count += 1
                            continue  # Retry with new session
                        except Exception as refresh_error:
                            logger.error(f"Failed to refresh session during batch ingestion: {refresh_error}")
                            RECORDS_FAILED.inc(len(valid_records))
                            raise
                    else:
                        # Not an auth error or max retries reached
                        RECORDS_FAILED.inc(len(valid_records))
                        raise
    
    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw record to match table schema"""
        transformed = record.copy()
        
        # Add metadata
        if self.config.get('processing', 'transformation', 'add_ingestion_metadata'):
            transformed['ingestion_time'] = datetime.now(timezone.utc).isoformat()
            transformed['source_system'] = 'MTA_REALTIME'
            transformed['processing_status'] = 'PROCESSED'
        
        # Generate UUID if not present
        if self.config.get('processing', 'transformation', 'generate_uuid') and not transformed.get('uuid'):
            transformed['uuid'] = str(uuid.uuid4())
        
        # Normalize timestamps
        if self.config.get('processing', 'transformation', 'normalize_timestamps'):
            timestamp_fields = ['expecteddeparturetime', 'expectedarrivaltime', 'recordedattime', 'aimedarrivaltime']
            for field in timestamp_fields:
                if transformed.get(field):
                    try:
                        # Convert to ISO format if needed
                        dt = datetime.fromisoformat(transformed[field].replace('Z', '+00:00'))
                        transformed[field] = dt.isoformat()
                    except (ValueError, TypeError):
                        pass  # Keep original value if conversion fails
        
        # Convert coordinates to float
        if self.config.get('processing', 'transformation', 'convert_coordinates_to_float'):
            for coord_field in ['vehiclelocationlatitude', 'vehiclelocationlongitude']:
                if transformed.get(coord_field):
                    try:
                        transformed[coord_field] = float(transformed[coord_field])
                    except (ValueError, TypeError):
                        transformed[coord_field] = None
        
        return transformed
    
    async def _real_streaming_ingest(self, records: List[Dict[str, Any]], channel: Dict[str, Any]):
        """Real streaming ingestion using direct table inserts"""
        try:
            # Track ingestion latency
            with INGESTION_LATENCY.time():
                # Add records to channel buffer for batch processing
                channel['buffer'].extend(records)
                
                # Check if we should flush the buffer (real-time mode with small batches)
                should_insert = (
                    len(channel['buffer']) >= self.batch_insert_size or
                    time.time() - channel['last_insert'] >= 2  # Insert every 2 seconds maximum
                )
                
                if should_insert:
                    await self._direct_table_insert(channel)
                    
        except Exception as e:
            logger.error(f"❌ Failed to ingest records via real streaming: {e}")
            raise
    
    def _safe_coordinate(self, coord_value):
        """Safely convert coordinate value to string or None for database insertion
        
        Args:
            coord_value: The coordinate value (could be string, float, None, or empty)
            
        Returns:
            str or None: Valid coordinate string or None for NULL database insertion
        """
        if coord_value is None:
            return None
        
        # Convert to string and strip whitespace
        coord_str = str(coord_value).strip()
        
        # Return None for empty strings or invalid values
        if not coord_str or coord_str == '' or coord_str.lower() in ['none', 'null', 'nan']:
            return None
        
        # Try to validate it's a valid number
        try:
            float(coord_str)
            return coord_str
        except (ValueError, TypeError):
            # If it's not a valid number, return None
            return None

    async def _direct_table_insert(self, channel: Dict[str, Any]):
        """Perform direct table insert for real-time streaming"""
        if not channel['buffer']:
            return

        try:
            # Convert records to DataFrame format for Snowpark
            # IMPORTANT: Column order must match exact table schema order!
            formatted_records = []
            for record in channel['buffer']:
                current_time = datetime.now(timezone.utc)
                # Format record in EXACT table column order (1-43)
                formatted_record = {
                    # 1. STOPPOINTREF
                    'STOPPOINTREF': str(record.get('StopPointRef', '')),
                    # 2. VEHICLEREF  
                    'VEHICLEREF': str(record.get('VehicleRef', '')),
                    # 3. PROGRESSRATE
                    'PROGRESSRATE': str(record.get('ProgressRate', '')),
                    # 4. EXPECTEDDEPARTURETIME
                    'EXPECTEDDEPARTURETIME': str(record.get('ExpectedDepartureTime', '')),
                    # 5. STOPPOINT
                    'STOPPOINT': str(record.get('StopPoint', '')),
                    # 6. VISITNUMBER
                    'VISITNUMBER': str(record.get('VisitNumber', '')),
                    # 7. DATAFRAMEREF
                    'DATAFRAMEREF': str(record.get('DataFrameRef', '')),
                    # 8. STOPPOINTNAME
                    'STOPPOINTNAME': str(record.get('StopPointName', '')),
                    # 9. SITUATIONSIMPLEREF5
                    'SITUATIONSIMPLEREF5': str(record.get('SituationSimpleRef5', '')),
                    # 10. SITUATIONSIMPLEREF3
                    'SITUATIONSIMPLEREF3': str(record.get('SituationSimpleRef3', '')),
                    # 11. BEARING
                    'BEARING': str(record.get('Bearing', '')),
                    # 12. SITUATIONSIMPLEREF4
                    'SITUATIONSIMPLEREF4': str(record.get('SituationSimpleRef4', '')),
                    # 13. SITUATIONSIMPLEREF1
                    'SITUATIONSIMPLEREF1': str(record.get('SituationSimpleRef1', '')),
                    # 14. ORIGINAIMEDDEPARTURETIME
                    'ORIGINAIMEDDEPARTURETIME': str(record.get('OriginAimedDepartureTime', '')),
                    # 15. SITUATIONSIMPLEREF2
                    'SITUATIONSIMPLEREF2': str(record.get('SituationSimpleRef2', '')),
                    # 16. JOURNEYPATTERNREF
                    'JOURNEYPATTERNREF': str(record.get('JourneyPatternRef', '')),
                    # 17. RECORDEDATTIME
                    'RECORDEDATTIME': str(record.get('RecordedAtTime', '')),
                    # 18. OPERATORREF
                    'OPERATORREF': str(record.get('OperatorRef', '')),
                    # 19. DESTINATIONNAME
                    'DESTINATIONNAME': str(record.get('DestinationName', '')),
                    # 20. EXPECTEDARRIVALTIME
                    'EXPECTEDARRIVALTIME': str(record.get('ExpectedArrivalTime', '')),
                    # 21. BLOCKREF
                    'BLOCKREF': str(record.get('BlockRef', '')),
                    # 22. LINEREF
                    'LINEREF': str(record.get('LineRef', '')),
                    # 23. VEHICLELOCATIONLONGITUDE
                    'VEHICLELOCATIONLONGITUDE': self._safe_coordinate(record.get('VehicleLocationLongitude')),
                    # 24. DIRECTIONREF
                    'DIRECTIONREF': str(record.get('DirectionRef', '')),
                    # 25. ARRIVALPROXIMITYTEXT
                    'ARRIVALPROXIMITYTEXT': str(record.get('ArrivalProximityText', '')),
                    # 26. DISTANCEFROMSTOP
                    'DISTANCEFROMSTOP': self._safe_coordinate(record.get('DistanceFromStop')),
                    # 27. ESTIMATEDPASSENGERCAPACITY
                    'ESTIMATEDPASSENGERCAPACITY': str(record.get('EstimatedPassengerCapacity', '')),
                    # 28. AIMEDARRIVALTIME
                    'AIMEDARRIVALTIME': str(record.get('AimedArrivalTime', '')),
                    # 29. PUBLISHEDLINENAME
                    'PUBLISHEDLINENAME': str(record.get('PublishedLineName', '')),
                    # 30. DATEDVEHICLEJOURNEYREF
                    'DATEDVEHICLEJOURNEYREF': str(record.get('DatedVehicleJourneyRef', '')),
                    # 31. DATE
                    'DATE': str(record.get('Date', '')),
                    # 32. MONITORED
                    'MONITORED': str(record.get('Monitored', '')),
                    # 33. PROGRESSSTATUS
                    'PROGRESSSTATUS': str(record.get('ProgressStatus', '')),
                    # 34. DESTINATIONREF
                    'DESTINATIONREF': str(record.get('DestinationRef', '')),
                    # 35. ESTIMATEDPASSENGERCOUNT
                    'ESTIMATEDPASSENGERCOUNT': str(record.get('EstimatedPassengerCount', '')),
                    # 36. VEHICLELOCATIONLATITUDE
                    'VEHICLELOCATIONLATITUDE': self._safe_coordinate(record.get('VehicleLocationLatitude')),
                    # 37. ORIGINREF
                    'ORIGINREF': str(record.get('OriginRef', '')),
                    # 38. NUMBEROFSTOPSAWAY
                    'NUMBEROFSTOPSAWAY': str(record.get('NumberOfStopsAway', '')),
                    # 39. TS
                    'TS': str(record.get('ts', '')),
                    # 40. UUID
                    'UUID': str(record.get('uuid', '')),
                    # 41. INGESTION_TIME (not INGESTION_TIMESTAMP!)
                    'INGESTION_TIME': current_time.replace(tzinfo=None),  # Remove timezone for TIMESTAMP_NTZ
                    # 42. SOURCE_SYSTEM
                    'SOURCE_SYSTEM': str('MTA_REALTIME'),
                    # 43. PROCESSING_STATUS
                    'PROCESSING_STATUS': str('PROCESSED')
                }
                formatted_records.append(formatted_record)
            
            # Create DataFrame and insert directly into table
            logger.info(f"🚀 Direct table insert: {len(formatted_records)} records -> {self.table_name}")
            
            df = self.session.create_dataframe(formatted_records)
            df.write.mode("append").save_as_table(self.table_name)
            
            # Update channel statistics
            channel['records_inserted'] += len(formatted_records)
            channel['last_insert'] = time.time()
            channel['buffer'].clear()
            
            logger.info(f"✅ Successfully inserted {len(formatted_records)} records into {self.table_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to insert records into table {self.table_name}: {e}")
            raise
    
    async def _close_streaming_channel(self, channel: Dict[str, Any]):
        """Close and flush a streaming channel gracefully"""
        try:
            channel_name = channel['name']
            logger.info(f"Closing streaming channel: {channel_name}")
            
            # Flush any remaining records in the buffer
            if channel['buffer']:
                await self._direct_table_insert(channel)
                
            logger.info(f"✅ Streaming channel closed successfully: {channel_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to close streaming channel: {e}")
            raise
    
    def _log_failed_record(self, record: Dict[str, Any], errors: List[str]):
        """Log failed record for debugging"""
        if self.config.get('development', 'debug', 'save_failed_records'):
            failed_records_path = Path(self.config.get('development', 'debug', 'failed_records_path', default='logs/failed_records/'))
            failed_records_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = failed_records_path / f"failed_record_{timestamp}_{uuid.uuid4().hex[:8]}.json"
            
            with open(filename, 'w') as f:
                json.dump({
                    'record': record,
                    'errors': errors,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }, f, indent=2)
    
    async def _refresh_snowflake_session(self):
        """Refresh Snowflake session when authentication token expires"""
        try:
            logger.info("🔄 Refreshing Snowflake session due to token expiration...")
            
            # Close existing session
            if self.session:
                try:
                    self.session.close()
                except Exception as e:
                    logger.warning(f"Error closing old session: {e}")
            
            # Create new session with stored connection params
            self.session = Session.builder.configs(self.connection_params).create()
            self.session_created_time = time.time()
            logger.info("✅ Snowflake session refreshed successfully")
            
            # Re-initialize streaming table and channels
            await self._initialize_streaming_table()
            await self._initialize_streaming_channels()
            logger.info("✅ Streaming table and channels re-initialized after session refresh")
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh Snowflake session: {e}")
            raise
    
    def _is_auth_error(self, error_str: str) -> bool:
        """Check if error is related to authentication/token expiration"""
        auth_error_keywords = [
            "Authentication token has expired",
            "JWT token is invalid", 
            "Authentication failed",
            "390114",  # Snowflake auth error code
            "250001"   # Another common auth error code
        ]
        return any(keyword in str(error_str) for keyword in auth_error_keywords)
    
    async def _periodic_session_refresh(self):
        """Periodically refresh Snowflake session to prevent token expiration"""
        # Refresh every 2 hours (7200 seconds) - well before typical 4-hour JWT expiration
        refresh_interval = 7200
        
        while self.running:
            try:
                await asyncio.sleep(refresh_interval)
                
                if self.running:  # Check again after sleep
                    logger.info("🔄 Performing periodic Snowflake session refresh...")
                    await self._refresh_snowflake_session()
                    logger.info("✅ Periodic session refresh completed successfully")
                    
            except Exception as e:
                logger.error(f"❌ Error during periodic session refresh: {e}")
                # Continue running even if refresh fails - reactive refresh will handle errors
                await asyncio.sleep(300)  # Wait 5 minutes before trying again

    async def _start_health_server(self):
        """Start health check HTTP server"""
        from aiohttp import web
        
        async def health_check(request):
            health_status = {
                'status': 'healthy' if self.running else 'unhealthy',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'records_processed': self.records_processed,
                'records_failed': self.records_failed,
                'active_channels': len(self.channels),
                'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024
            }
            return web.json_response(health_status)
        
        app = web.Application()
        app.router.add_get('/health', health_check)
        
        port = self.config.get('monitoring', 'health_checks', 'health_port', default=8001)
        host = '0.0.0.0'  # Bind to all interfaces for Docker compatibility
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"Health check server started on port {port}")
    
    async def stop(self):
        """Stop the streaming client gracefully"""
        logger.info("Stopping Snowpipe Streaming client...")
        self.running = False
        
        # Close streaming channels gracefully
        for channel in self.channels:
            try:
                await self._close_streaming_channel(channel)
            except Exception as e:
                logger.error(f"Error closing channel: {e}")
        
        # No specific streaming client to close - using direct table inserts
        logger.info("✅ Real streaming channels flushed and closed")
        
        # Close data source
        if hasattr(self.data_source, 'close'):
            self.data_source.close()
        
        # Close Snowflake session
        if self.session:
            self.session.close()
        
        logger.info("Snowpipe Streaming client stopped")


async def main():
    """Main entry point"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    client = None
    try:
        # Load configuration
        config = ConfigManager()
        
        # Create and initialize streaming client
        client = SnowpipeStreamingClient(config)
        await client.initialize()
        
        # Start streaming
        await client.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if client:
            await client.stop()


if __name__ == "__main__":
    asyncio.run(main())