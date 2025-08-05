#!/usr/bin/env python3
"""
Real-time MTA Snowpipe Streaming Dashboard
WebSocket-based dashboard for monitoring live data flow
"""

import asyncio
import json
import os
import yaml
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from snowflake.snowpark import Session
import pandas as pd
from threading import Thread
import time
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeDataSource:
    """Real-time data source from Snowflake"""
    
    def __init__(self, config_path: str = "/app/config.yaml"):
        self.session = None
        self.config = self._load_config(config_path)
        self._connect()
    
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
            
            return private_key_der
            
        except FileNotFoundError:
            logger.error(f"❌ Private key file not found: {private_key_path}")
            raise
        except Exception as e:
            logger.error(f"❌ Failed to load private key: {e}")
            raise
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def _connect(self):
        """Create Snowflake session with key-pair authentication"""
        try:
            # Base connection parameters
            connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'INGEST'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'DEMO'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DEMO'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
            }
            
            # Load private key for authentication
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            if not private_key_path:
                raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH environment variable is required")
                
            logger.info(f"Using key-pair authentication for Snowflake with key: {private_key_path}")
            
            # Load and add private key
            private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
            private_key_der = self._load_private_key(private_key_path, private_key_passphrase)
            connection_params['private_key'] = private_key_der
            logger.info("✅ Private key loaded successfully for dashboard authentication")
            
            self.session = Session.builder.configs(connection_params).create()
            logger.info("✅ Dashboard connected to Snowflake successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def get_latest_records(self, limit: int = 100) -> List[Dict]:
        """Get latest records from ICYMTA table"""
        try:
            query = f"""
            SELECT 
                VehicleRef,
                LineRef,
                PublishedLineName,
                StopPointName,
                VehicleLocationLatitude,
                VehicleLocationLongitude,
                RecordedAtTime,
                DestinationName,
                ProgressStatus,
                ts,
                uuid
            FROM DEMO.DEMO.ICYMTA
            ORDER BY RecordedAtTime DESC
            LIMIT {limit}
            """
            
            result = self.session.sql(query).collect()
            
            # Convert to list of dictionaries
            records = []
            for row in result:
                records.append({
                    'VehicleRef': row['VEHICLEREF'],
                    'LineRef': row['LINEREF'],
                    'PublishedLineName': row['PUBLISHEDLINENAME'],
                    'StopPointName': row['STOPPOINTNAME'],
                    'VehicleLocationLatitude': float(row['VEHICLELOCATIONLATITUDE']) if row['VEHICLELOCATIONLATITUDE'] else None,
                    'VehicleLocationLongitude': float(row['VEHICLELOCATIONLONGITUDE']) if row['VEHICLELOCATIONLONGITUDE'] else None,
                    'RecordedAtTime': str(row['RECORDEDATTIME']),
                    'DestinationName': row['DESTINATIONNAME'],
                    'ProgressStatus': row['PROGRESSSTATUS'],
                    'ts': str(row['TS']),
                    'uuid': row['UUID']
                })
            
            return records
            
        except Exception as e:
            logger.error(f"Error fetching latest records: {e}")
            return []
    
    def get_streaming_metrics(self) -> Dict:
        """Get real-time streaming metrics"""
        try:
            metrics = {}
            
            # Total record count
            total_query = "SELECT COUNT(*) as total_records FROM DEMO.DEMO.ICYMTA"
            total_result = self.session.sql(total_query).collect()
            metrics['total_records'] = total_result[0]['TOTAL_RECORDS'] if total_result else 0
            
            # Records in last hour
            hour_query = """
            SELECT COUNT(*) as recent_records 
            FROM DEMO.DEMO.ICYMTA 
            WHERE RecordedAtTime >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """
            hour_result = self.session.sql(hour_query).collect()
            metrics['records_last_hour'] = hour_result[0]['RECENT_RECORDS'] if hour_result else 0
            
            # Unique vehicles
            vehicles_query = "SELECT COUNT(DISTINCT VehicleRef) as unique_vehicles FROM DEMO.DEMO.ICYMTA"
            vehicles_result = self.session.sql(vehicles_query).collect()
            metrics['unique_vehicles'] = vehicles_result[0]['UNIQUE_VEHICLES'] if vehicles_result else 0
            
            # Unique lines
            lines_query = "SELECT COUNT(DISTINCT LineRef) as unique_lines FROM DEMO.DEMO.ICYMTA"
            lines_result = self.session.sql(lines_query).collect()
            metrics['unique_lines'] = lines_result[0]['UNIQUE_LINES'] if lines_result else 0
            
            # Last update time
            last_update_query = "SELECT MAX(RecordedAtTime) as last_update FROM DEMO.DEMO.ICYMTA"
            last_update_result = self.session.sql(last_update_query).collect()
            metrics['last_update'] = str(last_update_result[0]['LAST_UPDATE']) if last_update_result and last_update_result[0]['LAST_UPDATE'] else 'Never'
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error fetching metrics: {e}")
            return {
                'total_records': 0,
                'records_last_hour': 0,
                'unique_vehicles': 0,
                'unique_lines': 0,
                'last_update': 'Error'
            }
    
    def get_line_activity(self) -> List[Dict]:
        """Get activity by transit line"""
        try:
            query = """
            SELECT 
                PublishedLineName,
                COUNT(*) as record_count,
                COUNT(DISTINCT VehicleRef) as vehicle_count,
                MAX(RecordedAtTime) as last_seen
            FROM DEMO.DEMO.ICYMTA
            WHERE RecordedAtTime >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
            GROUP BY PublishedLineName
            ORDER BY record_count DESC
            LIMIT 20
            """
            
            result = self.session.sql(query).collect()
            
            lines = []
            for row in result:
                lines.append({
                    'line': row['PUBLISHEDLINENAME'],
                    'record_count': row['RECORD_COUNT'],
                    'vehicle_count': row['VEHICLE_COUNT'],
                    'last_seen': str(row['LAST_SEEN'])
                })
            
            return lines
            
        except Exception as e:
            logger.error(f"Error fetching line activity: {e}")
            return []

# Flask app setup
app = Flask(__name__)
app.config['SECRET_KEY'] = 'mta-streaming-dashboard-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global data source
data_source = None

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def api_metrics():
    """REST API endpoint for metrics"""
    if data_source:
        metrics = data_source.get_streaming_metrics()
        return jsonify(metrics)
    return jsonify({'error': 'Data source not available'}), 500

@app.route('/api/latest')
def api_latest():
    """REST API endpoint for latest records"""
    limit = request.args.get('limit', 50, type=int)
    if data_source:
        records = data_source.get_latest_records(limit)
        return jsonify(records)
    return jsonify({'error': 'Data source not available'}), 500

@app.route('/api/lines')
def api_lines():
    """REST API endpoint for line activity"""
    if data_source:
        lines = data_source.get_line_activity()
        return jsonify(lines)
    return jsonify({'error': 'Data source not available'}), 500

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    logger.info('Client connected')
    emit('status', {'message': 'Connected to MTA Streaming Dashboard'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection"""
    logger.info('Client disconnected')

def background_data_stream():
    """Background thread to stream real-time data"""
    while True:
        try:
            if data_source:
                # Get fresh metrics
                metrics = data_source.get_streaming_metrics()
                socketio.emit('metrics_update', metrics)
                
                # Get latest records
                latest = data_source.get_latest_records(10)
                socketio.emit('latest_records', latest)
                
                # Get line activity
                lines = data_source.get_line_activity()
                socketio.emit('line_activity', lines)
                
                logger.info(f"📊 Streamed update: {metrics['total_records']} total records")
            
            time.sleep(5)  # Update every 5 seconds
            
        except Exception as e:
            logger.error(f"Error in background stream: {e}")
            time.sleep(10)  # Wait longer on error

def initialize_data_source():
    """Initialize Snowflake data source"""
    global data_source
    try:
        data_source = SnowflakeDataSource()
        logger.info("✅ Data source initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize data source: {e}")
        data_source = None

if __name__ == '__main__':
    # Initialize data source
    initialize_data_source()
    
    # Start background streaming thread
    background_thread = Thread(target=background_data_stream, daemon=True)
    background_thread.start()
    
    # Run the app
    logger.info("🚀 Starting MTA Streaming Dashboard...")
    socketio.run(app, host='0.0.0.0', port=8080, debug=False)