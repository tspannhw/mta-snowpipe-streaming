#!/usr/bin/env python3
"""
Development script to run the dashboard standalone
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for config access
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set development environment
os.environ.setdefault('FLASK_ENV', 'development')
os.environ.setdefault('FLASK_DEBUG', '1')

# Import and run the app
from app import app, socketio, initialize_data_source, background_data_stream
from threading import Thread

if __name__ == '__main__':
    print("🚀 Starting MTA Dashboard in Development Mode")
    print("=" * 50)
    
    # Initialize data source
    try:
        initialize_data_source()
        print("✅ Data source initialized")
    except Exception as e:
        print(f"⚠️  Warning: Could not initialize data source: {e}")
        print("   Dashboard will still start but may not show live data")
    
    # Start background thread
    background_thread = Thread(target=background_data_stream, daemon=True)
    background_thread.start()
    print("✅ Background data stream started")
    
    print("\n📊 Dashboard will be available at:")
    print("   http://localhost:8080")
    print("\nPress Ctrl+C to stop")
    print("=" * 50)
    
    # Run the app
    socketio.run(app, host='0.0.0.0', port=8080, debug=True)