# MTA Real-time Streaming Dashboard

A real-time web dashboard for monitoring MTA transit data flowing through the Snowpipe Streaming pipeline.

## Features

🗺️ **Live Vehicle Map** - Real-time NYC transit vehicle locations  
📊 **Streaming Metrics** - Live data flow statistics  
📋 **Recent Records** - Latest transit updates  
📈 **Line Activity** - Transit line performance charts  
⚡ **WebSocket Updates** - Sub-second real-time updates  

## Quick Start

### With Docker Compose (Recommended)
```bash
# Start the full stack including dashboard
docker-compose up -d

# Access dashboard
open http://localhost:8080
```

### Development Mode
```bash
cd dashboard
pip install -r requirements.txt
python run_dashboard.py
```

## Dashboard Sections

### 📊 Metrics Row
- **Total Records**: All-time record count in Snowflake
- **Last Hour**: Recent streaming activity 
- **Active Vehicles**: Unique vehicles currently tracked
- **Transit Lines**: Number of different routes
- **Last Update**: Most recent data timestamp

### 🗺️ Live Vehicle Map
- Interactive map of NYC showing vehicle positions
- Real-time updates as vehicles move
- Click markers for vehicle details
- Automatic bounds adjustment

### 📈 Line Activity Chart
- Bar chart showing activity by transit line
- Vehicle counts per line
- Updates every 5 seconds

### 📋 Latest Records Table
- Most recent 50 streaming records
- Vehicle, line, stop, and status information
- Live updates as new data arrives

## Configuration

The dashboard connects to Snowflake using the same configuration as the main application:

```bash
# Required environment variables
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user  
SNOWFLAKE_PRIVATE_KEY_PATH=path/to/key.p8
SNOWFLAKE_WAREHOUSE=INGEST
SNOWFLAKE_DATABASE=DEMO
SNOWFLAKE_SCHEMA=DEMO
SNOWFLAKE_ROLE=your-role
```

## API Endpoints

### REST API
- `GET /api/metrics` - Current streaming metrics
- `GET /api/latest?limit=N` - Latest N records  
- `GET /api/lines` - Line activity data

### WebSocket Events
- `metrics_update` - Real-time metrics
- `latest_records` - New record batch
- `line_activity` - Updated line statistics

## Technical Details

### Stack
- **Backend**: Flask + SocketIO for WebSocket support
- **Frontend**: Vanilla JavaScript with Chart.js and Leaflet
- **Database**: Snowflake via Snowpark Python
- **Real-time**: WebSocket streaming every 5 seconds

### Performance
- Efficient Snowflake queries with LIMIT clauses
- WebSocket streaming minimizes HTTP overhead
- Chart.js for smooth data visualizations
- Responsive design for mobile/desktop

### Data Flow
```
Snowflake ICYMTA Table → Dashboard Backend → WebSocket → Frontend → Charts/Map
```

## Troubleshooting

### Dashboard Not Loading
```bash
# Check dashboard logs
docker-compose logs dashboard

# Check if service is running
docker-compose ps dashboard

# Test API directly
curl http://localhost:8080/api/metrics
```

### No Data Showing
1. Verify Snowflake connection in logs
2. Check if ICYMTA table has data
3. Confirm environment variables are set
4. Test with: `python test_snowflake_connection.py`

### WebSocket Issues
- Check browser console for connection errors
- Verify port 8080 is accessible
- Test REST API endpoints as fallback

## Development

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
# ... etc

# Run development server
python run_dashboard.py
```

### Adding Features
- Edit `app.py` for backend API changes
- Edit `templates/dashboard.html` for frontend updates
- Add new WebSocket events for real-time features
- Extend Snowflake queries for new metrics

## Security Notes

- Dashboard uses read-only Snowflake access
- No sensitive data is exposed in frontend
- Private keys are mounted as read-only volumes
- Health checks ensure service availability