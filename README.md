# iLoggerDocker

iLoggerDocker is a comprehensive telemetry and tracking system designed for autonomous monitoring, location prediction, and environmental awareness.

## System Architecture

The system is built as a set of Dockerized microservices communicating via a central WebSocket Mesh.

### Core Components

- **Socket Server (`modules/socket_server`)**: The central nervous system. It routes messages between all modules using a custom MeshSocket protocol. Now supports **Token-based Authentication**.
- **Socket Ears (`modules/socket_ears`)**: Ingests raw data from external sources and provides **Centralized Logging** for all system modules.
- **Database (`modules/db`)**: PostgreSQL + PostGIS + TimescaleDB for high-performance geospatial and time-series data storage.

### Security & Reliability

- **Authentication**: All mesh nodes must identify with a valid `MESH_AUTH_TOKEN`.
- **Health Checks**: Every service in the stack includes Docker health checks to ensure automatic recovery.
- **CI/CD**: GitHub Actions pipeline automatically runs unit tests on every push.
- **Centralized Logging**: Modules send their internal logs over the socket to `socket_ears`, which rotates and stores them daily.

### Intelligence Modules

- **LocationOracle (`modules/LocationOracle`)**: 
  - Predicts future device locations using Dead Reckoning (short-term) and Historical Analysis (long-term).
  - Listens for `request_prediction` and emits `prediction_result`.
  
- **WeatherMan (`modules/WeatherMan`)**: 
  - Monitors weather conditions for connected devices.
  - "WeatherMan 2.0" capability: Uses `LocationOracle` to fetch weather forecasts for *predicted* future locations, alerting on incoming storms.

- **TrafficCam (`modules/TrafficCam`)**: 
  - Scans for traffic cameras along the device's path.
  - Uses `LocationOracle` to identify cameras in the predicted path (45s ahead).

### Visualization

- **Dashboard (`modules/dashboard`)**: 
  - Real-time telemetry visualization (Speed, Heading, Battery).
  - **Ghost Marker**: Visualizes the predicted location of the device 5 minutes into the future.
  - **System Log Viewer**: Real-time broadcast of logs from all microservices.
  - **Node Status Metrics**: Live tracking of service uptime and custom metrics.
  - Historical data playback and analysis.

## Development

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### Running the Stack
```bash
docker-compose up --build
```

### Protocol
Modules communicate via JSON packets over WebSockets.
- `iCloudListen`: Real-time device position updates.
- `request_prediction`: Request for future location.
- `prediction_result`: Response with calculated coordinates.

## Project Structure
```
iLoggerDocker/
├── lib/                 # Shared libraries (SocketCore)
├── modules/             # Microservices
│   ├── dashboard/       # Web UI
│   ├── LocationOracle/  # Prediction Engine
│   ├── WeatherMan/      # Weather Analysis
│   ├── socket_server/   # Message Broker
│   └── ...
└── docker-compose.yml
```
