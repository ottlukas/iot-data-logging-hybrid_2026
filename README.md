# IoT Data Logging Hybrid (FastAPI + IoTDB 2.0.8)

A compliant, hybrid IoT data logging system for regulated manufacturing, combining:
- FastAPI for user management and dashboarding.
- IoTDB 2.0.8 for time-series storage and querying.
- Plotly for real-time visualization.

## Features
- **User Management**: Role-based access control (operator, supervisor, admin).
- **Local Storage**: TSFile for immediate persistence.
- **Async Sync**: Periodic or manual sync to IoTDB.
- **Plotly Dashboard**: Real-time sensor data visualization.
- **GMP/Annex 11 Compliance**: Audit trails, electronic signatures, and checksums.

## Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/iot-data-logging-hybrid.git
   cd iot-data-logging-hybrid
2. **Start IoTDB and FastAPI:**
   ```bash
   docker-compose up -d --build
3. **Access the Dashboard:**
    Open http://localhost:8000/dashboard in your browser.
4. **Simulate Data Ingestion:**
    ```bash
   python ingest_sensor_data.py --random --count 5
5. **Sync to IoTDB**
- Click the "Sync to IoTDB" button on the dashboard.
6. **Query IoTDB:**
    ```bash
    docker exec -it iot-data-logging-hybrid-iotdb-1 /iotdb-cli.sh




