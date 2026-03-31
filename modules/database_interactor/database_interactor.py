import asyncio
import os
import sys
import logging
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv

# Import your socket library
from lib.socketCore import MeshSocket

# --- 1. CONFIGURATION ---
load_dotenv()

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("db_listener_debug.log"),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")
DB_NAME = os.getenv("POSTGRES_DB", "tracking_db")
SOCKET_URL = "ws://socket_server:8765"


class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        attempt = 0
        while attempt <= 5:
            try:
                self.connection = psycopg2.connect(
                    host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME
                )
                logger.info(f"Connected to Database '{DB_NAME}'")
                return
            except psycopg2.Error as e:
                logger.error(f"DB Connection failed: {e}. Retrying in {attempt+1}s...")
                time.sleep(attempt + 1)
                attempt += 1
        logger.critical("Could not connect to Database after multiple attempts.")
        sys.exit(1)

    def get_cursor(self):
        # Check if connection is alive, reconnect if needed
        if self.connection.closed:
            logger.warning("Connection found closed. Reconnecting...")
            self.connect()
        return self.connection.cursor()

    def save_data(self, data: dict):
        # 1. Extract all required fields, providing defaults for missing ones
        device_id = data.get('id')
        device_name = data.get('name')
        
        # Timestamp logic
        device_timestamp_ms = data.get('timestamp_ms')
        if device_timestamp_ms:
            dt_object = datetime.fromtimestamp(float(device_timestamp_ms) / 1000.0, tz=timezone.utc)
        else:
            dt_object = datetime.now(timezone.utc)

        # Location
        device_latitude = data.get('latitude', 0.0)
        device_longitude = data.get('longitude', 0.0)
        
        # Battery & Status
        device_batteryLevel = data.get('batteryLevel')
        device_discharge_rate = data.get('discharge_rate', 0.0) # Was missing
        device_batteryStatus = data.get('batteryStatus')
        device_lowPowerMode = data.get('lowPowerMode', False)
        
        # Movement
        device_speed = data.get('speed', 0.0)
        device_positionType = data.get('positionType','Unknown')
        device_course = data.get('course', 0.0)
        device_is_moving = data.get('is_moving', False)
        device_movement_session_id = data.get('movement_session_id')
        device_move_delta = data.get('move_delta', 0.0)
        device_time_diff = data.get('time_diff', 0.0)
        
        # Extras (Were missing)
        device_timezone = data.get('timezone', 'UTC') 
        device_distance_cluster = data.get('distance_from_historical_cluster', 0.0)

        query = '''
        INSERT INTO device_readings (
            device_id, 
            device_name,
            recorded_at, 
            location, 
            battery_level,
            discharge_rate, 
            battery_status, 
            low_power_mode,
            positionType,
            speed_mps, 
            course, 
            is_moving, 
            trip_id, 
            move_delta, 
            time_diff,
            timezone,
            distance_from_cluster_center
        ) VALUES (
            %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s
        )
        '''

        try:
            with self.connection.cursor() as cur:
                cur.execute(query, (
                    device_id,                  # 1. matches device_id
                    device_name,                # 2. matches device_name
                    dt_object,                  # 3. matches recorded_at (Crucial Fix: moved here)
                    device_longitude,           # 4. matches ST_MakePoint X
                    device_latitude,            # 5. matches ST_MakePoint Y
                    device_batteryLevel,        # 6. matches battery_level
                    device_discharge_rate,      # 7. matches discharge_rate (Added)
                    device_batteryStatus,       # 8. matches battery_status
                    device_lowPowerMode,        # 9. matches low_power_mode
                    device_positionType,
                    device_speed,               # 10. matches speed_mps
                    device_course,              # 11. matches course
                    device_is_moving,           # 12. matches is_moving
                    device_movement_session_id, # 13. matches trip_id
                    device_move_delta,          # 14. matches move_delta
                    device_time_diff,           # 15. matches time_diff
                    device_timezone,            # 16. matches timezone (Added)
                    device_distance_cluster     # 17. matches distance_from_cluster_center (Added)
                ))
            self.connection.commit()
            logger.info(f"💾 Saved reading: {device_name}")
        except Exception as e:
            logger.error(f"Save Data Error: {e}")
            self.connection.rollback()


    # --- ACTION: SAVE TRAFFIC CAM RECORDING ---
    def save_camera_recording(self, data: dict):
        # Expects: { "timestamp": 12345, "outputFile": "path/to.mp4", "camera": {...} }
        epoch_ms = float(data.get("timestamp"))
        output_file = data.get("outputFile")
        cam_data = data.get("camera", {})
        cam_name = cam_data.get("name")

        if not (epoch_ms and output_file and cam_name):
            return

        dt_object = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)

        # 1. Ensure Camera Exists
        upsert_cam = """
        INSERT INTO cameras (camera_name, location) 
        VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ON CONFLICT (camera_name) DO NOTHING;
        """
        # 2. Insert Recording
        insert_rec = """
        INSERT INTO camera_recordings (camera_name, video_path, recorded_at)
        VALUES (%s, %s, %s);
        """

        try:
            with self.connection.cursor() as cur:
                cur.execute(upsert_cam, (
                    cam_name, 
                    cam_data.get("longitude", 0), 
                    cam_data.get("latitude", 0)
                ))
                cur.execute(insert_rec, (cam_name, output_file, dt_object))
            self.connection.commit()
            logger.info(f"📹 Logged video: {cam_name}")
        except Exception as e:
            logger.error(f"Recording Log Error: {e}")
            self.connection.rollback()

# --- 3. ASYNC SOCKET LOGIC ---

async def main():
    # A. Initialize DB (Synchronous, but fast enough for startup)
    db = DatabaseConnection()

    # B. Initialize Socket
    client = MeshSocket(SOCKET_URL)

    # --- HANDLERS ---
    
    # 1. Listen for iCloud Data
    @client.on('iCloudListen')
    async def handle_icloud_data(payload: dict):
        # Run blocking DB call in a separate thread so we don't block the socket heartbeat
        await asyncio.to_thread(db.save_data, payload)

    @client.on('healthcheck')
    async def health_check(payload):
        return {"status": "ok"}

    # 2. Listen for Traffic Cam Recordings
    @client.on('newTrafficCamRecording')
    async def handle_camera_recording(payload: dict):
        await asyncio.to_thread(db.save_camera_recording, payload)

    # C. Connect
    logger.info(f"Connecting to Socket at {SOCKET_URL}...")
    await client.start()
    await client.wait_until_ready()
    logger.info("✅ Database Listener Connected & Ready.")

    # D. Keep Alive Loop
    while True:
        # We perform a ping every 30s just to log status, 
        # but the MeshSocket handles the actual keepalive internally.
        try:
            latency = await client.request("ping", {"ts": datetime.now().timestamp()})
            if latency:
                logger.debug(f"Heartbeat OK")
        except Exception:
            pass # Socket library handles reconnection logging
            
        await asyncio.sleep(30)

if __name__ == "__main__":
    import time # Needed for the DB retry loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping DB Listener...")