from pyicloud import PyiCloudService
from dotenv import load_dotenv
import os
import time
import json
import logging
import asyncio
from math import radians, cos, sin, asin, sqrt, atan2, degrees
from datetime import datetime, timezone
from zoneinfo import ZoneInfo 
from timezonefinder import TimezoneFinder
import sys
import uuid
from collections import deque

from lib.socketCore import MeshSocket
from trip_engine import TripEngine

# Configure the root logger
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()
load_dotenv()

tf = TimezoneFinder()

class AppConfig:
    def __init__(self):
        self.data = {
            "icloud": {
                "max_attempts": 10,
                "attempt_cooldown": 30,
                "failed_conn_limit": 3
            },
            "history": {
                "size": 5,
                "battery_history_multiplier": 5,
                "pos_history_multiplier": 5
            },
            "tuning": {
                "trip_start_confirm_time": 30, # Seconds
                "trip_end_timeout": 180,       # Seconds (3 min)
                "gps_jitter_threshold": 0.4572,# 1.5 feet (meters)
                "recency_cluster_dist": 10     # Meters
            },
            "trip_logic": {
                "start_trip_speed": 2.5,       # ~5.6 mph
                "start_trip_dist": 60,         # Meters
                "stop_trip_speed": 1.5         # ~3.4 mph
            },
            "polling": {
                "base": 30,
                "min": 3,
                "max": 900
            },
            "intervals": {
                "thresholds": {
                    "low_battery": 20,
                    "fast_move": 6.71,    # 15 mph
                    "med_move": 1.34112   # 3 mph
                },
                "multipliers": {
                    "low_battery": 3,
                    "fast": 0.1,
                    "medium": 0.5,
                    "stationary": 1.0,
                    "deep_sleep": 30,
                    "light_sleep": 15
                }
            },
            "targets": [
                os.getenv("MAIN_DEVICE")
            ]
        }

    def update(self, new_config: dict):
        """Recursively update the config dictionary."""
        def recursive_update(d, u):
            for k, v in u.items():
                if isinstance(v, dict):
                    d[k] = recursive_update(d.get(k, {}), v)
                else:
                    d[k] = v
            return d
        
        self.data = recursive_update(self.data, new_config)
        logger.info("Configuration updated successfully.")

# Initialize Global Config
CONFIG = AppConfig()

# ==========================================

def get_timezone_from_coords(lat: float, lon: float) -> str:
    tz_name = tf.timezone_at(lng=lon, lat=lat) 
    return tz_name if tz_name else "UTC"

class Timestamp:
    def __init__(self, ms_value: int, tz_name: str = "UTC"):
        self.ms = ms_value
        self.utc_dt = datetime.fromtimestamp(self.ms / 1000.0, tz=timezone.utc)
        self.dt = self.utc_dt.astimezone(ZoneInfo(tz_name))

    @classmethod
    def from_now(cls, tz_name: str = "UTC"):
        ms_now = int(datetime.now(timezone.utc).timestamp() * 1000)
        return cls(ms_now, tz_name)

    def time_since(self) -> str:
        now = datetime.now(timezone.utc)
        diff = now - self.utc_dt
        seconds = int(diff.total_seconds())
        if seconds < 60: return f"{seconds}s ago"
        elif seconds < 3600: return f"{seconds // 60}m ago"
        elif seconds < 86400: return f"{seconds // 3600}h ago"
        else: return f"{seconds // 86400}d ago"

    def __sub__(self, other):
        if isinstance(other, Timestamp):
            return self.utc_dt - other.utc_dt
        return NotImplemented

    def format(self, fmt: str = "%Y-%m-%d %I:%M:%S %p %Z") -> str:
        return self.dt.strftime(fmt)

    def __repr__(self):
        return f"<Timestamp ms={self.ms} tz={self.dt.tzname()} date='{self.format()}'>"

async def connect_to_icloud(username, password, attempt_N=0):
    # Use Config values
    maxAttempts = CONFIG.data['icloud']['max_attempts']
    attemptCooldownSeconds = CONFIG.data['icloud']['attempt_cooldown']

    if attempt_N >= maxAttempts:
        logger.error(f"Could not connect to iCloud in under {maxAttempts} attempts.")
        exit(1)
        
    try:
        logger.info(f"iCloud connection attempt #{attempt_N + 1}")
        connectorObject = await asyncio.to_thread(PyiCloudService, username, password)
        return connectorObject
        
    except Exception as e:
        attempt_N += 1
        cooldown = attemptCooldownSeconds * attempt_N
        logger.error(f"iCloud connection failed, sleeping for {cooldown}: {e}")
        await asyncio.sleep(cooldown)
        return await connect_to_icloud(username, password, attempt_N=attempt_N)
    
class Device:
    def __init__(self, icloud_device_obj):
        self.device = icloud_device_obj
        self.content: dict = icloud_device_obj._content
        self.timestamp = None
        self.latitude = None
        self.longitude = None
        self.old_location_key = None

        # --- Polling config ---
        # Initialize with config, but this will be dynamically recalculated
        self.polling_interval = CONFIG.data['polling']['base']
        self.connection_error_counter = 0

        # --- Trip Engine ---
        self.trip_engine = TripEngine({
            "start_speed": CONFIG.data['trip_logic']['start_trip_speed'],
            "stop_speed": CONFIG.data['trip_logic']['stop_trip_speed'],
            "start_confirm_time": CONFIG.data['tuning']['trip_start_confirm_time'],
            "stop_confirm_time": CONFIG.data['tuning']['trip_end_timeout'],
        })
        self.is_moving = False
        self.session_id = str(uuid.uuid4())
        self.trip_result = {}  # Latest result from trip engine

        # Historical averaging (Configurable size)
        hist_size = CONFIG.data['history']['size']
        bat_mult = CONFIG.data['history']['battery_history_multiplier']
        pos_mult = CONFIG.data['history']['pos_history_multiplier']

        self.speed_history = deque(maxlen=hist_size)
        self.move_delta_history = deque(maxlen=hist_size)
        self.battery_level_history = deque(maxlen=hist_size * bat_mult)
        self.positional_history = deque(maxlen=hist_size * pos_mult)

    async def _reestablish_session(self):
        FAILED_CONNECTION_LIMIT = CONFIG.data['icloud']['failed_conn_limit']

        if self.connection_error_counter <= FAILED_CONNECTION_LIMIT:
            self.connection_error_counter += 1
            return False
        else:
            pass

        username = os.getenv("APPID")
        password = os.getenv("APPSECRET")
        
        if not username or not password:
            logger.error("Cannot re-auth: Missing APPID/APPSECRET.")
            return False

        logger.warning(f"Session expired for {self.deviceName}. Attempting re-authentication...")
        
        try:
            new_api = await connect_to_icloud(username, password)
            target_id = getattr(self, 'deviceID', self.content.get('id'))
            found_device_obj = None
            
            for dev in new_api.devices:
                if dev._content.get('id') == target_id:
                    found_device_obj = dev
                    break
            
            if found_device_obj:
                self.device = found_device_obj
                logger.info(f"Re-authentication successful. Session renewed for {self.deviceName}.")
                return True
            else:
                logger.error(f"Re-auth passed, but device ID {target_id} disappeared from iCloud account.")
                return False

        except Exception as e:
            logger.error(f"Re-authentication failed: {e}")
            return False

    @property
    def average_speed(self) -> float:
        if not self.speed_history:
            return 0.0
        return sum(self.speed_history) / len(self.speed_history)
    
    @property
    def discharge_rate(self) -> float:
        if len(self.battery_level_history) < 2:
            return 0.0
        
        start_time, start_level = self.battery_level_history[0]
        end_time, end_level = self.battery_level_history[-1]

        time_delta_seconds = (end_time - start_time).total_seconds()
        
        if time_delta_seconds <= 0:
            return 0.0
            
        hours_elapsed = time_delta_seconds / 3600.0
        level_drop = start_level - end_level
        rate = level_drop / hours_elapsed
        return round(rate, 2)
    
    @property
    def average_move_delta(self) -> float:
        if not self.move_delta_history:
            return 0.0
        return sum(self.move_delta_history) / len(self.move_delta_history)
    

    @property
    def historical_cluster_center_point(self) -> tuple[float,float]:
        if not self.positional_history or len(self.positional_history) < 2:
            return None
        
        lats, lons = zip(*self.positional_history)
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)

        return (center_lat, center_lon)

    @property
    def distance_from_historical_cluster(self) -> float:
        if not self.historical_cluster_center_point:
            return 0.0
        return self._get_haversine_distance(self.historical_cluster_center_point[0], self.historical_cluster_center_point[1],rounding=2)
        

    def _get_haversine_distance(self, lat2, lon2, rounding:int = 1) -> float:
        if not self.latitude or not lat2: return 0
        R = 6371000  
        lat1, lon1, lat2, lon2 = map(radians, [self.latitude, self.longitude, lat2, lon2])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        return round(2 * R * asin(sqrt(a)),rounding)
    

    def _get_bearing(self, lat1, lon1, lat2, lon2):
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)

        d_lon = lon2_rad - lon1_rad

        y = sin(d_lon) * cos(lat2_rad)
        x = cos(lat1_rad) * sin(lat2_rad) - \
            sin(lat1_rad) * cos(lat2_rad) * cos(d_lon)

        initial_bearing = atan2(y, x)
        initial_bearing = degrees(initial_bearing)
        compass_bearing = (initial_bearing + 360) % 360

        return round(compass_bearing, 1)

    def parse_content(self):
        locationInfo: dict = self.content.get('location')
        if not locationInfo:
            return False

        self.deviceID = self.content.get('id')
        self.deviceName = self.content.get('name', 'Unknown')
        
        if locationInfo.get('isOld', False):
            logger.debug(f"Skipping update for {self.deviceName}: Location is marked 'Old' by iCloud.")
            return False

        current_location_key = (locationInfo.get('latitude'), locationInfo.get('longitude'), locationInfo.get('timeStamp'))
        if current_location_key == self.old_location_key:
            return False
            
        self.tz_name = tf.timezone_at(lng=locationInfo.get('longitude'), lat=locationInfo.get('latitude')) or "UTC"
        new_ts = Timestamp(locationInfo.get('timeStamp'), self.tz_name)
        
        if self.timestamp:
            if new_ts.ms < self.timestamp.ms:
                logger.warning(f"Ignored out-of-order packet for {self.deviceName}.")
                return False

            self.timediff = (new_ts - self.timestamp).total_seconds()
            
            if self.timediff <= 0:
                logger.warning(f"Zero time difference detected for {self.deviceName}.")
                self.speed = 0.0
            else:
                new_lat = locationInfo.get('latitude')
                new_lon = locationInfo.get('longitude')
                self.positionType = locationInfo.get('positionType','Unknown')
                
                self.move_delta = self._get_haversine_distance(new_lat, new_lon)
                self.speed = round(self.move_delta / self.timediff, 2)
                
                self.battery_level_history.append((self.timestamp, self.batteryLevel))
                
                # Use CONFIG for jitter threshold
                jitter = CONFIG.data['tuning']['gps_jitter_threshold']
                if self.move_delta > jitter:
                    self.course = self._get_bearing(self.latitude, self.longitude, new_lat, new_lon)
                else:
                    self.course = getattr(self, 'course', -1)
        else:
            self.timediff = 0
            self.speed = 0
            self.move_delta = 0

        self.speed_history.append(self.speed)
        self.move_delta_history.append(self.move_delta)

        self.old_location_key = current_location_key
        self.latitude = locationInfo.get('latitude')
        self.longitude = locationInfo.get('longitude')
        self.timestamp = new_ts
        
        self.positional_history.append((self.latitude,self.longitude))

        tmpbat = self.content.get('batteryLevel')
        self.batteryLevel:int = round(tmpbat * 100) if tmpbat is not None else 0
        self.batteryStatus:str = self.content.get('batteryStatus')
        self.lowPowerMode:bool = self.content.get('lowPowerMode', False)
        
        return True

    async def update(self):
        manager = getattr(self.device, '_manager', None)
        
        if manager and hasattr(manager, '_refresh_client'):
            try:
                await asyncio.to_thread(manager._refresh_client)
            except Exception as e:
                logger.warning(f"Refresh failed for {self.deviceName}: {e}")
                
                if await self._reestablish_session():
                    try:
                        new_manager = getattr(self.device, '_manager', None)
                        await asyncio.to_thread(new_manager._refresh_client)
                    except Exception as retry_e:
                        logger.error(f"Retry after re-auth failed: {retry_e}")
                        return False
                else:
                    return False
        else:
            logger.warning(f"Device {self.deviceName} has no refreshable manager.")

        self.content = self.device._content
        return self.parse_content()
    
    def _update_trip_state(self):
        """Delegate trip detection to the TripEngine state machine."""
        if not self.latitude or not self.timestamp:
            return

        self.trip_result = self.trip_engine.process_reading(
            lat=self.latitude,
            lon=self.longitude,
            timestamp=self.timestamp.utc_dt,
            raw_speed_mps=getattr(self, 'speed', 0),
            raw_course=getattr(self, 'course', -1),
        )

        self.is_moving = self.trip_result["is_moving"]
        self.session_id = self.trip_result["trip_id"]

    def get_next_interval(self) -> int:
        """Calculates sleep time based on ROLLING AVERAGE speed using Global Config."""
        
        # --- CONFIG CONSTANTS ---
        MIN_INTERVAL = CONFIG.data['polling']['min']
        MAX_INTERVAL = CONFIG.data['polling']['max']
        BASE_INTERVAL = CONFIG.data['polling']['base'] # Default if needed
        
        # Thresholds
        LOW_BATTERY_THRESHOLD = CONFIG.data['intervals']['thresholds']['low_battery']
        FAST_MOVEMENT_THRESHOLD = CONFIG.data['intervals']['thresholds']['fast_move']
        MEDIUM_MOVEMENT_THRESHOLD = CONFIG.data['intervals']['thresholds']['med_move']
        
        DISTANCE_FROM_RECENCY_CLUSTER = CONFIG.data['tuning']['recency_cluster_dist']
        
        # Multipliers
        LOW_BATTERY_MULTIPLIER = CONFIG.data['intervals']['multipliers']['low_battery']
        FAST_MULTIPLIER = CONFIG.data['intervals']['multipliers']['fast']
        MEDIUM_MULTIPLIER = CONFIG.data['intervals']['multipliers']['medium']
        STATIONARY_MULTIPLIER = CONFIG.data['intervals']['multipliers']['stationary']
        DEEP_SLEEP_MULT = CONFIG.data['intervals']['multipliers']['deep_sleep']
        LIGHT_SLEEP_MULT = CONFIG.data['intervals']['multipliers']['light_sleep']
        
        # Metrics
        avg_speed = self.average_speed
        avg_dist = self.average_move_delta 
        
        # --- 1. DETERMINE BASE STATE ---
        if avg_speed >= FAST_MOVEMENT_THRESHOLD:
            return int(max(MIN_INTERVAL, BASE_INTERVAL * FAST_MULTIPLIER))

        elif avg_speed >= MEDIUM_MOVEMENT_THRESHOLD:
            current_interval = BASE_INTERVAL * MEDIUM_MULTIPLIER
            is_moving = True
        else:
            current_interval = BASE_INTERVAL * STATIONARY_MULTIPLIER
            is_moving = False

        # --- 2. APPLY PENALTIES ---
        
        if self.batteryLevel < LOW_BATTERY_THRESHOLD and self.batteryStatus != 'Charging':
            current_interval *= LOW_BATTERY_MULTIPLIER

        if self.lowPowerMode and self.distance_from_historical_cluster < DISTANCE_FROM_RECENCY_CLUSTER:
            current_interval *= 2

        if not is_moving and self.timestamp:
            try:
                hour = self.timestamp.dt.hour
                if 2 <= hour < 6: 
                    if avg_dist < 5:
                        current_interval *= DEEP_SLEEP_MULT
                    else:
                        current_interval *= LIGHT_SLEEP_MULT
            except Exception:
                pass

        # --- 3. CLAMP AND RETURN ---
        self.polling_interval = int(max(MIN_INTERVAL, min(MAX_INTERVAL, current_interval)))
        return self.polling_interval

    @classmethod
    async def create(cls, icloud_device_obj):
        instance = cls(icloud_device_obj)
        instance.parse_content()
        return instance
    
    async def package(self) -> str:
        return {
            'id': getattr(self, 'deviceID', None),
            'name': getattr(self, 'deviceName', 'Unknown'),
            'latitude': self.latitude,
            'longitude': self.longitude,
            'accuracy': getattr(self, 'accuracy', 0),
            'batteryLevel': getattr(self, 'batteryLevel', 0),
            'batteryStatus': getattr(self, 'batteryStatus', 'Unknown'), # Charging or not chargring
            'discharge_rate': getattr(self,'discharge_rate',0),
            'lowPowerMode': getattr(self, 'lowPowerMode', False),
            'positionType': getattr(self,'positionType','Unknown'),
            'speed': getattr(self, 'speed', 0.0), # meters per second
            'course': getattr(self,'course',-1), # heading (degrees)
            'is_moving': self.is_moving,
            'movement_session_id': self.session_id,
            'trip_id': self.session_id,
            'move_delta': getattr(self, 'move_delta', 0.0), # meters
            'time_diff': getattr(self, 'timediff', 0),
            'historical_cluster_center_point': getattr(self,'historical_cluster_center_point',None), # tuple ( lat, lon )
            'historical_cluster_center_radius': CONFIG.data['tuning']['recency_cluster_dist'], # int ( meters )
            'distance_from_historical_cluster': getattr(self,'distance_from_historical_cluster',0), # int meters
            'polling_interval': self.polling_interval,  # float seconds
            'timestamp_ms': self.timestamp.ms if self.timestamp else 0,
            'timestamp_str': self.timestamp.format() if self.timestamp else "N/A",
            'timezone': self.tz_name,
            'last_updated': self.timestamp.time_since() if self.timestamp else "Never"
        }

class DeviceGroup:
    def __init__(self, connection, devices):
        self.api = connection
        self.devices:dict[str,Device] = devices

    @classmethod
    async def create(cls, username, password):
        api = await connect_to_icloud(username, password)
        if api.requires_2fa:
            print("Two-factor authentication required.")
        devices = {}
        for dev_obj in api.devices:
            dev_id = dev_obj.data['id']
            devices[dev_id] = await Device.create(dev_obj)
        return cls(api, devices)

    async def refresh_all(self):
        tasks = [d.update() for d in self.devices.values()]
        return await asyncio.gather(*tasks)
    
    async def refresh_device(self,device_id:str):
        try:
            device = self.devices.get(device_id)
        except KeyError:
            return False
        updateResult = await device.update()
        return updateResult

async def monitor_device_loop(device: Device, socket: MeshSocket):
    logger.info(f"Started monitoring task for {device.deviceName}")
    
    while True:
        cycle_start = asyncio.get_running_loop().time()
        
        try:
            success = await device.update()
            if success:
                device._update_trip_state()
                await ship_data(socket, device)
        except Exception as e:
            logger.error(f"Error updating {device.deviceName}: {e}")

        sleep_seconds = device.get_next_interval()
        
        elapsed = asyncio.get_running_loop().time() - cycle_start
        actual_sleep = max(1, sleep_seconds - elapsed)
        
        logger.info(f"[{device.deviceName}] AvgSpd: {device.average_speed:.2f} | Sleep: {actual_sleep:.1f}s")
        
        await asyncio.sleep(actual_sleep)

async def ship_data(connection:MeshSocket,device:Device):
    data = await device.package()
    await connection.send("iCloud_data_Broadcast",data)
    
async def disconnect_handler():
    pass

async def reconnect_handler():
    pass

async def main():
    # 1. Setup
    api_id = os.getenv("APPID")
    api_secret = os.getenv("APPSECRET")
    
    if not api_id or not api_secret:
        logger.error("Missing APPID or APPSECRET in .env")
        return

    # 2. Connect and Create Group
    group = await DeviceGroup.create(api_id, api_secret)
    
    # 3. Setup Socket
    socket = MeshSocket(
        "ws://socket_server:8765",
        max_offline_buffer=10,
        offline_file_path="backup.jsonl",
        on_reconnect=reconnect_handler,
        on_disconnect=disconnect_handler
    )
    
    @socket.on('healthcheck')
    async def health_check(payload):
        return {"status": "ok"}
    
    @socket.on('configure')
    async def update_configuration(payload):
        """
        Accepts a JSON payload (partial or full) and updates the Global Config.
        Because asyncio is single-threaded, this update is atomic relative
        to other tasks. The next time a loop runs, it reads the new values.
        """
        logger.info(f"Received Configuration Update: {payload}")
        try:
            CONFIG.update(payload)
            return {"status": "updated", "current_config": CONFIG.data}
        except Exception as e:
            logger.error(f"Failed to update config: {e}")
            return {"status": "error", "message": str(e)}
    
    @socket.on('get_configure')
    async def retreive_configuration(payload):
        logger.info(f"Received Configuration Request.")
        try:
            return {"status": "sucess", "current_config": CONFIG.data}
        except Exception as e:
            logger.error(f"Failed to retrieve config: {e}")
            return {"status": "error", "message": str(e)}

    await socket.start()

    # 4. Launch Independent Tasks
    # Using IDs from Config now
    target_ids = CONFIG.data['targets']
    
    tasks = []
    
    for device_id in target_ids:
        if device_id in group.devices:
            device = group.devices[device_id]
            task = asyncio.create_task(monitor_device_loop(device, socket))
            tasks.append(task)
        else:
            logger.warning(f"Device ID {device_id} not found in iCloud account.")

    if not tasks:
        logger.error("No valid devices found to monitor. Exiting.")
        return

    # 5. Keep the Main Program Alive
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Main loop cancelled, shutting down.")
    finally:
        pass

asyncio.run(main())