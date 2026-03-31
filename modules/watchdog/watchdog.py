import asyncio
import logging
import os
import time
import docker
from dotenv import load_dotenv
load_dotenv()

from lib.socketCore import MeshSocket, LogColors

# --- CONFIGURATION ---
SOCKET_URL = os.getenv('SOCKET_URL', f"ws://{os.getenv('SOCKET_SERVER_CONTAINER_NAME')}:8765")
CHECK_INTERVAL = 10  # Seconds between checks
TIMEOUT = 5.0        # Seconds to wait for a pong
DOCKER_SOCK = os.getenv('DOCKER_HOST', 'unix://var/run/docker.sock')

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Watchdog")

class WatchdogService:
    def __init__(self):
        # 1. Setup Docker Client
        try:
            self.docker_client = docker.from_env()
            logger.info(f"{LogColors.GREEN}Docker Client Connected{LogColors.ENDC}")
        except Exception as e:
            logger.error(f"{LogColors.FAIL}Failed to connect to Docker: {e}{LogColors.ENDC}")
            self.docker_client = None

        # 2. Setup Socket
        self.socket = MeshSocket(SOCKET_URL, name="watchdog")
        
        # State
        self.server_failure_count = 0
        self.known_clients = {} # Format: { 'uuid': 'container_name' }

        # 3. Register Handlers
        @self.socket.on('server_client_list')
        async def update_client_list(payload):
            """Received whenever a client connects/disconnects."""
            raw_list = payload.get('clients', [])
            
            # Update our local map, filtering out the Watchdog itself
            new_map = {}
            for c in raw_list:
                c_id = c.get('id')
                c_name = c.get('name')
                
                # Ignore self and unknown names
                if c_name and "watchdog" not in c_name:
                    new_map[c_id] = c_name
            
            # Log changes
            added = set(new_map.values()) - set(self.known_clients.values())
            removed = set(self.known_clients.values()) - set(new_map.values())
            
            if added: logger.info(f"New targets: {added}")
            if removed: logger.info(f"Lost targets: {removed}")
            
            self.known_clients = new_map

    async def start(self):
        """Starts the socket connection and the monitoring loop."""
        await self.socket.start()
        
        # Wait for connection before starting monitor
        logger.info("Waiting for socket connection...")
        await self.socket.wait_until_ready()
        
        logger.info("Starting Monitoring Loop...")
        while True:
            await asyncio.sleep(CHECK_INTERVAL)

            server_healthy = await self._check_server_health()
            
            # --- STEP 2: CHECK CLIENTS (Only if Server is OK) ---
            if server_healthy:
                await self._check_all_clients()

    async def _check_server_health(self):
        """
        Verifies if the Server is reachable and responsive.
        Returns: True if healthy, False if restarted.
        """
        is_connected = self.socket.connected_event.is_set()
        
        # CASE A: Disconnected completely
        if not is_connected:
            self.server_failure_count += 1
            logger.warning(f"{LogColors.WARNING}Server Disconnected. Strike {self.server_failure_count}/3{LogColors.ENDC}")
        
        # CASE B: Connected, but let's check responsiveness (Zombie check)
        else:
            try:
                # Send a direct 'ping' to the server instance handling us
                # socketCore has a built-in 'ping' handler that returns 'pong'
                response = await self.socket.request('ping', timeout=TIMEOUT)
                
                if response == "pong":
                    self.server_failure_count = 0 # RESET COUNTER
                    return True # HEALTHY
                else:
                    self.server_failure_count += 1
                    logger.warning(f"Server sent invalid ping response: {response}")
            
            except Exception as e:
                self.server_failure_count += 1
                logger.warning(f"{LogColors.WARNING}Server Ping Timeout/Error. Strike {self.server_failure_count}/3{LogColors.ENDC}")

        # --- RESTART DECISION ---
        if self.server_failure_count >= 3:
            logger.error(f"{LogColors.FAIL}SERVER IS UNRESPONSIVE. RESTARTING SERVER CONTAINER...{LogColors.ENDC}")
            await self._restart_container(os.getenv("SOCKET_SERVER_CONTAINER_NAME"))
            
            # Reset counter and wait for boot
            self.server_failure_count = 0
            logger.info("Pausing Watchdog for 30s to allow Server reboot...")
            await asyncio.sleep(30) 
            return False
            
        return False # Not healthy enough to check clients yet, or disconnected

    async def _check_all_clients(self):
        """Iterates through all known clients and healthchecks them."""
        if not self.known_clients:
            return

        # Create a copy to avoid errors if list changes during iteration
        targets = self.known_clients.copy()
        
        for client_id, container_name in targets.items():
            await self._probe_client(client_id, container_name)

    async def _probe_client(self, client_id, container_name):
        """Sends a ping through the server to the specific client."""
        
        if 'ignore' in container_name:
            return

        try:
            # logger.info(f"Pinging {container_name}...")
            start_time = time.time()
            
            # 1. Send Request (routed through server)
            response = await self.socket.request(
                type='route_msg', 
                payload={
                    'target_id': client_id,
                    'type': 'healthcheck', # The client needs a handler for this!
                    'payload': {'t': start_time}
                }, 
                timeout=TIMEOUT
            )

            # 2. Validate Response
            if response:
                latency = (time.time() - start_time) * 1000
                # logger.info(f"{LogColors.GREEN}[OK]{LogColors.ENDC} {container_name} ({latency:.0f}ms)")
            else:
                # Response was None (Timeout or Error)
                logger.warning(f"{LogColors.FAIL}[TIMEOUT]{LogColors.ENDC} {container_name} failed to respond.")
                await self._restart_container(container_name)

        except Exception as e:
            logger.error(f"Error probing {container_name}: {e}")

    async def _restart_container(self, container_name):
        """Restarts the docker container."""
        if not self.docker_client:
            logger.error("Cannot restart: Docker client not active.")
            return

        logger.warning(f"{LogColors.WARNING}RESTARTING CONTAINER: {container_name}{LogColors.ENDC}")
        
        try:
            # Run the blocking Docker API call in a separate thread
            self.socket.send('broadcast_request',{"watchdog_message":f'restarting {container_name}!'})
            await asyncio.to_thread(self._restart_sync, container_name)
        except Exception as e:
            logger.error(f"Failed to restart {container_name}: {e}")

    def _restart_sync(self, name):
        """Blocking Docker restart call."""
        try:
            container = self.docker_client.containers.get(name)
            container.restart()
            logger.info(f"{LogColors.GREEN}Successfully restarted {name}{LogColors.ENDC}")
        except docker.errors.NotFound:
            logger.error(f"Container '{name}' not found on host.")
        except Exception as e:
            logger.error(f"Docker API Error: {e}")

if __name__ == "__main__":
    service = WatchdogService()
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        pass