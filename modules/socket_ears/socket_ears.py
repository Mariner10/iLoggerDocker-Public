import asyncio
import json
import os
import datetime
import logging
from lib.socketCore import MeshSocket, LogColors

# --- CONFIGURATION ---
LOG_DIR = os.getenv("LOG_DIR", "./logs")
SOCKET_URL = os.getenv("SOCKET_URL", "ws://socket_server:8765")
os.makedirs(LOG_DIR, exist_ok=True)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class TrafficListener:
    def __init__(self):
        # 1. Initialize Socket
        self.socket = MeshSocket(SOCKET_URL)

        # 2. INSTALL SPY HOOK IMMEDIATELY (Before connection starts)
        self._original_process = self.socket._process_packet
        self.socket._process_packet = self._spy_process_packet

        # 3. Register Specific Handlers
        @self.socket.on('iCloudListen')
        async def handle_icloud_data(payload: dict):
            logging.info(f"{LogColors.GREEN}[iCloud SAVE]{LogColors.ENDC} {json.dumps(payload)}")
            await self._append_to_jsonl(payload, "icloud")

        @self.socket.on('service_log')
        async def handle_service_log(payload: dict):
            # Save logs from other services
            await self._append_to_jsonl(payload, "services")

        @self.socket.on('healthcheck')
        async def health_check(payload):
            return {"status": "ok"}

    # --- THE SPY METHOD ---
    async def _spy_process_packet(self, packet: dict):
        """Intercepts all packets to log them, then passes them to the real handler."""
        msg_type = packet.get('type')
        
        # Log everything EXCEPT what we already explicitly handle/log
        if msg_type not in ['iCloudListen', 'healthcheck', 'service_log']: 
            payload_preview = str(packet.get('payload'))[:150] 
            logging.info(f"{LogColors.HEADER}[TRAFFIC]{LogColors.ENDC} Type: {msg_type} | Payload: {payload_preview}...")
        
        # IMPORTANT: Call the original method so the actual handlers still work!
        await self._original_process(packet)

    async def start(self):
        # 4. Start the Socket (The spy is already watching!)
        await self.socket.start()
        logging.info("Waiting for connection...")
        await self.socket.wait_until_ready()
        logging.info(f"{LogColors.BLUE}Listener Connected and Logging...{LogColors.ENDC}")

        # 5. Keep alive loop & Daily File Rotation
        while True:
            await asyncio.sleep(60)

    def _get_log_file_path(self, log_type="icloud"):
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        return os.path.join(LOG_DIR, f"{log_type}_{today}.jsonl")

    async def _append_to_jsonl(self, data: dict, log_type="icloud"):
        try:
            line = json.dumps(data)
            await asyncio.to_thread(self._write_line, line, log_type)
        except Exception as e:
            logging.error(f"{LogColors.FAIL}Failed to write to file: {e}{LogColors.ENDC}")

    def _write_line(self, line, log_type):
        with open(self._get_log_file_path(log_type), "a") as f:
            f.write(f"{line}\n")

if __name__ == "__main__":
    listener = TrafficListener()
    try:
        asyncio.run(listener.start())
    except KeyboardInterrupt:
        logging.info("Stopping Listener...")