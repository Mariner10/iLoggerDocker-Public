import asyncio
import websockets
from lib.socketCore import MeshSocket, LogColors
import logging
import os

class MeshServer:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.clients = set() # Set of MeshSocket objects

    async def start(self):
        logging.info(f"{LogColors.HEADER}Starting Server on {self.host}:{self.port}{LogColors.ENDC}")
        async with websockets.serve(self._handle_connection, self.host, self.port):
            await asyncio.Future() # Run forever

    async def _handle_connection(self, websocket):
        # 1. Wrap the raw websocket in our protocol
        client = MeshSocket(connection=websocket, name=f"Client-{id(websocket)}")
        
        # 2. Authentication Flow
        authenticated = asyncio.Future()
        server_token = os.getenv('MESH_AUTH_TOKEN')

        @client.on('identify')
        async def handle_identify(payload):
            client_token = payload.get('token')
            if server_token and client_token != server_token:
                logging.warning(f"{LogColors.FAIL}Auth Failed for {client.name}{LogColors.ENDC}")
                if not authenticated.done():
                    authenticated.set_result(False)
                return

            client.name = payload.get('name', client.name)
            client.id = payload.get('id', client.id)
            logging.info(f"{LogColors.GREEN}Client identified as: {client.name}{LogColors.ENDC}")
            if not authenticated.done():
                authenticated.set_result(True)
            # Optional: Broadcast new list to Watchdog immediately
            await self._broadcast_client_list()

        # We need to start processing packets to receive the 'identify' message
        listen_task = asyncio.create_task(client.listen())

        try:
            # Wait for authentication with a timeout
            is_auth = await asyncio.wait_for(authenticated, timeout=5.0)
            if not is_auth:
                await client.stop()
                return
        except asyncio.TimeoutError:
            logging.warning(f"{LogColors.FAIL}Auth Timeout for {client.name}{LogColors.ENDC}")
            await client.stop()
            return
        except Exception as e:
            logging.error(f"Auth error: {e}")
            await client.stop()
            return

        self.clients.add(client)
        logging.info(f"{LogColors.GREEN}New Authenticated Connection. Total Clients: {len(self.clients)}{LogColors.ENDC}")

        # 3. Register Server-Side Handlers for this specific client
        @client.on("broadcast_request")
        async def on_broadcast(payload):
            logging.info(f"Broadcast Request: {payload}")
            # Broadcast this message to everyone else
            await self.broadcast("broadcast", payload)
            return {"status": "sent"}

        @client.on("iCloud_data_Broadcast")
        async def on_iCloud_broadcast(payload):
            logging.info(f"iCloud Data: {payload}")
            # Broadcast this message to everyone else
            await self.broadcast("iCloudListen", payload)
            return {"status": "sent"}
        
        @client.on("request_prediction")
        async def on_prediction_request(payload):
            logging.info(f"Prediction Request: {payload}")
            await self.broadcast("request_prediction", payload)
            return {"status": "forwarded"}

        @client.on("prediction_result")
        async def on_prediction_result(payload):
            logging.info(f"Prediction Result: {payload}")
            await self.broadcast("prediction_result", payload)
            return {"status": "forwarded"}

        @client.on("service_log")
        async def on_service_log(payload):
            # Broadcast logs to all clients (e.g. Dashboard)
            await self.broadcast("service_log", payload)
            return {"status": "broadcasted"}

        @client.on("node_status")
        async def on_node_status(payload):
            # Broadcast status updates to all clients
            await self.broadcast("node_status", payload)
            return {"status": "broadcasted"}

        @client.on('route_msg')
        async def on_route(payload):
            """
            Allows a client to send a 'UDP' message to another specific client ( by ID ) 
            and get the response back.
            """
            target_id = payload.get('target_id')
            msg_type = payload.get('type')
            data = payload.get('payload')
            
            # Find the target socket object by ID
            target = next((c for c in self.clients if c.id == target_id), None)
            
            if target:
                # Forward the request to the target
                # The target's response will be returned here
                response = await target.request(msg_type, data, timeout=5.0)
                return response
            else:
                return {"error": "Target not found", "status": "failed"}
            
        @client.on('route_msg_noreply')
        async def on_noreply_route(payload):
            """Allows a client to send a 'TCP' message to another specific client ( by name )"""

            target_name = payload.get('target_name')
            msg_type = payload.get('type')
            data = payload.get('payload')
            
            # Find the target socket object by ID
            target = next((c for c in self.clients if c.name == target_name), None)

            if target:
                # Forward the request to the target
                await target.send(msg_type, data)
            else:
                return {"error": "Target not found", "status": "failed"}

        try:
            # Wait for the listen task to complete (when connection closes)
            await listen_task
        finally:
            # 4. Cleanup on disconnect
            if client in self.clients:
                self.clients.remove(client)
            logging.info(f"{LogColors.WARNING}Client Disconnected. Remaining: {len(self.clients)}{LogColors.ENDC}")

            
    async def _broadcast_client_list(self):
        """Sends the current list of connected clients to everyone."""
        # Serialize the set of objects into a list of dicts
        client_list = [{'id': c.id, 'name': c.name} for c in self.clients]
        await self.broadcast('server_client_list', {'clients': client_list})

    async def broadcast(self, type: str, payload: dict):
        """Sends a message to all connected clients."""
        if not self.clients:
            return
        
        # Create send tasks for all clients
        tasks = [client.send(type, payload) for client in self.clients]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    server = MeshServer(host='0.0.0.0')
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("Server Stopped.")
