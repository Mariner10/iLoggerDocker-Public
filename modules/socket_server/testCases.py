import asyncio
import websockets
import time
import unittest
from lib.socketCore import MeshSocket, LogColors
from socketServer import MeshServer

# We wrap the test in a function to run it inside the async loop
async def run_integration_tests():
    print(f"\n{LogColors.HEADER}=== STARTING INTEGRATION TESTS ==={LogColors.ENDC}\n")

    # 1. Start Server in Background
    server_instance = MeshServer(port=8765)
    server_task = asyncio.create_task(server_instance.start())
    
    # Give server a moment to spin up
    await asyncio.sleep(0.5)

    try:
        # 2. Connect a Test Client
        print(f"{LogColors.BLUE}[TEST 1] Connecting Client...{LogColors.ENDC}")
        async with websockets.connect("ws://localhost:8765") as ws:
            client = MeshSocket(connection=ws, name="TestClient")
            
            # Start client listener in background
            listen_task = asyncio.create_task(client.listen())

            # --- TEST 2: Handshake (Request/Response) ---
            print(f"{LogColors.BLUE}[TEST 2] Testing Handshake & Latency...{LogColors.ENDC}")
            start_time = time.time()
            response = await client.request("handshake", {"t": start_time})
            
            if response and "server_id" in response:
                print(f"✅ Handshake Success! Server ID: {response['server_id']}")
            else:
                print(f"❌ Handshake Failed: {response}")

            # --- TEST 3: RPC (Ping/Pong) ---
            print(f"{LogColors.BLUE}[TEST 3] Testing RPC (Ping)...{LogColors.ENDC}")
            pong = await client.request("ping")
            if pong == "pong":
                print(f"✅ RPC Success! Received: {pong}")
            else:
                print(f"❌ RPC Failed. Received: {pong}")

            # --- TEST 4: Broadcast Reception ---
            print(f"{LogColors.BLUE}[TEST 4] Testing Global Broadcast...{LogColors.ENDC}")
            
            # Use an asyncio Event to wait for the broadcast
            broadcast_received = asyncio.Event()
            
            @client.on("broadcast")
            async def on_broadcast(payload):
                print(f"   -> Client received broadcast: {payload}")
                if payload['msg'] == "Hello World":
                    broadcast_received.set()

            # Trigger the broadcast via the server instance directly
            print("   -> Server broadcasting 'Hello World'...")
            await server_instance.broadcast("chat_message", {"msg": "Hello World"})
            
            try:
                await asyncio.wait_for(broadcast_received.wait(), timeout=2.0)
                print("✅ Broadcast Test Success!")
            except asyncio.TimeoutError:
                print("❌ Broadcast Test Failed (Timeout)")

            # --- TEST 5: Client-to-Client trigger ---
            print(f"{LogColors.BLUE}[TEST 5] Client triggering broadcast via RPC...{LogColors.ENDC}")
            ack = await client.request("chat", {"msg": "Trigger from client"})
            if ack['status'] == "sent":
                print("✅ Server confirmed broadcast trigger")
            else:
                print("❌ Server failed to trigger")

            # Cleanup
            listen_task.cancel()

    except Exception as e:
        print(f"{LogColors.FAIL}TEST SUITE ERROR: {e}{LogColors.ENDC}")
    finally:
        server_task.cancel()
        print(f"\n{LogColors.HEADER}=== TESTS FINISHED ==={LogColors.ENDC}")

if __name__ == "__main__":
    asyncio.run(run_integration_tests())