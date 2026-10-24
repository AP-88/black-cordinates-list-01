import asyncio
import json
import os
import httpx
from aiokafka import AIOKafkaConsumer

# Fetch runtime environment targets matching our docker configuration keys
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-gateway")
SERVICE_B_URL = os.getenv("SERVICE_B_URL", "http://svc-b-cont:8080")

async def start_consumer_pipeline():
    print(f"[Consumer App]: Connecting to stream broker listener at: {KAFKA_SERVERS}...")
    
    # Initialize the background stream subscriber
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="user-gateway-group",
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    print(f"[Consumer App]: Successfully subscribed to stream updates on topic: '{KAFKA_TOPIC}'")
    
    async with httpx.AsyncClient() as client:
        try:
            async for message in consumer:
                try:
                    # Deserialize incoming streaming data event packets
                    event_data = json.loads(message.value.decode('utf-8'))
                    email = event_data.get("email")
                    ip = event_data.get("ip_address")
                    
                    print(f"\n[Consumer Event]: Intercepted sign-in trace packet for user email: {email}")
                    
                    # Call Service B to verify if the account entry exists in memory
                    check_response = await client.get(f"{SERVICE_B_URL}/check-user/{email}", timeout=5.0)
                    
                    if check_response.status_code == 200 and check_response.json().get("exists"):
                        print(f"[Consumer Logic]: User '{email}' exists in memory database profile. Sign-in case processed")
                    else:
                        print(f"[Consumer Logic]: User profile not found. Initializing SignUp record write task")
                        
                        # Write the fresh account profile to storage via Service B
                        signup_payload = {
                            "email": email,
                            "ip_address": ip
                        }
                        save_response = await client.post(f"{SERVICE_B_URL}/add-user", json=signup_payload, timeout=5.0)
                        
                        if save_response.status_code == 200:
                            print(f"[Consumer Success]: Registered account ID: {save_response.json().get('id')}")
                        else:
                            print(f"[Consumer Error]: Failed to save user details to database: {save_response.text}")
                            
                except json.JSONDecodeError:
                    print("[Consumer Error]: Encountered broken or unparseable text format packet stream.")
                except Exception as loop_err:
                    print(f"[Consumer Loop Exception Fault]: {str(loop_err)}")
        finally:
            await consumer.stop()

if __name__ == "__main__":
    asyncio.run(start_consumer_pipeline())
