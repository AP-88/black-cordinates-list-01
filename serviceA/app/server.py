from fastapi import FastAPI, HTTPException, Request
import connector as connector
import asyncio
from pydantic import BaseModel, RootModel, EmailStr, IPvAnyAddress, Field
from typing import List, Optional
from aiokafka import AIOKafkaProducer
import time
import json
#import uvicorn

app = FastAPI()

# Internal Docker Compose Service Names
SvcA_url = "http://svc-a-cont:8080"
SvcB_url = "http://svc-b-cont:8080"
SvcC_url = "http://svc-c-cont:8080"

# window timestamp tracker to protect FreeIpAPI (Max 60 requests / 60 seconds)
FREEIPAPI_LOOKUP_TIMESTAMPS = []
FREEIPAPI_MAX_LIMIT = 59  # Set  below 60 for a safe padding

# Kafka Integration
KAFKA_BROKER_URL = "kafka-broker:9092"
KAFKA_TOPIC = "user-gateway"

# list to track client sign-in IP timestamps over 60 seconds
RECENT_SIGNIN_IPS = []

class UserSignInDTO(BaseModel):
    email: EmailStr
    password: Optional[str] = Field(default=None)
    ip_address: Optional[IPvAnyAddress] = Field(default=None, alias="ip-address")

    class Config:
        populate_by_name = True  # Allows FastAPI to parse inputs whether using camelCase, snake_case, or aliases

@app.get("/favicon.ico")
def favicon():
    print(f"(svcA) START: favicon")
    return "ok"

@app.get("/")
async def GetData():
    print(f"(svcA) START: Get Data")
    return await connector.MakeRequest(f"{SvcB_url}/getAll", 2, "ServiceB-GetAll")

@app.get("/resolve-ip/{ip}")
async def AddIp(ip: str):
    global FREEIPAPI_LOOKUP_TIMESTAMPS
    print(f"(svcA): Initializing check loop for IP {ip}")
    ip = connector.ValidateIp(ip)
    
    # Check if the IP address is already tracked in local storage.json
    check_res = await connector.MakeRequest(
        f"{SvcB_url}/check-ip/{ip}", 3, f"ServiceB-Check-{ip}"
    )
    # If it exists in local storage.json, return it
    if check_res.get("exists"):
        print(f"(svcA): IP {ip} already exists in storage. not sending a provider request.")
        raise HTTPException(status_code=400, detail="IP entry already exists in local storage.")
    
    # RATE LIMIT COUNTER
    current_time = time.time()
    one_minute_ago = current_time - 60.0
    
    # Clean old timestamps from the tracking list that are older than 1 minute
    FREEIPAPI_LOOKUP_TIMESTAMPS = [t for t in FREEIPAPI_LOOKUP_TIMESTAMPS if t > one_minute_ago]

    # Evaluate if 1-minute window is currently full
    if len(FREEIPAPI_LOOKUP_TIMESTAMPS) >= FREEIPAPI_MAX_LIMIT:
        # Calculate how long the client needs to wait until a slot opens up
        oldest_request_age = FREEIPAPI_LOOKUP_TIMESTAMPS[0]
        wait_seconds = round(60.0 - (current_time - oldest_request_age), 1)
        
        print(f"(svcA) [Gatekeeper Block]: FreeIpAPI limit cushion hit ({len(FREEIPAPI_LOOKUP_TIMESTAMPS)} lookups in last 60s).")
        raise HTTPException(
            status_code=429, 
            detail=f"(svcA) Too Many Requests: Single lookup threshold reached, please wait {wait_seconds} seconds."
        )

    # Log the timestamp of this upcoming live external request
    FREEIPAPI_LOOKUP_TIMESTAMPS.append(current_time)
    
    print(f"(svcA): Unique IP confirmed, window count: {len(FREEIPAPI_LOOKUP_TIMESTAMPS)}/60s. asking for geo-data via SvcC")
    geo_data = await connector.MakeRequest(
        f"{SvcC_url}/lookup/{ip}", 5, "ServiceC-Lookup"
    )
    
    # add geo-data to svcB to store it
    return await connector.MakeRequest(
        f"{SvcB_url}/AddIp", 8, "ServiceB-AddIp", meth='POST', JsonData=geo_data
    )
    
@app.get("/delete/{iid}")
async def delete(iid):
    print(f"(svcA) START: delete {iid}")
    return await connector.MakeRequest(
        SvcB_url + f"/delete/{iid}", 2, "ServiceB-Delete", meth='DELETE'
    )

# Explictly maps an incoming plain JSON array: ["1.1.1.1", "2.2.2.2"]
class IpListRequest(RootModel):
    root: List[str]

@app.post("/resolve-ip-list")
async def resolve_ip_list(payload: IpListRequest):
    raw_list = payload.root
    # Clean whitespace and drop identical duplicates from the input list
    cleaned_unique_input = list(dict.fromkeys([ip.strip() for ip in raw_list if ip.strip()]))
    print(f"(svcA): Starting throttled batch pipeline for {len(cleaned_unique_input)} unique inputs.")
    
    final_results = []
    lookup_counter = 0  # Tracks live API lookups to determine if we need to sleep due to FreeIpAPI limitation

    for ip in cleaned_unique_input:
        try:
            # DUPLICATION CHECK: Query local storage.json first
            check_res = await connector.MakeRequest(f"{SvcB_url}/check-ip/{ip}", 3, f"ServiceB-Check-{ip}")
            
            if check_res.get("exists"):
                final_results.append({
                    "ip": ip,
                    "status": "skipped",
                    "message": "IP address already exists in storage."
                })
                continue  # proceed to the next IP with no delay
            # LIMIT THROTTLING: If this isn't the first live lookup, pause briefly
            if lookup_counter > 0:
                print("(svcA): Throttling control active. Sleeping for 1.1 seconds due to freeIpAPI limits...")
                await asyncio.sleep(1.1)  # wait 1.1 sec to stay under 60 requests/min
            # Execute the external lookup in FreeIpAPI via svcC
            lookup_counter += 1
            print(f"(svcA) Executing external lookup #{lookup_counter} for IP: {ip}")
            geo_data = await connector.MakeRequest(
                f"{SvcC_url}/lookup/{ip}", 5, f"ServiceC-Lookup-{ip}"
            )
            # SAVE: Write the fresh geo-data to disk via svcB
            save_response = await connector.MakeRequest(
                f"{SvcB_url}/AddIp", 8, f"ServiceB-AddIp-{ip}", meth='POST', JsonData=geo_data
            )
            final_results.append({
                "ip": ip,
                "status": "success",
                "id": save_response.get("id")
            })

        except HTTPException as e:
            final_results.append({
                "ip": ip,
                "status": "failed",
                "error": e.detail
            })
        except Exception as e:
            final_results.append({
                "ip": ip,
                "status": "failed",
                "error": f"Pipeline exception: {str(e)}"
            })
    # Create the final metrics report
    return {
        "summary": {
            "total_submitted": len(raw_list),
            "unique_processed": len(cleaned_unique_input),
            "live_external_lookups_made": lookup_counter,
            "successfully_added": len([r for r in final_results if r["status"] == "success"]),
            "skipped_duplicates": len([r for r in final_results if r["status"] == "skipped"]),
            "failed_validation": len([r for r in final_results if r["status"] == "failed"])
        },
        "results": final_results
    }

@app.get("/clusters")
async def get_clusters(threshold: float = 5.0):
    return await connector.MakeRequest(f"{SvcB_url}/generate-geo-clusters?threshold={threshold}", 5, "ServiceB-Clusters")

# SIGN-IN ENDPOINT WITH AUTOMATIC IP FALLBACK
@app.post("/signin")
async def user_signin(payload: UserSignInDTO, request: Request):
    # Handles user sign-in. prioritizes a user-provided IP argument, 
    # falling back to the client auto-detection IP if missing.
    global RECENT_SIGNIN_IPS
    print(f"(svcA) START: Processing sign-in validation payload for email: {payload.email}")

    # check if a valid IP was supplied by the user
    if payload.ip_address is not None:
        client_ip = str(payload.ip_address)
        print(f"(svcA): Using provided payload IP address: {client_ip}")
    else:
        # Fallback to reading client IP automatically
        # Check standard reverse proxy headers first (Nginx, Traefik, Docker gateway)
        client_ip = request.headers.get("X-Forwarded-For")
        if client_ip:
            # Grab the first entry in case a chain of proxies exists
            client_ip = client_ip.split(",")[0].strip()
        else:
            # Native direct fallback client lookup reference
            client_ip = request.client.host if request.client else "127.0.0.1"
            
        print(f"(svcA): IP omitted by user. Automatically grabbed network client IP: {client_ip}")

    # Run a quick IP validation to make sure auto-grabbed IPs are valid strings
    try:
        client_ip = str(connector.ValidateIp(client_ip))
    except Exception:
        raise HTTPException(status_code=400, detail="(svcA): Unable to recognise a valid IP address.")

    current_time = time.time()
    one_minute_ago = current_time - 60.0

    # Unpack the new tuple (timestamp, ip) to filter out logs older than 60 seconds
    RECENT_SIGNIN_IPS = [item for item in RECENT_SIGNIN_IPS if item[0] > one_minute_ago]

    # Evaluate if this IP string exists inside the tuple records
    duplicate_ip_detected = any(item[1] == client_ip for item in RECENT_SIGNIN_IPS)

    if duplicate_ip_detected:
        print(f"(svcA) [SignIn Filter]: Duplicate IP '{client_ip}' seen within 60s window. Event dropped.")
        # Returns a clean, explicit sign-in flag to identify duplicate interception events
        return {
            "message": "your request was received",
            "signin": "duplicate"
        }

    # Save the current execution timestamp and the string signature
    RECENT_SIGNIN_IPS.append((current_time, client_ip))
    
    # Broadcast event down to Apache Kafka
    event_payload = {
        "event_type": "USER_SIGNIN",
        "timestamp": current_time,
        "email": payload.email,
        "ip_address": client_ip,
        "has_password": payload.password is not None
    }

    try:
        print(f"(svcA) [Kafka Production]: Sending event down to topic '{KAFKA_TOPIC}'...")
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)
        await producer.start()
        try:
            serialized_msg = json.dumps(event_payload).encode('utf-8')
            await producer.send_and_wait(KAFKA_TOPIC, serialized_msg)
            print("(svcA) [Kafka Production]: Event broadcasted successfully.")
        finally:
            await producer.stop()
    except Exception as e:
        print(f"(svcA): [Kafka Connection Warning]: Event stream offline, message packet dropped: {str(e)}")

    return {"message": "your request was received"}

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8081, workers=1)