from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body
from memory import IpMemory, UserMemory

@asynccontextmanager
async def lifespan(app: FastAPI):
   # Initializes both JSON files inside the shared Docker Named Volume folder path
    IpMemory.initialize_storage()
    UserMemory.initialize_storage()
    print(f"(svcB): Multi-file storage validation completed successfully.")
    yield

# Init FastAPI with the lifespan event handler
app = FastAPI(lifespan=lifespan)

# Internal Docker Compose Service Names
SvcA_url = "http://svc-a-cont:8080"
SvcB_url = "http://svc-b-cont:8080"
SvcC_url = "http://svc-c-cont:8080"

@app.get("/check-ip/{ip}")
async def check_ip_existence(ip: str):
    # check IPs against the storage file
    exists = await IpMemory.doesIpExist(ip)
    return {"exists": exists}

@app.get("/getAll")
async def get_all():
    print(f"(svcB): Fetching all storage metrics")
    return await IpMemory.getData()

@app.post("/AddIp")
async def add_ip(payload: dict):
    ip_addr = payload.get("ipAddress")
    print(f"(svcB): Attempting to save record for IP: {ip_addr}")
    
    ip_exists = await IpMemory.doesIpExist(ip_addr)
    if ip_exists:
        raise HTTPException(status_code=400, detail="IP tracking entry already exists.")
        
    assigned_id = await IpMemory.addIpAndGeoData(payload)
    return {"status": "success", "id": assigned_id}


@app.delete("/delete/{iid}")
async def delete_ip_record(iid: str):
    print(f"(svcB): Processing deletion request for Key: {iid}")
    
    # Verify key identity exists before execution
    key_exists = await IpMemory.doesKeyExist(iid)
    if not key_exists:
        raise HTTPException(status_code=404, detail="Requested ID key not found.")
        
    await IpMemory.deleteKeyValuePair(iid)
    return {"status": "deleted", "id": iid}

@app.get("/generate-geo-clusters")
async def generate_clusters(threshold: float = 5.0):
    return await IpMemory.generate_geo_clusters(threshold=threshold)

######################################
# USER UTILITIES FOR KAFKA CONSUMER
######################################
@app.get("/check-user/{email}")
async def check_user_existence(email: str):
    # Allows the Kafka Consumer to check if a user is already signed up
    exists = await UserMemory.doesUserExist(email)
    return {"exists": exists}

@app.post("/add-user")
async def add_user_record(payload: dict = Body(...)):
    # Allows the Kafka Consumer to save a new verified user account
    email = payload.get("email")
    user_exists = await UserMemory.doesUserExist(email)
    
    if user_exists:
        raise HTTPException(status_code=400, detail="User already exists inside storage parameters.")
        
    assigned_id = await UserMemory.registerNewUser(payload)
    return {"status": "success", "id": assigned_id}

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8082, workers=1)