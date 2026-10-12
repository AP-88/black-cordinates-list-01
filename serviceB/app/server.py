from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from memory import IpMemory

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. This runs BEFORE the server starts accepting any web traffic
    IpMemory.initialize_storage()
    print(f"Service B: Storage path validation completed successfully.")
    yield
    # 2. Any shutdown/cleanup operations would go here

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
    print(f"svcB: Fetching all storage metrics")
    return await IpMemory.getData()

@app.post("/AddIp")
async def add_ip(payload: dict):
    ip_addr = payload.get("ipAddress")
    print(f"svcB: Attempting to save record for IP: {ip_addr}")
    
    ip_exists = await IpMemory.doesIpExist(ip_addr)
    if ip_exists:
        raise HTTPException(status_code=400, detail="IP tracking entry already exists.")
        
    assigned_id = await IpMemory.addIpAndGeoData(payload)
    return {"status": "success", "id": assigned_id}


@app.delete("/delete/{iid}")
async def delete_ip_record(iid: str):
    print(f"svcB: Processing deletion request for Key: {iid}")
    
    # Verify key identity exists before execution
    key_exists = await IpMemory.doesKeyExist(iid)
    if not key_exists:
        raise HTTPException(status_code=404, detail="Requested ID key not found.")
        
    await IpMemory.deleteKeyValuePair(iid)
    return {"status": "deleted", "id": iid}

@app.get("/generate-geo-clusters")
async def generate_clusters(threshold: float = 5.0):
    return await IpMemory.generate_geo_clusters(threshold=threshold)

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8082, workers=1)