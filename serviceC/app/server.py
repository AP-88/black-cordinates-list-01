from fastapi import FastAPI, HTTPException
import connector as connector
#import uvicorn

app = FastAPI()

# Internal Docker Compose Service Names
SvcA_url = "http://svc-a-cont:8080"
SvcB_url = "http://svc-b-cont:8080"
SvcC_url = "http://svc-c-cont:8080"

@app.get("/favicon.ico")
def favicon():
    print(f"START: favicon")
    return "ok"

@app.get("/lookup/{ip}")
async def lookup_ip(ip: str):
    print(f"srcC: Resolving geolocation data for IP: {ip}")
    ip = connector.ValidateIp(ip)
    
    raw_response = await connector.MakeRequest(
        url=f"https://free.freeipapi.com/api/v1/json/{ip}",
        tout=5,
        svsName="FreeIpAPI"
    )
    
    picked_data = connector.FreeIpApiKeyPicker(raw_response)
    if not picked_data:
        raise HTTPException(status_code=404, detail=f"(srcC) IP address '{ip}' was validated, but no geolocation data could be found.")
    
    print(f"(srcC) free api worked: {picked_data}")   
    return picked_data

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8081, workers=1)