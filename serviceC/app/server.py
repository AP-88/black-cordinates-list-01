from fastapi import FastAPI, HTTPException
import connector as connector
import time
#import uvicorn

app = FastAPI()

# Internal Docker Compose Service Names
SvcA_url = "http://svc-a-cont:8080"
SvcB_url = "http://svc-b-cont:8080"
SvcC_url = "http://svc-c-cont:8080"

# Global tracker to manage the rate limit window across incoming requests to "ip-api" provider
LOCK_UNTIL_TIMESTAMP = 0.0

@app.get("/favicon.ico")
def favicon():
    print(f"START: favicon")
    return "ok"

@app.get("/lookup/{ip}")
async def lookup_ip(ip: str):
    print(f"srcC: Resolving via freeIpAPI, data for IP: {ip}")
    ip = connector.ValidateIp(ip)
    
    raw_response = await connector.MakeRequest(
        url=f"https://free.freeipapi.com/api/v1/json/{ip}",
        tout=5,
        svsName="FreeIpAPI"
    )
    
    picked_data = connector.FreeIpApiKeyPicker(raw_response.json())
    if not picked_data:
        raise HTTPException(status_code=404, detail=f"(srcC) IP address '{ip}' was validated, but no geolocation data could be found.")
    
    print(f"(srcC) free api worked: {picked_data}")   
    return picked_data

@app.get("/lookup-provider-two/{ip}")
async def lookup_ip_provider_two(ip: str):
    global LOCK_UNTIL_TIMESTAMP
    print(f"srcC: Resolving via ip-api, data for IP: {ip}")
    ip = connector.ValidateIp(ip)
    
    # Check if the rate-limiter has issued a proactive lockdown
    current_time = time.time()
    if current_time < LOCK_UNTIL_TIMESTAMP:
        seconds_remaining = round(LOCK_UNTIL_TIMESTAMP - current_time, 2)
        print(f"[Rate Limiter] Blocked proactively. Cooldown active for {seconds_remaining} seconds.")
        raise HTTPException(
            status_code=429, 
            detail=f"(srcC) Rate limit hit on ip-api. Cooldown active for {seconds_remaining} seconds."
        )
    
    raw_response = await connector.MakeRequest(
        url=f"http://ip-api.com/json/{ip}",
        tout=5,
        svsName="IP-API"
    )
    
    # Extract rate limiting data from headers
    x_rl = raw_response.headers.get("X-Rl")
    x_ttl = raw_response.headers.get("X-Ttl")
    
    print(f"(srcC) [IP-API] - IP: {ip} | X-Rl: {x_rl} | X-Ttl: {x_ttl}")
    
    # Evaluate if the window is exhausted and trigger a proactive lock
    if x_rl is not None and x_ttl is not None:
        try:
            if int(x_rl) == 0:
                seconds_to_reset = int(x_ttl)
                print(f"(srcC) [IP-API] X-Rl is 0! Locking outbound queries for {seconds_to_reset} seconds.")
                LOCK_UNTIL_TIMESTAMP = time.time() + seconds_to_reset
        except ValueError:
            pass
            
    picked_data = connector.IpApiKeyPicker(raw_response.json())
    if not picked_data:
        raise HTTPException(status_code=404, detail=f"(srcC) IP address '{ip}' was validated, but no geolocation data could be found.")
    
    print(f"(srcC) IP-API worked: {picked_data}")   
    return picked_data

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8081, workers=1)