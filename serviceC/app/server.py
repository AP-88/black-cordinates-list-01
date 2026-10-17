from fastapi import FastAPI, HTTPException
import connector as connector
import time
import httpx
#import uvicorn

app = FastAPI()

# Internal Docker Compose Service Names
SvcA_url = "http://svc-a-cont:8080"
SvcB_url = "http://svc-b-cont:8080"
SvcC_url = "http://svc-c-cont:8080"

# Global tracker to manage the rate limit lock window for provider two "ip-api"
LOCK_PROVIDER_TWO_UNTIL = 0.0

@app.get("/favicon.ico")
def favicon():
    print(f"(srcC) START: favicon")
    return "ok"

@app.get("/lookup/{ip}")
async def lookup_ip_sequential_failover(ip: str):
    # Orchestrated failover endpoint, queries (FreeIpAPI) with fast timeout. 
    # falls back to (ip-api) only if needed
    
    global LOCK_PROVIDER_TWO_UNTIL
    print(f"(srcC): Initializing failover resolution for IP: {ip}")
    ip = connector.ValidateIp(ip)

    async with httpx.AsyncClient() as client:
        # STEP 1: EXECUTE PRIMARY LOOKUP ATTEMPT (FreeIpAPI)
        # ==================================================
        try:
            print("(srcC) [Try 1]: Querying FreeIpAPI provider")
            url_one = f"https://free.freeipapi.com/api/v1/json/{ip}"
            
            # Fast execution at 1.5 seconds
            response = await client.get(url_one, timeout=1.5)
            
            if response.status_code == 200:
                data = connector.FreeIpApiKeyPicker(response.json())
                if data:
                    print("(srcC): Success using FreeIpAPI provider")
                    return data
            else:
                print(f"[FreeIpAPI 1 Warning]: Returned HTTP status code {response.status_code}")

        except (httpx.TimeoutException, httpx.NetworkError) as err:
            print(f"[FreeIpAPI Warning]: Network issue or timeout encountered ({str(err)}).")
        except Exception as exp:
            print(f"[FreeIpAPI Warning]: Internal script exception ({str(exp)}).")

        # STEP 2: FALLBACK TRANSITION LOOP TO ip-api
        # ==========================================
        print("(srcC): FreeIpAPI failed or timed out. starting fallback to ip-api")
        
        # check if ip-api is in cooling-down lock window
        current_time = time.time()
        if current_time < LOCK_PROVIDER_TWO_UNTIL:
            seconds_remaining = round(LOCK_PROVIDER_TWO_UNTIL - current_time, 2)
            print(f"(srcC): [Rate Limiter]: Fallback aborted. ip-api locked for another {seconds_remaining}s.")
            raise HTTPException(
                status_code=503, 
                detail=f"(srcC) Service Unavailable: FreeIpAPI failed and ip-api is cooling down ({seconds_remaining}s remaining)."
            )

        try:
            print("(srcC) [Try 2]: Querying ip-api")
            url_two = f"http://ip-api.com/json/{ip}"
            response = await client.get(url_two, timeout=4.0)
            
            # Extract rate limit from ip-api
            x_rl = response.headers.get("X-Rl")
            x_ttl = response.headers.get("X-Ttl")
            
            # REQUIRED PRINT LOGGING STATEMENT OUTPUTS
            print(f"(srcC) [ip-api] - IP: {ip} | x-R1: {x_rl} | x-Ttl: {x_ttl}")
            
            # Evaluate if hit the limits and trigger proactive blocks
            if x_rl is not None and x_ttl is not None:
                try:
                    if int(x_rl) == 0:
                        seconds_to_block = int(x_ttl)
                        print(f"(srcC) [Rate Limiter] X-Rl is 0! Locking fallback queries for {seconds_to_block} seconds.")
                        LOCK_PROVIDER_TWO_UNTIL = time.time() + seconds_to_block
                except ValueError:
                    pass

            # handling of HTTP errors from ip-api
            if response.status_code == 429:
                LOCK_PROVIDER_TWO_UNTIL = time.time() + 60.0  # Safe safety cooling lock
                raise HTTPException(status_code=429, detail="(srcC): ip-api rate limit exception (HTTP 429)")
                
            if response.status_code in (401, 403):
                raise HTTPException(status_code=response.status_code, detail="(srcC): ip-api restriction conflict")

            if response.status_code == 200:
                data = connector.IpApiKeyPicker(response.json())
                if data:
                    print("(srcC): Success using fallback ip-api")
                    return data

        except (httpx.TimeoutException, httpx.NetworkError) as err:
            raise HTTPException(status_code=503, detail=f"(srcC) ip-api connection timed out: {str(err)}")
        except HTTPException as http_exc:
            raise http_exc
        except Exception as exp:
            raise HTTPException(status_code=500, detail=f"(srcC) ip-api internal server error: {str(exp)}")

        # STEP 3: ULTIMATE COMPILATION FAILURE ESCALATION
        # ===============================================
        raise HTTPException(
            status_code=500, 
            detail="(srcC) Pipeline error: Both geolocation modules failed or hit active limit blocks."
        )

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8081, workers=1)