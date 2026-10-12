from fastapi import FastAPI, HTTPException
import connector as connector
import asyncio
from pydantic import RootModel
from typing import List
#import uvicorn

app = FastAPI()

# Internal Docker Compose Service Names
SvcA_url = "http://svc-a-cont:8080"
SvcB_url = "http://svc-b-cont:8080"
SvcC_url = "http://svc-c-cont:8080"

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
    print(f"svcA: Routing IP {ip} through SvcC")
    ip = connector.ValidateIp(ip)

    # Get the geo-data from the svcC
    geo_data = await connector.MakeRequest(
        f"{SvcC_url}/lookup/{ip}", 5, "ServiceC-Lookup"
    )
    # Add verified geo-data down to svcB to store it
    return await connector.MakeRequest(
        f"{SvcB_url}/AddIp", 8, "ServiceB-AddIp", meth='POST', JsonData=geo_data
    )
    
@app.get("/delete/{iid}")
async def delete(iid):
    print(f"START: delete {iid}")
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
    print(f"SvcA: Starting throttled batch pipeline for {len(cleaned_unique_input)} unique inputs.")
    
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
                print("svcA: Throttling control active. Sleeping for 1.1 seconds due to freeIpAPI limits...")
                await asyncio.sleep(1.1)  # wait 1.1 sec to stay under 60 requests/min
            # Execute the external lookup in FreeIpAPI via svcC
            lookup_counter += 1
            print(f"ServiceA: Executing external lookup #{lookup_counter} for IP: {ip}")
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

#if __name__ == "__main__":
    #uvicorn.run("server:app", host="0.0.0.0", port=8081, workers=1)