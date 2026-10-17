import httpx
from pydantic import BaseModel, IPvAnyAddress
from fastapi import HTTPException

class ValidIpDTO(BaseModel):
    ip: IPvAnyAddress

def ValidateIp(ip: str):
    # Instantiate the Pydantic model to perform validation
    try: validated = ValidIpDTO(ip=ip)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    print("(src-c) validation passed: ", validated.ip)
    return validated.ip


async def MakeRequest(url, tout, svsName):
    async with httpx.AsyncClient() as client:
        try: 
            response = await client.get(url, timeout=tout)
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail=f"{svsName} timed out")

    # If the request fails, check if the response contains a detailed JSON error message
    if response.status_code != 200:
        try:
            error_data = response.json()
            error_detail = error_data.get("detail", f"Status code {response.status_code}")
        except Exception:
            error_detail = f"Status code {response.status_code}"
            
        # Match the status code and pass the real error detail
        raise HTTPException(status_code=response.status_code, detail=f"{svsName} failed: {error_detail}")
        
    return response

def FreeIpApiKeyPicker(js):
    # If js is None, or 'ipAddress' is missing, or 'ipAddress' is null/None
    if not js or "ipAddress" not in js or js["ipAddress"] is None:
        return {}
    
    ResultDict = {}
    OurKeys = {
        "ipAddress",
        "latitude",
        "longitude",
        "countryName",
        "cityName",
        "asn",
        "asnOrganization",
        "isProxy"
    }
    for k,v in js.items():
        if k in OurKeys: 
            ResultDict[k] = v
    return ResultDict

# mapping picker method for the ip-api fields
def IpApiKeyPicker(js):
    # If the provider returns a fail status
    if not js or js.get("status") == "fail":
        return {}
        
    # Maps fields to match your main architecture layout seamlessly
    return {
        "ipAddress": js.get("query"),
        "latitude": js.get("lat"),
        "longitude": js.get("lon"),
        "countryName": js.get("country"),
        "cityName": js.get("city"),
        "asn": js.get("as", "").split(" ")[0] if js.get("as") else None,
        "asnOrganization": " ".join(js.get("as", "").split(" ")[1:]) if js.get("as") else None,
        "isProxy": False # ip-api doesn't expose a proxy flag
    }