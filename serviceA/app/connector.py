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

    print("(svcA) validation passed: ", validated.ip)
    return validated.ip

async def MakeRequest(url, tout, svsName, meth='GET', JsonData=None):
    async with httpx.AsyncClient() as client:
        HttpMethod, kwargs = client.get, {"timeout": tout}
        if meth == 'POST':
            HttpMethod, kwargs['json'] = client.post, JsonData
        if meth == 'DELETE': HttpMethod = client.delete

        try: response = await HttpMethod(url, **kwargs)
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail=f"{svsName} timed out")

    # If the request fails, check if the response contains a detailed JSON error message
    if response.status_code != 200:
        try:
            error_data = response.json()
            error_detail = error_data.get("detail", f"Status code {response.status_code}")
        except Exception:
            error_detail = f"Status code {response.status_code}"
            
        # Match the status code from Service B (e.g., 400) and pass the real error detail
        raise HTTPException(status_code=response.status_code, detail=f"{svsName} failed: {error_detail}")
        
    return response.json()



def FreeIpApiKeyPicker(js):
    # FIX: If js is None, or 'ipAddress' is missing, or 'ipAddress' is null/None
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