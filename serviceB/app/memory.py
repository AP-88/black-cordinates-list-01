from uuid import uuid4
import asyncio
import os
import json
import aiofiles
import math
import statistics  # Leverage standard library for precise median math

class IpMemory:
    FILE_PATH = "/app/data/storage.json"
    memLock = asyncio.Lock()
    
    @classmethod
    def initialize_storage(cls):
       
        # Synchronous initializer to run on app startup 
        # to ensures directories and baseline files exist with proper structures.
        dir_name = os.path.dirname(cls.FILE_PATH)
        
        # Create directories recursively if they don't exist
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
            print(f"(srcB) Created shared volume directory path: {dir_name}")

        # Initialize the storage schema file if missing or empty
        if not os.path.exists(cls.FILE_PATH) or os.path.getsize(cls.FILE_PATH) == 0:
            with open(cls.FILE_PATH, 'w') as f:
                json.dump({"data": {}, "UsedIps": []}, f)
            print(f"(srcB) Initialized clean storage file template at: {cls.FILE_PATH}")

    @classmethod
    async def _load_file(cls) -> dict:
        # Helper to read the JSON file from disk
        try:
            async with aiofiles.open(cls.FILE_PATH, mode='r') as f:
                content = await f.read()
                if not content.strip():
                    return {"data": {}, "UsedIps": []}
                return json.loads(content)
        except (json.JSONDecodeError, IOError):
            return {"data": {}, "UsedIps": []}

    @classmethod
    async def _save_file(cls, storage_dict: dict):
        # Helper to write the dictionary state back to disk
        async with aiofiles.open(cls.FILE_PATH, mode='w') as f:
            await f.write(json.dumps(storage_dict))
    
    
    @classmethod
    async def aquireTheLock(cls, func, *args):
        async with cls.memLock:
            value = await func(*args)
        return value

    @classmethod
    async def getData(cls):
        async def _get():
            state = await cls._load_file()
            return state["data"]
        return await cls.aquireTheLock(_get)

    @classmethod
    async def lengthofData(cls):
        async def _length():
            state = await cls._load_file()
            return len(state["data"])
        return await cls.aquireTheLock(_length)
    
    @classmethod
    async def doesKeyExist(cls, key):
        async def _exists():
            state = await cls._load_file()
            return key in state["data"]
        return await cls.aquireTheLock(_exists)

    @classmethod
    async def doesIpExist(cls, ip):
        # checks if ip exists in memory and returns boolean
        async def _ip_exists():
            state = await cls._load_file()
            return ip in state["UsedIps"]
        return await cls.aquireTheLock(_ip_exists)

    @classmethod
    async def addIpAndGeoData(cls, requestData):
        async def _add():
            state = await cls._load_file()
            newId = str(uuid4())[:8]
            state["data"][newId] = requestData
            
            if requestData["ipAddress"] not in state["UsedIps"]:
                state["UsedIps"].append(requestData["ipAddress"])
                
            await cls._save_file(state)
            return newId
        return await cls.aquireTheLock(_add)
    
    @classmethod
    async def deleteKeyValuePair(cls, key):
        async def _delete():
            state = await cls._load_file()
            if key in state["data"]:
                # Get the IP address associated with this specific ID key
                associated_ip = state["data"][key].get("ipAddress")
                # If that IP exists inside the checklist array, remove it from UsedIps
                if associated_ip in state["UsedIps"]:
                    state["UsedIps"].remove(associated_ip)
                # Delete the tracking info entry from data
                del state["data"][key]
                await cls._save_file(state)
            return True
        return await cls.aquireTheLock(_delete)
    
    @classmethod
    async def generate_geo_clusters(cls, threshold=5.0):
        # Ultra-short geographic clustering using Euclidean grouping
        state = await cls._load_file()
        stored_data = state.get("data", {})
        clusters = {}

        for rec_id, rec in stored_data.items():
            lat, lon = rec.get("latitude"), rec.get("longitude")
            if lat is None or lon is None: 
                continue
            
            coord = [float(lat), float(lon)]
            matched_cluster = None

            # Look for an existing cluster that is mathematically close
            for c_id, c_data in clusters.items():
                if math.dist(coord, c_data["mean_coordinates"]) <= threshold:
                    matched_cluster = c_id
                    break

            if matched_cluster:
                clusters[matched_cluster]["all_coords"].append(coord)
                clusters[matched_cluster]["records"][rec_id] = rec
                # Short array re-calculation for means and medians
                all_pts = clusters[matched_cluster]["all_coords"]
                
                # Dynamic mean updates
                clusters[matched_cluster]["mean_coordinates"] = [round(sum(x)/len(all_pts), 5) for x in zip(*all_pts)]
                clusters[matched_cluster]["median_coordinates"] = [round(statistics.median(x), 5) for x in zip(*all_pts)]
            else:
                # Create a fresh cluster instantly
                c_name = f"cluster_{len(clusters) + 1}"
                clusters[c_name] = {
                    "mean_coordinates": coord,
                    "median_coordinates": coord,
                    "records": {rec_id: rec},
                    "all_coords": [coord]
                }

        # Flatten records to match the requested output structure exactly
        return {
            c_id: {
                "median_coordinates": c_data["median_coordinates"],
                "mean_coordinates": c_data["mean_coordinates"],
                **c_data["records"] # Unpacks all ip_record_id: data records directly into the top level
            } for c_id, c_data in clusters.items()
        }
