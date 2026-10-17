import asyncio
import httpx
import sys

# Target Orchestrator URL pointing to Service A
TARGET_URL = "http://localhost:8081"  # Replace with your local mapped container port if different

# A list of diverse test cases
TEST_IPS = [
    "8.8.8.8",        # Standard valid public IP
    "1.1.1.1",        # Standard valid public IP
    "8.8.8.8",        # DUPLICATE: Verifies SvcA local caching interception logic
    "192.168.1.1",    # Private IP range (Verifies how your Pydantic / validation handles flags)
    "invalid-ip-str", # Corrupt payload string (Verifies HTTP 400 validation error handling)
    "208.67.222.222"  # Clean target candidate
]

async def run_pipeline_tests():
    print("🚀 [TEST RUNNER] Starting integration loop for Coordinates project...\n")
    
    async with httpx.AsyncClient() as client:
        # =========================================================
        # TEST 1: Verify Single IP Entry Deduplication Interception
        # =========================================================
        print("--------------------------------------------------")
        print("🔬 TEST 1: Single Resolution Path Check")
        print("--------------------------------------------------")
        for ip in ["8.8.8.8", "8.8.8.8"]:
            print(f"Sending tracking dispatch request for: {ip}...")
            try:
                response = await client.get(f"{TARGET_URL}/resolve-ip/{ip}", timeout=10.0)
                print(f"▶️ Status: {response.status_code} | Payload: {response.json()}\n")
            except Exception as e:
                print(f"❌ Target context request dropped: {str(e)}\n")

        # =========================================================
        # TEST 2: Run Full Batch Pipeline Execution
        # =========================================================
        print("--------------------------------------------------")
        print("🔬 TEST 2: Multi-IP Throttled Batch Pipeline Check")
        print("--------------------------------------------------")
        print(f"Submitting batch array: {TEST_IPS}")
        try:
            batch_response = await client.post(
                f"{TARGET_URL}/resolve-ip-list", 
                json=TEST_IPS, 
                timeout=25.0
            )
            print(f"▶️ Status: {batch_response.status_code}")
            print("📊 Metrics Report Output Summary:")
            print(batch_response.json().get("summary", {}))
            print("\nDetailed Processing Trail Results:")
            for item in batch_response.json().get("results", []):
                print(f" • IP: {item.get('ip')} -> Status: {item.get('status')} | Details: {item.get('error', item.get('message', 'Processed successfully'))}")
        except Exception as e:
            print(f"❌ Batch runner request failed: {str(e)}")

        # =========================================================
        # TEST 3: Verify Geographic Clustering Processing Engine
        # =========================================================
        print("\n--------------------------------------------------")
        print("🔬 TEST 3: Euclidean Regional Clustering Computation")
        print("--------------------------------------------------")
        try:
            cluster_response = await client.get(f"{TARGET_URL}/clusters?threshold=5.0", timeout=5.0)
            print(f"▶️ Status: {cluster_response.status_code}")
            print(f"📦 Computed Clusters Payload Structural Map: {list(cluster_response.json().keys())}")
        except Exception as e:
            print(f"❌ Clustering logic failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_pipeline_tests())
