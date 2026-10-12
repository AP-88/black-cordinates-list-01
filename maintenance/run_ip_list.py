import json
import requests

# Open and load the JSON array from your file
with open('ips.json', 'r') as file:
    ip_list = json.load(file)

url = "http://localhost:8081/resolve-ip-list"

# Send the POST request
response = requests.post(url, json=ip_list)

# Check the results
print(f"Status Code: {response.status_code}")
print("Response:", response.json())
