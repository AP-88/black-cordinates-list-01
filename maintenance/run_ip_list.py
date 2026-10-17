import json
import requests

with open('ips.json', 'r') as file:
    ip_list = json.load(file)

url = "http://localhost:8081/resolve-ip-list"

response = requests.post(url, json=ip_list)

print(f"Status Code: {response.status_code}")
print("Response:", response.json())
