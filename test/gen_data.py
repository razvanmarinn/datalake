import requests
import random
import string
import json
from concurrent.futures import ThreadPoolExecutor

URL = 'http://localhost:8083/ingest/razvan1'
HEADERS = {'Content-Type': 'application/json', 'Authorization':  'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5YzkyOTk5Y2ViMWI5NTJkODBjNmY5MCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjU2Njc1NDMsInByb2plY3RzIjp7InJhenZhbiI6IjdmMjlkYzE3LTg5Y2EtNDliOC1iNzI3LTI4YjVhYzgwYjRkNiJ9LCJ1c2VybmFtZSI6InRlc3R4In0.t7WZ1hrHX6qPYNrLcW-gY1nzMzh1ctiQVMU9Gk97rIQNlx_Wt4_e1n7lOpsnJ8CiuV3e0yqtsUaxGVFszmRcUdne32F2hcY3kYt-0lN5JVEdstQ9GNdA0aVz-BHAZOUlAIG8QuuQYcsqMH9VSHiGsTfg0qljWF4aPhdL8guIwQwI52-TcL8J83SLk3uzRrnRLlyolwXpJuVAFPOJ7OOsZnQcMaXJjMCkN3-Zes0j5PVgPPA-QTLo5FC3Kx92N8UZZnMPsxr-x32t6gRyfn5vC2Ksf_jGyUPoRGt8cQVV6LxpEczfHArEX3HkAQpqhZL0e7GVU0TgiAsZut1e1Fk-j2IJBeYxWK-7IdGyCr6cK9invffesa4drqEunBbDD-tBgx4KKEVeWOsPfd0JIU5YSKLcnvYjyODLbATdvh02Ll5mW73Jv6owovvcLbxj1tHQz_6sx6ioEOr17Iu_edDONW8HxZ-YKzVEk0VeLQfSbDl8tljMi6Zx6XWOb0SIHUhO'}
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_int():
    return random.randint(1, 100000)

def send_request():
    payload = {
        "schema_name": "test",
        "data": {
            "test": random_int(),
        }
    }
    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        return response.status_code
    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_request) for _ in range(1_000)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            print(f"{i}: {result}")
