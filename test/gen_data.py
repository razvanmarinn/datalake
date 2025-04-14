import requests
import random
import string
import json
from concurrent.futures import ThreadPoolExecutor

URL = 'http://localhost:8080/test1/ingest'
HEADERS = {'Content-Type': 'application/json'}

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_int():
    return random.randint(1, 100000)

def send_request():
    payload = {
        "schema_name": "test1",
        "data": {
            "test1": random_string(),
            "test2": random_int()
        }
    }
    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        return response.status_code
    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(send_request) for _ in range(30000)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            print(f"{i}: {result}")
