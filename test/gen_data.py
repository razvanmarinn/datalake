import json
import os
import random
import string
from concurrent.futures import ThreadPoolExecutor

import requests

URL = "http://localhost:8083/ingest/yes"
JWT = os.getenv("JWT")
HEADERS = {"Content-Type": "application/json", "Authorization": JWT}


def random_string(length=10):
    return "".join(random.choices(string.ascii_letters, k=length))


def random_int():
    return random.randint(1, 100000)


def send_request():
    payload = {
        "schema_name": "hhh",
        "data": {
            "test": random_int(),
        },
    }
    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        return response.status_code
    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(send_request) for _ in range(1_000)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            print(f"{i}: {result}")
