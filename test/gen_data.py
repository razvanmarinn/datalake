import requests
import random
import string
import json
from concurrent.futures import ThreadPoolExecutor

URL = 'http://localhost:8083/ingest/razvan'
HEADERS = {'Content-Type': 'application/json', 'Authorization':  'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5YzkyOTk5Y2ViMWI5NTJkODBjNmY5MCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTYyNDQwODMsInByb2plY3RzIjp7InJhenZhbiI6ImFkOGY0NDRiLWQxMDQtNDYwYS1hN2RkLTlhOTNhMTZkZGZiNCIsInRlc3QxIjoiYWQ4ZjQ0NGItZDEwNC00NjBhLWE3ZGQtOWE5M2ExNmRkZmI0In0sInVzZXJuYW1lIjoidGVzdCJ9.QEy51aC_OmP9aViVKnxbV5Tbln4p1RlUbl-75e5BBK9MDAaxvJPhC7fxVpg3bwgZznRX7DnJvAn-MpAUmtZhQTXkzwEylk6MBdhZBNDV9S0RBNpOj70hcPBEzqxw4NQ4-5Fyzp9j68od_q_2xbwztPeXHbARVpjMzbt3Elr8TH_hNstpUuIqZx4KKebTnZ-uZskv3J2YpSnEMJec69fDxqc0OfcGDvo-9Ni1_oVDAjxSPOvLqvIap_CSzDoSZ6YkiHLKL0x6YzMMxFIi45LKXuLC_6zY8gZCA9HPyRYsI4TKRtcQGRIVxgIC2hy5Ro124hJAyfvCr89x8Zyv7DDifkhuWwoaYYdsedhCg-PZpBWXbbnuuL5jnyukPk-lhm2Xx_Qswax2YtKZQ1sWpRYPINnxPVfp-kb7aRMFF1sdSM5RSigBZHA5pINeJkI3f5FSvgqbAnOwOjyI6ZT6aW0_QXdhBOSkK2d7fYb_U8EkE_psBvlI5GrLGXhYDHk1Rp8a'}

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
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(send_request) for _ in range(30000)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            print(f"{i}: {result}")
