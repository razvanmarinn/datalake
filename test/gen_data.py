import requests
import random
import string
import json
from concurrent.futures import ThreadPoolExecutor

URL = 'http://localhost:8083/ingest/razvan'
HEADERS = {'Content-Type': 'application/json', 'Authorization':  'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5YzkyOTk5Y2ViMWI5NTJkODBjNmY5MCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTkyNjkwNDAsInByb2plY3RzIjp7InJhenZhbiI6IjY0MDY3ZGQwLTU0NTktNDY1Yi05ZTQyLTY4Y2E4Njg1NTA2YSJ9LCJ1c2VybmFtZSI6InRlc3R4In0.oFsN80vU_j5BffuFKXKalIjBjl19QPLLtuWGlkw45cf19emi-EhhodUnebOptmb5TQ7FUbOVjTE7MhD3nBMzDUHiUWlw6F8Uu00pbEHfvk1TpAUESBsEh-4aUEoMWglTnzja2fxNCKE6jeh5PPwYaHrDxFv-loGuk7rQ30bGw_ZYlg_liuChU9johMIQqN47M3KtOsiLfMLRuVgUw0AUMM-RlAXF9GlyRTZu2AOjWzu6Ns7lDIn-N4gB5WsaifNrXJZtW8wx_M5jSn6mt6FqeiLU9x1te6i6ATqSQMuwl4B9U7MDembqgxClZmc3te3V55hhlJhDE091rODK4bYHazY-hlSu-0gl1quim4tRXJyU8n3uMSIT9GV4rPDpbaoLkQzk0ZzAUqR7EbFiJIVsp79im8LGypgB2dkCARKeTo9gXvPxCjAf-oVnefK16VhjTI7S4f4QzjyI92DSZKBMJ2hTtOyKWRKEic6CB7QE6NwOmmTJPR-LdMGdZLYZaV8P' }
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
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(send_request) for _ in range(1_000_000)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            print(f"{i}: {result}")
