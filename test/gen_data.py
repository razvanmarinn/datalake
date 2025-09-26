import requests
import random
import string
import json
from concurrent.futures import ThreadPoolExecutor

URL = 'http://localhost:8083/ingest/razvan'
HEADERS = {'Content-Type': 'application/json', 'Authorization':  'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5YzkyOTk5Y2ViMWI5NTJkODBjNmY5MCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTg4OTY2NzYsInByb2plY3RzIjp7InJhenZhbiI6IjEzNWY2MDhhLWVlNzgtNDUyMC05OWE2LWJhMmRjN2ZiM2Y3NyJ9LCJ1c2VybmFtZSI6InRlc3QifQ.iaf8hJpWgD7ZalvaqXAE_GF94gxjRdqgYQF-6MMIxZYacKqnnEYCd3HUCxuwm-JSTue1SmxGfAcFpN5ASnybcPFqsDqCXLvbcLzo-fzhkH7yHpaELtIv-lUiXIbt0sZGYOAmSq9zthTh4fcC0wtWzvv2K6joXQKkR8-m4F5w66knBOAR7RmkGUlpRIevVgYb004W6zturZE1yN-AK1it2YX3GSiL-B4fdTlf19_GEMcF1i2PgJJYy3xvF7cCC43Hm5X8ivlSSNmZ9aDJNxjuhaH1xRLOmGjbPgJbX8TlQSqvMeHY60r_YRcIQUNbifPgKOqjy5JXXtm5qTxPe7N2vXp-DE8J5IDhT_zLNyVvXXZOWgHF3cIKd-B4dC-XghBFZF5oEA47Th74INDO169akI_tGS4W1JAY1t4S9EG90VysR9iZhHH1364-rBIQXh_xXA2KyHtzamx4T1M4s0JZHmiK1PastaU_Gp-Oqh9z4Dm_PNvF_MzCz9GQ-n-6a0az'}

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
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(send_request) for _ in range(5000)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            print(f"{i}: {result}")
