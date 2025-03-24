import json
import random
import string
import uuid
import requests
import time

def generate_random_string(length):
    """Generate a random string of specified length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_complex_data():
    """Generate a complex data structure"""
    def generate_user():
        """Generate a detailed user profile"""
        return {
            "id": str(uuid.uuid4()),
            "name": f"{generate_random_string(10)} {generate_random_string(10)}",
            "email": f"{generate_random_string(8)}@{generate_random_string(6)}.com",
            "age": random.randint(18, 80),
            "active": random.choice([True, False]),
            "registration_date": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "address": {
                "street": generate_random_string(20),
                "city": generate_random_string(12),
                "country": generate_random_string(10),
                "postal_code": generate_random_string(6)
            },
            "contact_details": {
                "phone": f"+{random.randint(1,999)}-{generate_random_string(10)}",
                "alternative_email": f"{generate_random_string(8)}@{generate_random_string(6)}.org"
            },
            "preferences": {
                "communication_channels": random.sample(
                    ["email", "phone", "sms", "whatsapp", "telegram"], 
                    random.randint(1, 4)
                ),
                "interests": [generate_random_string(8) for _ in range(random.randint(3, 10))]
            },
            "professional_info": {
                "occupation": generate_random_string(15),
                "skills": [generate_random_string(8) for _ in range(random.randint(5, 15))],
                "years_of_experience": random.randint(0, 30)
            },
            "additional_metadata": {
                key: generate_random_string(random.randint(5, 20)) 
                for key in [generate_random_string(6) for _ in range(10)]
            }
        }

    # Payload structure to match the original code
    payload = {
        "schema_name": "test1",
        "data": generate_user()
    }

    return payload

def send_payload_to_api(payload, url='http://localhost:8080/ingest'):
    """Send payload to the specified API endpoint"""
    try:
        response = requests.post(url, json=payload)
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.text}")
        return response
    except requests.RequestException as e:
        print(f"Error sending payload: {e}")
        return None

def main():
    # Generate a single payload to be used 30 times
    payload = generate_complex_data()
    
    # Check the size of the JSON
    json_string = json.dumps(payload)
    size_mb = len(json_string) / (1024 * 1024)
    print(f"Generated JSON size: {size_mb:.2f} MB")
    
    # Send payload 30 times
    for i in range(30):
        print(f"\nSending payload {i+1}/30:")
        send_payload_to_api(payload)
        
        # Optional: add a small delay between requests
        time.sleep(0.1)

if __name__ == "__main__":
    main()