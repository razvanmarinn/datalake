import requests
import random
import string
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

URL = 'http://localhost:8083/ingest/razvan'
HEADERS = {'Content-Type': 'application/json', 'Authorization':  'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5YzkyOTk5Y2ViMWI5NTJkODBjNmY5MCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTkyNjkwNDAsInByb2plY3RzIjp7InJhenZhbiI6IjY0MDY3ZGQwLTU0NTktNDY1Yi05ZTQyLTY4Y2E4Njg1NTA2YSJ9LCJ1c2VybmFtZSI6InRlc3R4In0.oFsN80vU_j5BffuFKXKalIjBjl19QPLLtuWGlkw45cf19emi-EhhodUnebOptmb5TQ7FUbOVjTE7MhD3nBMzDUHiUWlw6F8Uu00pbEHfvk1TpAUESBsEh-4aUEoMWglTnzja2fxNCKE6jeh5PPwYaHrDxFv-loGuk7rQ30bGw_ZYlg_liuChU9johMIQqN47M3KtOsiLfMLRuVgUw0AUMM-RlAXF9GlyRTZu2AOjWzu6Ns7lDIn-N4gB5WsaifNrXJZtW8wx_M5jSn6mt6FqeiLU9x1te6i6ATqSQMuwl4B9U7MDembqgxClZmc3te3V55hhlJhDE091rODK4bYHazY-hlSu-0gl1quim4tRXJyU8n3uMSIT9GV4rPDpbaoLkQzk0ZzAUqR7EbFiJIVsp79im8LGypgB2dkCARKeTo9gXvPxCjAf-oVnefK16VhjTI7S4f4QzjyI92DSZKBMJ2hTtOyKWRKEic6CB7QE6NwOmmTJPR-LdMGdZLYZaV8P' }

# Data generation functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'test.org']
    return f"{random_string(8).lower()}@{random.choice(domains)}"

def random_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def random_int(min_val=1, max_val=100000):
    return random.randint(min_val, max_val)

def random_float(min_val=0.0, max_val=10000.0):
    return round(random.uniform(min_val, max_val), 2)

def random_bool():
    return random.choice([True, False])

def random_datetime():
    start_date = datetime.now() - timedelta(days=365)
    random_days = random.randint(0, 365)
    random_seconds = random.randint(0, 86400)
    return (start_date + timedelta(days=random_days, seconds=random_seconds)).isoformat()

def random_status():
    return random.choice(['active', 'inactive', 'pending', 'suspended', 'verified', 'deleted'])

def random_country():
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'China']
    return random.choice(countries)

def random_city():
    cities = ['New York', 'London', 'Toronto', 'Berlin', 'Paris', 'Tokyo', 'Sydney', 'São Paulo', 'Mumbai', 'Beijing']
    return random.choice(cities)

def random_product():
    products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Camera', 'Speaker', 'Watch']
    return random.choice(products)

def random_category():
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports', 'Home', 'Garden', 'Automotive', 'Health', 'Beauty']
    return random.choice(categories)

def random_payment_method():
    return random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto', 'cash'])

def random_tags():
    all_tags = ['premium', 'sale', 'new', 'featured', 'trending', 'limited', 'exclusive', 'popular', 'recommended', 'bestseller']
    return random.sample(all_tags, random.randint(1, 4))

def random_ip():
    return f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"

def random_user_agent():
    browsers = ['Chrome/120.0', 'Firefox/121.0', 'Safari/17.0', 'Edge/120.0']
    os = ['Windows NT 10.0', 'Macintosh; Intel Mac OS X 10_15_7', 'X11; Linux x86_64']
    return f"Mozilla/5.0 ({random.choice(os)}) {random.choice(browsers)}"

# Complex data generation
def generate_user_data():
    return {
        "user_id": str(uuid4()),
        "username": random_string(12),
        "email": random_email(),
        "full_name": f"{random_string(6).capitalize()} {random_string(8).capitalize()}",
        "age": random_int(18, 80),
        "phone": random_phone(),
        "country": random_country(),
        "city": random_city(),
        "status": random_status(),
        "is_verified": random_bool(),
        "is_premium": random_bool(),
        "created_at": random_datetime(),
        "last_login": random_datetime(),
        "login_count": random_int(0, 1000),
        "account_balance": random_float(0.0, 50000.0),
        "tags": random_tags()
    }

def generate_transaction_data():
    return {
        "transaction_id": str(uuid4()),
        "user_id": str(uuid4()),
        "amount": random_float(1.0, 5000.0),
        "currency": random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CAD']),
        "payment_method": random_payment_method(),
        "product_name": random_product(),
        "category": random_category(),
        "quantity": random_int(1, 20),
        "discount_applied": random_bool(),
        "discount_amount": random_float(0.0, 500.0),
        "tax_amount": random_float(0.0, 200.0),
        "shipping_cost": random_float(0.0, 50.0),
        "status": random.choice(['completed', 'pending', 'failed', 'refunded', 'cancelled']),
        "created_at": random_datetime(),
        "updated_at": random_datetime(),
        "ip_address": random_ip(),
        "user_agent": random_user_agent(),
        "session_id": str(uuid4()),
        "referrer": random.choice(['google', 'facebook', 'twitter', 'direct', 'email', 'affiliate'])
    }

def generate_event_data():
    event_types = ['page_view', 'click', 'scroll', 'form_submit', 'video_play', 'download', 'purchase', 'signup', 'logout']
    return {
        "event_id": str(uuid4()),
        "event_type": random.choice(event_types),
        "user_id": str(uuid4()),
        "session_id": str(uuid4()),
        "timestamp": random_datetime(),
        "page_url": f"https://example.com/{random_string(10)}",
        "page_title": f"Page {random_string(15)}",
        "duration_ms": random_int(100, 60000),
        "screen_width": random.choice([1920, 1366, 1440, 2560, 1280]),
        "screen_height": random.choice([1080, 768, 900, 1440, 720]),
        "browser": random.choice(['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']),
        "os": random.choice(['Windows', 'macOS', 'Linux', 'iOS', 'Android']),
        "device_type": random.choice(['desktop', 'mobile', 'tablet']),
        "ip_address": random_ip(),
        "country": random_country(),
        "city": random_city(),
        "referrer": f"https://{random_string(8)}.com",
        "metadata": {
            "custom_field_1": random_string(20),
            "custom_field_2": random_int(),
            "custom_field_3": random_bool()
        }
    }

def send_request(data_type='user'):
    # Choose data type randomly if not specified
    if data_type == 'mixed':
        data_type = random.choice(['user', 'transaction', 'event'])

    if data_type == 'user':
        data = generate_user_data()
    elif data_type == 'transaction':
        data = generate_transaction_data()
    elif data_type == 'event':
        data = generate_event_data()
    else:
        data = generate_user_data()

    payload = {
        "schema_name": data_type,
        "data": data
    }

    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        return response.status_code
    except Exception as e:
        return f"Error: {e}"

if __name__ == "__main__":
    # Configuration
    NUM_REQUESTS = 1_000_000
    MAX_WORKERS = 100
    DATA_TYPE = 'mixed'  # Options: 'user', 'transaction', 'event', 'mixed'

    print(f"Starting data generation: {NUM_REQUESTS} requests with {MAX_WORKERS} workers")
    print(f"Data type: {DATA_TYPE}")
    print("-" * 60)

    success_count = 0
    error_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(send_request, DATA_TYPE) for _ in range(NUM_REQUESTS)]
        for i, future in enumerate(futures, 1):
            result = future.result()
            if isinstance(result, int) and 200 <= result < 300:
                success_count += 1
            else:
                error_count += 1

            # Print progress every 1000 requests
            if i % 1000 == 0:
                print(f"Progress: {i}/{NUM_REQUESTS} | Success: {success_count} | Errors: {error_count}")

    print("-" * 60)
    print(f"Completed! Total: {NUM_REQUESTS} | Success: {success_count} | Errors: {error_count}")