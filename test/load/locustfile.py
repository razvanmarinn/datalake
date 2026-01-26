from locust import HttpUser, task, between
import random
import string
import json

class DatalakeUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self):
        self.username = "loadtest_" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.password = "password123"
        self.headers = {"Content-Type": "application/json"}

        register_payload = {
            "username": self.username,
            "password": self.password,
            "email": f"{self.username}@example.com"
        }
        with self.client.post("/auth/register/", json=register_payload, catch_response=True) as response:
            if response.status_code == 200:
                print(f"Registered user: {self.username}")
            else:
                print(f"Failed to register {self.username}: {response.text}")
                response.failure(f"Registration failed: {response.text}")
                return

        login_payload = {
            "username": self.username,
            "password": self.password
        }
        with self.client.post("/auth/login/", json=login_payload, catch_response=True) as response:
            if response.status_code == 200:
                token = response.json().get("token")
                self.headers["Authorization"] = f"Bearer {token}"
                print(f"Logged in user: {self.username}")
            else:
                print(f"Failed to login {self.username}: {response.text}")
                response.failure(f"Login failed: {response.text}")

    @task(3)
    def browse_projects(self):
        with self.client.get(f"/meta/projects/by-username/{self.username}", headers=self.headers, catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"Browse projects failed: {response.status_code}")

    @task(1)
    def check_health(self):
        pass
