package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

var (
	registerProjectURL = "http://localhost:8082/project/register"
	loginURL           = "http://localhost:8082/login"
	username           = "test1"
	email              = "test@asd.com"
	password           = "test"
)

func registerUser(t *testing.T, username, email, password string) string {
	registerURL := "http://localhost:8082/register"
	registerPayload := map[string]string{
		"username": username,
		"email":    email,
		"password": password,
	}
	payloadBytes, err := json.Marshal(registerPayload)
	if err != nil {
		t.Fatalf("failed to marshal register payload: %v", err)
	}

	resp, err := http.Post(registerURL, "application/json", bytes.NewReader(payloadBytes))
	if err != nil {
		t.Fatalf("register request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("register failed with status %d: %s", resp.StatusCode, string(body))
	}
	return "asd"
}

func TestLoginAndCallProtectedEndpoint(t *testing.T) {
	user_id := registerUser(t, username, email, password)

	loginPayload := map[string]string{
		"username": username,
		"password": password,
	}
	payloadBytes, err := json.Marshal(loginPayload)
	if err != nil {
		t.Fatalf("failed to marshal login payload: %v", err)
	}

	resp, err := http.Post(loginURL, "application/json", bytes.NewReader(payloadBytes))
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("login failed with status %d: %s", resp.StatusCode, string(body))
	}

	var loginResp struct {
		Token string `json:"token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&loginResp)
	if err != nil {
		t.Fatalf("failed to decode login response: %v", err)
	}
	if loginResp.Token == "" {
		t.Fatal("empty token received from login")
	} else {
		fmt.Printf("Token received: %s\n", loginResp.Token)
	}

	registerProjectPayload := map[string]string{
		"name": "test1", "description": "integration-test ", "owner_id": user_id,
	}
	payloadBytes, err = json.Marshal(registerProjectPayload)
	resp, err = http.Post(registerProjectURL, "application/json", bytes.NewReader(payloadBytes))
	if err != nil {
		t.Fatalf("project register request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("project register failed with status %d: %s", resp.StatusCode, string(body))
	}

	t.Logf("Project registration succeeded")
}
