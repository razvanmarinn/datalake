package manager

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserID string `json:"username"`
	// Role   string `json:"role"`
	jwt.RegisteredClaims
}

var keyId string = "19c92999ceb1b952d80c6f90"

func CreateToken(username string) (string, error) {

	secret, err := os.ReadFile("/Users/marinrazvan/Developer/datalake/services/jwt/certs/private.pem")
	if err != nil {
		return "", err
	}
	secretKey, err := jwt.ParseRSAPrivateKeyFromPEM(secret)
	if err != nil {
		return "", err
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256,
		jwt.MapClaims{
			"username": username,
			"exp":      time.Now().Add(time.Hour * 24).Unix(),
		})

	token.Header["kid"] = keyId

	tokenString, err := token.SignedString(secretKey)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

func ParseToken(tokenStr string) (Claims, error) {
	ctx := context.Background()
	k, err := keyfunc.NewDefaultCtx(ctx, []string{"http://localhost:8082/.well-known/jwks.json"}) // Context is used to end the refresh goroutine.
	if err != nil {
		log.Fatalf("Failed to create a keyfunc.Keyfunc from the server's URL.\nError: %s", err)
	}
	parsed, err := jwt.Parse(tokenStr, k.Keyfunc)
	if err != nil {
		log.Fatalf("Failed to parse the JWT.\nError: %s", err)
	}
	if !parsed.Valid {
		log.Fatalf("The JWT is not valid.\nError: %s", err)
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		log.Fatalf("Failed to cast the claims to jwt.MapClaims.\nError: %s", err)
	}
	log.Printf("Parsed claims: %v", claims)
	return Claims{
		UserID: claims["username"].(string),
	}, nil
}
