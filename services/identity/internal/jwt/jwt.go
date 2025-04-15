package jwt

import (
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func CreateToken(username string) (string, error) {

	secret, err := os.ReadFile("/Users/marinrazvan/Developer/datalake/services/identity/certs/private.pem")
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

	tokenString, err := token.SignedString(secretKey)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}
