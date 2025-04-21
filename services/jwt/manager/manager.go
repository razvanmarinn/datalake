package manager

import (
	"context"
	_ "embed"
	"log"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type Claims struct {
	UserID   string               `json:"username"`
	Projects map[string]uuid.UUID `json:"projects"`
	// Role   string `json:"role"`
	jwt.RegisteredClaims
}

//go:embed certs/private.pem
var embeddedPrivateKey []byte
var keyId string = "19c92999ceb1b952d80c6f90"

func CreateToken(username string, projects map[string]uuid.UUID) (string, error) {
	secretKey, err := jwt.ParseRSAPrivateKeyFromPEM(embeddedPrivateKey)
	if err != nil {
		return "", err
	}

	if projects == nil {
		projects = make(map[string]uuid.UUID)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256,
		jwt.MapClaims{
			"username": username,
			"projects": projects,
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
	k, err := keyfunc.NewDefaultCtx(ctx, []string{"http://identity-service:8082/.well-known/jwks.json"}) // Context is used to end the refresh goroutine.
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
	rawProjects, ok := claims["projects"].(map[string]interface{})
	if !ok {
		log.Fatalf("Failed to parse 'projects' from claims")
	}
	projects := make(map[string]uuid.UUID)
	for k, v := range rawProjects {
		s, ok := v.(string)
		if !ok {
			log.Fatalf("Project ID is not a string: %v", v)
		}
		id, err := uuid.Parse(s)
		if err != nil {
			log.Fatalf("Invalid UUID format in project: %v", err)
		}
		projects[k] = id
	}

	return Claims{
		UserID:   claims["username"].(string),
		Projects: projects,
	}, nil
}
