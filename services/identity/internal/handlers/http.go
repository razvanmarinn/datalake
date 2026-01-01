package handlers

import (
	"database/sql"

	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/jwt/manager"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/identity-service/internal/db"
	"github.com/razvanmarinn/identity-service/internal/db/models"

	"go.uber.org/zap"
)

type LoginBody struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func SetupRouter(database *sql.DB, logger *logging.Logger) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	config := cors.Config{
		AllowOrigins:     []string{"http://localhost:3001"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	}
	r.Use(cors.New(config))
	r.Use(logger.GinMiddleware())

	r.StaticFile("/.well-known/jwks.json", "./public/.well-known/jwks.json")

	r.POST("/register/", func(c *gin.Context) {
		var user models.Client
		if err := c.ShouldBindJSON(&user); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := db.RegisterUser(database, &user); err != nil {
			logger.WithRequest(c).Error("Failed to register user", zap.Error(err))
			logger.LogUserRegistration(user.Username, false)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register user"})
			return
		}

		tkn, err := manager.CreateToken(user.Username)
		if err != nil {
			logger.WithRequest(c).Error("Failed to create token", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
			return
		}
		logger.LogUserRegistration(user.Username, true)
		c.JSON(http.StatusOK, gin.H{"token": tkn})
	})

	r.POST("/login/", func(c *gin.Context) {
		var loginBody LoginBody
		if err := c.ShouldBindJSON(&loginBody); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		user, err := db.GetUser(database, loginBody.Username)
		if err != nil {
			logger.WithRequest(c).Error("Failed to get user", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user"})
			return
		}
		if user == nil {
			logger.LogUserLogin(loginBody.Username, false)
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid username or password"})
			return
		}
		if user.Password != loginBody.Password {
			logger.LogUserLogin(loginBody.Username, false)
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid username or password"})
			return
		}
		//
		tkn, err := manager.CreateToken(loginBody.Username)
		if err != nil {
			logger.WithRequest(c).Error("Failed to create token", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
			return
		}
		logger.LogUserLogin(loginBody.Username, true)
		c.JSON(http.StatusOK, gin.H{"token": tkn, "user-id": user.ID})
	})

	return r
}
