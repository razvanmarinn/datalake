package handlers

import (
	"database/sql"

	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/razvanmarinn/datalake/pkg/jwt/manager"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/identity_service/internal/db"
	"github.com/razvanmarinn/identity_service/internal/db/models"
	kf "github.com/razvanmarinn/identity_service/internal/kafka"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type LoginBody struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func SetupRouter(database *sql.DB, kafkaWriter *kf.KafkaWriter, logger *logging.Logger) *gin.Engine {
	r := gin.New()

	// Add recovery middleware (since we're using gin.New() not gin.Default())
	r.Use(gin.Recovery())

	// Add our structured logging middleware
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
		projects := make(map[string]uuid.UUID)

		tkn, err := manager.CreateToken(user.Username, projects)
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
		projects, err := db.GetProjects(database, user.Username)
		if err != nil {
			logger.WithRequest(c).Error("Failed to get user projects", zap.Error(err))
		}
		tkn, err := manager.CreateToken(loginBody.Username, projects)
		if err != nil {
			logger.WithRequest(c).Error("Failed to create token", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
			return
		}
		logger.LogUserLogin(loginBody.Username, true)
		c.JSON(http.StatusOK, gin.H{"token": tkn, "user-id": user.ID})
	})

	r.POST("/project/register/", func(c *gin.Context) {
		var project models.Project
		if err := c.ShouldBindJSON(&project); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := db.RegisterProject(database, &project); err != nil {
			logger.WithRequest(c).Error("Failed to register project", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register project"})
			return
		}

		kafkaErr := kafkaWriter.WriteMessageForSchema(context.Background(), project.Name, kafka.Message{
			Key:   []byte(project.Name),
			Value: []byte(fmt.Sprintf("Project %s registered", project.Name)),
		})
		if kafkaErr != nil {
			logger.LogKafkaMessage(project.Name, project.Name, false)
		} else {
			logger.LogKafkaMessage(project.Name, project.Name, true)
		}
		logger.LogProjectRegistration(project.Name, project.OwnerID.String())
		c.JSON(http.StatusOK, gin.H{"message": "Project registered successfully"})
	})

	return r
}
