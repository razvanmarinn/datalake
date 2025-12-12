package handlers

import (
	"database/sql"

	"context"
	"fmt"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/razvanmarinn/datalake/pkg/jwt/manager"
	middleware "github.com/razvanmarinn/datalake/pkg/jwt/middleware"
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
	r.Use(gin.Recovery())
	config := cors.Config{
		AllowOrigins:     []string{"http://localhost:3001"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	}
	r.Use(cors.New(config))
	r.Use(logger.GinMiddleware())
	auth := r.Group("/")
	auth.Use(middleware.AuthMiddleware())

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

	auth.GET("/projects/by-username/:username", func(c *gin.Context) {
		requestedUsername := c.Param("username")

		// Username from JWT validated by middleware
		authUsernameValue, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthenticated"})
			return
		}
		authUsername := authUsernameValue.(string)

		// Security: one user cannot fetch another user's data
		if requestedUsername != authUsername {
			c.JSON(http.StatusForbidden, gin.H{"error": "Access denied"})
			return
		}

		// Load projects from DB (if you want DB as source of truth)
		projects, err := db.GetProjects(database, requestedUsername)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get projects"})
			return
		}

		// Or: If you want to avoid querying DB and rely purely on JWT data:
		// projectsValue, _ := c.Get("projects")
		// projects := projectsValue.(map[string]uuid.UUID)

		user, err := db.GetUser(database, requestedUsername)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load user"})
			return
		}

		if user == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"user-id":  user.ID,
			"username": user.Username,
			"projects": projects,
		})
	})
	return r
}
