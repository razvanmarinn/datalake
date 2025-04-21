package handlers

import (
	"database/sql"
	"log"

	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/razvanmarinn/identity_service/internal/db"
	"github.com/razvanmarinn/identity_service/internal/db/models"
	kf "github.com/razvanmarinn/identity_service/internal/kafka"
	"github.com/razvanmarinn/jwt/manager"

	"github.com/segmentio/kafka-go"
)

type LoginBody struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func SetupRouter(database *sql.DB, kafkaWriter *kf.KafkaWriter) *gin.Engine {
	r := gin.Default()
	r.StaticFile("/.well-known/jwks.json", "./public/.well-known/jwks.json")

	r.POST("/register/", func(c *gin.Context) {
		var user models.Client
		if err := c.ShouldBindJSON(&user); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := db.RegisterUser(database, &user); err != nil {
			log.Printf("Error registering user: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register user"})
			return
		}
		projects := make(map[string]uuid.UUID)

		tkn, err := manager.CreateToken(user.Username, projects)
		if err != nil {
			log.Printf("Error creating token: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
			return
		}
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
			log.Printf("Error getting user: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user"})
			return
		}
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid username or password"})
			return
		}
		if user.Password != loginBody.Password {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid username or password"})
			return
		}
		//
		projects, err := db.GetProjects(database, user.Username)
		tkn, err := manager.CreateToken(loginBody.Username, projects)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"token": tkn})
	})

	r.POST("/project/register/", func(c *gin.Context) {
		var project models.Project
		if err := c.ShouldBindJSON(&project); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := db.RegisterProject(database, &project); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register project"})
			return
		}

		kafkaWriter.WriteMessageForSchema(context.Background(), project.Name, kafka.Message{
			Key:   []byte(project.Name),
			Value: []byte(fmt.Sprintf("Project %s registered", project.Name)),
		})
		c.JSON(http.StatusOK, gin.H{"message": "Project registered successfully"})
	})

	return r
}
