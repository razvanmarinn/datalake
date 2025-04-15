package handlers

import (
	"database/sql"

	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/identity_service/internal/db"
	"github.com/razvanmarinn/identity_service/internal/db/models"
	"github.com/razvanmarinn/identity_service/internal/jwt"
	kf "github.com/razvanmarinn/identity_service/internal/kafka"
	"github.com/segmentio/kafka-go"
)

func SetupRouter(database *sql.DB, kafkaWriter *kf.KafkaWriter) *gin.Engine {
	r := gin.Default()
	r.StaticFile("/.well-known/jwks.json", "./public/.well-known/jwks.json")

	r.POST("/register/", func(c *gin.Context) {
		var user models.Client
		if err := c.ShouldBindJSON(&user); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		tkn, err := jwt.CreateToken(user.Username)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"token": tkn})
	})

	r.POST("/login/", func(c *gin.Context) {
		var user models.Client
		if err := c.ShouldBindJSON(&user); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		tkn, err := jwt.CreateToken(user.Username)
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
