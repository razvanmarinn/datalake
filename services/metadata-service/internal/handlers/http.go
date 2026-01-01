package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/jwt/middleware"
	"github.com/razvanmarinn/datalake/pkg/logging"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/metadata-service/internal/helpers"
	"github.com/razvanmarinn/metadata-service/internal/kafka"
)

func SetupRouter(database *sql.DB, logger *logging.Logger, prov *kafka.Provisioner, idClient pb.IdentityServiceClient) *gin.Engine {
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	auth := r.Group("/")
	auth.Use(middleware.AuthMiddleware())
	{
		// --- Project Management ---
		auth.POST("/project/register", helpers.RegisterProject(database, logger))
		auth.GET("/projects/by-username/:username", helpers.GetProjectsByUser(database, logger, idClient))

		// --- Schema Management ---
		auth.POST("/:project_name/schema", helpers.CreateSchema(database, logger, prov))
		auth.PUT("/:project_name/schema", helpers.UpdateSchema(database, logger))
		auth.GET("/:project_name/schema/:schema_name", helpers.GetSchema(database, logger))
	}

	return r
}