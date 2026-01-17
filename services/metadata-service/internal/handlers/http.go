package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/jwt/middleware"
	"github.com/razvanmarinn/datalake/pkg/logging"
	identityv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/identity/v1"
	"github.com/razvanmarinn/metadata-service/internal/helpers"
	"github.com/razvanmarinn/metadata-service/internal/kafka"
)

func SetupRouter(database *sql.DB, logger *logging.Logger, prov *kafka.Provisioner, idClient identityv1.IdentityServiceClient) *gin.Engine {
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/:project_name/schema/:schema_name", helpers.GetSchema(database, logger))

	auth := r.Group("/")
	auth.Use(middleware.AuthMiddleware())
	{
		auth.POST("/project/register", helpers.RegisterProject(database, logger, idClient))
		auth.GET("/projects/by-username/:username", helpers.GetProjectsByUser(database, logger, idClient))

		auth.POST("/:project_name/schema", helpers.CreateSchema(database, logger, prov))
		auth.PUT("/:project_name/schema", helpers.UpdateSchema(database, logger))
		auth.GET("/:project_name/schemas", helpers.ListSchemas(database, logger))
	}

	return r
}
