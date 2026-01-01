package helpers

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/metadata-service/internal/db"
	"github.com/razvanmarinn/metadata-service/internal/kafka"
	"github.com/razvanmarinn/metadata-service/internal/models"
	"go.uber.org/zap"
)

type CreateSchemaBody struct {
	SchemaName string         `json:"schema_name" binding:"required"`
	Fields     []models.Field `json:"fields" binding:"required"`
	Type       string         `json:"type" binding:"required"`
}

func CreateSchema(database *sql.DB, logger *logging.Logger, prov *kafka.Provisioner) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		schema := models.Schema{
			ProjectName: projectName,
			Name:        body.SchemaName,
			Fields:      body.Fields,
			Version:     1,
		}

		if err := db.CreateSchema(database, schema); err != nil {
			logger.WithContext(c).Error("DB Create Error", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save schema"})
			return
		}

		// Background provisioning
		if prov != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := prov.ProvisionSchemaTopic(ctx, projectName, body.SchemaName); err != nil {
					logger.Error("Kafka Provisioning Failure", zap.Error(err))
				}
			}()
		}

		c.JSON(http.StatusCreated, gin.H{"message": "Schema created and provisioning started"})
	}
}

func GetSchema(database *sql.DB, logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		projectName := c.Param("project_name")
		schemaName := c.Param("schema_name")

		// Retrieve from DB
		schema, err := db.GetSchema(database, projectName, schemaName)
		if err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "Schema not found"})
				return
			}
			logger.WithContext(c).Error("Failed to get schema", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve schema"})
			return
		}

		logger.WithContext(c).Info("Schema retrieved",
			zap.String("project", projectName),
			zap.String("schema", schemaName))

		c.JSON(http.StatusOK, schema)
	}
}

func UpdateSchema(database *sql.DB, logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		schema := models.Schema{
			ProjectName: projectName,
			Name:        body.SchemaName,
			Fields:      body.Fields,
		}

		err := db.UpdateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update schema"})
		}
		c.JSON(http.StatusOK, gin.H{"message": "Schema updated successfully"})
		logger.WithContext(c).Info("Schema updated", zap.String("project", projectName), zap.String("schema", body.SchemaName))
	}
}
