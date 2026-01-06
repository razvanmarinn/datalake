package helpers

import (
	"context"
	"database/sql"
	"encoding/json"
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
	Type       string         `json:"type" binding:"required"`
	AvroFields []models.Field `json:"avro_fields" binding:"required"`
	ParquetDef string         `json:"parquet_definition" binding:"required"`
}

func generateAvroSchema(name, namespace string, fields []models.Field) (string, error) {
	schemaMap := map[string]interface{}{
		"type":      "record",
		"name":      name,
		"namespace": namespace,
		"fields":    fields,
	}

	bytes, err := json.Marshal(schemaMap)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func CreateSchema(database *sql.DB, logger *logging.Logger, prov *kafka.Provisioner) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		avroJSON, err := generateAvroSchema(body.SchemaName, "org."+projectName, body.AvroFields)
		if err != nil {
			logger.WithContext(c).Error("Failed to generate Avro JSON", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Invalid Avro fields"})
			return
		}

		schema := models.SchemaWithDetails{
			ProjectName:   projectName,
			Name:          body.SchemaName,
			AvroSchema:    avroJSON,
			ParquetSchema: body.ParquetDef,
			Version:       1,
		}

		if err := db.CreateSchema(database, schema); err != nil {
			logger.WithContext(c).Error("DB Create Error", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save schema"})
			return
		}

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
			return
		}

		avroJSON, err := generateAvroSchema(body.SchemaName, "org."+projectName, body.AvroFields)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Avro fields"})
			return
		}

		schema := models.SchemaWithDetails{
			ProjectName:   projectName,
			Name:          body.SchemaName,
			AvroSchema:    avroJSON,
			ParquetSchema: body.ParquetDef,
		}

		err = db.UpdateSchema(database, schema)
		if err != nil {
			logger.WithContext(c).Error("Failed to update schema", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update schema"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Schema updated successfully"})
		logger.WithContext(c).Info("Schema updated",
			zap.String("project", projectName),
			zap.String("schema", body.SchemaName))
	}
}

func ListSchemas(database *sql.DB, logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		projectName := c.Param("project_name")

		_, exists := c.Get("userID")
		if !exists {
			logger.WithContext(c).Warn("User ID not found in context")
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			return
		}

		_, err := db.GetProjectOwnerID(database, projectName)
		if err != nil {
			logger.WithContext(c).Error("Failed to fetch project owner", zap.String("project", projectName), zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify project ownership"})
			return
		}

		schemas, err := db.ListSchemas(database, projectName)
		if err != nil {
			logger.WithContext(c).Error("Failed to list schemas", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve schemas"})
			return
		}

		logger.WithContext(c).Info("Schemas listed", zap.String("project", projectName))
		c.JSON(http.StatusOK, schemas)
	}
}
