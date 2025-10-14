package handlers

import (
	"database/sql"
	"net/http"
	"github.com/razvanmarinn/datalake/pkg/logging"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/schema-registry/internal/db"
	"github.com/razvanmarinn/schema-registry/internal/models"
)

type CreateSchemaBody struct {
	SchemaName string         `json:"schema_name" binding:"required"`
	Fields     []models.Field `json:"fields" binding:"required"`
	Type       string         `json:"type" binding:"required"`
}

func SetupRouter(database *sql.DB, logger *logging.Logger) *gin.Engine {
	r := gin.Default()

	r.POST("/:project_name/schema", func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
			logger.WithContext(c).Error("Invalid request body: " + err.Error())
			c.Abort()
			return
		}
		schema := models.Schema{
			ProjectName: projectName,
			Name:        body.SchemaName,
			Fields:      body.Fields,
			Version:     1,
		}
		err := db.CreateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create schema in database: " + err.Error()})
			logger.WithContext(c).Error("Failed to create schema in database: " + err.Error())
			c.Abort()
			return
		}
		c.JSON(http.StatusCreated, gin.H{"message": "Schema created successfully"})
		logger.WithContext(c).Info("Schema created successfully in project " + projectName + " with name " + body.SchemaName)
	})

	r.PUT("/:project_name/schema", func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			logger.WithContext(c).Error("Invalid request body: " + err.Error())
			c.Abort()
			return
		}
		schema := models.Schema{
			ProjectName: projectName,
			Name:        body.SchemaName,
			Fields:      body.Fields,
		}
		err := db.UpdateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			logger.WithContext(c).Error("Failed to update schema in database: " + err.Error())
			c.Abort()
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Schema created successfully"})
		logger.WithContext(c).Info("Schema updated successfully in project " + projectName + " with name " + body.SchemaName)
	})

	r.GET("/:project_name/schema/:schema_name", func(c *gin.Context) {
		projectName := c.Param("project_name")
		schemaName := c.Param("schema_name")
		schema, err := db.GetSchema(database, projectName, schemaName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			logger.WithContext(c).Error("Failed to get schema from database: " + err.Error())
			c.Abort()
			return
		}
		c.JSON(http.StatusOK, schema)
		logger.WithContext(c).Info("Schema retrieved successfully from project " + projectName + " with name " + schemaName)
	})

	return r
}
