package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/schema-registry/internal/db"
	"github.com/razvanmarinn/schema-registry/internal/models"
)

type CreateSchemaBody struct {
	SchemaName string         `json:"schema_name" binding:"required"`
	Fields     []models.Field `json:"fields" binding:"required"`
	Type       string         `json:"type" binding:"required"`
}

func SetupRouter(database *sql.DB) *gin.Engine {
	r := gin.Default()

	r.POST("/:project_name/schema", func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
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
			return
		}
		c.JSON(http.StatusCreated, gin.H{"message": "Schema created successfully"})
	})

	r.PUT("/:project_name/schema", func(c *gin.Context) {
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
		}
		err := db.UpdateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Schema created successfully"})
	})

	r.GET("/:project_name/schema/:schema_name", func(c *gin.Context) {
		projectName := c.Param("project_name")
		schemaName := c.Param("schema_name")
		schema, err := db.GetSchema(database, projectName, schemaName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, schema)
	})

	return r
}
