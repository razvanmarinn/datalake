package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/xwb1989/sqlparser"
	"go.uber.org/zap"
)

type RunSQLRequest struct {
	Query     string `json:"query"`
	ProjectID string `json:"project_id"`
}

func (h *QueryHandler) RunSQL(c *gin.Context) {
	var req RunSQLRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	if req.ProjectID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "project_id is required"})
		return
	}

	h.logger.Info("Received SQL Query", zap.String("query", req.Query), zap.String("project", req.ProjectID))

	stmt, err := sqlparser.Parse(req.Query)
	if err != nil {
		h.logger.Error("Failed to parse SQL", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid SQL syntax: " + err.Error()})
		return
	}

	replacements := make(map[string]string)
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if table, ok := node.(*sqlparser.TableName); ok {
			tableName := table.Name.String()

			files, err := h.resolveFilesForTable(c.Request.Context(), req.ProjectID, tableName)
			if err != nil {
				return false, err
			}
			if len(files) == 0 {
				return false, fmt.Errorf("schema empty or not found: %s", tableName)
			}

			var fileUrls []string
			for _, file := range files {
				url := fmt.Sprintf("'http://localhost:8086/virtual?project=%s&file=%s'", req.ProjectID, file)
				fileUrls = append(fileUrls, url)
			}

			placeholder := fmt.Sprintf("TOKEN_%s", tableName)
			replacement := fmt.Sprintf("read_parquet([%s])", strings.Join(fileUrls, ", "))

			replacements[placeholder] = replacement

			table.Qualifier = sqlparser.NewTableIdent("")
			table.Name = sqlparser.NewTableIdent(placeholder)
		}
		return true, nil
	}, stmt)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	generatedSQL := sqlparser.String(stmt)

	for token, replacement := range replacements {
		quotedToken := fmt.Sprintf("`%s`", token)
		generatedSQL = strings.ReplaceAll(generatedSQL, quotedToken, replacement)
		generatedSQL = strings.ReplaceAll(generatedSQL, token, replacement)
	}

	h.logger.Info("Executing DuckDB Query", zap.String("sql", generatedSQL))

	db, err := sql.Open("duckdb", "")
	if err != nil {
		h.logger.Error("Failed to open duckdb", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database engine failure"})
		return
	}
	defer db.Close()

	rows, err := db.QueryContext(c.Request.Context(), generatedSQL)
	if err != nil {
		h.logger.Error("DuckDB Query Failed", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "Query execution error: " + err.Error()})
		return
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	resultData := make([]map[string]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		resultData = append(resultData, rowMap)
	}

	c.JSON(http.StatusOK, resultData)
}

func (h *QueryHandler) resolveFilesForTable(ctx context.Context, projectID, schema string) ([]string, error) {
	prefix := fmt.Sprintf("%s/compacted", schema)
	resp, err := h.MasterClient.ListFiles(ctx, projectID, prefix)
	if err != nil {
		return nil, err
	}
	return resp.FilePaths, nil
}

func (h *QueryHandler) VirtualFileHandler(c *gin.Context) {
	projectID := c.Query("project")
	filePath := c.Query("file")

	if projectID == "" || filePath == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	// 1. Get Metadata
	metadata, err := h.MasterClient.GetFileMetadata(c.Request.Context(), projectID, filePath)
	if err != nil {
		h.logger.Error("VirtualProxy: Metadata failed", zap.Error(err))
		c.AbortWithStatus(http.StatusNotFound)
		return
	}

	if len(metadata.Blocks) == 1 {
		block := metadata.Blocks[0]
		loc := metadata.Locations[block.BlockId]

		parts := strings.Split(loc.Address, ":")
		if len(parts) >= 1 {
			host := parts[0]
			httpURL := fmt.Sprintf("http://%s:8080/blocks/%s", host, block.BlockId)
			h.logger.Debug("Redirecting to worker", zap.String("url", httpURL))
			c.Redirect(http.StatusTemporaryRedirect, httpURL)
			return
		}
	}

	c.Header("Content-Type", "application/octet-stream")

	for _, block := range metadata.Blocks {
		loc, ok := metadata.Locations[block.BlockId]
		if !ok {
			h.logger.Error("Missing location for block", zap.String("id", block.BlockId))
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		client, err := h.getDataNodeClient(loc.Address)
		if err != nil {
			c.AbortWithStatus(http.StatusBadGateway)
			return
		}

		data, err := client.FetchBlock(c.Request.Context(), block.BlockId)
		if err != nil {
			c.AbortWithStatus(http.StatusBadGateway)
			return
		}

		if _, err := c.Writer.Write(data); err != nil {
			return
		}
	}
}
