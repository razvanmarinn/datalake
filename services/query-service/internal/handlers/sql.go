package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
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

	// 1. Parse SQL
	stmt, err := sqlparser.Parse(req.Query)
	if err != nil {
		h.logger.Error("Failed to parse SQL", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid SQL syntax: " + err.Error()})
		return
	}

	replacements := make(map[string]string)

	// APPROACH A: Manual Inspection (Restored for reliability)
	if selectStmt, ok := stmt.(*sqlparser.Select); ok {
		h.logger.Info("Detected SELECT statement, inspecting FROM clause manually")
		for _, fromExpr := range selectStmt.From {
			if aliasedExpr, ok := fromExpr.(*sqlparser.AliasedTableExpr); ok {
				switch t := aliasedExpr.Expr.(type) {
				case sqlparser.TableName:
					h.logger.Info("Found TableName (Value)", zap.String("table", t.Name.String()))
					newT := &t
					if err := h.rewriteTable(c.Request.Context(), req.ProjectID, newT, replacements); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
						return
					}
					aliasedExpr.Expr = *newT

				case *sqlparser.TableName:
					h.logger.Info("Found TableName (Pointer)", zap.String("table", t.Name.String()))
					if err := h.rewriteTable(c.Request.Context(), req.ProjectID, t, replacements); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
						return
					}
				}
			}
		}
	} else {
		// APPROACH B: Fallback Walk for non-SELECT queries
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if table, ok := node.(*sqlparser.TableName); ok {
				_ = h.rewriteTable(c.Request.Context(), req.ProjectID, table, replacements)
			}
			return true, nil
		}, stmt)
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

	// FIX: Only install HTTPFS. Do NOT install 'avro' to prevent 404 crash on ARM64.
	if _, err = db.ExecContext(c.Request.Context(), "INSTALL httpfs; LOAD httpfs;"); err != nil {
		h.logger.Error("Failed to load extensions", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to initialize DB extensions"})
		return
	}

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

func (h *QueryHandler) rewriteTable(ctx context.Context, projectID string, table *sqlparser.TableName, replacements map[string]string) error {
	tableName := table.Name.String()
	placeholder := fmt.Sprintf("TOKEN_%s", tableName)

	if _, exists := replacements[placeholder]; exists {
		return nil
	}

	files, err := h.resolveFilesForTable(ctx, projectID, tableName)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("schema empty or not found: %s", tableName)
	}

	var parquetUrls []string

	for _, file := range files {
		// Only supporting Parquet for now to stabilize the service
		if strings.HasSuffix(file, ".parquet") {
			url := fmt.Sprintf("'http://localhost:8086/virtual?project=%s&file=%s'", projectID, file)
			parquetUrls = append(parquetUrls, url)
		} else if strings.HasSuffix(file, ".avro") {
			h.logger.Warn("Skipping Avro file (Avro extension disabled)", zap.String("file", file))
		}
	}

	if len(parquetUrls) == 0 {
		return fmt.Errorf("no parquet files found for table %s (avro temporarily disabled)", tableName)
	}

	replacement := fmt.Sprintf("read_parquet([%s])", strings.Join(parquetUrls, ", "))
	replacements[placeholder] = replacement

	table.Qualifier = sqlparser.NewTableIdent("")
	table.Name = sqlparser.NewTableIdent(placeholder)

	return nil
}

func (h *QueryHandler) resolveFilesForTable(ctx context.Context, projectID, schema string) ([]string, error) {
	prefix := fmt.Sprintf("%s/", schema)
	resp, err := h.MasterClient.ListFiles(ctx, projectID, prefix)
	if err != nil {
		return nil, err
	}
	return resp.FilePaths, nil
}

// VirtualFileHandler proxies file chunks to appear as a single file
func (h *QueryHandler) VirtualFileHandler(c *gin.Context) {
	projectID := c.Query("project")
	filePath := c.Query("file")

	if projectID == "" || filePath == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	fullPath := filepath.Join(projectID, filePath)

	metadata, err := h.MasterClient.GetFileMetadata(c.Request.Context(), projectID, fullPath)
	if err != nil {
		// Log the specific error from Master for debugging
		h.logger.Error("VirtualProxy: Metadata failed",
			zap.String("full_path", fullPath),
			zap.Error(err))

		// Return 404 if Master says file not found
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
