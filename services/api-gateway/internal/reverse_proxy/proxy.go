package reverse_proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/razvanmarinn/api_gateway/internal/handlers"
	"github.com/razvanmarinn/datalake/pkg/logging"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"go.uber.org/zap"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func StreamingIngestionProxy(vs pb.VerificationServiceClient, targetServiceURL string, logger *logging.Logger) gin.HandlerFunc {
	target, err := url.Parse(targetServiceURL)
	if err != nil {
		logger.Fatal("Invalid target URL for StreamingIngestionProxy")
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Director = func(req *http.Request) {
		// Set basic proxy headers
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host

		req.URL.Path = "/ingest"
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.Error("Reverse proxy error", zap.Error(err))
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {
		projectName := c.Param("project")
		if projectName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Project name missing in URL"})
			logger.WithRequest(c).Error("Project name missing in URL", zap.String("url", c.Request.URL.Path))
			return
		}

		// Verify project exists
		exists, err := handlers.CheckProjectExists(vs, projectName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify project existence"})
			logger.WithRequest(c).Error("Failed to verify project existence", zap.Error(err), zap.String("project", projectName))
			return
		}
		if !exists {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found: " + projectName})
			logger.WithRequest(c).Error("Project not found", zap.String("project", projectName))
			return
		}

		// Extract user context
		userIDIfc, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			logger.WithRequest(c).Error("Missing userID in context")
			return
		}
		userID := userIDIfc.(string)

		projectsIfc, exists := c.Get("projects")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing projects in token"})
			logger.WithRequest(c).Error("Missing projects in token")
			return
		}
		projects := projectsIfc.(map[string]uuid.UUID)
		projectID, ok := projects[projectName]
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized for project: " + projectName})
			logger.WithRequest(c).Error("Unauthorized for project", zap.String("project", projectName))
			return
		}

		c.Request.Header.Set("X-User-ID", userID)
		c.Request.Header.Set("X-Project-ID", projectID.String())

		otel.GetTextMapPropagator().Inject(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		logger.WithProject(projectName).Info("Forwarding request to Streaming Ingestion", zap.String("user_id", userID), zap.String("project_id", projectID.String()), zap.String("path", c.Request.URL.Path))

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

func SchemaRegistryProxy(targetServiceURL string, logger *logging.Logger) gin.HandlerFunc {

	target, err := url.Parse(targetServiceURL)
	if err != nil {
		logger.Fatal("Invalid target URL for SchemaRegistryProxy", zap.Error(err))
	}
	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Director = func(req *http.Request) {
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		// Path rewriting happens within the Gin handler
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.WithError(err).Info("Reverse proxy error", zap.Error(err))
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {

		projectName := c.Param("project") // You can get the project name here
		logger.WithRequest(c).Info("Forwarding schema request to Schema Registry", zap.String("path", c.Request.URL.Path))

		originalPath := c.Param("path") // Get the wildcard path part

		userIDIfc, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			logger.WithRequest(c).Error("Missing userID in context")
			return
		}
		userID := userIDIfc.(string)

		projectsIfc, exists := c.Get("projects")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing projects in token"})
			logger.WithRequest(c).Error("Missing projects in token")
			return
		}
		projects := projectsIfc.(map[string]string)

		projectID, ok := projects[projectName]
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized for project: " + projectName})
			logger.WithRequest(c).Error("Unauthorized for project", zap.String("project", projectName))
			return
		}

		c.Request.Header.Set("X-Project-ID", userID)
		c.Request.Header.Set("X-User-ID", projectID)

		c.Request.URL.Path = "/schema" + originalPath

		proxy.ServeHTTP(c.Writer, c.Request)
		logger.WithProject(projectName).Info("Request forwarded to Schema Registry", zap.String("user_id", userID), zap.String("project_id", projectID), zap.String("path", c.Request.URL.Path))
	}
}

// user -> api gateway ( jwt token and req )
//  req (/streaming-ingestion/project_name)
//  if project_name exists
//  forward to streaming-ingestion service with project_id ( uuid )
