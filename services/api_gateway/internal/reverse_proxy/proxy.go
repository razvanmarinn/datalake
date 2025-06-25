package reverse_proxy

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/razvanmarinn/api_gateway/internal/handlers"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func StreamingIngestionProxy(vs pb.VerificationServiceClient, targetServiceURL string) gin.HandlerFunc {
	target, err := url.Parse(targetServiceURL)
	if err != nil {
		log.Fatalf("Invalid target URL for StreamingIngestionProxy: %v", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	// Custom director to handle headers and trace propagation
	proxy.Director = func(req *http.Request) {
		// Set basic proxy headers
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host

		// The path should be set here, before propagation
		req.URL.Path = "/ingest"
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		log.Printf("Reverse proxy error: %v", err)
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {
		projectName := c.Param("project")
		if projectName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Project name missing in URL"})
			c.Abort()
			return
		}

		// Verify project exists
		exists, err := handlers.CheckProjectExists(vs, projectName)
		if err != nil {
			log.Printf("Error verifying project %s: %v", projectName, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify project existence"})
			c.Abort()
			return
		}
		if !exists {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found: " + projectName})
			c.Abort()
			return
		}

		// Extract user context
		userIDIfc, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			return
		}
		userID := userIDIfc.(string)

		projectsIfc, exists := c.Get("projects")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing projects in token"})
			return
		}
		projects := projectsIfc.(map[string]uuid.UUID)
		projectID, ok := projects[projectName]
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized for project: " + projectName})
			return
		}

		// FIXED: Correct header assignment
		c.Request.Header.Set("X-User-ID", userID)
		c.Request.Header.Set("X-Project-ID", projectID.String())

		// Inject OpenTelemetry trace context into the request headers
		// This must be done BEFORE calling ServeHTTP
		otel.GetTextMapPropagator().Inject(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		log.Printf("Forwarding request for project %s to %s with trace context", projectName, target.Host)

		// Log trace headers for debugging
		if traceParent := c.Request.Header.Get("traceparent"); traceParent != "" {
			log.Printf("[TRACE] Forwarding traceparent: %s", traceParent)
		}
		if traceState := c.Request.Header.Get("tracestate"); traceState != "" {
			log.Printf("[TRACE] Forwarding tracestate: %s", traceState)
		}

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

func SchemaRegistryProxy(targetServiceURL string) gin.HandlerFunc {

	target, err := url.Parse(targetServiceURL)
	if err != nil {
		log.Fatalf("Invalid target URL for SchemaRegistryProxy: %v", err)
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
		log.Printf("Reverse proxy error: %v", err)
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {

		projectName := c.Param("project") // You can get the project name here
		log.Printf("Forwarding schema request for project %s to %s", projectName, target.Host)

		originalPath := c.Param("path") // Get the wildcard path part

		userIDIfc, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			return
		}
		userID := userIDIfc.(string)

		projectsIfc, exists := c.Get("projects")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing projects in token"})
			return
		}
		projects := projectsIfc.(map[string]string)

		projectID, ok := projects[projectName]
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized for project: " + projectName})
			return
		}

		c.Request.Header.Set("X-Project-ID", userID)
		c.Request.Header.Set("X-User-ID", projectID)

		c.Request.URL.Path = "/schema" + originalPath

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

// user -> api gateway ( jwt token and req )
//  req (/streaming-ingestion/project_name)
//  if project_name exists
//  forward to streaming-ingestion service with project_id ( uuid )
