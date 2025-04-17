package reverse_proxy

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/razvanmarinn/api_gateway/internal/handlers"
	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/gin-gonic/gin"
)

func StreamingIngestionProxy(vs pb.VerificationServiceClient, targetServiceURL string) gin.HandlerFunc {
	target, err := url.Parse(targetServiceURL)
	if err != nil {

		log.Fatalf("Invalid target URL for StreamingIngestionProxy: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Director = func(req *http.Request) {
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host

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

		exists, err := handlers.CheckProjectExists(vs, projectName) 
		if err != nil {
			log.Printf("Error verifying project %s: %v", projectName, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify project existence"})
			c.Abort() // Stop processing
			return
		}

		if !exists {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found: " + projectName})
			c.Abort() // Stop processing
			return
		}
		
		log.Printf("Forwarding request for project %s to %s", projectName, target.Host)
		c.Request.URL.Path = "/ingest" // Adjust if your target service needs a different path

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
		c.Request.URL.Path = "/schema" + originalPath

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}
