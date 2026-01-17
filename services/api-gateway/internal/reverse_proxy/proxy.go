package reverse_proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func StreamingIngestionProxy(targetServiceURL string, logger *logging.Logger) gin.HandlerFunc {
	target, err := url.Parse(targetServiceURL)
	if err != nil {
		logger.Fatal("Invalid target URL for StreamingIngestionProxy")
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Director = func(req *http.Request) {
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.Error("Reverse proxy error", zap.Error(err))
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {
		projectName := c.Param("project")
		if projectName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Project name is required in URL"})
			return
		}

		userID, ok := c.Get("userID")
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			return
		}

		c.Request.Header.Set("X-Project-ID", projectName)
		c.Request.Header.Set("X-User-ID", userID.(string))

		otel.GetTextMapPropagator().Inject(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		c.Request.URL.Path = "/ingest"

		logger.Info("Forwarding request to Streaming Ingestion",
			zap.String("project", projectName),
			zap.String("user_id", userID.(string)))

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

func MetadataServiceProxy(targetServiceURL string, logger *logging.Logger) gin.HandlerFunc {

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
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.WithError(err).Info("Reverse proxy error", zap.Error(err))
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {

		logger.WithRequest(c).Info("Forwarding schema request to Schema Registry", zap.String("path", c.Request.URL.Path))

		originalPath := c.Param("path")

		userIDIfc, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			logger.WithRequest(c).Error("Missing userID in context")
			return
		}
		userID := userIDIfc.(string)

		c.Request.Header.Set("X-User-ID", userID)

		c.Request.URL.Path = "/schema" + originalPath

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

func QueryServiceProxy(targetServiceURL string, logger *logging.Logger) gin.HandlerFunc {
	target, err := url.Parse(targetServiceURL)
	if err != nil {
		logger.Fatal("Invalid target URL for QueryServiceProxy", zap.Error(err))
	}
	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Director = func(req *http.Request) {
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.WithError(err).Info("Reverse proxy error for QueryService", zap.Error(err))
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {
		userIDIfc, exists := c.Get("userID")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing userID in context"})
			return
		}
		userID := userIDIfc.(string)
		c.Request.Header.Set("X-User-ID", userID)

		path := c.Param("path")
		c.Request.URL.Path = path

		logger.WithRequest(c).Info("Forwarding to Query Service", zap.String("path", path))
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

func GenericProxy(targetServiceURL string, logger *logging.Logger, stripPrefix string) gin.HandlerFunc {
	target, err := url.Parse(targetServiceURL)
	if err != nil {
		logger.Fatal("Invalid target URL for GenericProxy", zap.Error(err))
	}
	proxy := httputil.NewSingleHostReverseProxy(target)

	proxy.Director = func(req *http.Request) {
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.Error("GenericProxy error", zap.Error(err), zap.String("target", targetServiceURL))
		rw.WriteHeader(http.StatusBadGateway)
	}

	return func(c *gin.Context) {
		otel.GetTextMapPropagator().Inject(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		if userID, ok := c.Get("userID"); ok {
			c.Request.Header.Set("X-User-ID", userID.(string))
		}

		path := c.Param("path")
		if stripPrefix != "" {
			if len(path) > 0 && path[0] != '/' {
				path = "/" + path
			}
			c.Request.URL.Path = path
		}

		logger.Info("Forwarding request",
			zap.String("target", targetServiceURL),
			zap.String("original_path", c.Request.URL.Path),
			zap.String("forwarded_path", path))

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}
