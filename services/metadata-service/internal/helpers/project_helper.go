package helpers

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/metadata-service/internal/db"
	"github.com/razvanmarinn/metadata-service/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func GetProjectsByUser(database *sql.DB, logger *logging.Logger, idClient pb.IdentityServiceClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		requestedUsername := c.Param("username")
		authID, _ := c.Get("userID")

		if requestedUsername != authID.(string) {
			c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden"})
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		userResp, err := idClient.GetUserInfo(ctx, &pb.GetUserInfoRequest{Username: requestedUsername})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
				return
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Identity service error"})
			return
		}

		projects, _ := db.GetProjects(database, requestedUsername)
		c.JSON(http.StatusOK, gin.H{
			"user-id":  userResp.UserId,
			"projects": projects,
		})
	}
}

func RegisterProject(database *sql.DB, logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		var project models.ProjectMetadata
		if err := c.ShouldBindJSON(&project); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid project data: " + err.Error()})
			return
		}

		if err := db.RegisterProject(database, &project); err != nil {
			logger.WithContext(c).Error("Failed to register project", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal database error"})
			return
		}

		logger.LogProjectRegistration(project.ProjectName, project.Owner)

		c.JSON(http.StatusCreated, gin.H{
			"message":      "Project registered successfully",
			"project_name": project.ProjectName,
		})
	}
}
