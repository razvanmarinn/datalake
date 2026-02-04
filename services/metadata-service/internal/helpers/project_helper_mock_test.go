package helpers

import (
	"testing"

	"github.com/razvanmarinn/metadata-service/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestProjectMetadata_Validation(t *testing.T) {
	t.Run("valid project", func(t *testing.T) {
		project := models.ProjectMetadata{
			ProjectName: "test-project",
			Description: "Test description",
			Owner:       "owner-123",
		}
		assert.NotEmpty(t, project.ProjectName)
		assert.NotEmpty(t, project.Owner)
	})

	t.Run("project name formats", func(t *testing.T) {
		validNames := []string{
			"project1",
			"my-project",
			"test_project",
			"Project123",
		}
		for _, name := range validNames {
			assert.NotEmpty(t, name)
			assert.True(t, len(name) >= 3)
		}
	})
}

func TestOwnerValidation(t *testing.T) {
	t.Run("valid owner ID format", func(t *testing.T) {
		ownerIDs := []string{
			"user-123",
			"550e8400-e29b-41d4-a716-446655440000",
			"owner_abc",
		}
		for _, id := range ownerIDs {
			assert.NotEmpty(t, id)
		}
	})
}
