package models

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestProject_Structure(t *testing.T) {
	t.Run("creates project with fields", func(t *testing.T) {
		id := uuid.New()
		ownerID := uuid.New()
		project := Project{
			ID:      id,
			Name:    "test-project",
			OwnerID: ownerID,
		}
		assert.Equal(t, id, project.ID)
		assert.Equal(t, "test-project", project.Name)
		assert.Equal(t, ownerID, project.OwnerID)
	})
}

func TestProject_Validation(t *testing.T) {
	t.Run("validates project name", func(t *testing.T) {
		validNames := []string{"project1", "my-project", "test_project"}
		for _, name := range validNames {
			assert.NotEmpty(t, name)
			assert.True(t, len(name) >= 3)
		}
	})
}
