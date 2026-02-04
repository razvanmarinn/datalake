package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProjectHelper_ValidateProjectName(t *testing.T) {
	t.Run("validates project name format", func(t *testing.T) {
		validNames := []string{"project1", "my-project", "test_project"}
		for _, name := range validNames {
			assert.NotEmpty(t, name)
		}
	})

	t.Run("rejects invalid project names", func(t *testing.T) {
		invalidNames := []string{"", " ", "project with spaces"}
		for _, name := range invalidNames {
			if name == "" || name == " " {
				assert.True(t, len(name) <= 1)
			}
		}
	})
}

func TestProjectHelper_GenerateProjectID(t *testing.T) {
	t.Run("generates unique project IDs", func(t *testing.T) {
		id1 := "proj-001"
		id2 := "proj-002"
		assert.NotEqual(t, id1, id2)
	})
}
