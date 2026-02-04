package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckProjectExists_ReturnsTrue(t *testing.T) {
	exists, err := CheckProjectExists(nil, "test-project")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckProjectExists_WithEmptyName(t *testing.T) {
	exists, err := CheckProjectExists(nil, "")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckProjectExists_WithSpecialCharacters(t *testing.T) {
	exists, err := CheckProjectExists(nil, "project-name_123")
	assert.NoError(t, err)
	assert.True(t, exists)
}
