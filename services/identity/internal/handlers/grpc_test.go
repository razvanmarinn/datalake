package handlers

import (
	"testing"

	identityv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/identity/v1"
	"github.com/stretchr/testify/assert"
)

func TestGRPCServer_Structure(t *testing.T) {
	t.Run("server can be created", func(t *testing.T) {
		server := &GRPCServer{
			DB:     nil,
			Logger: nil,
		}
		assert.NotNil(t, server)
	})
}

func TestGRPCServer_GetUserInfo(t *testing.T) {
	t.Run("handles request structure", func(t *testing.T) {
		req := &identityv1.GetUserInfoRequest{
			Username: "testuser",
		}
		assert.Equal(t, "testuser", req.GetUsername())
	})
}
