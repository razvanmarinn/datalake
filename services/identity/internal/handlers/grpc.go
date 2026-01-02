package handlers

import (
	"context"
	"database/sql"

	"github.com/razvanmarinn/datalake/pkg/logging"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/identity-service/internal/db"
)

type GRPCServer struct {
	pb.UnimplementedIdentityServiceServer
	DB     *sql.DB
	Logger *logging.Logger
}

func (s *GRPCServer) GetUserInfo(ctx context.Context, in *pb.GetUserInfoRequest) (*pb.GetUserInfoResponse, error) {
	user, _:= db.GetUser(s.DB, in.GetUsername())
	return &pb.GetUserInfoResponse{UserId: user.ID.String(), Email: user.Email}, nil
}
