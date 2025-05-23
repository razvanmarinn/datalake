// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: lb.proto

package protobuf

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	LBService_GetWorkerID_FullMethodName = "/lb_dfs.LBService/GetWorkerID"
)

// LBServiceClient is the client API for LBService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type LBServiceClient interface {
	GetWorkerID(ctx context.Context, in *WorkerIDRequest, opts ...grpc.CallOption) (*WorkerIDResponse, error)
}

type lBServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewLBServiceClient(cc grpc.ClientConnInterface) LBServiceClient {
	return &lBServiceClient{cc}
}

func (c *lBServiceClient) GetWorkerID(ctx context.Context, in *WorkerIDRequest, opts ...grpc.CallOption) (*WorkerIDResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(WorkerIDResponse)
	err := c.cc.Invoke(ctx, LBService_GetWorkerID_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LBServiceServer is the server API for LBService service.
// All implementations must embed UnimplementedLBServiceServer
// for forward compatibility.
type LBServiceServer interface {
	GetWorkerID(context.Context, *WorkerIDRequest) (*WorkerIDResponse, error)
	mustEmbedUnimplementedLBServiceServer()
}

// UnimplementedLBServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedLBServiceServer struct{}

func (UnimplementedLBServiceServer) GetWorkerID(context.Context, *WorkerIDRequest) (*WorkerIDResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkerID not implemented")
}
func (UnimplementedLBServiceServer) mustEmbedUnimplementedLBServiceServer() {}
func (UnimplementedLBServiceServer) testEmbeddedByValue()                   {}

// UnsafeLBServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to LBServiceServer will
// result in compilation errors.
type UnsafeLBServiceServer interface {
	mustEmbedUnimplementedLBServiceServer()
}

func RegisterLBServiceServer(s grpc.ServiceRegistrar, srv LBServiceServer) {
	// If the following call pancis, it indicates UnimplementedLBServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&LBService_ServiceDesc, srv)
}

func _LBService_GetWorkerID_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WorkerIDRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LBServiceServer).GetWorkerID(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LBService_GetWorkerID_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LBServiceServer).GetWorkerID(ctx, req.(*WorkerIDRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// LBService_ServiceDesc is the grpc.ServiceDesc for LBService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var LBService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "lb_dfs.LBService",
	HandlerType: (*LBServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetWorkerID",
			Handler:    _LBService_GetWorkerID_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "lb.proto",
}
