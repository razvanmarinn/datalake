// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: worker.proto

package proto

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
	BatchReceiverService_ReceiveBatch_FullMethodName           = "/worker_dfs.BatchReceiverService/ReceiveBatch"
	BatchReceiverService_RetrieveBatchForClient_FullMethodName = "/worker_dfs.BatchReceiverService/RetrieveBatchForClient"
)

// BatchReceiverServiceClient is the client API for BatchReceiverService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// BatchReceiverService handles batch operations between clients and workers
type BatchReceiverServiceClient interface {
	ReceiveBatch(ctx context.Context, in *ClientRequestToWorker, opts ...grpc.CallOption) (*WorkerResponse, error)
	RetrieveBatchForClient(ctx context.Context, in *ClientRequestToWorker, opts ...grpc.CallOption) (*WorkerBatchResponse, error)
}

type batchReceiverServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBatchReceiverServiceClient(cc grpc.ClientConnInterface) BatchReceiverServiceClient {
	return &batchReceiverServiceClient{cc}
}

func (c *batchReceiverServiceClient) ReceiveBatch(ctx context.Context, in *ClientRequestToWorker, opts ...grpc.CallOption) (*WorkerResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(WorkerResponse)
	err := c.cc.Invoke(ctx, BatchReceiverService_ReceiveBatch_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *batchReceiverServiceClient) RetrieveBatchForClient(ctx context.Context, in *ClientRequestToWorker, opts ...grpc.CallOption) (*WorkerBatchResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(WorkerBatchResponse)
	err := c.cc.Invoke(ctx, BatchReceiverService_RetrieveBatchForClient_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BatchReceiverServiceServer is the server API for BatchReceiverService service.
// All implementations must embed UnimplementedBatchReceiverServiceServer
// for forward compatibility.
//
// BatchReceiverService handles batch operations between clients and workers
type BatchReceiverServiceServer interface {
	ReceiveBatch(context.Context, *ClientRequestToWorker) (*WorkerResponse, error)
	RetrieveBatchForClient(context.Context, *ClientRequestToWorker) (*WorkerBatchResponse, error)
	mustEmbedUnimplementedBatchReceiverServiceServer()
}

// UnimplementedBatchReceiverServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedBatchReceiverServiceServer struct{}

func (UnimplementedBatchReceiverServiceServer) ReceiveBatch(context.Context, *ClientRequestToWorker) (*WorkerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReceiveBatch not implemented")
}
func (UnimplementedBatchReceiverServiceServer) RetrieveBatchForClient(context.Context, *ClientRequestToWorker) (*WorkerBatchResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RetrieveBatchForClient not implemented")
}
func (UnimplementedBatchReceiverServiceServer) mustEmbedUnimplementedBatchReceiverServiceServer() {}
func (UnimplementedBatchReceiverServiceServer) testEmbeddedByValue()                              {}

// UnsafeBatchReceiverServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BatchReceiverServiceServer will
// result in compilation errors.
type UnsafeBatchReceiverServiceServer interface {
	mustEmbedUnimplementedBatchReceiverServiceServer()
}

func RegisterBatchReceiverServiceServer(s grpc.ServiceRegistrar, srv BatchReceiverServiceServer) {
	// If the following call pancis, it indicates UnimplementedBatchReceiverServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&BatchReceiverService_ServiceDesc, srv)
}

func _BatchReceiverService_ReceiveBatch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClientRequestToWorker)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BatchReceiverServiceServer).ReceiveBatch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BatchReceiverService_ReceiveBatch_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BatchReceiverServiceServer).ReceiveBatch(ctx, req.(*ClientRequestToWorker))
	}
	return interceptor(ctx, in, info, handler)
}

func _BatchReceiverService_RetrieveBatchForClient_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClientRequestToWorker)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BatchReceiverServiceServer).RetrieveBatchForClient(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BatchReceiverService_RetrieveBatchForClient_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BatchReceiverServiceServer).RetrieveBatchForClient(ctx, req.(*ClientRequestToWorker))
	}
	return interceptor(ctx, in, info, handler)
}

// BatchReceiverService_ServiceDesc is the grpc.ServiceDesc for BatchReceiverService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BatchReceiverService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "worker_dfs.BatchReceiverService",
	HandlerType: (*BatchReceiverServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReceiveBatch",
			Handler:    _BatchReceiverService_ReceiveBatch_Handler,
		},
		{
			MethodName: "RetrieveBatchForClient",
			Handler:    _BatchReceiverService_RetrieveBatchForClient_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "worker.proto",
}
