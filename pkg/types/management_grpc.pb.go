// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package types

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion7

// ManagementClient is the client API for Management service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ManagementClient interface {
	CreateWorkerToken(ctx context.Context, in *CreateWorkerTokenRequest, opts ...grpc.CallOption) (*WorkerToken, error)
}

type managementClient struct {
	cc grpc.ClientConnInterface
}

func NewManagementClient(cc grpc.ClientConnInterface) ManagementClient {
	return &managementClient{cc}
}

func (c *managementClient) CreateWorkerToken(ctx context.Context, in *CreateWorkerTokenRequest, opts ...grpc.CallOption) (*WorkerToken, error) {
	out := new(WorkerToken)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Management/CreateWorkerToken", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ManagementServer is the server API for Management service.
// All implementations must embed UnimplementedManagementServer
// for forward compatibility
type ManagementServer interface {
	CreateWorkerToken(context.Context, *CreateWorkerTokenRequest) (*WorkerToken, error)
	mustEmbedUnimplementedManagementServer()
}

// UnimplementedManagementServer must be embedded to have forward compatible implementations.
type UnimplementedManagementServer struct {
}

func (UnimplementedManagementServer) CreateWorkerToken(context.Context, *CreateWorkerTokenRequest) (*WorkerToken, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateWorkerToken not implemented")
}
func (UnimplementedManagementServer) mustEmbedUnimplementedManagementServer() {}

// UnsafeManagementServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ManagementServer will
// result in compilation errors.
type UnsafeManagementServer interface {
	mustEmbedUnimplementedManagementServer()
}

func RegisterManagementServer(s grpc.ServiceRegistrar, srv ManagementServer) {
	s.RegisterService(&_Management_serviceDesc, srv)
}

func _Management_CreateWorkerToken_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateWorkerTokenRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).CreateWorkerToken(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Management/CreateWorkerToken",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).CreateWorkerToken(ctx, req.(*CreateWorkerTokenRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Management_serviceDesc = grpc.ServiceDesc{
	ServiceName: "od2_network.hive.Management",
	HandlerType: (*ManagementServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateWorkerToken",
			Handler:    _Management_CreateWorkerToken_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "management.proto",
}