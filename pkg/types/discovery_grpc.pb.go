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

// DiscoveryClient is the client API for Discovery service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DiscoveryClient interface {
	// ReportDiscovered reports related items.
	ReportDiscovered(ctx context.Context, in *ReportDiscoveredRequest, opts ...grpc.CallOption) (*ReportDiscoveredResponse, error)
}

type discoveryClient struct {
	cc grpc.ClientConnInterface
}

func NewDiscoveryClient(cc grpc.ClientConnInterface) DiscoveryClient {
	return &discoveryClient{cc}
}

func (c *discoveryClient) ReportDiscovered(ctx context.Context, in *ReportDiscoveredRequest, opts ...grpc.CallOption) (*ReportDiscoveredResponse, error) {
	out := new(ReportDiscoveredResponse)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Discovery/ReportDiscovered", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DiscoveryServer is the server API for Discovery service.
// All implementations must embed UnimplementedDiscoveryServer
// for forward compatibility
type DiscoveryServer interface {
	// ReportDiscovered reports related items.
	ReportDiscovered(context.Context, *ReportDiscoveredRequest) (*ReportDiscoveredResponse, error)
	mustEmbedUnimplementedDiscoveryServer()
}

// UnimplementedDiscoveryServer must be embedded to have forward compatible implementations.
type UnimplementedDiscoveryServer struct {
}

func (UnimplementedDiscoveryServer) ReportDiscovered(context.Context, *ReportDiscoveredRequest) (*ReportDiscoveredResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportDiscovered not implemented")
}
func (UnimplementedDiscoveryServer) mustEmbedUnimplementedDiscoveryServer() {}

// UnsafeDiscoveryServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DiscoveryServer will
// result in compilation errors.
type UnsafeDiscoveryServer interface {
	mustEmbedUnimplementedDiscoveryServer()
}

func RegisterDiscoveryServer(s grpc.ServiceRegistrar, srv DiscoveryServer) {
	s.RegisterService(&_Discovery_serviceDesc, srv)
}

func _Discovery_ReportDiscovered_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReportDiscoveredRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).ReportDiscovered(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Discovery/ReportDiscovered",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).ReportDiscovered(ctx, req.(*ReportDiscoveredRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Discovery_serviceDesc = grpc.ServiceDesc{
	ServiceName: "od2_network.hive.Discovery",
	HandlerType: (*DiscoveryServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReportDiscovered",
			Handler:    _Discovery_ReportDiscovered_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "discovery.proto",
}
