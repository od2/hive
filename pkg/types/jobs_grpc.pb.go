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

// AssignmentsClient is the client API for Assignments service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type AssignmentsClient interface {
	// OpenAssignmentsStream opens a new gRPC assignments stream.
	// The assignments are load-balanced over all the active assignment streams.
	OpenAssignmentsStream(ctx context.Context, in *OpenAssignmentsStreamRequest, opts ...grpc.CallOption) (*OpenAssignmentsStreamResponse, error)
	// CloseAssignmentsStream closes a gRPC assignments stream.
	CloseAssignmentsStream(ctx context.Context, in *CloseAssignmentsStreamRequest, opts ...grpc.CallOption) (*CloseAssignmentsStreamResponse, error)
	// GetPendingAssignmentsCount returns the number of assignments that the server will push to the worker.
	// Calling WantAssignments increases this number.
	GetPendingAssignmentsCount(ctx context.Context, in *GetPendingAssignmentsCountRequest, opts ...grpc.CallOption) (*PendingAssignmentsCount, error)
	// WantAssignments tells the server to send more assignments.
	WantAssignments(ctx context.Context, in *WantAssignmentsRequest, opts ...grpc.CallOption) (*WantAssignmentsResponse, error)
	// StreamAssignments connects to the server-to-client assignment stream.
	StreamAssignments(ctx context.Context, in *StreamAssignmentsRequest, opts ...grpc.CallOption) (Assignments_StreamAssignmentsClient, error)
	// ReportAssignments reports about completed tasks.
	ReportAssignments(ctx context.Context, in *ReportAssignmentsRequest, opts ...grpc.CallOption) (*ReportAssignmentsResponse, error)
}

type assignmentsClient struct {
	cc grpc.ClientConnInterface
}

func NewAssignmentsClient(cc grpc.ClientConnInterface) AssignmentsClient {
	return &assignmentsClient{cc}
}

func (c *assignmentsClient) OpenAssignmentsStream(ctx context.Context, in *OpenAssignmentsStreamRequest, opts ...grpc.CallOption) (*OpenAssignmentsStreamResponse, error) {
	out := new(OpenAssignmentsStreamResponse)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Assignments/OpenAssignmentsStream", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *assignmentsClient) CloseAssignmentsStream(ctx context.Context, in *CloseAssignmentsStreamRequest, opts ...grpc.CallOption) (*CloseAssignmentsStreamResponse, error) {
	out := new(CloseAssignmentsStreamResponse)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Assignments/CloseAssignmentsStream", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *assignmentsClient) GetPendingAssignmentsCount(ctx context.Context, in *GetPendingAssignmentsCountRequest, opts ...grpc.CallOption) (*PendingAssignmentsCount, error) {
	out := new(PendingAssignmentsCount)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Assignments/GetPendingAssignmentsCount", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *assignmentsClient) WantAssignments(ctx context.Context, in *WantAssignmentsRequest, opts ...grpc.CallOption) (*WantAssignmentsResponse, error) {
	out := new(WantAssignmentsResponse)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Assignments/WantAssignments", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *assignmentsClient) StreamAssignments(ctx context.Context, in *StreamAssignmentsRequest, opts ...grpc.CallOption) (Assignments_StreamAssignmentsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Assignments_serviceDesc.Streams[0], "/od2_network.hive.Assignments/StreamAssignments", opts...)
	if err != nil {
		return nil, err
	}
	x := &assignmentsStreamAssignmentsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Assignments_StreamAssignmentsClient interface {
	Recv() (*AssignmentBatch, error)
	grpc.ClientStream
}

type assignmentsStreamAssignmentsClient struct {
	grpc.ClientStream
}

func (x *assignmentsStreamAssignmentsClient) Recv() (*AssignmentBatch, error) {
	m := new(AssignmentBatch)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *assignmentsClient) ReportAssignments(ctx context.Context, in *ReportAssignmentsRequest, opts ...grpc.CallOption) (*ReportAssignmentsResponse, error) {
	out := new(ReportAssignmentsResponse)
	err := c.cc.Invoke(ctx, "/od2_network.hive.Assignments/ReportAssignments", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AssignmentsServer is the server API for Assignments service.
// All implementations must embed UnimplementedAssignmentsServer
// for forward compatibility
type AssignmentsServer interface {
	// OpenAssignmentsStream opens a new gRPC assignments stream.
	// The assignments are load-balanced over all the active assignment streams.
	OpenAssignmentsStream(context.Context, *OpenAssignmentsStreamRequest) (*OpenAssignmentsStreamResponse, error)
	// CloseAssignmentsStream closes a gRPC assignments stream.
	CloseAssignmentsStream(context.Context, *CloseAssignmentsStreamRequest) (*CloseAssignmentsStreamResponse, error)
	// GetPendingAssignmentsCount returns the number of assignments that the server will push to the worker.
	// Calling WantAssignments increases this number.
	GetPendingAssignmentsCount(context.Context, *GetPendingAssignmentsCountRequest) (*PendingAssignmentsCount, error)
	// WantAssignments tells the server to send more assignments.
	WantAssignments(context.Context, *WantAssignmentsRequest) (*WantAssignmentsResponse, error)
	// StreamAssignments connects to the server-to-client assignment stream.
	StreamAssignments(*StreamAssignmentsRequest, Assignments_StreamAssignmentsServer) error
	// ReportAssignments reports about completed tasks.
	ReportAssignments(context.Context, *ReportAssignmentsRequest) (*ReportAssignmentsResponse, error)
	mustEmbedUnimplementedAssignmentsServer()
}

// UnimplementedAssignmentsServer must be embedded to have forward compatible implementations.
type UnimplementedAssignmentsServer struct {
}

func (UnimplementedAssignmentsServer) OpenAssignmentsStream(context.Context, *OpenAssignmentsStreamRequest) (*OpenAssignmentsStreamResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method OpenAssignmentsStream not implemented")
}
func (UnimplementedAssignmentsServer) CloseAssignmentsStream(context.Context, *CloseAssignmentsStreamRequest) (*CloseAssignmentsStreamResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CloseAssignmentsStream not implemented")
}
func (UnimplementedAssignmentsServer) GetPendingAssignmentsCount(context.Context, *GetPendingAssignmentsCountRequest) (*PendingAssignmentsCount, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPendingAssignmentsCount not implemented")
}
func (UnimplementedAssignmentsServer) WantAssignments(context.Context, *WantAssignmentsRequest) (*WantAssignmentsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WantAssignments not implemented")
}
func (UnimplementedAssignmentsServer) StreamAssignments(*StreamAssignmentsRequest, Assignments_StreamAssignmentsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamAssignments not implemented")
}
func (UnimplementedAssignmentsServer) ReportAssignments(context.Context, *ReportAssignmentsRequest) (*ReportAssignmentsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportAssignments not implemented")
}
func (UnimplementedAssignmentsServer) mustEmbedUnimplementedAssignmentsServer() {}

// UnsafeAssignmentsServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to AssignmentsServer will
// result in compilation errors.
type UnsafeAssignmentsServer interface {
	mustEmbedUnimplementedAssignmentsServer()
}

func RegisterAssignmentsServer(s grpc.ServiceRegistrar, srv AssignmentsServer) {
	s.RegisterService(&_Assignments_serviceDesc, srv)
}

func _Assignments_OpenAssignmentsStream_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OpenAssignmentsStreamRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AssignmentsServer).OpenAssignmentsStream(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Assignments/OpenAssignmentsStream",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AssignmentsServer).OpenAssignmentsStream(ctx, req.(*OpenAssignmentsStreamRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Assignments_CloseAssignmentsStream_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CloseAssignmentsStreamRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AssignmentsServer).CloseAssignmentsStream(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Assignments/CloseAssignmentsStream",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AssignmentsServer).CloseAssignmentsStream(ctx, req.(*CloseAssignmentsStreamRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Assignments_GetPendingAssignmentsCount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetPendingAssignmentsCountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AssignmentsServer).GetPendingAssignmentsCount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Assignments/GetPendingAssignmentsCount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AssignmentsServer).GetPendingAssignmentsCount(ctx, req.(*GetPendingAssignmentsCountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Assignments_WantAssignments_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WantAssignmentsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AssignmentsServer).WantAssignments(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Assignments/WantAssignments",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AssignmentsServer).WantAssignments(ctx, req.(*WantAssignmentsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Assignments_StreamAssignments_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StreamAssignmentsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(AssignmentsServer).StreamAssignments(m, &assignmentsStreamAssignmentsServer{stream})
}

type Assignments_StreamAssignmentsServer interface {
	Send(*AssignmentBatch) error
	grpc.ServerStream
}

type assignmentsStreamAssignmentsServer struct {
	grpc.ServerStream
}

func (x *assignmentsStreamAssignmentsServer) Send(m *AssignmentBatch) error {
	return x.ServerStream.SendMsg(m)
}

func _Assignments_ReportAssignments_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReportAssignmentsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AssignmentsServer).ReportAssignments(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/od2_network.hive.Assignments/ReportAssignments",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AssignmentsServer).ReportAssignments(ctx, req.(*ReportAssignmentsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Assignments_serviceDesc = grpc.ServiceDesc{
	ServiceName: "od2_network.hive.Assignments",
	HandlerType: (*AssignmentsServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "OpenAssignmentsStream",
			Handler:    _Assignments_OpenAssignmentsStream_Handler,
		},
		{
			MethodName: "CloseAssignmentsStream",
			Handler:    _Assignments_CloseAssignmentsStream_Handler,
		},
		{
			MethodName: "GetPendingAssignmentsCount",
			Handler:    _Assignments_GetPendingAssignmentsCount_Handler,
		},
		{
			MethodName: "WantAssignments",
			Handler:    _Assignments_WantAssignments_Handler,
		},
		{
			MethodName: "ReportAssignments",
			Handler:    _Assignments_ReportAssignments_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamAssignments",
			Handler:       _Assignments_StreamAssignments_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "jobs.proto",
}
