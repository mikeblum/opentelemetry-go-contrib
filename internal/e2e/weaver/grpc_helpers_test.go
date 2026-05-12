// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package weaver_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

// newGRPCTestClient starts an in-process gRPC server backed by a minimal TestService implementation.
func newGRPCTestClient(t *testing.T, srvOpts []grpc.ServerOption, dialOpts []grpc.DialOption) pb.TestServiceClient {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(srvOpts...)
	pb.RegisterTestServiceServer(srv, grpcTestServer{})
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewTestServiceClient(conn)
}

type grpcTestServer struct {
	pb.UnimplementedTestServiceServer
}

func (grpcTestServer) EmptyCall(_ context.Context, _ *pb.Empty) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
