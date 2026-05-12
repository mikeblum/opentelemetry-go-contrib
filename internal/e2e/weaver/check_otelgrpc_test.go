// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package weaver_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/interop/grpc_testing"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

var _ WeaverLiveCheck = otelgrpcCheck{}

type otelgrpcCheck struct{}

func (otelgrpcCheck) Name() string { return "otelgrpc" }

func (otelgrpcCheck) Exercise(t *testing.T, ctx context.Context, tp trace.TracerProvider, mp metric.MeterProvider) {
	t.Helper()

	client := newGRPCTestClient(t,
		[]grpc.ServerOption{
			grpc.StatsHandler(otelgrpc.NewServerHandler(
				otelgrpc.WithTracerProvider(tp),
				otelgrpc.WithMeterProvider(mp),
			)),
		},
		[]grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(
				otelgrpc.WithTracerProvider(tp),
				otelgrpc.WithMeterProvider(mp),
			)),
		},
	)
	if _, err := client.EmptyCall(ctx, &pb.Empty{}); err != nil {
		t.Fatalf("empty call: %v", err)
	}
}
