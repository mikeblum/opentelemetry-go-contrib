// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package weaver_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/contrib/instrumentation/host"
)

var _ WeaverLiveCheck = hostCheck{}

type hostCheck struct{}

func (hostCheck) Name() string { return "host" }

func (hostCheck) Exercise(t *testing.T, _ context.Context, _ trace.TracerProvider, mp metric.MeterProvider) {
	t.Helper()

	if err := host.Start(host.WithMeterProvider(mp)); err != nil {
		t.Fatalf("start host instrumentation: %v", err)
	}
	// Allow at least one collection interval to fire before OTLP shutdown.
	time.Sleep(600 * time.Millisecond)
}
