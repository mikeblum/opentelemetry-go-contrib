// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package weaver_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var _ WeaverLiveCheck = otelHTTPCheck{}

type otelHTTPCheck struct{}

func (otelHTTPCheck) Name() string { return "otelhttp" }

func (otelHTTPCheck) Exercise(t *testing.T, ctx context.Context, tp trace.TracerProvider, mp metric.MeterProvider) {
	t.Helper()

	handler := otelhttp.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}),
		"test-server",
		otelhttp.WithTracerProvider(tp),
		otelhttp.WithMeterProvider(mp),
	)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport,
			otelhttp.WithTracerProvider(tp),
			otelhttp.WithMeterProvider(mp),
		),
	}

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequestWithContext(ctx, method, srv.URL+"/test", http.NoBody)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("do request: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}
	}
}
