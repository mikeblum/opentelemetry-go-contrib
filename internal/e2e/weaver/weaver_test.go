// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package weaver_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	gRPCPort    = "4317/tcp"
	weaverImage = "otel/weaver"
	weaverTag   = "v0.23.0"
)

// WeaverLiveCheck is implemented by each instrumentation to exercise its
// telemetry and generate a weaver live-check report.
type WeaverLiveCheck interface {
	// Name is used as the t.Run subtest name and the testdata subdirectory.
	Name() string
	// Exercise generates telemetry through the instrumentation under test.
	// tp and mp are scoped to a single container run — do not use global providers.
	Exercise(t *testing.T, ctx context.Context, tp trace.TracerProvider, mp metric.MeterProvider)
}

var checks = []WeaverLiveCheck{
	otelHTTPCheck{},
	otelgrpcCheck{},
	hostCheck{},
}

func TestWeaverLiveCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping weaver live-check in short mode")
	}
	pool := setupDockerPool(t)

	for _, check := range checks {
		t.Run(check.Name(), func(t *testing.T) {
			t.Parallel()
			reports := runWeaverCheck(t, pool, check)
			reports = normalizeReports(reports)
			outDir := filepath.Join(reportsBaseDir(), check.Name())
			writeReports(t, reports, outDir)
		})
	}
}

func setupDockerPool(t *testing.T) *dockertest.Pool {
	t.Helper()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("skipping: docker not available: %v", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("skipping: docker daemon not reachable: DOCKER_HOST=%q: %v", os.Getenv("DOCKER_HOST"), err)
	}
	pool.MaxWait = 2 * time.Minute
	return pool
}

func reportsBaseDir() string {
	if d := os.Getenv("WEAVER_REPORTS_DIR"); d != "" {
		return d
	}
	return "testdata"
}

func runWeaverCheck(t *testing.T, pool *dockertest.Pool, check WeaverLiveCheck) map[string]string {
	t.Helper()

	reportsDir := t.TempDir()
	if err := os.Chmod(reportsDir, 0o777); err != nil {
		t.Fatalf("chmod reports dir: %v", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: weaverImage,
		Tag:        weaverTag,
		Cmd: []string{
			"registry", "live-check",
			"--format", "json",
			"--output", "/reports",
			"--inactivity-timeout", "30", // seconds
		},
		ExposedPorts: []string{gRPCPort},
	}, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		config.Binds = []string{reportsDir + ":/reports"}
	})
	if err != nil {
		t.Fatalf("start weaver container: %v", err)
	}
	t.Cleanup(func() {
		if purgeErr := pool.Purge(resource); purgeErr != nil {
			t.Logf("purge weaver container: %v", purgeErr)
		}
	})

	waitLogs := streamWeaverLogs(t, pool, resource.Container.ID, check.Name())

	otlpEndpoint := fmt.Sprintf("localhost:%s", resource.GetPort(gRPCPort))
	t.Logf("weaver OTLP endpoint: %s", otlpEndpoint)

	ctx := t.Context()

	// Probe until the gRPC service is Ready, not just the TCP port.
	// Weaver binds the port before the HTTP/2 layer is initialized, so a
	// plain TCP dial would pass too early and produce "server preface" errors.
	if err := pool.Retry(func() error {
		probeConn, err := grpc.NewClient(
			otlpEndpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return err
		}
		defer probeConn.Close()
		probeConn.Connect()
		probeCtx, probeCancel := context.WithTimeout(ctx, 2*time.Second)
		defer probeCancel()
		for {
			state := probeConn.GetState()
			if state == connectivity.Ready {
				return nil
			}
			if !probeConn.WaitForStateChange(probeCtx, state) {
				return probeCtx.Err()
			}
		}
	}); err != nil {
		t.Fatalf("weaver OTLP gRPC not ready: %v", err)
	}

	tp, mp, shutdown, err := initOTLP(ctx, otlpEndpoint)
	if err != nil {
		t.Fatalf("init OTLP: %v", err)
	}
	defer func() {
		fn := shutdown
		shutdown = nil // prevent double-call if explicit shutdown below fails
		if fn == nil {
			return
		}
		if shutdownErr := fn(ctx); shutdownErr != nil {
			t.Logf("shutdown OTLP: %v", shutdownErr)
		}
	}()

	check.Exercise(t, ctx, tp, mp)

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown OTLP: %v", err)
	}
	shutdown = nil

	exitCode, err := waitForWeaver(ctx, pool, resource.Container.ID)
	waitLogs()
	if err != nil {
		t.Fatalf("wait for weaver live-check: %v", err)
	}
	t.Logf("weaver: live-check exit code: %d", exitCode)

	reports, err := readWeaverReports(reportsDir)
	if err != nil {
		t.Fatalf("read weaver reports: %v", err)
	}
	if len(reports) == 0 {
		t.Fatal("weaver did not produce any JSON reports")
	}
	return reports
}

func initOTLP(ctx context.Context, endpoint string) (trace.TracerProvider, metric.MeterProvider, func(context.Context) error, error) {
	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp))

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, nil, nil, fmt.Errorf("metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
	)

	return tp, mp, func(c context.Context) error {
		tpErr := tp.Shutdown(c)
		mpErr := mp.Shutdown(c)
		if tpErr != nil {
			return fmt.Errorf("trace provider: %w", tpErr)
		}
		if mpErr != nil {
			return fmt.Errorf("metric provider: %w", mpErr)
		}
		return nil
	}, nil
}

func waitForWeaver(ctx context.Context, pool *dockertest.Pool, containerID string) (int, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	exitCode, err := pool.Client.WaitContainerWithContext(containerID, waitCtx)
	if err == nil {
		return exitCode, nil
	}
	if waitCtx.Err() == nil {
		return 0, err
	}

	// Give weaver a chance to finish and write reports before forcing cleanup.
	hupCtx, hupCancel := context.WithTimeout(ctx, 5*time.Second)
	defer hupCancel()
	if killErr := pool.Client.KillContainer(docker.KillContainerOptions{
		ID:      containerID,
		Signal:  docker.SIGHUP,
		Context: hupCtx,
	}); killErr != nil {
		return 0, fmt.Errorf("timed out waiting for inactivity shutdown; send SIGHUP: %w", killErr)
	}

	finalCtx, finalCancel := context.WithTimeout(ctx, 15*time.Second)
	defer finalCancel()
	exitCode, err = pool.Client.WaitContainerWithContext(containerID, finalCtx)
	if err != nil {
		return 0, fmt.Errorf("timed out waiting for inactivity shutdown; wait after SIGHUP: %w", err)
	}
	return exitCode, nil
}

func streamWeaverLogs(t *testing.T, pool *dockertest.Pool, containerID, name string) func() {
	t.Helper()
	logPR, logPW := io.Pipe()
	var wg sync.WaitGroup
	wg.Go(func() {
		scanner := bufio.NewScanner(logPR)
		for scanner.Scan() {
			t.Logf("weaver(%s): %s", name, scanner.Text())
		}
	})
	wg.Go(func() {
		defer logPW.Close()
		_ = pool.Client.Logs(docker.LogsOptions{
			Context:      t.Context(),
			Container:    containerID,
			OutputStream: logPW,
			ErrorStream:  logPW,
			Stdout:       true,
			Stderr:       true,
			Follow:       true,
		})
	})
	return wg.Wait
}

func readWeaverReports(dir string) (map[string]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	reports := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		reports[entry.Name()] = string(data)
	}
	return reports, nil
}

func writeReports(t *testing.T, reports map[string]string, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create reports output dir %s: %v", dir, err)
	}
	for name, content := range reports {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write report %s: %v", path, err)
		}
		t.Logf("weaver: report: %s", path)
	}
}

// normalizeReports stabilizes all reports for committed golden files.
func normalizeReports(reports map[string]string) map[string]string {
	out := make(map[string]string, len(reports))
	for name, content := range reports {
		normalized, err := normalizeReport([]byte(content))
		if err != nil {
			out[name] = content
			continue
		}
		out[name] = string(normalized)
	}
	return out
}

// normalizeReport produces a deterministic JSON representation of a weaver live-check report
func normalizeReport(data []byte) ([]byte, error) {
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	normalizeSamples(root)
	redactServiceName(root)
	return json.MarshalIndent(root, "", "  ")
}

// sampleTypeOrder defines canonical ordering: resources first, then spans, then metrics.
var sampleTypeOrder = map[string]int{"resource": 0, "span": 1, "metric": 2}

func normalizeSamples(root map[string]any) {
	samples, ok := root["samples"].([]any)
	if !ok {
		return
	}
	for _, s := range samples {
		sample, ok := s.(map[string]any)
		if !ok {
			continue
		}
		for _, v := range sample {
			if entity, ok := v.(map[string]any); ok {
				sortAttributes(entity)
			}
		}
	}
	sort.SliceStable(samples, func(i, j int) bool {
		ti, ni := sampleKey(samples[i])
		tj, nj := sampleKey(samples[j])
		if ti != tj {
			return ti < tj
		}
		return ni < nj
	})
	root["samples"] = samples
}

func sampleKey(s any) (typeOrder int, name string) {
	sample, ok := s.(map[string]any)
	if !ok {
		return 99, ""
	}
	for k, v := range sample {
		order, known := sampleTypeOrder[k]
		if !known {
			return 99, k
		}
		if entity, ok := v.(map[string]any); ok {
			if n, ok := entity["name"].(string); ok {
				return order, n
			}
		}
		return order, ""
	}
	return 99, ""
}

func sortAttributes(entity map[string]any) {
	attrs, ok := entity["attributes"].([]any)
	if !ok {
		return
	}
	sort.SliceStable(attrs, func(i, j int) bool {
		ai, _ := attrs[i].(map[string]any)
		aj, _ := attrs[j].(map[string]any)
		ni, _ := ai["name"].(string)
		nj, _ := aj["name"].(string)
		return ni < nj
	})
}

func redactServiceName(root map[string]any) {
	samples, ok := root["samples"].([]any)
	if !ok {
		return
	}
	for _, s := range samples {
		sample, ok := s.(map[string]any)
		if !ok {
			continue
		}
		resource, ok := sample["resource"].(map[string]any)
		if !ok {
			continue
		}
		attrs, ok := resource["attributes"].([]any)
		if !ok {
			continue
		}
		for _, a := range attrs {
			attr, ok := a.(map[string]any)
			if !ok {
				continue
			}
			if attr["name"] == "service.name" {
				if v, ok := attr["value"].(string); ok && strings.HasPrefix(v, "unknown_service:") {
					attr["value"] = "unknown_service"
				}
			}
		}
	}
}
