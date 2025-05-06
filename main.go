package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collectorProtobuf "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonProtobuf "go.opentelemetry.io/proto/otlp/common/v1"
	logsProtobuf "go.opentelemetry.io/proto/otlp/logs/v1"
	resourceProtobuf "go.opentelemetry.io/proto/otlp/resource/v1"
)

func spam(endpoint string, duration int64, step bool) {
	var after func()

	if step {
		after = func() {
			fmt.Println("Sent a log")
			time.Sleep(500 * time.Millisecond)
		}
	} else {
		after = func() {}
	}

	var ctx = context.Background()

	var conn, err = grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		fmt.Printf("Failed to dial: %v\n", err)
		return
	}

	defer conn.Close()

	var client = collectorProtobuf.NewLogsServiceClient(conn)

	var endTime = time.Now().Add(time.Duration(duration * 1000_000_000))

	for now := time.Now(); now.Before(endTime); {
		var logRecord = &logsProtobuf.LogRecord{
			TimeUnixNano:         uint64(now.UnixNano()),
			ObservedTimeUnixNano: uint64(now.UnixNano()),
			SeverityText:         "debug",
			Body: &commonProtobuf.AnyValue{
				Value: &commonProtobuf.AnyValue_StringValue{
					StringValue: "spam",
				},
			},
		}

		var resourceLogs = &logsProtobuf.ResourceLogs{
			Resource: &resourceProtobuf.Resource{
				Attributes: []*commonProtobuf.KeyValue{
					{
						Key: "job",
						Value: &commonProtobuf.AnyValue{
							Value: &commonProtobuf.AnyValue_StringValue{
								StringValue: "otel-spammer",
							},
						},
					},
				},
			},
			ScopeLogs: []*logsProtobuf.ScopeLogs{
				{
					Scope: &commonProtobuf.InstrumentationScope{
						Name:    "otel-spammer",
						Version: "0.1.0",
					},
					LogRecords: []*logsProtobuf.LogRecord{logRecord},
				},
			},
		}

		var request = &collectorProtobuf.ExportLogsServiceRequest{
			ResourceLogs: []*logsProtobuf.ResourceLogs{resourceLogs},
		}

		_, err = client.Export(ctx, request)

		if err != nil {
			fmt.Printf("Failed to export log: %v\n", err)
		}

		after()
	}
}

func main() {
	var nThreads = flag.Int("w", 1, "Number of HW/OS threads to spawn")
	var nConnections = flag.Int("c", 1, "Number of connections to keep open (~ # green threads)")
	var duration = flag.Int64("d", 60, "duration of spam in seconds")
	var endpoint = flag.String("endpoint", "localhost:4317", "endpoint to hit")
	var step = flag.Bool("step", false, "sleep after each log sent")

	flag.Parse()

	runtime.GOMAXPROCS(*nThreads)
	fmt.Printf("Using %v threads\n", *nThreads)

	if *nThreads > *nConnections {
		*nConnections = *nThreads
	}
	fmt.Printf("Using %v green threads\n", *nConnections)

	fmt.Printf("Enpoint set to: %v\n", *endpoint)
	fmt.Printf("Will run test for %v seconds\n", *duration)

	var wg sync.WaitGroup
	wg.Add(*nConnections)

	for range *nConnections {
		go func() {
			defer wg.Done()
			spam(*endpoint, *duration, *step)
		}()
	}

	wg.Wait()
}
