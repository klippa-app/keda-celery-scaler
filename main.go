package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/klippa-app/keda-celery-scaler/brokers"
	pb "github.com/klippa-app/keda-celery-scaler/externalscaler"
	"github.com/klippa-app/keda-celery-scaler/workers"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ExternalScaler struct{}

func (e *ExternalScaler) IsActive(ctx context.Context, scaledObject *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	queue := scaledObject.ScalerMetadata["queue"]

	// Set default queue.
	if queue == "" {
		queue = "celery"
	}

	activationLoadValue := int64(0)
	activationLoadValueString := scaledObject.ScalerMetadata["activationLoadValue"]
	if activationLoadValueString != "" {
		activationLoadValueParsed, err := strconv.ParseInt(activationLoadValueString, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("could not parse activationLoadValue into an integer: %s", err.Error()))
		}

		activationLoadValue = activationLoadValueParsed
	}

	load, err := getLoad(ctx, queue)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.IsActiveResponse{
		Result: load >= activationLoadValue,
	}, nil
}

func getLoad(ctx context.Context, queue string) (int64, error) {
	totalWorkersAvailable, totalActiveTasks := workers.GetQueueWorkers(queue)
	queueLength, err := brokers.GetQueueLength(ctx, queue)
	if err != nil {
		return 0, err
	}

	if totalActiveTasks+queueLength == 0 {
		log.Tracef("Load info for queue %s: workers: %d, active tasks: %d, queue length: %d", queue, totalWorkersAvailable, totalActiveTasks, queueLength)

		return 0, nil
	}

	taskCount := float64(totalActiveTasks + queueLength)

	if totalWorkersAvailable == 0 {
		log.Tracef("Load info for queue %s: workers: %d, active tasks: %d, queue length: %d", queue, totalWorkersAvailable, totalActiveTasks, queueLength)

		return int64(taskCount * float64(100)), nil
	}

	load := taskCount / float64(totalWorkersAvailable)

	log.Tracef("Load info for queue %s: workers: %d, active tasks: %d, queue length: %d", queue, totalWorkersAvailable, totalActiveTasks, queueLength)

	return int64(load * float64(100)), nil
}

func (e *ExternalScaler) GetMetricSpec(ctx context.Context, scaledObject *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	// By default, scale at a load of 100 (1.0, all workers taken for the queue)
	scaleLoadValue := int64(100)
	scaleLoadValueString := scaledObject.ScalerMetadata["scaleLoadValue"]
	if scaleLoadValueString != "" {
		scaleLoadValueParsed, err := strconv.ParseInt(scaleLoadValueString, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("could not parse scaleLoadValue into an integer: %s", err.Error()))
		}

		scaleLoadValue = scaleLoadValueParsed
	}

	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{
			MetricName: "workerLoad",
			TargetSize: scaleLoadValue,
		}},
	}, nil
}

func (e *ExternalScaler) GetMetrics(ctx context.Context, metricRequest *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	queue := metricRequest.ScaledObjectRef.ScalerMetadata["queue"]

	// Set default queue.
	if queue == "" {
		queue = "celery"
	}

	start := time.Now()

	load, err := getLoad(ctx, queue)
	if err != nil {
		log.Errorf("Could not load load for queue %s, error: %s", queue, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	log.Debugf("Calculating load for queue %s took %s, calculated load is %d", queue, time.Since(start).String(), load)

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{
			MetricName:  "workerLoad",
			MetricValue: load,
		}},
	}, nil
}

func (e *ExternalScaler) StreamIsActive(scaledObject *pb.ScaledObjectRef, epsServer pb.ExternalScaler_StreamIsActiveServer) error {
	// Only needed for external-push.
	return status.Error(codes.Unimplemented, "this scaler does not support StreamIsActive")
}

func main() {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("kcs")
	viper.AutomaticEnv()

	logLevel := viper.GetString("log_level")
	if logLevel != "" {
		parsedLogLevel, err := log.ParseLevel(logLevel)
		if err == nil {
			log.SetLevel(parsedLogLevel)
		} else {
			log.Warnf("invalid log level given: %s", logLevel)
		}
	}

	err := brokers.ConnectBroker()
	if err != nil {
		log.Fatalf("Could not connect to broker: %s", err.Error())
	}

	// Run worker cleaner task.
	go func() {
		// Clean every x seconds.
		workerCleanupInterval := viper.GetInt("worker_cleanup_interval")
		if workerCleanupInterval == 0 {
			workerCleanupInterval = 5
		}
		ticker := time.NewTicker(time.Duration(workerCleanupInterval) * time.Second)
		for {
			<-ticker.C
			workers.CleanupWorkers()
		}
	}()

	address := ":6000"
	grpcServer := grpc.NewServer()
	lis, _ := net.Listen("tcp", address)
	pb.RegisterExternalScalerServer(grpcServer, &ExternalScaler{})

	log.Infof("listenting on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Could not start GRPC listener on %s: %s", address, err.Error())
	}
}
