package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	pb "github.com/klippa-app/keda-celery-scaler/externalscaler"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlowerWorker struct {
	Active       []interface{} `json:"active"`
	ActiveQueues []struct {
		Name string `json:"name"`
	} `json:"active_queues"`
	Stats struct {
		Pool struct {
			MaxConcurrency int `json:"max-concurrency"`
		} `json:"pool"`
	}
}

type FlowerWorkerResult = map[string]FlowerWorker
type FlowerWorkerStatusResult = map[string]bool

type FlowerQueueLength struct {
	ActiveQueues []struct {
		Name     string `json:"name"`
		Messages int    `json:"messages"`
	} `json:"active_queues"`
}

type ExternalScaler struct{}

func (e *ExternalScaler) IsActive(ctx context.Context, scaledObject *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	flowerAddress := scaledObject.ScalerMetadata["flowerAddress"]
	queue := scaledObject.ScalerMetadata["queue"]

	if flowerAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "flowerAddress must be specified")
	}

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

	load, err := getLoad(flowerAddress, queue)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.IsActiveResponse{
		Result: load >= activationLoadValue,
	}, nil
}

var flowerClient = http.Client{}

func getLoad(flowerAddress, queue string) (int64, error) {
	totalWorkersAvailable, totalActiveTasks, err := getQueueWorkers(flowerAddress, queue)
	if err != nil {
		return 0, err
	}

	queueLength, err := getQueueLength(flowerAddress, queue)
	if err != nil {
		return 0, err
	}

	load := float64(totalActiveTasks+queueLength) / float64(totalWorkersAvailable)
	return int64(load * float64(100)), nil
}

func getQueueWorkers(flowerAddress, queue string) (int64, int64, error) {
	// @too: decide whether we want to keep refresh.
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/workers?refresh=1", flowerAddress), nil)
	if err != nil {
		return 0, 0, err
	}

	resp, err := flowerClient.Do(req)
	if err != nil {
		return 0, 0, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, status.Error(codes.Internal, err.Error())
	}

	payload := FlowerWorkerResult{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		return 0, 0, status.Error(codes.Internal, err.Error())
	}

	workerStatus, err := getQueueWorkerStatus(flowerAddress)
	if err != nil {
		return 0, 0, status.Error(codes.Internal, err.Error())
	}

	totalWorkersAvailable := int64(0)
	totalActiveTasks := int64(0)

	// Loop through all available workers.
	for worker := range payload {
		if !(*workerStatus)[worker] {
			continue
		}

		// Only count the workers that are listening to our queue.
		shouldCountWorker := false
		for i := range payload[worker].ActiveQueues {
			if payload[worker].ActiveQueues[i].Name == queue {
				shouldCountWorker = true
				break
			}
		}

		if shouldCountWorker {
			totalActiveTasks += int64(len(payload[worker].Active))
			totalWorkersAvailable += int64(payload[worker].Stats.Pool.MaxConcurrency)
		}
	}

	return totalWorkersAvailable, totalActiveTasks, nil
}

func getQueueWorkerStatus(flowerAddress string) (*FlowerWorkerStatusResult, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/workers?status=1", flowerAddress), nil)
	if err != nil {
		return nil, err
	}

	resp, err := flowerClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	payload := FlowerWorkerStatusResult{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &payload, nil
}

func getQueueLength(flowerAddress, queue string) (int64, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/queues/length", flowerAddress), nil)
	if err != nil {
		return 0, err
	}

	resp, err := flowerClient.Do(req)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, status.Error(codes.Internal, err.Error())
	}

	payload := FlowerQueueLength{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		return 0, status.Error(codes.Internal, err.Error())
	}

	for i := range payload.ActiveQueues {
		if payload.ActiveQueues[i].Name == queue {
			return int64(payload.ActiveQueues[i].Messages), nil
		}
	}

	return 0, nil
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

func (e *ExternalScaler) GetMetrics(_ context.Context, metricRequest *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	flowerAddress := metricRequest.ScaledObjectRef.ScalerMetadata["flowerAddress"]
	queue := metricRequest.ScaledObjectRef.ScalerMetadata["queue"]

	if flowerAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "flowerAddress must be specified")
	}

	// Set default queue.
	if queue == "" {
		queue = "celery"
	}

	load, err := getLoad(flowerAddress, queue)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

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
	grpcServer := grpc.NewServer()
	lis, _ := net.Listen("tcp", ":6000")
	pb.RegisterExternalScalerServer(grpcServer, &ExternalScaler{})

	fmt.Println("listenting on :6000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

// Helper function to check load without KEDA.
func checker() {
	ticker := time.NewTicker(500 * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				load, err := getLoad("http://127.0.0.1:8888", "celery")
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("The current load is %d", load)
			}
		}
	}()
}