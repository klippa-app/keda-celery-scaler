package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/klippa-app/keda-celery-scaler/externalscaler"

	"github.com/go-chi/stampede"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	reqCache = stampede.NewCache(512, time.Second*5, 0)
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

var FlowerAddress = ""

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

var flowerClient = http.Client{
	Timeout: time.Second * 30,
}

func getLoad(ctx context.Context, queue string) (int64, error) {
	totalWorkersAvailable, totalActiveTasks, err := getQueueWorkers(ctx, queue)
	if err != nil {
		return 0, err
	}

	queueLength, err := getQueueLength(ctx, queue)
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

func loadWorkerInfo(ctx context.Context) (interface{}, error) {
	// We need to add refresh so that we always have the latest info.
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/workers?refresh=1", FlowerAddress), nil)
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

	payload := FlowerWorkerResult{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return payload, nil
}

func getQueueWorkers(ctx context.Context, queue string) (int64, int64, error) {
	start := time.Now()

	workerInfoData, err := reqCache.Get(ctx, "worker_info", loadWorkerInfo)
	if err != nil {
		return 0, 0, status.Error(codes.Internal, err.Error())
	}

	log.Tracef("Fetching worker info for queue %s took %s", queue, time.Since(start).String())

	workerInfo := workerInfoData.(FlowerWorkerResult)

	workerStatus, err := getQueueWorkerStatus(ctx, queue)
	if err != nil {
		return 0, 0, status.Error(codes.Internal, err.Error())
	}

	totalWorkersAvailable := int64(0)
	totalActiveTasks := int64(0)

	// Loop through all available workers.
	for worker := range workerInfo {
		if !(*workerStatus)[worker] {
			continue
		}

		// Only count the workers that are listening to our queue.
		shouldCountWorker := false
		for i := range workerInfo[worker].ActiveQueues {
			if workerInfo[worker].ActiveQueues[i].Name == queue {
				shouldCountWorker = true
				break
			}
		}

		if shouldCountWorker {
			totalActiveTasks += int64(len(workerInfo[worker].Active))
			totalWorkersAvailable += int64(workerInfo[worker].Stats.Pool.MaxConcurrency)
		}
	}

	log.Tracef("Calculating worker and task counts for queue %s took %s", queue, time.Since(start).String())

	return totalWorkersAvailable, totalActiveTasks, nil
}

func loadQueueWorkerStatus(ctx context.Context) (interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/workers?status=1", FlowerAddress), nil)
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

	return payload, nil
}

func getQueueWorkerStatus(ctx context.Context, queue string) (*FlowerWorkerStatusResult, error) {
	start := time.Now()

	queueWorkerStatusData, err := reqCache.Get(ctx, "worker_stats", loadQueueWorkerStatus)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	queueWorkerStatus := queueWorkerStatusData.(FlowerWorkerStatusResult)

	log.Tracef("Fetching worker status for queue %s took %s", queue, time.Since(start).String())

	return &queueWorkerStatus, nil
}

func getQueueLength(ctx context.Context, queue string) (int64, error) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/queues/length", FlowerAddress), nil)
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

	log.Tracef("Fetching queue length for queue %s took %s", queue, time.Since(start).String())

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
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		parsedLogLevel, err := log.ParseLevel(logLevel)
		if err == nil {
			log.SetLevel(parsedLogLevel)
		} else {
			log.Warnf("invalid log level given: %s", logLevel)
		}
	}

	FlowerAddress = os.Getenv("FLOWER_ADDRESS")
	FlowerAddress = strings.TrimSpace(FlowerAddress)
	if FlowerAddress == "" {
		log.Fatal("Env variable FLOWER_ADDRESS needs to be set to a valid URL, value is empty")
	}

	_, err := url.Parse(FlowerAddress)
	if err != nil {
		log.Fatalf("Env variable FLOWER_ADDRESS needs to be set to a valid URL: %s", err.Error())
	}

	address := ":6000"
	grpcServer := grpc.NewServer()
	lis, _ := net.Listen("tcp", address)
	pb.RegisterExternalScalerServer(grpcServer, &ExternalScaler{})

	log.Infof("listenting on %s", address)
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
				FlowerAddress = "http://127.0.0.1:8888"
				load, err := getLoad(context.Background(), "celery")
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("The current load is %d", load)
			}
		}
	}()
}
