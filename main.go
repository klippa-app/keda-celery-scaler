package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/klippa-app/keda-celery-scaler/externalscaler"

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

var flowerClient = http.Client{
	Timeout: time.Second * 30,
}

func getLoad(ctx context.Context, queue string) (int64, error) {
	totalWorkersAvailable, totalActiveTasks := getQueueWorkers(queue)
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

func getQueueWorkers(queue string) (int64, int64) {
	workerMapLock.Lock()
	defer workerMapLock.Unlock()

	start := time.Now()

	totalWorkersAvailable := int64(0)
	totalActiveTasks := int64(0)

	// Loop through all available workers.
	for worker := range celeryWorkers {

		// Only count the workers that are listening to our queue.
		shouldCountWorker := false
		for i := range celeryWorkers[worker].Queues {
			if celeryWorkers[worker].Queues[i] == queue {
				shouldCountWorker = true
				break
			}
		}

		if shouldCountWorker {
			totalActiveTasks += celeryWorkers[worker].Active
			totalWorkersAvailable += celeryWorkers[worker].Concurrency
		}
	}

	log.Tracef("Calculating worker and task counts for queue %s took %s", queue, time.Since(start).String())

	return totalWorkersAvailable, totalActiveTasks
}

func getQueueLength(ctx context.Context, queue string) (int64, error) {
	start := time.Now()

	listLength := RedisClient.LLen(ctx, queue)
	if listLength.Err() != nil {
		return 0, listLength.Err()
	}

	log.Tracef("Fetching queue length for queue %s took %s", queue, time.Since(start).String())

	return listLength.Val(), nil
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

	err := connectRedis()
	if err != nil {
		log.Fatalf("could not connect to redis: %s", err.Error())
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

var RedisClient *redis.Client

func connectRedis() error {
	var redisTLSConfig *tls.Config
	if viper.GetBool("Redis.TLS.Enabled") {
		redisTLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: viper.GetString("Redis.TLS.ServerName"),
		}
	}

	if viper.GetString("Redis.Type") == "sentinel" {
		sentinelServers := strings.Split(viper.GetString("Redis.Server"), ",")

		// Automatically calculate hostname.
		if redisTLSConfig != nil && redisTLSConfig.ServerName != "" && len(sentinelServers) > 0 {
			firstServerParts := strings.Split(sentinelServers[0], ":")
			firstServerName := firstServerParts[0]
			redisTLSConfig.ServerName = firstServerName
		}

		options := &redis.FailoverOptions{
			MasterName:       viper.GetString("Redis.Master"),
			SentinelUsername: viper.GetString("Redis.SentinelUsername"),
			SentinelPassword: viper.GetString("Redis.SentinelPassword"),
			Username:         viper.GetString("Redis.Username"),
			Password:         viper.GetString("Redis.Password"),
			DB:               viper.GetInt("Redis.DB"),
			SentinelAddrs:    sentinelServers,
			TLSConfig:        redisTLSConfig,
		}

		if viper.GetInt("Redis.Timeouts.Dial") > 0 {
			options.DialTimeout = time.Millisecond * time.Duration(viper.GetInt("Redis.Timeouts.Dial"))
		}

		if viper.GetInt("Redis.Timeouts.Read") > 0 {
			options.ReadTimeout = time.Millisecond * time.Duration(viper.GetInt("Redis.Timeouts.Read"))
		}

		if viper.GetInt("Redis.Timeouts.Write") > 0 {
			options.WriteTimeout = time.Millisecond * time.Duration(viper.GetInt("Redis.Timeouts.Write"))
		}

		if viper.GetInt("Redis.Connections.Min") > 0 {
			options.MinIdleConns = viper.GetInt("Redis.Connections.Min")
		}

		if viper.GetInt("Redis.Connections.Max") > 0 {
			options.PoolSize = viper.GetInt("Redis.Connections.Max")
		}

		RedisClient = redis.NewFailoverClient(options)
	} else {
		server := viper.GetString("Redis.Server")

		// Automatically calculate hostname.
		if redisTLSConfig != nil && redisTLSConfig.ServerName != "" {
			serverParts := strings.Split(server, ":")
			serverName := serverParts[0]
			redisTLSConfig.ServerName = serverName
		}

		options := &redis.Options{
			Addr:      server,
			Username:  viper.GetString("Redis.Username"),
			Password:  viper.GetString("Redis.Password"),
			DB:        viper.GetInt("Redis.DB"),
			TLSConfig: redisTLSConfig,
		}

		if viper.GetInt("Redis.Timeouts.Dial") > 0 {
			options.DialTimeout = time.Millisecond * time.Duration(viper.GetInt("Redis.Timeouts.Dial"))
		}

		if viper.GetInt("Redis.Timeouts.Read") > 0 {
			options.ReadTimeout = time.Millisecond * time.Duration(viper.GetInt("Redis.Timeouts.Read"))
		}

		if viper.GetInt("Redis.Timeouts.Write") > 0 {
			options.WriteTimeout = time.Millisecond * time.Duration(viper.GetInt("Redis.Timeouts.Write"))
		}

		if viper.GetInt("Redis.Connections.Min") > 0 {
			options.MinIdleConns = viper.GetInt("Redis.Connections.Min")
		}

		if viper.GetInt("Redis.Connections.Max") > 0 {
			options.PoolSize = viper.GetInt("Redis.Connections.Max")
		}

		RedisClient = redis.NewClient(options)
	}

	pong := RedisClient.Ping(context.Background())
	if pong.Err() != nil {
		return fmt.Errorf("could not connect to redis: %w", pong.Err())
	}

	// Listen for heartbeats and process them.
	pubsub := RedisClient.PSubscribe(context.Background(), "/*.celeryev/worker.heartbeat")
	go func() {
		ch := pubsub.Channel()
		for msg := range ch {
			processHeartbeat(msg)
		}
	}()

	// Run worker cleaner task.
	go func() {
		// Clean every x seconds.
		workerCleanupInterval := viper.GetInt("worker_cleanup_interval")
		if workerCleanupInterval == 0 {
			workerCleanupInterval = 5
		}
		ticker := time.NewTicker(time.Duration(workerCleanupInterval) * time.Second)
		for {
			select {
			case <-ticker.C:
				cleanupWorkers()
			}
		}
	}()

	return nil
}

type CeleryWorker struct {
	Hostname    string
	LastSeen    time.Time
	Queues      []string
	Concurrency int64
	Active      int64
}

type CeleryHeartbeat struct {
	Hostname    string                     `json:"hostname"`
	Utcoffset   int64                      `json:"utcoffset"`
	Pid         int64                      `json:"pid"`
	Clock       int64                      `json:"clock"`
	Freq        float64                    `json:"freq"`
	Active      int64                      `json:"active"`
	Processed   int64                      `json:"processed"`
	Loadavg     []float64                  `json:"loadavg"`
	SwIdent     string                     `json:"sw_ident"`
	SwVer       string                     `json:"sw_ver"`
	SwSys       string                     `json:"sw_sys"`
	Timestamp   float64                    `json:"timestamp"`
	Type        string                     `json:"type"`
	XWorkerInfo *CeleryHeartbeatWorkerInfo `json:"x_worker_info"`
}

type CeleryHeartbeatWorkerInfo struct {
	Queues      []string `json:"queues"`
	Concurrency int64    `json:"concurrency"`
}

var celeryWorkers = map[string]*CeleryWorker{}
var workerMapLock sync.Mutex

func updateWorker(worker CeleryHeartbeat) {
	workerMapLock.Lock()
	defer workerMapLock.Unlock()
	_, ok := celeryWorkers[worker.Hostname]
	if !ok {
		celeryWorkers[worker.Hostname] = &CeleryWorker{
			Hostname: worker.Hostname,
		}
	}

	celeryWorkers[worker.Hostname].LastSeen = time.Now()
	celeryWorkers[worker.Hostname].Active = worker.Active

	if worker.XWorkerInfo != nil {
		celeryWorkers[worker.Hostname].Queues = worker.XWorkerInfo.Queues
		celeryWorkers[worker.Hostname].Concurrency = worker.XWorkerInfo.Concurrency
	} else {
		// No worker info found, map queues from hostname and set concurrency to 1.
		celeryWorkers[worker.Hostname].Queues = getWorkerQueues(worker)
		celeryWorkers[worker.Hostname].Concurrency = 1
	}

	d, _ := json.Marshal(celeryWorkers[worker.Hostname])

	log.Debugf("Updated worker %s: %s", worker.Hostname, string(d))
}

func getWorkerQueues(worker CeleryHeartbeat) []string {
	workerQueueMapSetting := viper.GetString("worker_queue_map")
	if workerQueueMapSetting != "" {
		workerQueueMaps := strings.Split(workerQueueMapSetting, ";")
		for i := range workerQueueMaps {
			workerQueueMap := workerQueueMaps[i]
			workerQueueMapParts := strings.Split(workerQueueMap, ":")
			workerQueueMapIdentifier := workerQueueMapParts[0]
			workerQueueMapQueues := strings.Split(workerQueueMapParts[1], ",")
			if strings.Contains(worker.Hostname, workerQueueMapIdentifier) {
				log.Debugf("Mapped queues for worker %s to %s", worker.Hostname, workerQueueMap)
				return workerQueueMapQueues
			}
		}
	}

	return []string{"celery"}
}

type CeleryHeartbeatMessage struct {
	Body            string `json:"body"`
	ContentEncoding string `json:"content-encoding"`
	ContentType     string `json:"content-type"`
	Headers         struct {
		Hostname string `json:"hostname"`
	} `json:"headers"`
	Properties struct {
		DeliveryMode int64 `json:"delivery_mode"`
		DeliveryInfo struct {
			Exchange   string `json:"exchange"`
			RoutingKey string `json:"routing_key"`
		} `json:"delivery_info"`
		Priority     int64  `json:"priority"`
		BodyEncoding string `json:"body_encoding"`
		DeliveryTag  string `json:"delivery_tag"`
	} `json:"properties"`
}

func processHeartbeat(msg *redis.Message) {
	decodedMessage := CeleryHeartbeatMessage{}
	err := json.Unmarshal([]byte(msg.Payload), &decodedMessage)
	if err != nil {
		log.Warnf("could not decode heartbeat message: %s", err.Error())
	}

	sDec, err := base64.StdEncoding.DecodeString(decodedMessage.Body)
	if err != nil {
		log.Warnf("could not decode heartbeat message body: %s", err.Error())
	}

	decodedHeartbeat := CeleryHeartbeat{}
	err = json.Unmarshal(sDec, &decodedHeartbeat)
	if err != nil {
		log.Warnf("could not decode heartbeat: %s", err.Error())
	}

	updateWorker(decodedHeartbeat)
}

func cleanupWorkers() {
	log.Debugf("Running worker cleanup")
	workerMapLock.Lock()
	defer workerMapLock.Unlock()

	workerStaleTime := viper.GetFloat64("worker_stale_time")
	if workerStaleTime == 0 {
		workerStaleTime = 10
	}

	for i := range celeryWorkers {
		if time.Now().Sub(celeryWorkers[i].LastSeen).Seconds() > workerStaleTime {
			log.Debugf("Removing worker %s because it has not been since for more than %.2f seconds", i, time.Now().Sub(celeryWorkers[i].LastSeen).Seconds())
			delete(celeryWorkers, i)
		}
	}
}
