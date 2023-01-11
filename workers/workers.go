package workers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klippa-app/keda-celery-scaler/celery"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type celeryWorker struct {
	Hostname    string
	LastSeen    time.Time
	Queues      []string
	Concurrency int64
	Active      int64
}

// A map with information about the current workers.
var celeryWorkers = map[string]*celeryWorker{}

// A lock for the map above.
var celeryWorkersLock sync.Mutex

// UpdateWorker will update the current state of a worker with the information
// from the Celery heartbeat.
func UpdateWorker(heartbeat celery.Heartbeat) {
	celeryWorkersLock.Lock()
	defer celeryWorkersLock.Unlock()

	// Register the worker if we don't know it yet.
	_, ok := celeryWorkers[heartbeat.Hostname]
	if !ok {
		celeryWorkers[heartbeat.Hostname] = &celeryWorker{
			Hostname: heartbeat.Hostname,
		}
	}

	// Set the last seen date and amount of active tasks.
	celeryWorkers[heartbeat.Hostname].LastSeen = time.Now()
	celeryWorkers[heartbeat.Hostname].Active = heartbeat.Active

	// If we have the worker info we can automatically fill it in.
	if heartbeat.XWorkerInfo != nil {
		celeryWorkers[heartbeat.Hostname].Queues = heartbeat.XWorkerInfo.Queues
		celeryWorkers[heartbeat.Hostname].Concurrency = heartbeat.XWorkerInfo.Concurrency
	} else {
		// No worker info found, map queues and concurrency from hostname.
		queues, concurrency := mapQueuesToWorker(heartbeat)
		celeryWorkers[heartbeat.Hostname].Queues = queues
		celeryWorkers[heartbeat.Hostname].Concurrency = concurrency
	}

	// Log the current state of the worker.
	d, _ := json.Marshal(celeryWorkers[heartbeat.Hostname])
	log.Debugf("Updated worker %s: %s", heartbeat.Hostname, string(d))
}

type workerQueueMap struct {
	Identifier  string
	Concurrency int
	Queues      []string
}

var workerQueueMaps []workerQueueMap

// BuildWorkerQueueMaps parses the environment variable KCS_WORKER_QUEUE_MAP
// into a queue map. The format is
// {identifier1}:{queue1},{queue2}:{concurrency1};{identifier2}:{queue3},{queue4}:{concurrency2}.
func BuildWorkerQueueMaps() error {
	workerQueueMapSettingValue := viper.GetString("worker_queue_map")
	if strings.TrimSpace(workerQueueMapSettingValue) == "" {
		log.Debugf("No worker queue mapping given")
		return nil
	}

	workerQueueMapSettingValues := strings.Split(workerQueueMapSettingValue, ";")
	for i := range workerQueueMapSettingValues {
		workerQueueMapSetting := workerQueueMapSettingValues[i]
		workerQueueMapSettingParts := strings.Split(workerQueueMapSetting, ":")
		workerQueueMapSettingIdentifier := workerQueueMapSettingParts[0]

		// Default concurrency.
		concurrency := 1

		// Check if there is a concurrency set in the map.
		if len(workerQueueMapSettingParts) > 2 {
			concurrencyString := workerQueueMapSettingParts[2]
			concurrencyStringParsed, err := strconv.Atoi(concurrencyString)
			if err != nil {
				return fmt.Errorf("worker map value %s has an invalid concurrency: %s", workerQueueMapSetting, concurrencyString)
			} else {
				concurrency = concurrencyStringParsed
			}
		}

		// Split up the queues by comma.
		workerQueueMapQueues := strings.Split(workerQueueMapSettingParts[1], ",")

		workerQueueMaps = append(workerQueueMaps, workerQueueMap{
			Identifier:  workerQueueMapSettingIdentifier,
			Concurrency: concurrency,
			Queues:      workerQueueMapQueues,
		})
	}

	return nil
}

// getWorkerQueues maps the name of the worker to a list of queues.
func mapQueuesToWorker(heartbeat celery.Heartbeat) ([]string, int64) {
	for i := range workerQueueMaps {
		if strings.Contains(heartbeat.Hostname, workerQueueMaps[i].Identifier) {
			log.Debugf("Mapped queues for worker %s to identifier %s with queues %s and concurrency %d", heartbeat.Hostname, workerQueueMaps[i].Identifier, strings.Join(workerQueueMaps[i].Queues, ", "), workerQueueMaps[i].Concurrency)
			return workerQueueMaps[i].Queues, int64(workerQueueMaps[i].Concurrency)
		}
	}

	log.Warnf("Could not map queue for worker %s", heartbeat.Hostname)

	return []string{}, 0
}

// CleanupWorkers will check all known workers for the last hearbeat time and
// will remove the worker if it has become stale.
func CleanupWorkers() {
	log.Debugf("Running worker cleanup")
	celeryWorkersLock.Lock()
	defer celeryWorkersLock.Unlock()

	workerStaleTime := viper.GetFloat64("worker_stale_time")
	if workerStaleTime == 0 {
		workerStaleTime = 10
	}

	for i := range celeryWorkers {
		if time.Since(celeryWorkers[i].LastSeen).Seconds() > workerStaleTime {
			log.Debugf("Removing worker %s because it has not been seen for %.2f seconds", i, time.Since(celeryWorkers[i].LastSeen).Seconds())
			delete(celeryWorkers, i)
		}
	}
}

func GetQueueWorkers(queue string) (int64, int64) {
	start := time.Now()

	celeryWorkersLock.Lock()
	defer celeryWorkersLock.Unlock()

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
