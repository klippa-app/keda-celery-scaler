package workers

import (
	"encoding/json"
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
var workerMapLock sync.Mutex

// UpdateWorker will update the current state of a worker with the information
// from the Celery heartbeat.
func UpdateWorker(heartbeat celery.Heartbeat) {
	workerMapLock.Lock()
	defer workerMapLock.Unlock()

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
		queues, concurrency := getWorkerQueues(heartbeat)
		celeryWorkers[heartbeat.Hostname].Queues = queues
		celeryWorkers[heartbeat.Hostname].Concurrency = concurrency
	}

	// Log the current state of the worker.
	d, _ := json.Marshal(celeryWorkers[heartbeat.Hostname])
	log.Debugf("Updated worker %s: %s", heartbeat.Hostname, string(d))
}

// getWorkerQueues maps the name of the worker to a list of queues set in the
// environment variable KCS_WORKER_QUEUE_MAP.
func getWorkerQueues(heartbeat celery.Heartbeat) ([]string, int64) {
	workerQueueMapSetting := viper.GetString("worker_queue_map")
	if workerQueueMapSetting != "" {
		workerQueueMaps := strings.Split(workerQueueMapSetting, ";")
		for i := range workerQueueMaps {
			workerQueueMap := workerQueueMaps[i]
			workerQueueMapParts := strings.Split(workerQueueMap, ":")
			workerQueueMapIdentifier := workerQueueMapParts[0]
			if strings.Contains(heartbeat.Hostname, workerQueueMapIdentifier) {
				workerQueueMapQueues := strings.Split(workerQueueMapParts[1], ",")

				// Default concurrency.
				concurrency := 1

				// Check if there is a concurrency set in the map.
				if len(workerQueueMapParts) > 2 {
					concurrencyString := workerQueueMapParts[2]
					concurrencyStringParsed, err := strconv.Atoi(concurrencyString)
					if err == nil {
						concurrency = concurrencyStringParsed
					}
				}

				log.Debugf("Mapped queues for worker %s to %s", heartbeat.Hostname, workerQueueMap)
				return workerQueueMapQueues, int64(concurrency)
			}
		}
	}

	log.Warnf("Could not map queue for worker %s", heartbeat.Hostname)

	return []string{}, 0
}

// CleanupWorkers will check all known workers for the last hearbeat time and
// will remove the worker if it has become stale.
func CleanupWorkers() {
	log.Debugf("Running worker cleanup")
	workerMapLock.Lock()
	defer workerMapLock.Unlock()

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

	workerMapLock.Lock()
	defer workerMapLock.Unlock()

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
