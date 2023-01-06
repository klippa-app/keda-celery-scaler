package brokers

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

// ConnectBroker will connect to the configured broker.
// Only Redis for now.
func ConnectBroker() error {
	err := connectRedis()
	if err != nil {
		return err
	}

	return nil
}

func GetQueueLength(ctx context.Context, queue string) (int64, error) {
	start := time.Now()

	listLength := redisClient.LLen(ctx, queue)
	if listLength.Err() != nil {
		return 0, listLength.Err()
	}

	log.Tracef("Fetching queue length for queue %s took %s", queue, time.Since(start).String())

	return listLength.Val(), nil
}
