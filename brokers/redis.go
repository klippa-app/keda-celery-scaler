package brokers

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/klippa-app/keda-celery-scaler/celery"
	"github.com/klippa-app/keda-celery-scaler/workers"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// processRedisHeartbeat will process a Redis Pub/Sub heartbeat message from
// celery and update the worker with the information.
func processRedisHeartbeat(msg *redis.Message) {
	decodedMessage := celery.Message{}
	err := json.Unmarshal([]byte(msg.Payload), &decodedMessage)
	if err != nil {
		log.Warnf("could not decode heartbeat message: %s", err.Error())
		return
	}

	sDec, err := base64.StdEncoding.DecodeString(decodedMessage.Body)
	if err != nil {
		log.Warnf("could not decode heartbeat message body: %s", err.Error())
		return
	}

	decodedHeartbeat := celery.Heartbeat{}
	err = json.Unmarshal(sDec, &decodedHeartbeat)
	if err != nil {
		log.Warnf("could not decode heartbeat: %s", err.Error())
		return
	}

	workers.UpdateWorker(decodedHeartbeat)
}

var redisClient *redis.Client

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

		redisClient = redis.NewFailoverClient(options)
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

		redisClient = redis.NewClient(options)
	}

	// Make sure the connection works.
	pong := redisClient.Ping(context.Background())
	if pong.Err() != nil {
		return fmt.Errorf("could not connect to redis: %w", pong.Err())
	}

	// Listen for heartbeats and process them.
	// @todo: subscribe only on DB we want/need?
	pubsub := redisClient.PSubscribe(context.Background(), "/*.celeryev/worker.heartbeat")
	go func() {
		ch := pubsub.Channel()
		for msg := range ch {
			processRedisHeartbeat(msg)
		}
	}()

	return nil
}
