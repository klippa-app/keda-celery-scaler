# KEDA Celery Scaler

An [External Scaler](https://keda.sh/docs/2.9/concepts/external-scalers/) for KEDA.

**Only supports Celery Redis Broker for now, other brokers also have heartbeats so PRs are welcome!**

## Why?

KEDA allows you to autoscale based on the length of a queue, but that's probably not what you want.

What you want is scaling based on the load `(active tasks + queue length) / available workers` so that you're able to
scale _before_ a queue starts building up. You could example start to scale up when the load is at 50%. This way you're
able to handle peak load way better.

## How?

Since Celery is quite unstable in a production situation where you have a lot of workers that scale up and down due to
how workers communicate (we would advise everyone to set the Env variables `CELERY_ENABLE_REMOTE_CONTROL=False`
and `CELERY_SEND_EVENTS=False` to prevent the pidbox madness), we had to figure out a way to get the current load of a
queue.

Luckily, Celery with a Redis broker sends heartbeats on a Pub/Sub channel called `/{db}.celeryev/worker.heartbeat`, 
where `{db}` is the used Redis database (since Pub/Sub channels are not scoped to a database).

This heartbeat contains the hostname of the worker and the current active tasks. It does not include the queue and
concurrency information of a worker. See the section "Determining the queue(s) and concurrency of a worker" for
information on how to pass in those details.

## Determining the queue(s) and concurrency of a worker

### Automatically

It's possible to use a Celery hook called `@celeryd_init.connect` to insert the information that we need into the
heartbeat.

The code that is needed for this is:

```python
import celery
from celery.signals import celeryd_init
from celery.worker.state import SOFTWARE_INFO

@celeryd_init.connect
def on_celeryd_init(options: Dict[str, Any], **_):
    """
    Updates the celery SOFTWARE_INFO dict to contain worker specific settings
    that will be published with each celery heartbeat. These settings can then
    be used for load-based autoscaling.

    The SOFTWARE_INFO key is defined in celery/worker/state.py, and is inserted
    into heartbeats in celery/worker/heartbeat.py in the _send() method.

    Args:
        options: the options using which the worker is initialized.
        **_: Other keyword parameters are given by the signal, but these
             are ignored.
    """
    queues = options["queues"]

    if queues is None or len(queues) == 0:
        queues = ["celery"]

    SOFTWARE_INFO["x_worker_info"] = {"queues": queues, "concurrency": options["concurrency"]}
```

The scaler will automatically pick up the information from the heartbeat.

### Manual mapping

If you do not have the ability to change your Celery implementation, or you do not want it (for now), you can also use
our manual mapping. The downside of this is that you still don't have information about the concurrency of a worker, but
if every worker has a concurrency of 1 this does not matter.

This works with the environment variable `KCS_WORKER_QUEUE_MAP`. The format
is `{identifier}:{queue1},{queue2};{identifier2}:{queue3},{queue4}`.

This means that if `{identifier}` is found in the worker hostname, it will be assigned the queues `{queue1}`
and `{queue2}`. If `{identifier2}` is found in the worker hostname, it will be assigned the queues `{queue3}`
and `{queue4}`.

An example mapping could look like `email-worker:emails;notifications-worker:notifications`.

## Configuration Env variables

| Name                        | Description                                                                                                                                                                                                                                                      | Default                      |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| KCS_LOG_LEVEL               | The log level to print (panic, fatal, error, warn(ing), info, debug, trace)                                                                                                                                                                                      | info                         |
| KCS_WORKER_STALE_TIME       | When we should mark a worker as lost in seconds.                                                                                                                                                                                                                 | 10                           |
| KCS_WORKER_CLEANUP_INTERVAL | How often to clean up lost workers in seconds.                                                                                                                                                                                                                   | 5                            |
| KCS_WORKER_QUEUE_MAP        | When your Celery apps don't send the queue in the heartbeat, you can use this to map the workers by hostname. The format is {identifier}:{queue1},{queue2};{identifier2}:{queue3},{queue4}. When the identifier is found in the worker hostname, it will map it. |                              |
| KCS_REDIS_TYPE              | What type of redis to use, standalone or sentinel.                                                                                                                                                                                                               | standalone                   |
| KCS_REDIS_SERVER            | host:port list, seperated by a comma (standalone uses only the first in case of multiple given)                                                                                                                                                                  | localhost:6379               |
| KCS_REDIS_DB                | The DB to use for the connection.                                                                                                                                                                                                                                | 0                            |
| KCS_REDIS_USERNAME          | The username to use for the connection.                                                                                                                                                                                                                          |                              |
| KCS_REDIS_PASSWORD          | The password to use for the connection.                                                                                                                                                                                                                          |                              |
| KCS_REDIS_TIMEOUTS_DIAL     | The dial timeout in seconds to use for the connection.                                                                                                                                                                                                           | 5                            |
| KCS_REDIS_TIMEOUTS_READ     | The read timeout in seconds to use for the connection.                                                                                                                                                                                                           | 3                            |
| KCS_REDIS_TIMEOUTS_WRITE    | The write timeout in seconds to use for the connection.                                                                                                                                                                                                          | 3                            |
| KCS_REDIS_CONNECTIONS_MIN   | The minimum amount of connections to keep around in the pool.                                                                                                                                                                                                    | 0                            |
| KCS_REDIS_CONNECTIONS_MAX   | The maximum amount of connections to have in the pool.                                                                                                                                                                                                           | 10 * CPU COUNT               |
| KCS_REDIS_TLS_ENABLED       | Whether to use TLS in the connection.                                                                                                                                                                                                                            | False                        |
| KCS_REDIS_TLS_SERVERNAME    | The hostname to validate the certificate with.                                                                                                                                                                                                                   | Parsed from KCS_REDIS_SERVER |
| KCS_REDIS_MASTER            | The Sentinel master name in case of Sentinel.                                                                                                                                                                                                                    |                              |
| KCS_REDIS_SENTINELUSERNAME  | The username for the sentinel server.                                                                                                                                                                                                                            |                              |
| KCS_REDIS_SENTINELPASSWORD  | The password for the sentinel server.                                                                                                                                                                                                                            |                              |

## Docker

This Docker image is stored on the Docker registry
at [jerbob92/keda-celery-scaler](https://hub.docker.com/r/jerbob92/keda-celery-scaler).

## Kubernetes

An example deployment could look like this:

Autoscaler K8S deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: keda-celery-autoscaler
  labels:
    component: keda-celery-autoscaler
spec:
  replicas: 1
  selector:
    matchLabels:
      component: keda-celery-autoscaler
  template:
    metadata:
      labels:
        component: keda-celery-autoscaler
    spec:
      containers:
        - name: keda-celery-autoscaler
          image: "jerbob92/keda-celery-scaler:latest"
          env:
            - name: KCS_LOG_LEVEL
              value: "info"
            - name: KCS_WORKER_STALE_TIME
              value: "10"
            - name: KCS_WORKER_CLEANUP_INTERVAL
              value: "5"
            - name: KCS_WORKER_QUEUE_MAP
              value: "email-worker:emails;notifications-worker:notifications"
            - name: KCS_REDIS_TYPE
              value: "standalone"
            - name: KCS_REDIS_SERVER
              value: "redis-service.default.svc.cluster.local"
          ports:
            - containerPort: 6000
              name: keda-celery-autoscaler
```

Autoscaler K8S service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: keda-celery-autoscaler
spec:
  ports:
    - port: 6000
      protocol: TCP
      name: keda-celery-autoscaler
  selector:
    component: keda-celery-autoscaler
```

KEDA ScaledObjects:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: email-worker-scaler
spec:
  scaleTargetRef:
    name: email-worker
  pollingInterval: 5
  cooldownPeriod: 300
  minReplicaCount: 5
  maxReplicaCount: 15

  triggers:
    - type: external
      metadata:
        scalerAddress: "keda-celery-autoscaler.default.svc.cluster.local:6000"
        queue: "emails"
        scaleLoadValue: "70" # Scale at a load of 70%.
      metricType: Value
```

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: notifications-worker-scaler
spec:
  scaleTargetRef:
    name: notifications-worker
  pollingInterval: 5
  cooldownPeriod: 300
  minReplicaCount: 5
  maxReplicaCount: 15

  triggers:
    - type: external
      metadata:
        scalerAddress: "keda-celery-autoscaler.default.svc.cluster.local:6000"
        queue: "notifications"
        scaleLoadValue: "70" # Scale at a load of 70%.
      metricType: Value
```

This is all just an example, you will need to change this to your own setup.

## About Klippa

[Klippa](https://www.klippa.com/en) is a scale-up from [Groningen, The Netherlands](https://goo.gl/maps/CcCGaPTBz3u8noSd6) and was founded in 2015 by six Dutch IT specialists with the goal to digitize paper processes with modern technologies.

We help clients enhance the effectiveness of their organization by using machine learning and OCR. Since 2015 more than a 1000 happy clients have been served with a variety of the software solutions that Klippa offers. Our passion is to help our clients to digitize paper processes by using smart apps, accounts payable software and data extraction by using OCR.

## License

The MIT License (MIT)