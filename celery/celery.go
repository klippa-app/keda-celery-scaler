package celery

type Message struct {
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

type Heartbeat struct {
	Hostname    string               `json:"hostname"`
	Utcoffset   int64                `json:"utcoffset"`
	Pid         int64                `json:"pid"`
	Clock       int64                `json:"clock"`
	Freq        float64              `json:"freq"`
	Active      int64                `json:"active"`
	Processed   int64                `json:"processed"`
	Loadavg     []float64            `json:"loadavg"`
	SwIdent     string               `json:"sw_ident"`
	SwVer       string               `json:"sw_ver"`
	SwSys       string               `json:"sw_sys"`
	Timestamp   float64              `json:"timestamp"`
	Type        string               `json:"type"`
	XWorkerInfo *HeartbeatWorkerInfo `json:"x_worker_info"`
}

type HeartbeatWorkerInfo struct {
	Queues      []string `json:"queues"`
	Concurrency int64    `json:"concurrency"`
}
