package rail

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ngaut/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Exporter struct {
	httpServer *http.Server
	listen     int //httpserver监听端口
	rail       *Rail

	up                 prometheus.Gauge
	totalScrapes       prometheus.Counter
	totalQuery         *prometheus.GaugeVec
	qps                *prometheus.GaugeVec
	maxCost            *prometheus.GaugeVec
	minCost            *prometheus.GaugeVec
	avgCost            *prometheus.GaugeVec
	topicQueue         prometheus.Gauge
	channelQueues      *prometheus.GaugeVec
	channelRetryQueues *prometheus.GaugeVec
}

//NewExporter metric输出服务，支持多个 StatManager
// listen 服务的监听端口
// namespace exporter的命名空间
// labels
func NewExporter(listen int, namespace string, rail *Rail) *Exporter {

	labels := []string{"name", "status"}

	return &Exporter{
		listen: listen,
		rail:   rail,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Was the last scrape of recommendproxy successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_total_scrapes",
			Help:      "Current total recommendproxy scrapes.",
		}),
		totalQuery: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "request_num_total",
			Help:      "总请求数",
		}, labels),
		qps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "qps",
			Help:      "qps",
		}, labels),
		maxCost: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "max_cost",
			Help:      "最大耗时",
		}, labels),
		minCost: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "min_cost",
			Help:      "最小耗时",
		}, labels),
		avgCost: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "avg_cost",
			Help:      "平均耗时",
		}, labels),
		topicQueue: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "topic_queue_size",
			Help:      "主题缓冲队列大小",
		}),
		channelQueues: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "channel_queue_size",
			Help:      "渠道缓冲队列大小",
		}, []string{"name"}),
		channelRetryQueues: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "channel_retry_queue_size",
			Help:      "渠道重试队列大小",
		}, []string{"name"}),
	}
}

// It implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.up.Desc()
	ch <- e.totalScrapes.Desc()
	ch <- e.topicQueue.Desc()

	e.totalQuery.Describe(ch)
	e.qps.Describe(ch)
	e.maxCost.Describe(ch)
	e.minCost.Describe(ch)
	e.avgCost.Describe(ch)
	e.channelQueues.Describe(ch)
	e.channelRetryQueues.Describe(ch)
}

//It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("exporter collect error - %s", err)
		}
	}()

	//重置
	e.maxCost.Reset()
	e.minCost.Reset()
	e.qps.Reset()
	e.avgCost.Reset()
	e.totalQuery.Reset()
	e.channelQueues.Reset()
	e.channelRetryQueues.Reset()

	e.totalScrapes.Inc()

	statinfo := GetCountGlobal().GetStat()

	if len(statinfo) == 0 {
		e.up.Set(0)
	} else {
		e.up.Set(1)
		for _, si := range statinfo {
			maplabels := make(map[string]string)
			maplabels["name"] = si.Name
			maplabels["status"] = si.Status
			labels := prometheus.Labels(maplabels)

			e.totalQuery.With(labels).Add(float64(si.Count))
			e.qps.With(labels).Add(float64(si.Qps))
			e.maxCost.With(labels).Add(float64(si.Max))
			e.minCost.With(labels).Add(float64(si.Min))
			e.avgCost.With(labels).Add(float64(si.Avg))

		}

		e.topicQueue.Set(float64(e.rail.topic.Depth()))

		for channelName, c := range e.rail.topic.channelMap {
			maplabels := make(map[string]string)
			maplabels["name"] = channelName
			labels := prometheus.Labels(maplabels)
			e.channelQueues.With(labels).Add(float64(c.Depth()))
			e.channelRetryQueues.With(labels).Add(float64(c.deferredPQ.Len()))
		}
	}

	ch <- e.up
	ch <- e.totalScrapes
	ch <- e.topicQueue

	e.totalQuery.Collect(ch)
	e.qps.Collect(ch)
	e.maxCost.Collect(ch)
	e.minCost.Collect(ch)
	e.avgCost.Collect(ch)
	e.channelQueues.Collect(ch)
	e.channelRetryQueues.Collect(ch)

	log.Infof("prometheus collect over from datasouce")
}

func (e *Exporter) Start() error {
	err := prometheus.Register(e)
	if err != nil {
		return fmt.Errorf("register prometheus fail:%s", err)
	}
	listenAddr := fmt.Sprintf(":%d", e.listen)

	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	e.httpServer = &http.Server{
		Handler:      promhttp.Handler(),
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}

	go func() {
		err = e.httpServer.Serve(l)
		if err != nil {
			log.Errorf(" prometheus server start error - %s", err)
		}
	}()

	log.Info("start prometheus server succ")
	return nil
}

func (e *Exporter) Stop() {
	log.Infof("exporter exit normally.")
}
