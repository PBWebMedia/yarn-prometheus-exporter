package main

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"net/http"
)

type clusterMetricsResponse struct {
	ClusterMetrics clusterMetrics `json:"clusterMetrics"`
}

type clusterMetrics struct {
	AppsSubmitted         int `json:"appsSubmitted"`
	AppsCompleted         int `json:"appsCompleted"`
	AppsPending           int `json:"appsPending"`
	AppsRunning           int `json:"appsRunning"`
	AppsFailed            int `json:"appsFailed"`
	AppsKilled            int `json:"appsKilled"`
	ReservedMB            int `json:"reservedMB"`
	AvailableMB           int `json:"availableMB"`
	AllocatedMB           int `json:"allocatedMB"`
	ReservedVirtualCores  int `json:"reservedVirtualCores"`
	AvailableVirtualCores int `json:"availableVirtualCores"`
	AllocatedVirtualCores int `json:"allocatedVirtualCores"`
	ContainersAllocated   int `json:"containersAllocated"`
	ContainersReserved    int `json:"containersReserved"`
	ContainersPending     int `json:"containersPending"`
	TotalMB               int `json:"totalMB"`
	TotalVirtualCores     int `json:"totalVirtualCores"`
	TotalNodes            int `json:"totalNodes"`
	LostNodes             int `json:"lostNodes"`
	UnhealthyNodes        int `json:"unhealthyNodes"`
	DecommissioningNodes  int `json:"decommissioningNodes"`
	DecommissionedNodes   int `json:"decommissionedNodes"`
	RebootedNodes         int `json:"rebootedNodes"`
	ActiveNodes           int `json:"activeNodes"`
	ShutdownNodes         int `json:"shutdownNodes"`
}

type schedulerResponse struct {
	Scheduler scheduler `json:"scheduler"`
}

type scheduler struct {
	SchedulerInfo schedulerInfo `json:"schedulerInfo"`
}

type schedulerInfo struct {
	Queues schedulerQueues `json:"queues"`
}

type schedulerQueues struct {
	Queue []schedulerQueue `json:"queue"`
}

type schedulerQueue struct {
	Capacity        float64 `json:"capacity"`
	UsedCapacity    float64 `json:"usedCapacity"`
	MaxCapacity     float64 `json:"maxCapacity"`
	NumApplications int     `json:"numApplications"`
	QueueName       string  `json:"queueName"`
	State           string  `json:"state"`

	ResourcesUsed SchedulerQueueResourcesUsed `json:"resourcesUsed"`
}

type SchedulerQueueResourcesUsed struct {
	Memory int `json:"memory,omitempty"`
	VCores int `json:"vCores,omitempty"`
}

type collector struct {
	endpoint string

	up             *prometheus.Desc
	scrapeFailures *prometheus.Desc
	failureCount   int
	applications   *prometheus.Desc
	memory         *prometheus.Desc
	cores          *prometheus.Desc
	containers     *prometheus.Desc
	nodes          *prometheus.Desc

	queueCapacity         *prometheus.Desc
	queueMaxCapacity      *prometheus.Desc
	queueApplicationCount *prometheus.Desc
	queueUsedMemoryBytes  *prometheus.Desc
	queueUsedVCoresCount  *prometheus.Desc
}

const metricsNamespace = "yarn"

func newFuncMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "", metricName), docString, labels, nil)
}

func newCollector(endpoint string) *collector {
	return &collector{
		endpoint:       endpoint,
		up:             newFuncMetric("up", "Able to contact YARN", nil),
		scrapeFailures: newFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics", nil),
		applications:   newFuncMetric("applications_total", "Applications stats", []string{"status"}),
		memory:         newFuncMetric("memory_bytes", "Memory allocation stats", []string{"status"}),
		cores:          newFuncMetric("cores_total", "Cpu allocation stats", []string{"status"}),
		containers:     newFuncMetric("containers_total", "Container stats", []string{"status"}),
		nodes:          newFuncMetric("nodes_total", "Node stats", []string{"status"}),

		queueCapacity:         newFuncMetric("queue_capacity", "Queue capacity", []string{"queue"}),
		queueMaxCapacity:      newFuncMetric("queue_max_capacity", "Queue max capacity", []string{"queue"}),
		queueApplicationCount: newFuncMetric("queue_application_count", "Queue application count", []string{"queue"}),
		queueUsedMemoryBytes:  newFuncMetric("queue_used_memory_bytes", "Queue used memory bytes", []string{"queue"}),
		queueUsedVCoresCount:  newFuncMetric("queue_used_vcores_count", "Queue used vcores count", []string{"queue"}),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.scrapeFailures
	ch <- c.applications
	ch <- c.memory
	ch <- c.cores
	ch <- c.containers
	ch <- c.nodes
	ch <- c.queueCapacity
	ch <- c.queueMaxCapacity
	ch <- c.queueApplicationCount
	ch <- c.queueUsedMemoryBytes
	ch <- c.queueUsedVCoresCount
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0

	m, err := c.fetchClusterMetrics()
	if err != nil {
		up = 0.0
		c.failureCount++

		log.Println("Error while collecting data from YARN: " + err.Error())
	}

	s, err := c.fetchSchedulerMetrics()
	if err != nil {
		up = 0.0
		c.failureCount++

		log.Println("Error while collecting data from YARN: " + err.Error())
	}

	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, up)
	ch <- prometheus.MustNewConstMetric(c.scrapeFailures, prometheus.CounterValue, float64(c.failureCount))

	if up == 0.0 {
		return
	}

	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(m.AppsSubmitted), "submitted")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(m.AppsCompleted), "completed")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(m.AppsPending), "pending")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(m.AppsRunning), "running")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(m.AppsFailed), "failed")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(m.AppsKilled), "killed")

	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(m.ReservedMB)*1024*1024, "submitted")
	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(m.AvailableMB)*1024*1024, "available")
	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(m.AllocatedMB)*1024*1024, "allocated")
	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(m.TotalMB)*1024*1024, "total")

	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(m.ReservedVirtualCores), "reserved")
	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(m.AvailableVirtualCores), "available")
	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(m.AllocatedVirtualCores), "allocated")
	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(m.TotalVirtualCores), "total")

	ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(m.ContainersAllocated), "allocated")
	ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(m.ContainersReserved), "reserved")
	ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(m.ContainersPending), "pending")

	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.TotalNodes), "total")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.LostNodes), "lost")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.UnhealthyNodes), "unhealthy")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.DecommissionedNodes), "decommissioned")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.DecommissioningNodes), "decommissioning")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.RebootedNodes), "rebooted")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.ActiveNodes), "active")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(m.ShutdownNodes), "shutdown")

	for _, q := range s.SchedulerInfo.Queues.Queue {
		ch <- prometheus.MustNewConstMetric(c.queueCapacity, prometheus.GaugeValue, q.Capacity, q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueMaxCapacity, prometheus.GaugeValue, q.MaxCapacity, q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueApplicationCount, prometheus.GaugeValue, float64(q.NumApplications), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueUsedMemoryBytes, prometheus.GaugeValue, float64(q.ResourcesUsed.Memory)*1024*1024, q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueUsedVCoresCount, prometheus.GaugeValue, float64(q.ResourcesUsed.VCores), q.QueueName)
	}
}

func (c *collector) fetchClusterMetrics() (*clusterMetrics, error) {
	var r clusterMetricsResponse
	err := c.fetch("ws/v1/cluster/metrics", &r)
	if err != nil {
		return nil, err
	}
	return &r.ClusterMetrics, nil
}

func (c *collector) fetchSchedulerMetrics() (*scheduler, error) {
	var r schedulerResponse
	err := c.fetch("ws/v1/cluster/scheduler", &r)
	if err != nil {
		return nil, err
	}
	return &r.Scheduler, nil
}

func (c *collector) fetch(path string, v any) error {
	req, err := http.NewRequest(http.MethodGet, c.endpoint+path, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status: %v", resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(v)
	if err != nil {
		return err
	}

	return nil
}
