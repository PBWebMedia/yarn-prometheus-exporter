package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
)

type clusterMetrics struct {
	ClusterMetrics metrics `json:"clusterMetrics"`
}

type metrics struct {
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

type collector struct {
	endpoint       *url.URL
	up             *prometheus.Desc
	scrapeFailures *prometheus.Desc
	failureCount   int
	applications   *prometheus.Desc
	memory         *prometheus.Desc
	cores          *prometheus.Desc
	containers     *prometheus.Desc
	nodes          *prometheus.Desc
}

const metricsNamespace = "yarn"

func newFuncMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "", metricName), docString, labels, nil)
}

func newCollector(endpoint *url.URL) *collector {
	return &collector{
		endpoint:       endpoint,
		up:             newFuncMetric("up", "Able to contact YARN", nil),
		scrapeFailures: newFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics", nil),
		applications:   newFuncMetric("applications_total", "Applications stats", []string{"status"}),
		memory:         newFuncMetric("memory_bytes", "Memory allocation stats", []string{"status"}),
		cores:          newFuncMetric("cores_total", "Cpu allocation stats", []string{"status"}),
		containers:     newFuncMetric("containers_total", "Container stats", []string{"status"}),
		nodes:          newFuncMetric("nodes_total", "Node stats", []string{"status"}),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.applications
	ch <- c.memory
	ch <- c.cores
	ch <- c.containers
	ch <- c.nodes
	ch <- c.scrapeFailures
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0

	metrics, err := fetch(c.endpoint)
	if err != nil {
		up = 0.0
		c.failureCount++

		fmt.Print("Error while collecting data from YARN: " + err.Error() + "\r\n")
	}

	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, up)
	ch <- prometheus.MustNewConstMetric(c.scrapeFailures, prometheus.CounterValue, float64(c.failureCount))

	if up == 0.0 {
		return
	}

	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(metrics.AppsSubmitted), "submited")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(metrics.AppsCompleted), "completed")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(metrics.AppsPending), "pending")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(metrics.AppsRunning), "running")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(metrics.AppsFailed), "failed")
	ch <- prometheus.MustNewConstMetric(c.applications, prometheus.GaugeValue, float64(metrics.AppsKilled), "killed")

	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(metrics.ReservedMB)*1024*1024, "submited")
	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(metrics.AvailableMB)*1024*1024, "available")
	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(metrics.AllocatedMB)*1024*1024, "allocated")
	ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(metrics.TotalMB)*1024*1024, "total")

	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(metrics.ReservedVirtualCores), "reserved")
	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(metrics.AvailableVirtualCores), "available")
	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(metrics.AllocatedVirtualCores), "allocated")
	ch <- prometheus.MustNewConstMetric(c.cores, prometheus.GaugeValue, float64(metrics.TotalVirtualCores), "total")

	ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(metrics.ContainersAllocated), "allocated")
	ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(metrics.ContainersReserved), "reserved")
	ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(metrics.ContainersPending), "pending")

	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.TotalNodes), "total")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.LostNodes), "lost")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.UnhealthyNodes), "unhealthy")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.DecommissionedNodes), "decommissioned")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.DecommissioningNodes), "decommisioning")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.RebootedNodes), "rebooted")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.ActiveNodes), "active")
	ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(metrics.ShutdownNodes), "shutdown")

}

func fetch(u *url.URL) (*metrics, error) {
	req := http.Request{
		Method:     "GET",
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}

	resp, err := http.DefaultClient.Do(&req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected HTTP status: %v\r\n", resp.StatusCode)
	}

	var c clusterMetrics
	err = json.NewDecoder(resp.Body).Decode(&c)
	if err != nil {
		return nil, err
	}

	return &c.ClusterMetrics, nil
}
