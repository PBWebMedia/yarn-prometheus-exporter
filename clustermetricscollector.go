package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	ClusterMetrics struct {
		AppsSubmitted                     float64 `json:"appsSubmitted"`
		AppsCompleted                     float64 `json:"appsCompleted"`
		AppsPending                       float64 `json:"appsPending"`
		AppsRunning                       float64 `json:"appsRunning"`
		AppsFailed                        float64 `json:"appsFailed"`
		AppsKilled                        float64 `json:"appsKilled"`
		ReservedMB                        float64 `json:"reservedMB"`
		AvailableMB                       float64 `json:"availableMB"`
		AllocatedMB                       float64 `json:"allocatedMB"`
		ReservedVirtualCores              float64 `json:"reservedVirtualCores"`
		AvailableVirtualCores             float64 `json:"availableVirtualCores"`
		AllocatedVirtualCores             float64 `json:"allocatedVirtualCores"`
		ContainersAllocated               float64 `json:"containersAllocated"`
		ContainersReserved                float64 `json:"containersReserved"`
		ContainersPending                 float64 `json:"containersPending"`
		TotalMB                           float64 `json:"totalMB"`
		TotalVirtualCores                 float64 `json:"totalVirtualCores"`
		TotalNodes                        float64 `json:"totalNodes"`
		LostNodes                         float64 `json:"lostNodes"`
		UnhealthyNodes                    float64 `json:"unhealthyNodes"`
		DecommissioningNodes              float64 `json:"decommissioningNodes"`
		DecommissionedNodes               float64 `json:"decommissionedNodes"`
		RebootedNodes                     float64 `json:"rebootedNodes"`
		ActiveNodes                       float64 `json:"activeNodes"`
		ShutdownNodes                     float64 `json:"shutdownNodes"`
		TotalUsedResourcesAcrossPartition struct {
			Memory               float64 `json:"memory"`
			VCores               float64 `json:"vCores"`
			ResourceInformations struct {
				ResourceInformation []struct {
					MaximumAllocation float64 `json:"maximumAllocation"`
					MinimumAllocation float64 `json:"minimumAllocation"`
					Name              string  `json:"name"`
					ResourceType      string  `json:"resourceType"`
					Units             string  `json:"units"`
					Value             float64 `json:"value"`
				} `json:"resourceInformation"`
			} `json:"resourceInformations"`
		} `json:"totalUsedResourcesAcrossPartition"`
		TotalClusterResourcesAcrossPartition struct {
			Memory               float64 `json:"memory"`
			VCores               float64 `json:"vCores"`
			ResourceInformations struct {
				ResourceInformation []struct {
					MaximumAllocation float64 `json:"maximumAllocation"`
					MinimumAllocation float64 `json:"minimumAllocation"`
					Name              string  `json:"name"`
					ResourceType      string  `json:"resourceType"`
					Units             string  `json:"units"`
					Value             float64 `json:"value"`
				} `json:"resourceInformation"`
			} `json:"resourceInformations"`
		} `json:"totalClusterResourcesAcrossPartition"`
	} `json:"clusterMetrics"`
}

type clusterMetricsCollector struct {
	endpoint              *url.URL
	up                    *prometheus.Desc
	applicationsSubmitted *prometheus.Desc
	applicationsCompleted *prometheus.Desc
	applicationsPending   *prometheus.Desc
	applicationsRunning   *prometheus.Desc
	applicationsFailed    *prometheus.Desc
	applicationsKilled    *prometheus.Desc
	memoryReserved        *prometheus.Desc
	memoryAvailable       *prometheus.Desc
	memoryAllocated       *prometheus.Desc
	memoryTotal           *prometheus.Desc
	virtualCoresReserved  *prometheus.Desc
	virtualCoresAvailable *prometheus.Desc
	virtualCoresAllocated *prometheus.Desc
	virtualCoresTotal     *prometheus.Desc
	containersAllocated   *prometheus.Desc
	containersReserved    *prometheus.Desc
	containersPending     *prometheus.Desc
	nodesTotal            *prometheus.Desc
	nodesLost             *prometheus.Desc
	nodesUnhealthy        *prometheus.Desc
	nodesDecommissioned   *prometheus.Desc
	nodesDecommissioning  *prometheus.Desc
	nodesRebooted         *prometheus.Desc
	nodesActive           *prometheus.Desc
	scrapeFailures        *prometheus.Desc
	failureCount          int
}

func newClusterMetricsFuncMetric(metricName string, docString string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName("yarn", "", metricName), docString, nil, nil)
}

func newClusterMetricsCollector(endpoint *url.URL) *clusterMetricsCollector {
	return &clusterMetricsCollector{
		endpoint:              endpoint,
		up:                    newClusterMetricsFuncMetric("up", "Able to contact YARN"),
		applicationsSubmitted: newClusterMetricsFuncMetric("applications_submitted", "Total applications submitted"),
		applicationsCompleted: newClusterMetricsFuncMetric("applications_completed", "Total applications completed"),
		applicationsPending:   newClusterMetricsFuncMetric("applications_pending", "Applications pending"),
		applicationsRunning:   newClusterMetricsFuncMetric("applications_running", "Applications running"),
		applicationsFailed:    newClusterMetricsFuncMetric("applications_failed", "Total application failed"),
		applicationsKilled:    newClusterMetricsFuncMetric("applications_killed", "Total application killed"),
		memoryReserved:        newClusterMetricsFuncMetric("memory_reserved", "Memory reserved"),
		memoryAvailable:       newClusterMetricsFuncMetric("memory_available", "Memory available"),
		memoryAllocated:       newClusterMetricsFuncMetric("memory_allocated", "Memory allocated"),
		memoryTotal:           newClusterMetricsFuncMetric("memory_total", "Total memory"),
		virtualCoresReserved:  newClusterMetricsFuncMetric("virtual_cores_reserved", "Virtual cores reserved"),
		virtualCoresAvailable: newClusterMetricsFuncMetric("virtual_cores_available", "Virtual cores available"),
		virtualCoresAllocated: newClusterMetricsFuncMetric("virtual_cores_allocated", "Virtual cores allocated"),
		virtualCoresTotal:     newClusterMetricsFuncMetric("virtual_cores_total", "Total virtual cores"),
		containersAllocated:   newClusterMetricsFuncMetric("containers_allocated", "Containers allocated"),
		containersReserved:    newClusterMetricsFuncMetric("containers_reserved", "Containers reserved"),
		containersPending:     newClusterMetricsFuncMetric("containers_pending", "Containers pending"),
		nodesTotal:            newClusterMetricsFuncMetric("nodes_total", "Nodes total"),
		nodesLost:             newClusterMetricsFuncMetric("nodes_lost", "Nodes lost"),
		nodesUnhealthy:        newClusterMetricsFuncMetric("nodes_unhealthy", "Nodes unhealthy"),
		nodesDecommissioned:   newClusterMetricsFuncMetric("nodes_decommissioned", "Nodes decommissioned"),
		nodesDecommissioning:  newClusterMetricsFuncMetric("nodes_decommissioning", "Nodes decommissioning"),
		nodesRebooted:         newClusterMetricsFuncMetric("nodes_rebooted", "Nodes rebooted"),
		nodesActive:           newClusterMetricsFuncMetric("nodes_active", "Nodes active"),
		scrapeFailures:        newClusterMetricsFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics"),
	}
}

func (c *clusterMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.applicationsSubmitted
	ch <- c.applicationsCompleted
	ch <- c.applicationsPending
	ch <- c.applicationsRunning
	ch <- c.applicationsFailed
	ch <- c.applicationsKilled
	ch <- c.memoryReserved
	ch <- c.memoryAvailable
	ch <- c.memoryAllocated
	ch <- c.memoryTotal
	ch <- c.virtualCoresReserved
	ch <- c.virtualCoresAvailable
	ch <- c.virtualCoresAllocated
	ch <- c.virtualCoresTotal
	ch <- c.containersAllocated
	ch <- c.containersReserved
	ch <- c.containersPending
	ch <- c.nodesTotal
	ch <- c.nodesLost
	ch <- c.nodesUnhealthy
	ch <- c.nodesDecommissioned
	ch <- c.nodesDecommissioning
	ch <- c.nodesRebooted
	ch <- c.nodesActive
	ch <- c.scrapeFailures
}

func (c *clusterMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0

	data, err := fetchClusterMetrics(c.endpoint)
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

	metrics := data.ClusterMetrics
	ch <- prometheus.MustNewConstMetric(c.applicationsSubmitted, prometheus.CounterValue, metrics.AppsSubmitted)
	ch <- prometheus.MustNewConstMetric(c.applicationsCompleted, prometheus.CounterValue, metrics.AppsCompleted)
	ch <- prometheus.MustNewConstMetric(c.applicationsPending, prometheus.GaugeValue, metrics.AppsPending)
	ch <- prometheus.MustNewConstMetric(c.applicationsRunning, prometheus.GaugeValue, metrics.AppsRunning)
	ch <- prometheus.MustNewConstMetric(c.applicationsFailed, prometheus.CounterValue, metrics.AppsFailed)
	ch <- prometheus.MustNewConstMetric(c.applicationsKilled, prometheus.CounterValue, metrics.AppsKilled)
	ch <- prometheus.MustNewConstMetric(c.memoryReserved, prometheus.GaugeValue, metrics.ReservedMB)
	ch <- prometheus.MustNewConstMetric(c.memoryAvailable, prometheus.GaugeValue, metrics.AvailableMB)
	ch <- prometheus.MustNewConstMetric(c.memoryAllocated, prometheus.GaugeValue, metrics.AllocatedMB)
	ch <- prometheus.MustNewConstMetric(c.memoryTotal, prometheus.GaugeValue, metrics.TotalMB)
	ch <- prometheus.MustNewConstMetric(c.virtualCoresReserved, prometheus.GaugeValue, metrics.ReservedVirtualCores)
	ch <- prometheus.MustNewConstMetric(c.virtualCoresAvailable, prometheus.GaugeValue, metrics.AvailableVirtualCores)
	ch <- prometheus.MustNewConstMetric(c.virtualCoresAllocated, prometheus.GaugeValue, metrics.AllocatedVirtualCores)
	ch <- prometheus.MustNewConstMetric(c.virtualCoresTotal, prometheus.GaugeValue, metrics.TotalVirtualCores)
	ch <- prometheus.MustNewConstMetric(c.containersAllocated, prometheus.GaugeValue, metrics.ContainersAllocated)
	ch <- prometheus.MustNewConstMetric(c.containersReserved, prometheus.GaugeValue, metrics.ContainersReserved)
	ch <- prometheus.MustNewConstMetric(c.containersPending, prometheus.GaugeValue, metrics.ContainersPending)
	ch <- prometheus.MustNewConstMetric(c.nodesTotal, prometheus.GaugeValue, metrics.TotalNodes)
	ch <- prometheus.MustNewConstMetric(c.nodesLost, prometheus.GaugeValue, metrics.LostNodes)
	ch <- prometheus.MustNewConstMetric(c.nodesUnhealthy, prometheus.GaugeValue, metrics.UnhealthyNodes)
	ch <- prometheus.MustNewConstMetric(c.nodesDecommissioned, prometheus.GaugeValue, metrics.DecommissionedNodes)
	ch <- prometheus.MustNewConstMetric(c.nodesDecommissioning, prometheus.GaugeValue, metrics.DecommissioningNodes)
	ch <- prometheus.MustNewConstMetric(c.nodesRebooted, prometheus.GaugeValue, metrics.RebootedNodes)
	ch <- prometheus.MustNewConstMetric(c.nodesActive, prometheus.GaugeValue, metrics.ActiveNodes)

	return
}

func fetchClusterMetrics(u *url.URL) (Metrics, error) {
	var met Metrics

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
		return met, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return met, errors.New("unexpected HTTP status: " + string(resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return met, err
	}

	// var data map[string]map[string]float64
	err = json.Unmarshal(body, &met)

	if err != nil {
		return met, err
	}

	return met, nil
}
