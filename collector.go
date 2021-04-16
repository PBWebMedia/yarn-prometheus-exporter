package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
)

type collector struct {
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

const metricsNamespace = "yarn"

func newFuncMetric(metricName string, docString string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "", metricName), docString, nil, nil)
}

func newCollector(endpoint *url.URL) *collector {
	return &collector{
		endpoint:              endpoint,
		up:                    newFuncMetric("up", "Able to contact YARN"),
		applicationsSubmitted: newFuncMetric("applications_submitted", "Total applications submitted"),
		applicationsCompleted: newFuncMetric("applications_completed", "Total applications completed"),
		applicationsPending:   newFuncMetric("applications_pending", "Applications pending"),
		applicationsRunning:   newFuncMetric("applications_running", "Applications running"),
		applicationsFailed:    newFuncMetric("applications_failed", "Total application failed"),
		applicationsKilled:    newFuncMetric("applications_killed", "Total application killed"),
		memoryReserved:        newFuncMetric("memory_reserved", "Memory reserved"),
		memoryAvailable:       newFuncMetric("memory_available", "Memory available"),
		memoryAllocated:       newFuncMetric("memory_allocated", "Memory allocated"),
		memoryTotal:           newFuncMetric("memory_total", "Total memory"),
		virtualCoresReserved:  newFuncMetric("virtual_cores_reserved", "Virtual cores reserved"),
		virtualCoresAvailable: newFuncMetric("virtual_cores_available", "Virtual cores available"),
		virtualCoresAllocated: newFuncMetric("virtual_cores_allocated", "Virtual cores allocated"),
		virtualCoresTotal:     newFuncMetric("virtual_cores_total", "Total virtual cores"),
		containersAllocated:   newFuncMetric("containers_allocated", "Containers allocated"),
		containersReserved:    newFuncMetric("containers_reserved", "Containers reserved"),
		containersPending:     newFuncMetric("containers_pending", "Containers pending"),
		nodesTotal:            newFuncMetric("nodes_total", "Nodes total"),
		nodesLost:             newFuncMetric("nodes_lost", "Nodes lost"),
		nodesUnhealthy:        newFuncMetric("nodes_unhealthy", "Nodes unhealthy"),
		nodesDecommissioned:   newFuncMetric("nodes_decommissioned", "Nodes decommissioned"),
		nodesDecommissioning:  newFuncMetric("nodes_decommissioning", "Nodes decommissioning"),
		nodesRebooted:         newFuncMetric("nodes_rebooted", "Nodes rebooted"),
		nodesActive:           newFuncMetric("nodes_active", "Nodes active"),
		scrapeFailures:        newFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics"),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
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

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0

	data, err := fetch(c.endpoint)
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

	metrics := data["clusterMetrics"]

	ch <- prometheus.MustNewConstMetric(c.applicationsSubmitted, prometheus.CounterValue, metrics["appsSubmitted"])
	ch <- prometheus.MustNewConstMetric(c.applicationsCompleted, prometheus.CounterValue, metrics["appsCompleted"])
	ch <- prometheus.MustNewConstMetric(c.applicationsPending, prometheus.GaugeValue, metrics["appsPending"])
	ch <- prometheus.MustNewConstMetric(c.applicationsRunning, prometheus.GaugeValue, metrics["appsRunning"])
	ch <- prometheus.MustNewConstMetric(c.applicationsFailed, prometheus.CounterValue, metrics["appsFailed"])
	ch <- prometheus.MustNewConstMetric(c.applicationsKilled, prometheus.CounterValue, metrics["appsKilled"])
	ch <- prometheus.MustNewConstMetric(c.memoryReserved, prometheus.GaugeValue, metrics["reservedMB"])
	ch <- prometheus.MustNewConstMetric(c.memoryAvailable, prometheus.GaugeValue, metrics["availableMB"])
	ch <- prometheus.MustNewConstMetric(c.memoryAllocated, prometheus.GaugeValue, metrics["allocatedMB"])
	ch <- prometheus.MustNewConstMetric(c.memoryTotal, prometheus.GaugeValue, metrics["totalMB"])
	ch <- prometheus.MustNewConstMetric(c.virtualCoresReserved, prometheus.GaugeValue, metrics["reservedVirtualCores"])
	ch <- prometheus.MustNewConstMetric(c.virtualCoresAvailable, prometheus.GaugeValue, metrics["availableVirtualCores"])
	ch <- prometheus.MustNewConstMetric(c.virtualCoresAllocated, prometheus.GaugeValue, metrics["allocatedVirtualCores"])
	ch <- prometheus.MustNewConstMetric(c.virtualCoresTotal, prometheus.GaugeValue, metrics["totalVirtualCores"])
	ch <- prometheus.MustNewConstMetric(c.containersAllocated, prometheus.GaugeValue, metrics["containersAllocated"])
	ch <- prometheus.MustNewConstMetric(c.containersReserved, prometheus.GaugeValue, metrics["containersReserved"])
	ch <- prometheus.MustNewConstMetric(c.containersPending, prometheus.GaugeValue, metrics["containersPending"])
	ch <- prometheus.MustNewConstMetric(c.nodesTotal, prometheus.GaugeValue, metrics["totalNodes"])
	ch <- prometheus.MustNewConstMetric(c.nodesLost, prometheus.GaugeValue, metrics["lostNodes"])
	ch <- prometheus.MustNewConstMetric(c.nodesUnhealthy, prometheus.GaugeValue, metrics["unhealthyNodes"])
	ch <- prometheus.MustNewConstMetric(c.nodesDecommissioned, prometheus.GaugeValue, metrics["decommissionedNodes"])
	ch <- prometheus.MustNewConstMetric(c.nodesDecommissioning, prometheus.GaugeValue, metrics["decommissioningNodes"])
	ch <- prometheus.MustNewConstMetric(c.nodesRebooted, prometheus.GaugeValue, metrics["rebootedNodes"])
	ch <- prometheus.MustNewConstMetric(c.nodesActive, prometheus.GaugeValue, metrics["activeNodes"])

	return
}

func fetch(u *url.URL) (map[string]map[string]float64, error) {
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
		return nil, errors.New(fmt.Sprintf("unexpected HTTP status: %v", resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	var data map[string]map[string]float64

	err = json.Unmarshal(body, &data)

	if err != nil {
		return nil, err
	}

	return data, nil
}
