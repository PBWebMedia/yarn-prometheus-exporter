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

type Applications struct {
	Apps struct {
		App []struct {
			ID                         string  `json:"id"`
			User                       string  `json:"user"`
			Name                       string  `json:"name"`
			Queue                      string  `json:"queue"`
			State                      string  `json:"state"`
			FinalStatus                string  `json:"finalStatus"`
			Progress                   float64 `json:"progress"`
			TrackingUI                 string  `json:"trackingUI"`
			TrackingURL                string  `json:"trackingUrl"`
			Diagnostics                string  `json:"diagnostics"`
			ClusterID                  int64   `json:"clusterId"`
			ApplicationType            string  `json:"applicationType"`
			ApplicationTags            string  `json:"applicationTags"`
			Priority                   int     `json:"priority"`
			StartedTime                int64   `json:"startedTime"`
			FinishedTime               int     `json:"finishedTime"`
			ElapsedTime                int64   `json:"elapsedTime"`
			AmContainerLogs            string  `json:"amContainerLogs"`
			AmHostHTTPAddress          string  `json:"amHostHttpAddress"`
			AllocatedMB                int     `json:"allocatedMB"`
			AllocatedVCores            int     `json:"allocatedVCores"`
			RunningContainers          int     `json:"runningContainers"`
			MemorySeconds              int64   `json:"memorySeconds"`
			VcoreSeconds               int     `json:"vcoreSeconds"`
			QueueUsagePercentage       float64 `json:"queueUsagePercentage"`
			ClusterUsagePercentage     float64 `json:"clusterUsagePercentage"`
			PreemptedResourceMB        int     `json:"preemptedResourceMB"`
			PreemptedResourceVCores    int     `json:"preemptedResourceVCores"`
			NumNonAMContainerPreempted int     `json:"numNonAMContainerPreempted"`
			NumAMContainerPreempted    int     `json:"numAMContainerPreempted"`
			LogAggregationStatus       string  `json:"logAggregationStatus"`
			UnmanagedApplication       bool    `json:"unmanagedApplication"`
			AmNodeLabelExpression      string  `json:"amNodeLabelExpression"`
			ResourceRequests           []struct {
				Capability struct {
					Memory       int `json:"memory"`
					MemorySize   int `json:"memorySize"`
					VirtualCores int `json:"virtualCores"`
				} `json:"capability"`
				NodeLabelExpression string `json:"nodeLabelExpression"`
				NumContainers       int    `json:"numContainers"`
				Priority            struct {
					Priority int `json:"priority"`
				} `json:"priority"`
				RelaxLocality bool   `json:"relaxLocality"`
				ResourceName  string `json:"resourceName"`
			} `json:"resourceRequests,omitempty"`
		} `json:"app"`
	} `json:"apps"`
}

type appsCollector struct {
	endpoint          *url.URL
	appStateRunning   *prometheus.Desc
	appStateFailed    *prometheus.Desc
	appStateFinished  *prometheus.Desc
	appStateAccepted  *prometheus.Desc
	appStateSubmitted *prometheus.Desc
	appStateKilled    *prometheus.Desc
	appStateUnknown   *prometheus.Desc
}

func newAppsFuncMetric(metricName string, docString string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName("yarn", "", metricName), docString, []string{"id", "name", "container_logs_url"}, nil)
}

func newAppsCollector(endpoint *url.URL) *appsCollector {
	return &appsCollector{
		endpoint:          endpoint,
		appStateRunning:   newAppsFuncMetric("app_state_running", "Running applications"),
		appStateFailed:    newAppsFuncMetric("app_state_failed", "Failed applications"),
		appStateFinished:  newAppsFuncMetric("app_state_finished", "Finished applications"),
		appStateAccepted:  newAppsFuncMetric("app_state_accepted", "Accepted applications"),
		appStateSubmitted: newAppsFuncMetric("app_state_submitted", "Submitted applications"),
		appStateKilled:    newAppsFuncMetric("app_state_killed", "Killed applications"),
		appStateUnknown:   newAppsFuncMetric("app_state_unknown", "Unknown applications"),
	}
}

func (c *appsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.appStateRunning
	ch <- c.appStateFailed
	ch <- c.appStateFinished
	ch <- c.appStateAccepted
	ch <- c.appStateSubmitted
	ch <- c.appStateKilled
	ch <- c.appStateUnknown
}

func (c *appsCollector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0

	data, err := fetchApps(c.endpoint)
	if err != nil {
		up = 0.0
		log.Println("Error while collecting data from YARN: " + err.Error())
	}

	if up == 0.0 {
		return
	}

	apps := data.Apps.App
	for _, app := range apps {
		switch state := app.State; state {
		case "RUNNING":
			ch <- prometheus.MustNewConstMetric(c.appStateRunning, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		case "FAILED":
			ch <- prometheus.MustNewConstMetric(c.appStateFailed, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		case "FINISHED":
			ch <- prometheus.MustNewConstMetric(c.appStateFinished, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		case "ACCEPTED":
			ch <- prometheus.MustNewConstMetric(c.appStateAccepted, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		case "SUBMITTED":
			ch <- prometheus.MustNewConstMetric(c.appStateSubmitted, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		case "KILLED":
			ch <- prometheus.MustNewConstMetric(c.appStateKilled, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		default:
			ch <- prometheus.MustNewConstMetric(c.appStateUnknown, prometheus.GaugeValue, 1, app.ID, app.Name, app.AmContainerLogs)
		}
	}

	return
}

func fetchApps(u *url.URL) (Applications, error) {
	var apps Applications

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
		return apps, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return apps, errors.New("unexpected HTTP status: " + string(resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return apps, err
	}

	err = json.Unmarshal(body, &apps)

	if err != nil {
		return apps, err
	}

	return apps, nil
}
