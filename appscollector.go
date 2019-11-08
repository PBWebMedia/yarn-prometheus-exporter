package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var persistedApps map[string]time.Time
var persistedAppMaxMinutes int

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
	endpoint        *url.URL
	appStateRunning *prometheus.Desc
}

func newAppsFuncMetric(metricName string, docString string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName("yarn", "", metricName), docString, []string{"name"}, nil)
}

func newAppsCollector(endpoint *url.URL, persistAppMaxMinutes int) *appsCollector {
	persistedApps = make(map[string]time.Time)
	persistedAppMaxMinutes = persistAppMaxMinutes
	return &appsCollector{
		endpoint:        endpoint,
		appStateRunning: newAppsFuncMetric("app_state_running", "Running applications"),
	}
}

func (c *appsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.appStateRunning
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
	appStates := map[string][]string{}
	now := time.Now()

	// generate a unique map of apps with their associated states
	for _, app := range apps {
		if _, ok := appStates[app.Name]; ok {
			if !stringInSlice(app.State, appStates[app.Name]) {
				appStates[app.Name] = append(appStates[app.Name], app.State)
			}
		} else {
			appStates[app.Name] = []string{app.State}
		}

		persistedApps[app.Name] = now
	}

	// dump the metrics based on app state map
	var keys []string
	for k, v := range appStates {
		var running bool
		if stringInSlice("RUNNING", v) {
			running = true
		} else {
			running = false
		}
		ch <- prometheus.MustNewConstMetric(c.appStateRunning, prometheus.GaugeValue, float64(boolToInt(running)), k)
		keys = append(keys, k)
	}

	// clean up list of persisted apps if they haven't been seen in a while, otherwise set them to a non-running state if weren't found to be running
	for k, v := range persistedApps {
		delta := now.Sub(v)
		deltaMinutes := int(delta.Minutes())

		if deltaMinutes > persistedAppMaxMinutes {
			delete(persistedApps, k)
		} else {
			if !stringInSlice(k, keys) {
				ch <- prometheus.MustNewConstMetric(c.appStateRunning, prometheus.GaugeValue, float64(boolToInt(false)), k)
			}
		}
	}

	return
}

func boolToInt(boo bool) int {
	if boo {
		return 1
	}
	return 0
}

func stringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
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
