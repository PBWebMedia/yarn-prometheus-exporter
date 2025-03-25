package main

import (
	"encoding/json"
	"fmt"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

const (
	UrlMetricsCluster   = "ws/v1/cluster/metrics"
	UrlMetricsScheduler = "ws/v1/cluster/scheduler"
	UrlMetricsApps      = "ws/v1/cluster/apps?states=RUNNING"
	UrlMetricsNodes     = "ws/v1/cluster/nodes?states=RUNNING"
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

type nodesMetricsResponse struct {
	NodesMetrics nodesDetails `json:"nodes"`
}

type nodesDetails struct {
	Nodes []nodesMetrics `json:"node"`
}

type nodesMetrics struct {
	Rack                  string  `json:"rack"`
	State                 string  `json:"state"`
	NodeHostName          string  `json:"nodeHostName"`
	LastHealthUpdate      int64   `json:"lastHealthUpdate"`
	Version               string  `json:"version"`
	NumContainers         int     `json:"numContainers"`
	UsedMemoryMB          float64 `json:"usedMemoryMB"`
	AvailMemoryMB         int     `json:"availMemoryMB"`
	UsedVirtualCores      int     `json:"usedVirtualCores"`
	AvailableVirtualCores int     `json:"availableVirtualCores"`
	MemUtilization        float64 `json:"memUtilization"`
	CpuUtilization        float64 `json:"cpuUtilization"`
	NumQueuedContainers   int     `json:"numQueuedContainers"`
}

type appsMetrics struct {
	Apps appsDetails `json:"apps"`
}

type appsDetails struct {
	App []appMetrics `json:"app"`
}

type appMetrics struct {
	Id                     string  `json:"id"`
	User                   string  `json:"user"`
	Name                   string  `json:"name"`
	Queue                  string  `json:"queue"`
	ClusterId              int64   `json:"clusterId"`
	ApplicationType        string  `json:"applicationType"`
	StartedTime            int64   `json:"startedTime"`
	LaunchTime             int64   `json:"launchTime"`
	FinishedTime           int     `json:"finishedTime"`
	ElapsedTime            int     `json:"elapsedTime"`
	QueueUsagePercentage   float64 `json:"queueUsagePercentage"`
	ClusterUsagePercentage float64 `json:"clusterUsagePercentage"`
}

type KerberosConfig struct {
	Krb5ConfigPath string
	KeyTabFile     string
	Username       string
	Realm          string
}

func NewKerberosConfig(krb5ConfigPath, keyTabFile, username string) *KerberosConfig {
	k := &KerberosConfig{}

	k.SetCredentials(krb5ConfigPath, keyTabFile, username)

	return k
}

func (k *KerberosConfig) SetCredentials(krb5ConfigPath, keyTabFile, username string) {
	k.KeyTabFile = keyTabFile
	k.Krb5ConfigPath = krb5ConfigPath

	parts := strings.Split(username, "@")

	k.Username = parts[0]

	if len(parts) > 1 {
		k.Realm = parts[1]
	}
}

type collector struct {
	endpoint []string

	up             *prometheus.Desc
	scrapeFailures *prometheus.Desc
	failureCount   atomic.Int32
	applications   *prometheus.Desc
	memory         *prometheus.Desc
	cores          *prometheus.Desc
	containers     *prometheus.Desc
	nodes          *prometheus.Desc

	queueCapacity         *prometheus.Desc
	queueUsedCapacity     *prometheus.Desc
	queueMaxCapacity      *prometheus.Desc
	queueApplicationCount *prometheus.Desc
	queueUsedMemoryBytes  *prometheus.Desc
	queueUsedVCoresCount  *prometheus.Desc

	nodesContainersCount *prometheus.GaugeVec
	nodesMemUsed         *prometheus.GaugeVec
	nodesMemAvailable    *prometheus.GaugeVec
	nodesMemUtilization  *prometheus.GaugeVec
	nodesVCoresUsed      *prometheus.GaugeVec
	nodesVCoresAvailable *prometheus.GaugeVec
	nodesCpuUtilization  *prometheus.GaugeVec
	nodesStatusCount     *prometheus.GaugeVec

	appsElapsedTime  *prometheus.GaugeVec
	appsQueueUsage   *prometheus.GaugeVec
	appsClusterUsage *prometheus.GaugeVec

	wg sync.WaitGroup

	kerberosConfig *KerberosConfig
}

const metricsNamespace = "yarn"

func newMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", metricName),
		docString,
		labels,
		nil)
}

func newGaugeVecMetric(metricName string, docString string, labels []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(metricsNamespace, "", metricName),
			Help: docString,
		},
		labels,
	)
}

func newCollector(endpoints []string) *collector {
	c := &collector{
		endpoint:       endpoints,
		up:             newMetric("up", "Able to contact YARN", nil),
		scrapeFailures: newMetric("scrape_failures_total", "Number of errors while scraping YARN metrics", nil),
		applications:   newMetric("applications_total", "Applications stats", []string{"status"}),
		memory:         newMetric("memory_bytes", "Memory allocation stats", []string{"status"}),
		cores:          newMetric("cores_total", "Cpu allocation stats", []string{"status"}),
		containers:     newMetric("containers_total", "Container stats", []string{"status"}),
		nodes:          newMetric("nodes_total", "Node stats", []string{"status"}),

		queueCapacity:         newMetric("queue_capacity", "Queue capacity", []string{"queue"}),
		queueUsedCapacity:     newMetric("queue_used_capacity", "Queue used capacity", []string{"queue"}),
		queueMaxCapacity:      newMetric("queue_max_capacity", "Queue max capacity", []string{"queue"}),
		queueApplicationCount: newMetric("queue_application_count", "Queue application count", []string{"queue"}),
		queueUsedMemoryBytes:  newMetric("queue_used_memory_bytes", "Queue used memory bytes", []string{"queue"}),
		queueUsedVCoresCount:  newMetric("queue_used_vcores_count", "Queue used vcores count", []string{"queue"}),

		nodesContainersCount: newGaugeVecMetric("nodes_containers_count", "Nodes containers count", []string{"host", "rack"}),
		nodesMemUsed:         newGaugeVecMetric("nodes_mem_used_mb", "Nodes memory usage", []string{"host", "rack"}),
		nodesMemAvailable:    newGaugeVecMetric("nodes_mem_available_mb", "Nodes memory available", []string{"host", "rack"}),
		nodesMemUtilization:  newGaugeVecMetric("nodes_mem_utilization", "Nodes memory utilization", []string{"host", "rack"}),
		nodesVCoresUsed:      newGaugeVecMetric("nodes_vcore_used_count", "Nodes VCores usage", []string{"host", "rack"}),
		nodesVCoresAvailable: newGaugeVecMetric("nodes_vcore_available_count", "Nodes VCores available", []string{"host", "rack"}),
		nodesCpuUtilization:  newGaugeVecMetric("nodes_cpu_utilization", "Nodes CPU utilization", []string{"host", "rack"}),
		nodesStatusCount:     newGaugeVecMetric("nodes_state", "Nodes states", []string{"status"}),

		appsElapsedTime:  newGaugeVecMetric("apps_elapsed_time", "Applications elapsed time", []string{"id", "type", "name", "user", "queue"}),
		appsQueueUsage:   newGaugeVecMetric("apps_queue_usage", "Applications usage percent in queue", []string{"id", "type", "name", "user", "queue"}),
		appsClusterUsage: newGaugeVecMetric("apps_cluster_usage", "Applications usage percents in cluster", []string{"id", "type", "name", "user", "queue"}),

		wg: sync.WaitGroup{},
	}

	prometheus.MustRegister(c.nodesMemUsed)
	prometheus.MustRegister(c.nodesContainersCount)
	prometheus.MustRegister(c.nodesMemAvailable)
	prometheus.MustRegister(c.nodesVCoresUsed)
	prometheus.MustRegister(c.nodesStatusCount)
	prometheus.MustRegister(c.nodesMemUtilization)
	prometheus.MustRegister(c.nodesCpuUtilization)

	prometheus.MustRegister(c.appsQueueUsage)
	prometheus.MustRegister(c.appsElapsedTime)
	prometheus.MustRegister(c.appsClusterUsage)

	return c
}

func (c *collector) setKerberosCredentials(k *KerberosConfig) {
	c.kerberosConfig = k
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
	ch <- c.queueUsedCapacity
	ch <- c.queueMaxCapacity
	ch <- c.queueApplicationCount
	ch <- c.queueUsedMemoryBytes
	ch <- c.queueUsedVCoresCount
}

// Collect is called by the Prometheus registry when collecting
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	var up atomic.Int32
	up.Store(1)

	c.wg.Add(1)
	go func(ch chan<- prometheus.Metric) {
		defer func() {
			c.wg.Done()
		}()
		m, err := c.fetchClusterMetrics()

		if err != nil {
			up.Store(0)
			c.failureCount.Add(1)

			log.Println("Error while collecting cluster data from YARN: " + err.Error())
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
	}(ch)

	c.wg.Add(1)
	go func(ch chan<- prometheus.Metric) {
		defer func() {
			c.wg.Done()
		}()

		s, err := c.fetchSchedulerMetrics()
		if err != nil {
			up.Store(0)
			c.failureCount.Add(1)

			log.Println("Error while collecting scheduler data from YARN: " + err.Error())
			return
		}

		for _, q := range s.SchedulerInfo.Queues.Queue {
			ch <- prometheus.MustNewConstMetric(c.queueCapacity, prometheus.GaugeValue, q.Capacity, q.QueueName)
			ch <- prometheus.MustNewConstMetric(c.queueUsedCapacity, prometheus.GaugeValue, q.UsedCapacity, q.QueueName)
			ch <- prometheus.MustNewConstMetric(c.queueMaxCapacity, prometheus.GaugeValue, q.MaxCapacity, q.QueueName)
			ch <- prometheus.MustNewConstMetric(c.queueApplicationCount, prometheus.GaugeValue, float64(q.NumApplications), q.QueueName)
			ch <- prometheus.MustNewConstMetric(c.queueUsedMemoryBytes, prometheus.GaugeValue, float64(q.ResourcesUsed.Memory)*1024*1024, q.QueueName)
			ch <- prometheus.MustNewConstMetric(c.queueUsedVCoresCount, prometheus.GaugeValue, float64(q.ResourcesUsed.VCores), q.QueueName)
		}
	}(ch)

	c.wg.Add(1)
	go func() {
		defer func() {
			c.wg.Done()
		}()

		n, err := c.fetchNodesMetrics()
		if err != nil {
			up.Store(0)
			c.failureCount.Add(1)

			log.Println("Error while collecting nodes data from YARN: " + err.Error())
			return
		}

		for _, node := range n.Nodes {
			// remove trailing "/"
			if strings.HasPrefix(node.Rack, "/") {
				node.Rack = node.Rack[1:]
			}

			c.nodesContainersCount.WithLabelValues(node.NodeHostName, node.Rack).Set(float64(node.NumContainers))

			c.nodesMemUsed.WithLabelValues(node.NodeHostName, node.Rack).Set(node.UsedMemoryMB)
			c.nodesMemAvailable.WithLabelValues(node.NodeHostName, node.Rack).Set(float64(node.AvailMemoryMB))
			c.nodesMemUtilization.WithLabelValues(node.NodeHostName, node.Rack).Set(node.MemUtilization)

			c.nodesVCoresUsed.WithLabelValues(node.NodeHostName, node.Rack).Set(float64(node.UsedVirtualCores))
			c.nodesVCoresAvailable.WithLabelValues(node.NodeHostName, node.Rack).Set(float64(node.AvailableVirtualCores))
			c.nodesCpuUtilization.WithLabelValues(node.NodeHostName, node.Rack).Set(node.CpuUtilization)
		}
	}()

	c.wg.Add(1)
	go func() {
		defer func() {
			c.wg.Done()
		}()

		a, err := c.fetchAppMetrics()
		if err != nil {
			up.Store(0)
			c.failureCount.Add(1)

			log.Println("Error while collecting app data from YARN: " + err.Error())
			return
		}

		for _, app := range a.App {
			labels := []string{app.Id, app.ApplicationType, app.Name, app.User, app.Queue}
			c.appsQueueUsage.WithLabelValues(labels...).Set(app.QueueUsagePercentage)
			c.appsElapsedTime.WithLabelValues(labels...).Set(float64(app.ElapsedTime))
			c.appsClusterUsage.WithLabelValues(labels...).Set(app.ClusterUsagePercentage)
		}
	}()

	c.wg.Wait()
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, float64(up.Load()))
	ch <- prometheus.MustNewConstMetric(c.scrapeFailures, prometheus.CounterValue, float64(c.failureCount.Load()))
}

// fetchClusterMetrics loads data from YARN endpoints about cluster
func (c *collector) fetchClusterMetrics() (*clusterMetrics, error) {
	var r clusterMetricsResponse
	err := c.fetch(UrlMetricsCluster, &r)
	if err != nil {
		return nil, err
	}
	return &r.ClusterMetrics, nil
}

// fetchAppMetrics loads data from Yarn about scheduler
func (c *collector) fetchSchedulerMetrics() (*scheduler, error) {
	var r schedulerResponse
	err := c.fetch(UrlMetricsScheduler, &r)
	if err != nil {
		return nil, err
	}
	return &r.Scheduler, nil
}

// fetchAppMetrics loads data from Yarn about applications
func (c *collector) fetchAppMetrics() (*appsDetails, error) {
	var r appsMetrics
	err := c.fetch(UrlMetricsApps, &r)

	if err != nil {
		return nil, err
	}
	return &r.Apps, nil
}

// fetchNodesMetrics loads data from Yarn about nodes
func (c *collector) fetchNodesMetrics() (*nodesDetails, error) {
	var r nodesMetricsResponse
	err := c.fetch(UrlMetricsNodes, &r)

	if err != nil {
		return nil, err
	}
	return &r.NodesMetrics, nil
}

// fetch Executes call for HTTP endpoint
func (c *collector) fetch(path string, v any) error {
	// If we've setup of kerberos, make special call
	if c.kerberosConfig != nil && c.kerberosConfig.Username != "" {
		return c.httpKrbRequest(path, v)
	}

	var (
		resp *http.Response
		err  error
	)

	for i := 0; i < len(c.endpoint); i++ {
		resp, err = http.Get(c.endpoint[i] + path)
		if err != nil {
			continue
		}

		defer func(resp *http.Response) {
			_ = resp.Body.Close()
		}(resp)

		if resp.StatusCode == http.StatusOK {
			break
		}
		err = fmt.Errorf("unexpected HTTP status: %v", resp.StatusCode)
	}

	if err != nil {
		return err
	}

	return json.NewDecoder(resp.Body).Decode(v)
}

// httpKrbRequest performs request to kerberos-protected endpoint via HTTP client
func (c *collector) httpKrbRequest(path string, v any) error {
	var (
		resp *http.Response
		req  *http.Request
		err  error
	)

	krb5Conf, err := config.Load(c.kerberosConfig.Krb5ConfigPath)
	if err != nil {
		return fmt.Errorf("error loading krb5.conf: %v", err)
	}

	kt, err := keytab.Load(c.kerberosConfig.KeyTabFile)
	if err != nil {
		return fmt.Errorf("error loading keytab file: %v", err)
	}

	cli := client.NewWithKeytab(c.kerberosConfig.Username, c.kerberosConfig.Realm, kt, krb5Conf)
	if err != nil {
		return fmt.Errorf("error parsing kerberos keytab file: %s, error: %v", c.kerberosConfig.KeyTabFile, err)
	}

	for i := 0; i < len(c.endpoint); i++ {
		req, err = http.NewRequest("GET", c.endpoint[i]+path, nil)
		if err != nil {
			continue
		}

		spnegoCl := spnego.NewClient(cli, nil, "")
		resp, err = spnegoCl.Do(req)
		if err != nil {
			continue
		}

		defer func(resp *http.Response) {
			_ = resp.Body.Close()
		}(resp)

		if resp.StatusCode == http.StatusOK {
			break
		}

		err = fmt.Errorf("unexpected HTTP status: %v", resp.StatusCode)
	}

	if err != nil {
		return err
	}

	return json.NewDecoder(resp.Body).Decode(v)
}
