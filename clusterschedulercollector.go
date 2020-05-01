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

type SchedulerMetrics struct {
	Scheduler struct {
		SchedulerInfo struct {
			Capacities struct {
				QueueCapacitiesByPartition []struct {
					AbsoluteCapacity     float64 `json:"absoluteCapacity"`
					AbsoluteMaxCapacity  float64 `json:"absoluteMaxCapacity"`
					AbsoluteUsedCapacity float64 `json:"absoluteUsedCapacity"`
					Capacity             float64 `json:"capacity"`
					MaxAMLimitPercentage float64 `json:"maxAMLimitPercentage"`
					MaxCapacity          float64 `json:"maxCapacity"`
					PartitionName        string  `json:"partitionName"`
					UsedCapacity         float64 `json:"usedCapacity"`
				} `json:"queueCapacitiesByPartition"`
			} `json:"capacities"`
			Capacity    float64 `json:"capacity"`
			MaxCapacity float64 `json:"maxCapacity"`
			QueueName   string  `json:"queueName"`
			Queues      struct {
				Queue []struct {
					AMResourceLimit struct {
						Memory float64 `json:"memory"`
						VCores float64 `json:"vCores"`
					} `json:"AMResourceLimit"`
					AbsoluteCapacity     float64 `json:"absoluteCapacity"`
					AbsoluteMaxCapacity  float64 `json:"absoluteMaxCapacity"`
					AbsoluteUsedCapacity float64 `json:"absoluteUsedCapacity"`
					AllocatedContainers  float64 `json:"allocatedContainers"`
					Capacities           struct {
						QueueCapacitiesByPartition []struct {
							AbsoluteCapacity     float64 `json:"absoluteCapacity"`
							AbsoluteMaxCapacity  float64 `json:"absoluteMaxCapacity"`
							AbsoluteUsedCapacity float64 `json:"absoluteUsedCapacity"`
							Capacity             float64 `json:"capacity"`
							MaxAMLimitPercentage float64 `json:"maxAMLimitPercentage"`
							MaxCapacity          float64 `json:"maxCapacity"`
							PartitionName        string  `json:"partitionName"`
							UsedCapacity         float64 `json:"usedCapacity"`
						} `json:"queueCapacitiesByPartition"`
					} `json:"capacities"`
					Capacity               float64  `json:"capacity"`
					HideReservationQueues  bool     `json:"hideReservationQueues"`
					MaxApplications        float64  `json:"maxApplications"`
					MaxApplicationsPerUser float64  `json:"maxApplicationsPerUser"`
					MaxCapacity            float64  `json:"maxCapacity"`
					NodeLabels             []string `json:"nodeLabels"`
					NumActiveApplications  float64  `json:"numActiveApplications"`
					NumApplications        float64  `json:"numApplications"`
					NumContainers          float64  `json:"numContainers"`
					NumPendingApplications float64  `json:"numPendingApplications"`
					PendingContainers      float64  `json:"pendingContainers"`
					PreemptionDisabled     bool     `json:"preemptionDisabled"`
					QueueName              string   `json:"queueName"`
					ReservedContainers     float64  `json:"reservedContainers"`
					Resources              struct {
						ResourceUsagesByPartition []struct {
							AmLimit struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"amLimit"`
							AmUsed struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"amUsed"`
							PartitionName string `json:"partitionName"`
							Pending       struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"pending"`
							Reserved struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"reserved"`
							Used struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"used"`
						} `json:"resourceUsagesByPartition"`
					} `json:"resources"`
					ResourcesUsed struct {
						Memory float64 `json:"memory"`
						VCores float64 `json:"vCores"`
					} `json:"resourcesUsed"`
					State          string `json:"state"`
					Type           string `json:"type"`
					UsedAMResource struct {
						Memory float64 `json:"memory"`
						VCores float64 `json:"vCores"`
					} `json:"usedAMResource"`
					UsedCapacity        float64 `json:"usedCapacity"`
					UserAMResourceLimit struct {
						Memory float64 `json:"memory"`
						VCores float64 `json:"vCores"`
					} `json:"userAMResourceLimit"`
					UserLimit       float64 `json:"userLimit"`
					UserLimitFactor float64 `json:"userLimitFactor"`
					Users           struct {
						User []struct {
							AMResourceUsed struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"AMResourceUsed"`
							NumActiveApplications  float64 `json:"numActiveApplications"`
							NumPendingApplications float64 `json:"numPendingApplications"`
							Resources              struct {
								ResourceUsagesByPartition []struct {
									AmLimit struct {
										Memory float64 `json:"memory"`
										VCores float64 `json:"vCores"`
									} `json:"amLimit"`
									AmUsed struct {
										Memory float64 `json:"memory"`
										VCores float64 `json:"vCores"`
									} `json:"amUsed"`
									PartitionName string `json:"partitionName"`
									Pending       struct {
										Memory float64 `json:"memory"`
										VCores float64 `json:"vCores"`
									} `json:"pending"`
									Reserved struct {
										Memory float64 `json:"memory"`
										VCores float64 `json:"vCores"`
									} `json:"reserved"`
									Used struct {
										Memory float64 `json:"memory"`
										VCores float64 `json:"vCores"`
									} `json:"used"`
								} `json:"resourceUsagesByPartition"`
							} `json:"resources"`
							ResourcesUsed struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"resourcesUsed"`
							UserResourceLimit struct {
								Memory float64 `json:"memory"`
								VCores float64 `json:"vCores"`
							} `json:"userResourceLimit"`
							Username string `json:"username"`
						} `json:"user"`
					} `json:"users"`
				} `json:"queue"`
			} `json:"queues"`
			UsedCapacity float64 `json:"usedCapacity"`
		} `json:"schedulerInfo"`
	} `json:"scheduler"`
}

type clusterSchedulerCollector struct {
	endpoint                       *url.URL
	up                             *prometheus.Desc
	capacity                       *prometheus.Desc
	maxCapacity                    *prometheus.Desc
	usedCapacity                   *prometheus.Desc
	queueNumContainers             *prometheus.Desc
	queueNumApplications           *prometheus.Desc
	queueCapacity                  *prometheus.Desc
	queueMaxCapacity               *prometheus.Desc
	queueUsedCapacity              *prometheus.Desc
	queueAbsoluteCapacity          *prometheus.Desc
	queueAbsoluteMaxCapacity       *prometheus.Desc
	queueAbsoluteUsedCapacity      *prometheus.Desc
	queueAllocatedContainers       *prometheus.Desc
	queueReservedContainers        *prometheus.Desc
	queuePendingContainers         *prometheus.Desc
	queueNumActiveApplications     *prometheus.Desc
	queueNumPendingApplications    *prometheus.Desc
	queueNumMaxApplications        *prometheus.Desc
	queueNumMaxApplicationsPerUser *prometheus.Desc
	scrapeFailures                 *prometheus.Desc
	failureCount                   int
}

func newClusterSchedulerFuncMetric(metricName string, docString string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName("yarn", "scheduler", metricName), docString, nil, nil)
}

func newClusterSchedulerCollector(endpoint *url.URL) *clusterSchedulerCollector {
	var queueLabels []string
	queueLabels = []string{"yarn_queue_name"}

	return &clusterSchedulerCollector{
		endpoint:                       endpoint,
		up:                             newClusterSchedulerFuncMetric("up", "Able to contact YARN"),
		capacity:                       newClusterSchedulerFuncMetric("capacity", "Scheduler capacity"),
		maxCapacity:                    newClusterSchedulerFuncMetric("max_capacity", "Scheduler max capacity"),
		usedCapacity:                   newClusterSchedulerFuncMetric("used_capacity", "Scheduler used capacity"),
		queueNumContainers:             prometheus.NewDesc("queue_containers", "Scheduler queue num containers", queueLabels, nil),
		queueNumApplications:           prometheus.NewDesc("queue_applications", "Scheduler queue num applications", queueLabels, nil),
		queueCapacity:                  prometheus.NewDesc("queue_capacity", "Scheduler queue capacity", queueLabels, nil),
		queueMaxCapacity:               prometheus.NewDesc("queue_max_capacity", "Scheduler queue max capacity", queueLabels, nil),
		queueUsedCapacity:              prometheus.NewDesc("queue_used_capacity", "Scheduler queue used capacity", queueLabels, nil),
		queueAbsoluteCapacity:          prometheus.NewDesc("queue_absolute_capacity", "Scheduler queue absolute capacity", queueLabels, nil),
		queueAbsoluteMaxCapacity:       prometheus.NewDesc("queue_absolute_max_capacity", "Scheduler queue absolute max capacity", queueLabels, nil),
		queueAbsoluteUsedCapacity:      prometheus.NewDesc("queue_absolute_used_capacity", "Scheduler queue absolute used capacity", queueLabels, nil),
		queueAllocatedContainers:       prometheus.NewDesc("queue_allocated_containers", "Scheduler queue allocated containers", queueLabels, nil),
		queueReservedContainers:        prometheus.NewDesc("queue_reserved_containers", "Scheduler queue reserved containers", queueLabels, nil),
		queuePendingContainers:         prometheus.NewDesc("queue_pending_containers", "Scheduler queue pending containers", queueLabels, nil),
		queueNumActiveApplications:     prometheus.NewDesc("queue_active_applications", "Scheduler queue num active applications", queueLabels, nil),
		queueNumPendingApplications:    prometheus.NewDesc("queue_pending_applications", "Scheduler queue num pending applications", queueLabels, nil),
		queueNumMaxApplications:        prometheus.NewDesc("queue_max_applications", "Scheduler queue num max applications", queueLabels, nil),
		queueNumMaxApplicationsPerUser: prometheus.NewDesc("queue_max_applications_per_user", "Scheduler queue num max applications per users", queueLabels, nil),
		scrapeFailures:                 newClusterSchedulerFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics"),
	}
}

func (c *clusterSchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.capacity
	ch <- c.maxCapacity
	ch <- c.usedCapacity
	ch <- c.queueNumContainers
	ch <- c.queueCapacity
	ch <- c.queueMaxCapacity
	ch <- c.queueUsedCapacity
	ch <- c.queueAbsoluteCapacity
	ch <- c.queueAbsoluteMaxCapacity
	ch <- c.queueAbsoluteUsedCapacity
	ch <- c.queueAllocatedContainers
	ch <- c.queueReservedContainers
	ch <- c.queuePendingContainers
	ch <- c.queueNumActiveApplications
	ch <- c.queueNumPendingApplications
	ch <- c.queueNumMaxApplications
	ch <- c.queueNumMaxApplicationsPerUser
	ch <- c.scrapeFailures
}

func (c *clusterSchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0

	data, err := fetchClusterScheduler(c.endpoint)
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

	metrics := data.Scheduler.SchedulerInfo
	ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.CounterValue, metrics.Capacity)
	ch <- prometheus.MustNewConstMetric(c.maxCapacity, prometheus.CounterValue, metrics.MaxCapacity)
	ch <- prometheus.MustNewConstMetric(c.usedCapacity, prometheus.CounterValue, metrics.UsedCapacity)
	for _, q := range metrics.Queues.Queue {
		ch <- prometheus.MustNewConstMetric(c.queueNumContainers, prometheus.CounterValue, float64(q.NumContainers), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueNumApplications, prometheus.CounterValue, float64(q.NumApplications), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueCapacity, prometheus.CounterValue, float64(q.Capacity), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueUsedCapacity, prometheus.CounterValue, float64(q.UsedCapacity), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueMaxCapacity, prometheus.CounterValue, float64(q.MaxCapacity), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueAbsoluteCapacity, prometheus.CounterValue, float64(q.AbsoluteCapacity), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueAbsoluteMaxCapacity, prometheus.CounterValue, float64(q.AbsoluteMaxCapacity), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueAbsoluteUsedCapacity, prometheus.CounterValue, float64(q.AbsoluteUsedCapacity), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueAllocatedContainers, prometheus.CounterValue, float64(q.AllocatedContainers), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueReservedContainers, prometheus.CounterValue, float64(q.ReservedContainers), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queuePendingContainers, prometheus.CounterValue, float64(q.PendingContainers), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueNumActiveApplications, prometheus.CounterValue, float64(q.NumActiveApplications), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueNumPendingApplications, prometheus.CounterValue, float64(q.NumPendingApplications), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueNumMaxApplications, prometheus.CounterValue, float64(q.MaxApplications), q.QueueName)
		ch <- prometheus.MustNewConstMetric(c.queueNumMaxApplicationsPerUser, prometheus.CounterValue, float64(q.MaxApplicationsPerUser), q.QueueName)
	}

	return
}

func fetchClusterScheduler(u *url.URL) (SchedulerMetrics, error) {
	var met SchedulerMetrics

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
