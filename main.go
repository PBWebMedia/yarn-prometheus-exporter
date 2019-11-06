package main

import (
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr                   string
	clusterMetricsEndpoint *url.URL
	appsEndpoint           *url.URL
)

func main() {
	loadEnv()

	prometheus.MustRegister(newClusterMetricsCollector(clusterMetricsEndpoint))
	prometheus.MustRegister(newAppsCollector(appsEndpoint))

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}

func loadEnv() {
	addr = getEnvOr("YARN_PROMETHEUS_LISTEN_ADDR", ":9113")
	scheme := getEnvOr("YARN_PROMETHEUS_ENDPOINT_SCHEME", "http")
	host := getEnvOr("YARN_PROMETHEUS_ENDPOINT_HOST", "localhost")
	port := getEnvOr("YARN_PROMETHEUS_ENDPOINT_PORT", "8088")
	clusterMetricsPath := getEnvOr("YARN_PROMETHEUS_CLUSTERMETRICS_PATH", "ws/v1/cluster/metrics")
	appsPath := getEnvOr("YARN_PROMETHEUS_APPLICATION_PATH", "ws/v1/cluster/apps")
	// ?applicationStates=running&applicationTypes=spark&queue=default

	cme, err := url.Parse(scheme + "://" + host + ":" + port + "/" + clusterMetricsPath)
	if err != nil {
		log.Fatal()
	}
	clusterMetricsEndpoint = cme

	ae, err := url.Parse(scheme + "://" + host + ":" + port + "/" + appsPath)
	if err != nil {
		log.Fatal()
	}
	appsEndpoint = ae
}

func getEnvOr(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}
