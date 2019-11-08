package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr                   string
	clusterMetricsEndpoint *url.URL
	appsEndpoint           *url.URL
	appExpirationMinutes   int
)

func main() {
	loadEnv()

	prometheus.MustRegister(newClusterMetricsCollector(clusterMetricsEndpoint))
	prometheus.MustRegister(newAppsCollector(appsEndpoint, appExpirationMinutes))

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
	appExpiration := getEnvOr("YARN_PROMETHEUS_APPLICATION_EXPIRATION_MINUTES", "1440")
	// ?applicationStates=running&applicationTypes=spark&queue=default

	aem, err := strconv.Atoi(appExpiration)
	if err != nil {
		log.Fatal()
	}
	appExpirationMinutes = aem

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
