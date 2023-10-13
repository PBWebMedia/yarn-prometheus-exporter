package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr     string
	endpoint *url.URL
)

func main() {
	loadEnv()

	c := newCollector(endpoint)
	err := prometheus.Register(c)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}

func loadEnv() {
	addr = getEnvOr("YARN_PROMETHEUS_LISTEN_ADDR", ":9113")

	scheme := getEnvOr("YARN_PROMETHEUS_ENDPOINT_SCHEME", "http")
	host := getEnvOr("YARN_PROMETHEUS_ENDPOINT_HOST", "localhost")
	port := getEnvOr("YARN_PROMETHEUS_ENDPOINT_PORT", "8088")
	path := getEnvOr("YARN_PROMETHEUS_ENDPOINT_PATH", "ws/v1/cluster/metrics")

	e, err := url.Parse(scheme + "://" + host + ":" + port + "/" + path)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("Getting metrics from: " + scheme + "://" + host + ":" + port + "/" + path + "\r\n")

	endpoint = e
}

func getEnvOr(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}
