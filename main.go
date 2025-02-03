package main

import (
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr     string
	endpoint string
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Print("Starting yarn-prometheus-exporter")
	loadEnv()

	c := newCollector(endpoint)
	err := prometheus.Register(c)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Listening on: ", addr)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}

func loadEnv() {
	addr = getEnvOr("YARN_PROMETHEUS_LISTEN_ADDR", ":9113")

	scheme := getEnvOr("YARN_PROMETHEUS_ENDPOINT_SCHEME", "http")
	host := getEnvOr("YARN_PROMETHEUS_ENDPOINT_HOST", "localhost")
	port := getEnvOr("YARN_PROMETHEUS_ENDPOINT_PORT", "8088")

	endpoint = scheme + "://" + host + ":" + port + "/"
}

func getEnvOr(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}
