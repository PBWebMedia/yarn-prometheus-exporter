package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const helpText = `
YARN Prometheus Exporter

Environment variables:
  YARN_PROMETHEUS_ENDPOINT_HOST    Comma-separated list of YARN ResourceManager hosts (default: localhost)
                         		     Example: yarn-rm1,yarn-rm2
  YARN_PROMETHEUS_ENDPOINT_PORT    Port to expose Prometheus metrics (default: 8088)
  YARN_PROMETHEUS_ENDPOINT_SCHEME  Scheme to use for YARN ResourceManager endpoints (default: http)
  YARN_PROMETHEUS_LISTEN_ADDR  	   Port to expose Prometheus metrics (default: :9113)

  Kerberos settings:
  	YARN_KERBEROS_CONFIG    	   Path to krb5.conf file for Kerberos authentication (optional)
  	YARN_KERBEROS_KEYTAB    	   Path to keytab file for Kerberos authentication (optional)
  	YARN_KERBEROS_PRINCIPAL 	   Principal name (username) for Kerberos authentication (optional)
									 Format: name@REALM

 Usage:
    yarn-prometheus-exporter [flags]

Flags:
  --help                 Show this help message
`

func main() {
	help := flag.Bool("help", false, "Show help message")
	flag.Parse()

	if *help {
		fmt.Print(helpText)
		os.Exit(0)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Print("Starting yarn-prometheus-exporter")

	endpoints := loadEndpoints()
	log.Printf("YARN Endpoints: %v", endpoints)

	c := newCollector(endpoints)
	err := prometheus.Register(c)
	if err != nil {
		log.Fatal(err)
	}

	if krbUser := getEnvOr("YARN_KERBEROS_PRINCIPAL", ""); krbUser != "" {
		krb5Config := getEnvOr("YARN_KERBEROS_CONFIG", "/etc/krb5.conf")
		keytabFile := getEnvOr("YARN_KERBEROS_KEYTAB", "")

		c.kerberosConfig = NewKerberosConfig(krb5Config, keytabFile, krbUser)
	}

	addr := getEnvOr("YARN_PROMETHEUS_LISTEN_ADDR", ":9113")
	log.Print("Listening on: ", addr)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}

func loadEndpoints() []string {
	scheme := getEnvOr("YARN_PROMETHEUS_ENDPOINT_SCHEME", "http")
	host := getEnvOr("YARN_PROMETHEUS_ENDPOINT_HOST", "localhost")
	port := getEnvOr("YARN_PROMETHEUS_ENDPOINT_PORT", "8088")

	if host == "" {
		log.Fatal("YARN_PROMETHEUS_ENDPOINT_HOST is not set")
	}

	if !strings.Contains(host, ",") {
		return []string{scheme + "://" + host + ":" + port + "/"}
	}

	res := make([]string, 0, 1)
	for i, h := range strings.Split(host, ",") {
		res[i] = scheme + "://" + h + ":" + port + "/"
	}

	return res
}

func getEnvOr(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}
