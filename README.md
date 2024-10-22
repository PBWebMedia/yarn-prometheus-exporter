# YARN prometheus exporter

Export YARN metrics in [Prometheus](https://prometheus.io/) format.

# Build

Requires [Go](https://golang.org/doc/install). Tested with Go 1.9+.

    go get
    go build -o yarn-prometheus-exporter .

# Run

The exporter can be configured using environment variables. These are the defaults:

    YARN_PROMETHEUS_LISTEN_ADDR=:9113
    YARN_PROMETHEUS_ENDPOINT_SCHEME=http
    YARN_PROMETHEUS_ENDPOINT_HOST=localhost
    YARN_PROMETHEUS_ENDPOINT_PORT=8088

Run the exporter:

    ./yarn-prometheus-exporter

The metrics can be scraped from:

    http://localhost:9113/metrics

# Run using docker

Run using docker:

    docker run -p 9113:9113 pbweb/yarn-prometheus-exporter

Or using docker-compose:

    services:
        image: pbweb/yarn-prometheus-exporter
        restart: always
        environment:
            - "YARN_PROMETHEUS_ENDPOINT_HOST=yarn.hadoop.lan"
        ports:
            - "9113:9113"

# License

See [LICENSE.md](LICENSE.md)
