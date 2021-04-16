all: yarn-prometheus-exporter
.PHONY: all

yarn-prometheus-exporter: main.go collector.go
	go build .

show-gofmt-complains:
	@gofmt -l -d -s ./
