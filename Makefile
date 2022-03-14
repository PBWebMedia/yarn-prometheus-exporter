all: yarn-prometheus-exporter
.PHONY: all

yarn-prometheus-exporter: main.go collector.go
	CGO_ENABLED=0 go build .

show-gofmt-complains:
	@gofmt -l -d -s ./
