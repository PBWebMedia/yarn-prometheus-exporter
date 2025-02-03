.PHONY: all show-gofmt-complains

all: build/linux/amd64/yarn-prometheus-exporter build/linux/arm64/yarn-prometheus-exporter

build/linux/amd64/yarn-prometheus-exporter: main.go collector.go
	mkdir -p build/linux/amd64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o build/linux/amd64/yarn-prometheus-exporter .

build/linux/arm64/yarn-prometheus-exporter: main.go collector.go
	mkdir -p build/linux/arm64
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-w -s" -o build/linux/arm64/yarn-prometheus-exporter .

show-gofmt-complains:
	@gofmt -l -d -s ./
