FROM golang:1.20 as builder

WORKDIR /build
COPY *.go go.mod go.sum Makefile ./

RUN go get && CGO_ENABLED=0 go build -ldflags="-extldflags=-static" .

FROM scratch
COPY --from=builder --chmod=0755 /build/yarn-prometheus-exporter /yarn-prometheus-exporter
ENTRYPOINT ["/yarn-prometheus-exporter"]

