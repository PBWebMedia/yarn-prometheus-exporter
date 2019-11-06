## Compile phase
FROM golang:alpine AS builder

## Install Git to all 'go get' to work
RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/src/yarn-prometheus-exporter/
COPY . .

# Go get stuff
RUN go get -d -v

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /yarn-prometheus-exporter

## Container build
FROM scratch

# Copy the binary
COPY --from=builder /yarn-prometheus-exporter /yarn-prometheus-exporter

# Run the binary
ENTRYPOINT ["/yarn-prometheus-exporter"]