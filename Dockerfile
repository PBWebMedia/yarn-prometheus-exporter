FROM scratch

COPY . /yarn-prometheus-exporter
ENTRYPOINT ["/yarn-prometheus-exporter"]
