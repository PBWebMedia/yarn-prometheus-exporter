FROM scratch

COPY ./yarn-prometheus-exporter /yarn-prometheus-exporter
ENTRYPOINT ["/yarn-prometheus-exporter"]
