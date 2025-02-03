FROM scratch

ARG TARGETPLATFORM

COPY /build/$TARGETPLATFORM/ /

ENTRYPOINT ["/yarn-prometheus-exporter"]
