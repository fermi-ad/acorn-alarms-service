FROM adregistry.fnal.gov/dev-containers/redhat-ubi9-minimal

RUN useradd -u 10001 -r -M -s /sbin/nologin appuser

COPY --chown=10001:10001 target/release/grpc-alarms /usr/local/bin/grpc-alarms

EXPOSE 6802

USER 10001

ENTRYPOINT [ "/usr/local/bin/grpc-alarms" ]
