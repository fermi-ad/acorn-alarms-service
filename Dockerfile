FROM debian:bookworm-slim

WORKDIR /app
COPY target/release/acorn-alarms-service /app/acorn-alarms-service
EXPOSE 6802
CMD [ "./acorn-alarms-service" ]
