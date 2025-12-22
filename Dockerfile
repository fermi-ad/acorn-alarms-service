FROM debian:bookworm-slim

# Need libssl for connection to Kafka
RUN apt-get update -y && apt-get install -y libssl3 && apt-get clean -y

WORKDIR /app
COPY target/release/acorn-alarms-service /app/acorn-alarms-service
EXPOSE 6802
CMD [ "./acorn-alarms-service" ]
