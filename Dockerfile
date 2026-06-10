FROM debian:trixie-slim

# Need libssl for connection to Kafka
RUN apt-get update -y && apt-get install -y libssl3 libsasl2-2 && apt-get clean -y

WORKDIR /app
COPY target/release/grpc-alarms /app/grpc-alarms
EXPOSE 6802
CMD [ "./grpc-alarms" ]
