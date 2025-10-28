FROM debian:bookworm-slim

WORKDIR /app
ADD target/release/acorn-alarms-service /app/acorn-alarms-service
EXPOSE 6802
CMD [ "./acorn-alarms-service" ]