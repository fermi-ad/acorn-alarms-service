FROM debian:bookworm-slim

WORKDIR /app
ADD target/release/acorn-alarm-service /app/acorn-alarm-service
EXPOSE 6802
CMD [ "./acorn-alarm-service" ]