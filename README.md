# grpc-alarms

`grpc-alarms` is an alarm-state orchestration service. It ingests automated alarm updates from Redis Streams, accepts operator commands over gRPC, applies domain policy to those inputs, and publishes the resulting alarm state to Kafka for downstream consumers.

The codebase is organized around a small event-driven runtime:

- Redis ingestion turns external alarm events into internal domain input
- gRPC handlers turn operator requests into internal domain input
- a coordinator owns alarm-state decisions and reconciliation
- a publish engine owns Kafka delivery and retry behavior

This README describes the repository as it exists now: module layout, runtime behavior, configuration, and the places maintainers should start when changing behavior.

## What the service does

At a high level, the service:

- reads automated alarm events from a Redis Stream
- exposes gRPC endpoints for acknowledge, bypass, activate, snooze, and snapshot operations
- normalizes alarm identity as `DEVICE#Source`
- applies transition policy before emitting updates
- publishes alarm status records to Kafka as JSON payloads keyed by alarm identity
- keeps in-memory confirmed and speculative caches so publish outcomes can be reconciled safely
- records low-cardinality in-process metrics about overload, retries, and latency

The main architectural boundary is between domain logic and transport logic:

- the coordinator decides what state the service wants to publish
- the publish engine decides how that state gets delivered

## Runtime architecture

At startup, [`src/main.rs`](src/main.rs) does five things:

1. configures tracing
2. creates the Kafka publisher
3. reads queue-capacity and metrics-log configuration from environment variables
4. starts the runtime in [`src/runtime.rs`](src/runtime.rs)
5. spawns the gRPC server, Redis reader restart loop, and periodic metrics logging task

The runtime wiring in [`src/runtime.rs`](src/runtime.rs) creates three bounded channels:

- automated ingress queue for Redis-driven alarm updates
- priority ingress queue for user commands and effect results
- effect queue for downstream publish work

Those queues connect the main subsystems:

1. adapters convert external traffic into internal messages
2. the coordinator decides whether state should change and emits publish effects
3. the publish engine performs Kafka writes and returns success/failure outcomes
4. the coordinator reconciles those outcomes into confirmed state and user confirmations

## Repository map

### Entrypoints and runtime

- [`src/main.rs`](src/main.rs): process entrypoint, tracing setup, Kafka publisher creation, queue sizing, metrics logging, gRPC server startup, Redis reader restart loop
- [`src/runtime.rs`](src/runtime.rs): runtime wiring, queue sizing types, task startup, ingress handles returned to adapters

### Adapters

- [`src/adapters/grpc.rs`](src/adapters/grpc.rs): gRPC service implementation for user-driven commands and snapshots
- [`src/adapters/redis.rs`](src/adapters/redis.rs): Redis Stream subscriber for automated alarm updates
- [`src/adapters.rs`](src/adapters.rs): adapter module exports

### Domain engine

- [`src/engine/coordinator.rs`](src/engine/coordinator.rs): semantic authority for alarm state, speculative state, confirmed state, and publish reconciliation
- [`src/engine/ingress.rs`](src/engine/ingress.rs): ingress handles and overload behavior for automated vs. user traffic
- [`src/engine/policy.rs`](src/engine/policy.rs): transition rules for automated updates and user actions
- [`src/engine/messages.rs`](src/engine/messages.rs): internal message types exchanged between runtime stages
- [`src/engine.rs`](src/engine.rs): engine module structure

### Effects

- [`src/effects/publish.rs`](src/effects/publish.rs): Kafka publish worker, retry/supersession handling, batching of automated outcomes
- [`src/effects.rs`](src/effects.rs): effect module exports

### Domain model

- [`src/model/key.rs`](src/model/key.rs): normalized alarm identity and `DEVICE#Source` parsing/formatting
- [`src/model/user_action.rs`](src/model/user_action.rs): user actions and their target states
- [`src/model/publish.rs`](src/model/publish.rs): publish request, attempt, and outcome types
- [`src/model/errors.rs`](src/model/errors.rs): domain/update errors surfaced back to callers
- [`src/model.rs`](src/model.rs): model module exports

### Metrics and test support

- [`src/metrics.rs`](src/metrics.rs): low-cardinality in-process metrics for overload, queue pressure proxies, retries, and latency
- [`src/test_utils.rs`](src/test_utils.rs): shared test runtime helpers and test alarm builders

### Build and packaging

- [`Cargo.toml`](Cargo.toml): crate metadata and dependencies
- [`build.rs`](build.rs): protobuf generation via `rust-grpc-lib`
- [`Dockerfile`](Dockerfile): minimal runtime image for the release binary

### Supporting docs

- [`docs/design_notes/effect_workflow.md`](docs/design_notes/effect_workflow.md): notes on effect execution and reconciliation behavior
- [`docs/performance/`](docs/performance): performance notes and test artifacts
- [`docs/historical/`](docs/historical/): older design and requirements material; useful as background context, but not the source of truth for current behavior

## How data moves through the system

### Automated path

Automated alarm updates arrive through the Redis adapter in [`src/adapters/redis.rs`](src/adapters/redis.rs).

That adapter:

- reads Redis Stream entries using `rust-pubsub-lib`
- converts stream fields into protobuf `Status` values
- normalizes the `device` field to uppercase
- derives alarm state from the incoming severity field
- forwards the result into automated ingress

Automated ingress is handled by [`AutomatedIngressHandle::send_automated_update()`](src/engine/ingress.rs).

### User-command path

User commands arrive through the gRPC service in [`src/adapters/grpc.rs`](src/adapters/grpc.rs).

That adapter:

- parses requested device identifiers as `DEVICE#Source`
- converts gRPC requests into internal user actions
- submits them to the priority ingress queue
- waits for per-key confirmation from the coordinator
- maps domain and runtime failures back into gRPC status codes

User ingress is handled by [`UserIngressHandle::try_send()`](src/engine/ingress.rs).

### Coordination and reconciliation

The coordinator in [`src/engine/coordinator.rs`](src/engine/coordinator.rs) is the semantic authority for the service.

It is responsible for:

- deciding whether an automated update is meaningful enough to publish
- validating whether a user action is allowed from the latest known state
- assigning monotonically increasing publish ids
- tracking speculative state until publish outcomes return
- committing confirmed state only when publish results are accepted
- serving snapshots from confirmed state

The most important internal distinction is:

- `confirmed_state`: state known to have been published successfully
- `speculative_state`: newer desired state emitted for publish but not yet reconciled

This is what allows the service to tolerate stale publish completions without letting them overwrite newer intent.

### Publish execution

The publish worker in [`src/effects/publish.rs`](src/effects/publish.rs) owns transport-facing behavior.

It:

- accepts publish effects from the coordinator
- keeps at most one tracked publish attempt per alarm key
- lets newer attempts supersede older tracked attempts for the same key
- retries only the attempt that is still current for that key
- publishes Kafka records keyed by [`Key`](src/model/key.rs)
- returns publish outcomes to the coordinator

User-initiated outcomes are returned immediately. Automated outcomes are batched before being sent back to the coordinator.

## Runtime behavior that matters

### Two ingress classes with different overload policies

The service treats automated traffic and user traffic differently.

Automated updates use [`AutomatedIngressHandle::send_automated_update()`](src/engine/ingress.rs):

- queue policy is bounded-and-await after coalescing
- if the automated queue has room, the update is sent immediately
- if the queue is full, the latest update for a given key is retained in a coalescing map
- a single background drain loop flushes retained updates toward the coordinator while overload mode is active
- this slows ingestion under pressure instead of allowing unbounded memory growth

User commands use [`UserIngressHandle::try_send()`](src/engine/ingress.rs):

- queue policy is bounded-and-reject
- gRPC handlers fail fast when the priority queue is full
- operator requests and publish outcomes are not forced to wait behind automated storm traffic

This split is one of the most important design choices in the repository.

### Priority-first coordination

[`AlarmStateCoordinator::start()`](src/engine/coordinator.rs) uses a biased `tokio::select!` so messages from the priority queue are handled before automated ingress. That means:

- user commands are processed ahead of automated backlog
- publish outcomes are reconciled promptly
- confirmations for user actions are not structurally delayed by Redis traffic

### Snapshot semantics

The gRPC snapshot path ultimately calls [`build_snapshot()`](src/engine/coordinator.rs), which returns only confirmed alarms whose state is not `Ok` and not `Unbypassed`.

Maintainers changing snapshot behavior should start there.

### Metrics logging

[`main()`](src/main.rs) spawns [`log_metrics_periodically()`](src/main.rs), which logs [`Metrics::snapshot()`](src/metrics.rs) at a configurable interval. The metrics module is intentionally in-process and low-cardinality; it does not currently export Prometheus or OpenTelemetry metrics.

## Alarm identity and policy

Alarm identity is represented by [`Key`](src/model/key.rs), which normalizes device names and combines them with a source enum.

The string form is:

- `DEVICE#Analog`
- `DEVICE#Digital`
- `DEVICE#Epics`

The current transition rules live in [`src/engine/policy.rs`](src/engine/policy.rs).

That file answers two questions:

- should an automated update be published?
- is a requested user action allowed from the latest known state?

Two details are especially important:

- bypass is source-specific, not device-wide
- user actions are validated against the latest known state before a publish is emitted

## External interfaces

### gRPC

The gRPC adapter in [`src/adapters/grpc.rs`](src/adapters/grpc.rs) exposes operations for:

- acknowledge
- activate
- bypass
- snooze
- snapshot retrieval

Behavior worth noting:

- device identifiers are parsed as `DEVICE#Source`
- malformed keys are rejected as invalid arguments
- invalid state transitions are returned as invalid arguments
- queue saturation returns `resource_exhausted`
- internal coordinator or publish failures return internal errors

The server is started in [`src/main.rs`](src/main.rs) and listens on port `6802`.

### Redis Stream ingestion

The Redis adapter in [`src/adapters/redis.rs`](src/adapters/redis.rs) subscribes to a Redis Stream using `rust-pubsub-lib` and converts stream entries into alarm `Status` messages.

Current parsing behavior includes:

- `device` is required and normalized to uppercase
- `severity` drives both severity and derived alarm state
- `source` is mapped to `Analog`, `Digital`, or `Epics`
- missing or malformed fields degrade to `Unknown` values where applicable

The reader loop in [`src/main.rs`](src/main.rs) restarts the Redis reader whenever it exits with either success or error, logging the condition and reconnecting.

### Kafka publishing

The publish effect worker in [`src/effects/publish.rs`](src/effects/publish.rs) serializes protobuf `Status` values to JSON and publishes them as keyed messages.

The Kafka message key is the string form of [`Key`](src/model/key.rs), and the message body is the JSON serialization of the alarm status.

## Configuration

The current implementation reads the following environment variables:

| Variable | Purpose | Default |
|---|---|---|
| `CONTROLS_KAFKA_HOST` | Kafka bootstrap host used by the publisher | `kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092` |
| `CONTROLS_ALARMS_TOPIC` | Kafka topic for published alarm states | `alarms` |
| `EPICS_ALARM_REDIS_HOST` | Redis host for EPICS alarm stream ingestion | `127.0.0.1` |
| `EPICS_ALARM_REDIS_PORT` | Redis port for EPICS alarm stream ingestion | `6379` |
| `EPICS_ALARM_REDIS_KEY` | Redis Stream key to subscribe to | `acorn:alarms` |
| `ALARMS_AUTOMATED_QUEUE_CAPACITY` | Capacity of the automated ingress queue | `4096` |
| `ALARMS_PRIORITY_QUEUE_CAPACITY` | Capacity of the priority ingress queue | `128` |
| `ALARMS_EFFECT_QUEUE_CAPACITY` | Capacity of the effect queue | `4096` |
| `ALARMS_METRICS_LOG_INTERVAL_SECS` | Interval for periodic metrics snapshot logging | `30` |

For configuration changes, start with [`src/main.rs`](src/main.rs) and [`src/adapters/redis.rs`](src/adapters/redis.rs).

## Building and running

### Local build

```bash
cargo build
```

### Running for local development

```bash
cargo run
```

Use [`cargo run`](README.md) for local development only, and only when you want Cargo-managed development defaults to apply.

Cargo applies environment configuration from [`.cargo/config.toml`](.cargo/config.toml), and this repository currently defines test-oriented values there for Kafka-related variables. That is convenient for development and testing, but it means [`cargo run`](README.md) is not the right way to launch the service in production-like environments.

When you use [`cargo run`](README.md), make sure you understand that:

- Cargo may inject environment variables from [`.cargo/config.toml`](.cargo/config.toml)
- those values can differ from the runtime configuration you intend to use
- behavior observed under [`cargo run`](README.md) may therefore differ from behavior of the compiled binary launched directly

For production or production-like execution, build the binary and invoke it directly instead of going through Cargo. In practice, maintainers should assume production deployment is automated through the GitHub Actions workflow in [`.github/workflows/deployment.yaml`](.github/workflows/deployment.yaml), rather than launched manually via Cargo.

### Running the compiled binary

Typical production-style flow:

```bash
cargo build --release
./target/release/grpc-alarms
```

Running the compiled binary directly avoids Cargo-managed test configuration and more closely matches how the service is intended to be shipped and operated. It is also the execution model reflected by the deployment automation in [`.github/workflows/deployment.yaml`](.github/workflows/deployment.yaml).

Whether launched via [`cargo run`](README.md) or as a compiled binary, the service will:

- start tracing output to stdout
- start the gRPC server on port `6802`
- connect to Kafka using the configured host and topic
- connect to the configured Redis Stream and begin forwarding automated updates
- log periodic metrics snapshots at the configured interval

### Tests

```bash
cargo test
```

There is substantial module-level test coverage under `src/**/tests.rs`, including coordinator, policy, ingress, publish, key parsing, metrics, and adapter behavior.

### Container image

The runtime container in [`Dockerfile`](Dockerfile) expects a release binary at [`target/release/grpc-alarms`](target/release/grpc-alarms). A typical flow is:

```bash
cargo build --release
docker build -t grpc-alarms .
```

The container exposes port `6802` and launches the compiled binary directly, which is the intended deployment model for this service and aligns with the automated deployment workflow in [`.github/workflows/deployment.yaml`](.github/workflows/deployment.yaml).

## Maintainer guide

### If you need to change alarm semantics

Start with:

- [`src/engine/policy.rs`](src/engine/policy.rs)
- [`src/engine/coordinator.rs`](src/engine/coordinator.rs)
- [`src/model/user_action.rs`](src/model/user_action.rs)

These files define transition rules, user-action validity, and how requested state becomes published state.

### If you need to change overload or storm behavior

Start with:

- [`src/engine/ingress.rs`](src/engine/ingress.rs)
- [`src/runtime.rs`](src/runtime.rs)
- [`src/metrics.rs`](src/metrics.rs)

These files define queue capacities, overload behavior, and the separation between automated and priority traffic.

### If you need to change publish guarantees or reconciliation

Start with:

- [`src/effects/publish.rs`](src/effects/publish.rs)
- [`src/engine/coordinator.rs`](src/engine/coordinator.rs)
- [`src/model/publish.rs`](src/model/publish.rs)

These files jointly define retry behavior, supersession, batching, and how publish ids are interpreted.

### If you need to change external APIs

Start with:

- [`src/adapters/grpc.rs`](src/adapters/grpc.rs)
- [`src/adapters/redis.rs`](src/adapters/redis.rs)
- [`build.rs`](build.rs)

The generated protobuf code is included at build time from [`main.rs`](src/main.rs), so API-shape changes usually involve both proto generation and adapter logic.

### If you need to change alarm identity or serialization

Start with:

- [`src/model/key.rs`](src/model/key.rs)
- [`src/effects/publish.rs`](src/effects/publish.rs)
- [`build.rs`](build.rs)

These files define how alarms are keyed, how payloads are serialized, and how generated types are prepared for JSON transport.

## Operational notes

A few runtime details deserve explicit attention:

- the process runs until interrupted and exits on `Ctrl-C`
- the gRPC server is spawned as a background task
- the Redis reader is spawned as a background task and restarted in a loop
- the coordinator and publish engine are background tasks started by the runtime
- a periodic metrics logger is spawned from [`main()`](src/main.rs)
- if the coordinator stops unexpectedly, adapters begin surfacing queue-closed or internal errors
- tracing is configured at debug level in [`main()`](src/main.rs)

## Dependencies

This service depends on shared Fermilab Rust libraries for environment-variable handling, protobuf generation, gRPC support, and pub/sub integration, as declared in [`Cargo.toml`](Cargo.toml). In particular:

- `rust-env-var-lib` provides typed environment-variable access
- `rust-grpc-lib` is used during build-time protobuf generation
- `rust-pubsub-lib` provides Kafka publishing and Redis Stream subscription
- `tonic` provides the gRPC server runtime
- `tokio` provides the async runtime and channel primitives
- `ringmap` is used for latest-by-key retention during automated-ingress overload
- `futures` is used by the publish engine to manage in-flight publish work

## Side note: historical documents

The material under [`docs/historical/`](docs/historical/) can still be useful when you want background on earlier requirements or design intent. It should be treated as reference material rather than the source of truth for current behavior.

## Development prerequisites

Depending on the build environment, native packages called out in older project notes may still be relevant when compiling dependencies:

- `cmake`
- `libcurl4-openssl-dev`
- `libsasl2-dev`
- `zlib`
