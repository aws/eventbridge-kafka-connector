# End-to-end test assets

| File                       | Target bus                         | Used by                          |
|----------------------------|------------------------------------|----------------------------------|
| `connect-config.json`      | Classic event bus (LocalStack)     | `EventBridgeSinkConnectorIT`     |
| `connect-config-s3.json`   | Classic event bus + S3 offloading  | `EventBridgeSinkConnectorS3IT`   |
| `connect-config-v2.json`   | Custom Event Bus (`eventsv2` stub) | `EventBridgeSinkConnectorV2IT`   |
| `connect-config-v2-cloudevents.json` | Custom Event Bus (`eventsv2` stub), CloudEvents strategy | `EventBridgeSinkConnectorV2CloudEventsIT` |

## Custom Event Bus (v2)

LocalStack does not implement the `eventsv2` API. Each compose file therefore runs an `eventsv2-stub`
service: a small test stub of `PutRawEvents` (Smithy RPC v2 CBOR) built from
`src/test/java/software/amazon/event/kafkaconnector/stub/EventsV2Stub.java` and run from
`target/test-classes` in a plain JRE image. `connect-config-v2.json` points the connector at it via
`aws.eventbridge.endpoint.uri=http://eventsv2-stub:8080`.

The stub accepts every entry as `PUBLISHED` and records each request. `EventBridgeSinkConnectorV2IT`
reads the recorded requests from `GET /_stub/requests` and asserts on the payload bytes, the
`ContentType`/`EventGroupId`/`DeduplicationId` system metadata, and the `kafka.*` metadata. The stub
cannot reproduce server-side behavior such as deduplication, FIFO delivery, or schema registry
deserialization; those need a real Custom Event Bus.

The stub is unauthenticated. It binds `0.0.0.0` only inside the compose network, and its host port is
published to `127.0.0.1` only, like every other service in these files.

## Running locally

The integration tests need a Docker-API engine that Testcontainers supports (Docker Engine, Docker
Desktop, Colima, Rancher Desktop, OrbStack) plus the `docker compose` CLI plugin, and the shaded
connector jar in `target/`. Finch's `finch-daemon` is not compatible with Testcontainers' compose
support.

Local prerequisites:

- **Memory:** give the engine's VM at least 8 GiB. Each test class runs Kafka, Kafka Connect,
  LocalStack, and the stub together; at 2 GiB (Colima's default) the kernel OOM-kills the Connect
  JVM at random and the test times out waiting for the Connect REST API.
- **Maven 3.9.x:** `kafka-connect-maven-plugin` 0.12.0 fails during `package` under Maven 3.10.0.

With Colima, for example:

```console
colima start --cpu 4 --memory 8
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
export COMPOSE_FILE=e2e/docker_compose.yaml KAFKA_VERSION=4.1.1
mvn clean verify -Drevision=$(git describe --tags --always)
```
