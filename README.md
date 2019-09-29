# Mongoproxy the Observable
There are many MongoDb proxies, but this one is about observability. It passes bytes between client and the server and keeps track of latencies, response sizes, document counts and exposes them as Prometheus metrics. It also does Jaeger tracing if the client passes trace id in the `$comment` field of the operation.

It does not meddle with the traffic, but passes bytes between the application and the database as they are.

## Current state
Mostly works -- rarely crashes and does collect useful metrics. Performance is acceptable -- sacrifice a millisecond or two in exchange of metrics. Runs a thread per-connection, so may run into trouble with large number of connections.

Jaeger tracing is very experimental. I've seen it work. Here's a screenshot.

## Usage

```
mongoproxy \
    --listen 0.0.0.0:27113 \
    --hostport 127.0.0.1:27017 \
    --service-name mongoproxy-ftw \
    --enable-jaeger \
    --jaeger-addr localhost:6831
```
This will proxy all requests on port `27113` to a MongoDb instance running on `localhost:27017`. Jaeger tracing is enabled and the spans will be sent to collector on `localhost:6831`. 

More verbose logging can be enabled by specifying `RUST_LOG` level as `info` or `debug`. Add `RUST_BACKTRACE=1` for troubleshooting those (rare) crashes.

## Metrics

Per-request histograms:
* `mongoproxy_response_first_byte_latency_seconds` - Response latency (to be renamed)
* `mongoproxy_documents_returned_total` - How many documents were returned.
* `mongoproxy_documents_changed_total` - How many documents were changed by insert, update or delete.
* `mongoproxy_server_response_bytes_total` - Response size distribution.

All per-request metrics are labeled with "client" (IP), "app" (appName from connection metadata), "op", "collection" and "db". 

Connection counters
* `mongoproxy_client_connections_established_total`
* `mongoproxy_client_bytes_sent_total`
* `mongoproxy_client_bytes_received_total`
* `mongoproxy_client_disconnections_total`
* `mongoproxy_client_connection_errors_total`

Per connection metrics are labeled with "client" (IP).
* mongoproxy_client_bytes_received_total
