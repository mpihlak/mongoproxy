# Mongoproxy the Observable
There are many MongoDb proxies, but this one is about observability. It passes bytes between client and the server and keeps track of latencies, response sizes, document counts and exposes them as Prometheus metrics. It also does Jaeger tracing if the client passes a trace id in the `$comment` field of the operation.

All bytes are passed through unchanged.

## Current state
Works pretty damn well. That is, doesn't crash and collects useful metrics. Performance overhead is minimal, memory usage depends on the max request/response size (currently needs to have the whole BSON in memory for parsing). 

Since it uses a thread per connection, `MALLOC_ARENA_MAX` needs to be tuned to avoid excessive memory usage due to how `malloc()` handles per-thread arenas. 2 is a good starting value.

Jaeger tracing is still experimental. It does generate traces but the interface is likely to change in the future. Specifically, the `$comment` field could be used for passing along other information as well (eg. additional Prometheus labels, function name, file:line, etc.).

## Usage

### Sidecar with iptables port forwarding
```
mongoproxy --proxy 27111
```

This mode is used when running the proxy as a sidecar on a K8s pod. `iptables` rules need to be set up to redirect all port 27017 traffic through the proxy. The proxy then determines the original destination address via `getsockopt` and forwards the requests to its original destination. Because it captures all traffic to Mongo ports, it automatically supports replicaset connections.

See the [manually added](examples/sidecar) or [automatically injected](examples/k8s-sidecar-injector) sidecar examples.

### Static proxy with pre-determined server address
```
mongoproxy --proxy 27113:localhost:27017
```
This will proxy all requests on port `27113` to a fixed MongoDb instance running on `localhost:27017`. Useful when running as a shared front-proxy. However this mode does not easily support replica sets, as replicaset connections can be redirected to any host in the set.

See the [front proxy](examples/front-proxy) example.

### With Jaeger tracing
```
mongoproxy --proxy 27113:localhost:27017 \
    --service-name mongoproxy-ftw \
    --enable-jaeger \
    --jaeger-addr localhost:6831
```

Same as above but with Jaeger tracing enabled. Spans will be sent to collector on `localhost:6831`. The service name for the traces is set to `mongoproxy-ftw`.

### Other tips
More verbose logging can be enabled by specifying `RUST_LOG` level as `info` or `debug`. Add `RUST_BACKTRACE=1` for troubleshooting those (rare) crashes.

## Metrics

Per-request histograms:
* `mongoproxy_response_latency_seconds` - Response latency
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

Example:

![Metrics example](https://github.com/mpihlak/mongoproxy/blob/master/img/metrics.png)

## Tracing
Mongoproxy will not create tracing spans unless the application explicitly requests it. The application does this by passing the trace id in the `$comment` field of the MongoDb query. So, for example if a `find` operation has `uber-trace-id:6d697c0f076183c:6d697c0f076183c:0:1` in the comment, the proxy picks this up and will create a child span for the `find` operation. Like this:

![Trace example](https://github.com/mpihlak/mongoproxy/blob/master/img/trace.png)

## Notes

MongoDb `OP_COMMAND` protocol is not supported. This makes the proxy mostly unusable for MongoDb versions < `3.5`.
