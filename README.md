Experimental proxy to extract MongoDb stats and expose as Prometheus metrics. 

All bytes are passed as-is between client and server, however a copy of the bytes is sent to a parser that 
parses the message and calculates per-request metrics (latency distribution, response size, document counts, etc.).

## Current state
Mostly works -- rarely crashes and seems to collect useful metrics. Performance penalty is acceptable -- sacrifice a millisecond or two in exchange of metrics. Runs a thread per-connection, so may run into trouble with large number of connections.

## Usage
```
USAGE:
    mongoproxy [FLAGS] [OPTIONS] --hostport <server host:port>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -l, --listen <LISTEN_ADDR>           Hostport where the proxy will listen on (0.0.0.0:27111)
    -m, --metrics <METRICS_ADDR>         Hostport for Prometheus metrics endpoint (0.0.0.0:9898)
    -h, --hostport <server host:port>    MongoDb server hostport to proxy to
```

To enable more verbose logging, specify `RUST_LOG` level. 

## Metrics

Per-request histograms:
* mongoproxy_response_first_byte_latency_seconds
* mongoproxy_documents_returned_total
* mongoproxy_documents_changed_total
* mongoproxy_server_response_bytes_total

Connection counters
* mongoproxy_client_connections_established_total
* mongoproxy_client_bytes_sent_total
* mongoproxy_client_bytes_received_total
