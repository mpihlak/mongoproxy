Experimental proxy to extract MongoDb stats and expose as Prometheus metrics.
Motivated by Envoy's lack of support for current MongoDb protocol.

Build and run with:
```
RUST_LOG=info RUST_BACKTRACE=1 cargo run -- --hostport 127.0.0.1:27017
```
