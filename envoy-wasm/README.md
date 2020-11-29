# Mongoproxy as an Envoy WASM filter
This is a skeleton of a WASM implementation of Mongoproxy. Currently it doesn't
do anything useful other than illustrating how one would add a WASM filter to
Envoy.

Compile with: `cargo build --target=wasm32-unknown-unknown --release`
Add WASM target if needed: `rustup target add wasm32-unknown-unknown`

There's an example Envoy config in [testing](testing) folder.

Useful links:
* https://github.com/proxy-wasm/spec/tree/master/abi-versions/vNEXT
* https://github.com/proxy-wasm/proxy-wasm-rust-sdk

## Metrics
The metrics support in proxy-wasm Rust SDK is not stable as of yet, so we pull the latest
`proxy-wasm` crate directly from Github.

Defining metrics is relatively straightforward, however with labels it gets a little bit more
trickier. Envoy expects labels to be part of the metric name and extracts the label names and
values using regex. So, to get `mongoproxy{app="foo"}` we would define metric as `mongoproxy.app.foo` and
provide a custom metrics definition to extract the `app` and `foo` as label name and value respectively.

Alternatively we could maybe try and use the existing Envoy Mongo proxy
[metric labels](https://github.com/envoyproxy/envoy/blob/master/source/common/config/well_known_names.cc).

## Performance of the WASM filter
So how does the WASM filter do, compared to native Envoy `tcp_proxy` or native
Mongoproxy? In short, it's not too bad.

Ran a simple benchmark that populates a collection with 1000 documents and
queries all documents from that collection 1000 times. Obviously smaller values are better.

```
Direct MongoDb, no proxies      :  9.16 sec
Envoy tcp_proxy                 :  9.75 sec
Envoy dummy WASM filter         :  9.69 sec (hah, got lucky here)
Envoy WASM MongoDb decoder      : 12.46 sec
Mongoproxy, v0.5.4x             :  9.63 sec
```

Native Mongoproxy and Envoy with just `tcp_proxy` are pretty much on par. Also
the dummy WASM filter that just does `get_downstream_data()` doesn't have any
noticeable overhead. However as soon as we start actually decoding the MongoDb
protocol does the overhead become noticeable - roughly 28%. Still, in many cases
this is not a too high price to pay for metrics.

**Note**: Mongoproxy v0.5.3 had considerably worse results initially (up to 17
seconds and a lot of variability). This was traced back to using a too small
buffer for proxying the bytes.
