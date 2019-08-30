use std::{thread};
use hyper::{header::CONTENT_TYPE, rt::Future, service::service_fn_ok, Body, Response, Server};
use prometheus::{CounterVec, Opts, Encoder, TextEncoder};

pub struct Metrics {
    pub connection_count: CounterVec,
}

impl Metrics {
    pub fn new() -> Metrics {
        let connection_count = CounterVec::new(
            Opts::new(
                "connections_established_total",
                "Total number of client connections established"),
                &["client"]).unwrap();
        prometheus::register(Box::new(connection_count.clone())).unwrap();
        Metrics {
            connection_count,
        }
    }
}

pub fn start_listener(endpoint: &str) {
    let serve_metrics = || {
        let encoder = TextEncoder::new();
        service_fn_ok(move |_request| {
            let metric_families = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();

            let response = Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
                .unwrap();
            response
        })
    };

    let server = Server::bind(&endpoint.parse().unwrap())
        .serve(serve_metrics)
        .map_err(|e| eprintln!("Metrics server error: {}", e));

    thread::spawn(|| hyper::rt::run(server));
}