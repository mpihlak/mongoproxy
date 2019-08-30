use std::{thread};
use hyper::{header::CONTENT_TYPE, rt::Future, service::service_fn_ok, Body, Response, Server};

use prometheus::{Counter, CounterVec, Encoder, TextEncoder};

#[derive(Clone)]
pub struct Metrics {
    pub connection_count: CounterVec,
    pub connection_errors: Counter,
    pub client_bytes_recv: CounterVec,
    pub client_bytes_sent: CounterVec,
}

impl Metrics {
    pub fn new() -> Metrics {
        let connection_count = register_counter_vec!(
            opts!("client_connections_established_total",
                  "Total number of client connections established"),
            &["client"]).unwrap();

        let connection_errors = register_counter!(
            "client_connection_errors_total",
            "Total number of errors from handle_connections").unwrap();

        let client_bytes_recv = register_counter_vec!(
            "client_bytes_received",
            "Total number of bytes received from the client",
            &["client"]).unwrap();

        let client_bytes_sent = register_counter_vec!(
            "client_bytes_sent",
            "Total number of bytes received from the client",
            &["client"]).unwrap();

        Metrics {
            connection_count,
            connection_errors,
            client_bytes_recv,
            client_bytes_sent,
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