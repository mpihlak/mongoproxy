use mio::{self, Token, Poll, PollOpt, Events, Ready};
use mio::net::{TcpStream};
use std::net::{TcpListener};
use std::io::{self, Read, Write};
use std::{thread, str};
use log::{info,warn,debug};
use prometheus::{CounterVec,Counter,Encoder,TextEncoder};
use hyper::{header::CONTENT_TYPE, rt::Future, service::service_fn_ok, Body, Response, Server};
use env_logger;

#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;

mod mongodb;

use mongodb::parser::{MongoStatsTracker};

const SERVER_ADDR: &str = "127.0.0.1:27017";
const LISTEN_ADDR: &str = "127.0.0.1:27111";
const METRICS_ADDR: &str = "127.0.0.1:9898";

lazy_static! {
    static ref CONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "client_connections_established_total",
            "Total number of client connections established",
            &["client"]).unwrap();

    static ref CONNECTION_ERRORS_TOTAL: Counter =
        register_counter!(
            "client_connection_errors_total",
            "Total number of errors from handle_connections").unwrap();
}

fn main() {
    env_logger::init();

    let listener = TcpListener::bind(LISTEN_ADDR).unwrap();
    info!("Listening on {}", LISTEN_ADDR);

    start_metrics_listener(METRICS_ADDR);
    info!("Metrics endpoint at http://{}", METRICS_ADDR);

    info!("^C to exit");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer_addr = stream.peer_addr().unwrap().clone();
                CONNECTION_COUNT_TOTAL.with_label_values(&[&peer_addr.to_string()]).inc();

                thread::spawn(move || {
                    info!("new connection from {}", peer_addr);
                    match handle_connection(TcpStream::from_stream(stream).unwrap()) {
                        Ok(_) => info!("{} closing connection.", peer_addr),
                        Err(e) => {
                            warn!("{} connection error: {}", peer_addr, e);
                            CONNECTION_ERRORS_TOTAL.inc();
                        },
                    };
                });
            },
            Err(e) => {
                warn!("accept: {:?}", e)
            },
        }
    }
}

pub fn start_metrics_listener(endpoint: &str) {
    let serve_metrics = || {
        let encoder = TextEncoder::new();
        service_fn_ok(move |_request| {
            let metric_families = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
                .unwrap()
        })
    };

    let server = Server::bind(&endpoint.parse().unwrap())
        .serve(serve_metrics)
        .map_err(|e| eprintln!("Metrics server error: {}", e));

    thread::spawn(|| hyper::rt::run(server));
}

// Main proxy logic. Open a connection to the server and start passing bytes
// between the client and the server. Also split the traffic to MongoDb protocol
// parser, so that we can get some stats out of this.
//
// In addition to the message payloads we should also be keeping track
// of MongoDb headers. That'd enable us to match responses to requests
// and calculate latency stats.
// Unclear if we need to keep multiple headers or is it enough just to
// keep the latest header that we received from the client.
//
// TODO: Consider using a pcap based solution instead of proxying the bytes.
// The difficulty there would be reassembling the stream, and we wouldn't
// easily able to track connections that have already been established.
//
fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    info!("connecting to server: {}", SERVER_ADDR);
    let mut server_stream = TcpStream::connect(&SERVER_ADDR.parse().unwrap())?;

    let client_addr = client_stream.peer_addr().unwrap().to_string();

    let mut done = false;

    let mut tracker = MongoStatsTracker::new(&client_addr);

    const CLIENT: Token = Token(1);
    const SERVER: Token = Token(2);

    let poll = Poll::new().unwrap();
    poll.register(&server_stream, SERVER, Ready::readable() | Ready::writable(),
                  PollOpt::edge()).unwrap();
    poll.register(&client_stream, CLIENT, Ready::readable() | Ready::writable(),
                  PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(1024);

    while !done {
        debug!("Polling");
        poll.poll(&mut events, None).unwrap();
        debug!("poll done");

        for event in events.iter() {
            match event.token() {
                CLIENT => {
                    let mut data_from_client = Vec::new();

                    debug!("Reading from client");
                    if !copy_stream(&mut client_stream, &mut server_stream, &mut data_from_client)? {
                        info!("{} client EOF", client_stream.peer_addr()?);
                        done = true;
                    }
                    debug!("Read {} bytes from client", data_from_client.len());

                    tracker.track_client_request(&data_from_client);
                },
                SERVER => {
                    let mut data_from_server = Vec::new();

                    debug!("Reading from server");
                    if !copy_stream(&mut server_stream, &mut client_stream, &mut data_from_server)? {
                        info!("{} server EOF", server_stream.peer_addr()?);
                        done = true;
                    }
                    debug!("Read {} bytes from server", data_from_server.len());
                    tracker.track_server_response(&data_from_server);
                },
                _ => {}
            }
        }
    }

    Ok(())
}

// Copy bytes from one stream to another. Collect the processed bytes
// to "output_buf" for further processing.
//
// TODO: Use a user supplied buffer, so that we're no unnecessarily allocating
// and copying bytes around.
//
// Return false on EOF
//
fn copy_stream(from_stream: &mut TcpStream, to_stream: &mut TcpStream,
               output_buf: &mut Vec<u8>) -> std::io::Result<bool> {
    let mut buf = [0; 64];

    loop {
        match from_stream.read(&mut buf) {
            Ok(len) => {
                if len > 0 {
                    to_stream.write_all(&buf[0..len])?;
                    output_buf.extend_from_slice(&buf[0..len]);
                } else {
                    return Ok(false);
                }
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(true);
            },
            Err(e) => {
                return Err(e);
            },
        }
    }
}
