use mio::{self, Token, Poll, PollOpt, Events, Ready};
use mio::net::{TcpStream};
use std::net::{TcpListener,SocketAddr,ToSocketAddrs};
use std::io::{self, Read, Write};
use std::{thread, str};
use log::{info,warn,debug};
use prometheus::{CounterVec,Counter,Encoder,TextEncoder};
use hyper::{header::CONTENT_TYPE, rt::Future, service::service_fn_ok, Body, Response, Server};
use clap::{Arg, App};

use env_logger;

#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;

mod mongodb;

use mongodb::tracker::{MongoStatsTracker};

const SERVER_ADDR: &str = "127.0.0.1:27017";
const LISTEN_ADDR: &str = "0.0.0.0:27111";
const METRICS_ADDR: &str = "0.0.0.0:9898";

lazy_static! {
    static ref CONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_connections_established_total",
            "Total number of client connections established",
            &["client"]).unwrap();

    static ref CONNECTION_ERRORS_TOTAL: Counter =
        register_counter!(
            "mongoproxy_client_connection_errors_total",
            "Total number of errors from handle_connections").unwrap();
}

fn main() {
    let matches = App::new("mongoproxy")
        .version("0.1.0")
        .about("Proxies MongoDb requests to obtain metrics")
        .arg(Arg::with_name("server_addr")
            .short("h")
            .long("hostport")
            .value_name("server host:port")
            .help("MongoDb server hostport to proxy to")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("listen_addr")
            .short("l")
            .long("listen")
            .value_name("LISTEN_ADDR")
            .help("Hostport where the proxy will listen on (0.0.0.0:27111)")
            .takes_value(true))
        .arg(Arg::with_name("metrics_addr")
            .short("m")
            .long("metrics")
            .value_name("METRICS_ADDR")
            .help("Hostport for Prometheus metrics endpoint")
            .takes_value(true))
        .arg(Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity")
        ).get_matches();

    let server_addr = String::from(matches.value_of("server_addr").unwrap_or(SERVER_ADDR));
    let listen_addr = matches.value_of("listen_addr").unwrap_or(LISTEN_ADDR);
    let metrics_addrs = matches.value_of("metrics_addr").unwrap_or(METRICS_ADDR);

    env_logger::init();

    let listener = TcpListener::bind(listen_addr).unwrap();
    info!("Listening on {}", listen_addr);
    info!(" proxying to {}", server_addr);

    start_metrics_listener(metrics_addrs);
    info!("Metrics endpoint at http://{}", metrics_addrs);

    info!("^C to exit");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer_addr = stream.peer_addr().unwrap();
                let server_addr = server_addr.clone();
                CONNECTION_COUNT_TOTAL.with_label_values(&[&peer_addr.to_string()]).inc();

                thread::spawn(move || {
                    info!("new connection from {}", peer_addr);
                    match handle_connection(&server_addr, TcpStream::from_stream(stream).unwrap()) {
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
fn handle_connection(server_addr: &str, mut client_stream: TcpStream) -> std::io::Result<()> {
    let _peer_addr = client_stream.peer_addr()?.to_string();
    let client_addr = _peer_addr.split(":").next().unwrap_or(&_peer_addr);

    let mut done = false;
    let mut tracker = MongoStatsTracker::new(&client_addr);

    const CLIENT: Token = Token(1);
    const SERVER: Token = Token(2);

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    info!("connecting to server: {}", server_addr);
    let server_addr = lookup_address(server_addr)?;
    let mut server_stream = TcpStream::connect(&server_addr)?;
    poll.register(&server_stream, SERVER, Ready::all(), PollOpt::edge()).unwrap();

    debug!("Waiting server connection to become ready.");
    'outer: loop {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            if let SERVER = event.token() {
                if event.readiness().is_writable() {
                    break 'outer;
                }
            }
        }
    }

    poll.register(&client_stream, CLIENT, Ready::readable() | Ready::writable(),
        PollOpt::edge()).unwrap();

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

fn lookup_address(addr: &str) -> std::io::Result<SocketAddr> {
    for sockaddr in addr.to_socket_addrs()? {
        debug!("{} resolves to {}", addr, sockaddr);
        return Ok(sockaddr);
    }
    Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "no usable address found"))
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