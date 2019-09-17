use mio::{self, Token, Poll, PollOpt, Events, Ready};
use mio::net::{TcpStream};
use std::net::{TcpListener,SocketAddr,ToSocketAddrs};
use std::io::{self, Read, Write};
use std::{thread, str};
use log::{info,warn,debug};
use prometheus::{CounterVec,Counter,HistogramVec,Encoder,TextEncoder};
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

    static ref SERVER_CONNECT_TIME_SECONDS: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_server_connect_time_seconds",
            "Time it takes to look up and connect to a server",
            &["server_addr"]).unwrap();

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
            .help(&format!("Hostport where the proxy will listen on ({})", LISTEN_ADDR))
            .takes_value(true))
        .arg(Arg::with_name("metrics_addr")
            .short("m")
            .long("metrics")
            .value_name("METRICS_ADDR")
            .help(&format!("Hostport for Prometheus metrics endpoint ({})", METRICS_ADDR))
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
                // The client address may be changed to localhost by various sidecards
                // such as linkerd. So not as useful, but we keep it anyway.
                let client_addr = format_client_address(&stream.peer_addr().unwrap());
                let server_addr = server_addr.clone();
                CONNECTION_COUNT_TOTAL.with_label_values(&[&client_addr.to_string()]).inc();

                thread::spawn(move || {
                    info!("new connection from {}", client_addr);
                    match handle_connection(&server_addr, TcpStream::from_stream(stream).unwrap()) {
                        Ok(_) => info!("{} closing connection.", client_addr),
                        Err(e) => {
                            warn!("{} connection error: {}", client_addr, e);
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
    let client_addr = format_client_address(&client_stream.peer_addr()?);

    let mut done = false;
    let mut tracker = MongoStatsTracker::new(&client_addr);

    const CLIENT: Token = Token(1);
    const SERVER: Token = Token(2);

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    info!("connecting to server: {}", server_addr);
    let timer = SERVER_CONNECT_TIME_SECONDS.with_label_values(&[server_addr]).start_timer();
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
    timer.observe_duration();

    poll.register(&client_stream, CLIENT, Ready::readable() | Ready::writable(),
        PollOpt::edge()).unwrap();

    while !done {
        debug!("Polling");
        poll.poll(&mut events, None).unwrap();
        debug!("poll done");

        for event in events.iter() {
            match event.token() {
                CLIENT => {
                    debug!("Reading from client");
                    let mut track_client = |buf: &[u8]| {
                        tracker.track_client_request(buf);
                    };

                    if !copy_stream_with_fn(&mut client_stream, &mut server_stream, &mut track_client)? {
                        info!("{} client EOF", client_stream.peer_addr()?);
                        done = true;
                    }
                },
                SERVER => {
                    debug!("Reading from server");
                    let mut track_server = |buf: &[u8]| {
                        tracker.track_server_response(buf);
                    };

                    if !copy_stream_with_fn(&mut server_stream, &mut client_stream, &mut track_server)? {
                        info!("{} server EOF", server_stream.peer_addr()?);
                        done = true;
                    }
                },
                _ => {}
            }
        }
    }

    Ok(())
}

// Copy bytes from one stream to another, passing the read bytes to a callback.
// Consumes all input until read would block. Assumes that we are always ready
// to send bytes.
//
// Returns false if EOF reached on the from_stream.
fn copy_stream_with_fn(from_stream: &mut TcpStream, to_stream: &mut TcpStream,
        process_bytes: &mut dyn FnMut(&[u8])) -> std::io::Result<bool> {

    let mut buf = [0; 64];

    loop {
        match from_stream.read(&mut buf) {
            Ok(0) => {
                return Ok(false);   // EOF
            }
            Ok(len) => {
                to_stream.write_all(&buf[0..len])?;
                process_bytes(&buf[0..len]);
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(true)
            },
            Err(e) => {
                return Err(e);
            },
        }
    }
}

fn lookup_address(addr: &str) -> std::io::Result<SocketAddr> {
    if let Some(sockaddr) = addr.to_socket_addrs()?.next() {
        debug!("{} resolves to {}", addr, sockaddr);
        return Ok(sockaddr);
    }
    Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "no usable address found"))
}

// Return the peer address of the stream without the :port
fn format_client_address(sockaddr: &SocketAddr) -> String {
    let mut addr_str = sockaddr.to_string();
    if let Some(pos) = addr_str.find(':') {
        addr_str.split_off(pos);
    }
    addr_str
}