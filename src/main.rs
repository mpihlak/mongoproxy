use std::net::{TcpListener,SocketAddr,ToSocketAddrs};
use std::io::{self, Read, Write};
use std::error::{Error};
use std::{thread, time, str};
use std::time::{Duration,Instant};

use mio::{self, Token, Poll, PollOpt, Events, Ready};
use mio::net::{TcpStream};
use prometheus::{CounterVec,HistogramVec,Encoder,TextEncoder};
use clap::{Arg, App, crate_version};
use hyper::{Request, Response, Body, header::CONTENT_TYPE};
use hyper::server::Server;
use hyper::rt::Future;
use hyper_router::{Route, RouterBuilder, RouterService};
use rustracing_jaeger::{Tracer};
use log::{info,warn,error,debug};
use env_logger;
use lazy_static::lazy_static;

#[macro_use] extern crate prometheus;

use mongoproxy::tracing;
use mongoproxy::dstaddr;
use mongoproxy::mongodb::tracker::{MongoStatsTracker};


const JAEGER_ADDR: &str = "127.0.0.1:6831";
const ADMIN_PORT: &str = "9898";
const SERVICE_NAME: &str = "mongoproxy";

lazy_static! {
    static ref CONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_connections_established_total",
            "Total number of client connections established",
            &["client"]).unwrap();

    static ref DISCONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_disconnections_total",
            "Total number of client disconnections",
            &["client"]).unwrap();

    static ref CONNECTION_ERRORS_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_connection_errors_total",
            "Total number of errors from handle_connections",
            &["client"]).unwrap();

    static ref SERVER_CONNECT_TIME_SECONDS: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_server_connect_time_seconds",
            "Time it takes to look up and connect to a server",
            &["server_addr"]).unwrap();

    static ref CLIENT_CONNECT_TIME_SECONDS: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_connect_time_seconds",
            "How long has this client been connected",
            &["client"]).unwrap();

    static ref CLIENT_IDLE_TIME_SECONDS: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_idle_time_seconds",
            "How long has this connection been idle",
            &["client"]).unwrap();

}

struct ProxyDef {
    local_addr: String,
    remote_addr: String,
}

impl ProxyDef {
    /// Parse a proxy definition from a string of <local_port>[:remote-host:remote-port]
    fn from_str(proxy_def: &str) -> Result<Self, Box<dyn Error>> {
        if let Some(pos) = proxy_def.find(':') {
            let (local_port, remote_hostport) = proxy_def.split_at(pos);
            let local_addr = format!("0.0.0.0:{}", local_port);

            Ok(ProxyDef{
                local_addr,
                remote_addr: remote_hostport[1..].to_string()
            })
        } else {
            Ok(ProxyDef{
                local_addr: format!("0.0.0.0:{}", proxy_def),
                remote_addr: String::from("")
            })
        }
    }
}

fn main() {
    let matches = App::new("mongoproxy")
        .version(crate_version!())
        .about("Proxies MongoDb requests to obtain metrics")
        .arg(Arg::with_name("proxy")
            .long("proxy")
            .value_name("local-port:remote-host:remote-port")
            .help("MongoDb server hostport to proxy to")
            .multiple(true)
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("enable_jaeger")
            .long("enable-jaeger")
            .help("Enable distributed tracing with Jaeger")
            .takes_value(false)
            .required(false))
        .arg(Arg::with_name("jaeger_addr")
            .long("jaeger-addr")
            .value_name("Jaeger agent host:port")
            .help("Jaeger agent to send traces to (compact thrift protocol)")
            .takes_value(true)
            .required(false))
        .arg(Arg::with_name("service_name")
            .long("service-name")
            .value_name("SERVICE_NAME")
            .help("Service name that will be used in Jaeger traces and metric labels")
            .takes_value(true))
        .arg(Arg::with_name("admin_port")
            .long("admin-port")
            .value_name("ADMIN_PORT")
            .help(&format!("Hostport for admin endpoint ({})", ADMIN_PORT))
            .takes_value(true))
        .get_matches();

    let admin_port = matches.value_of("admin_port").unwrap_or(ADMIN_PORT);
    let admin_addr = format!("0.0.0.0:{}", admin_port);
    let service_name = matches.value_of("service_name").unwrap_or(SERVICE_NAME);

    let enable_jaeger = matches.occurrences_of("enable_jaeger") > 0;
    let jaeger_addr = lookup_address(matches.value_of("jaeger_addr").unwrap_or(JAEGER_ADDR)).unwrap();

    env_logger::init();

    start_admin_listener(&admin_addr);
    info!("Admin endpoint at http://{}", admin_addr);

    let tracer = tracing::init_tracer(enable_jaeger, &service_name, jaeger_addr);

    // Finally, start a listener for each proxy spec on the command line
    let mut proxy_threads = Vec::new();
    for proxy_spec in matches.values_of("proxy").unwrap() {
        let proxy = ProxyDef::from_str(proxy_spec).unwrap();
        let tracer = tracer.clone();

        proxy_threads.push(thread::spawn(|| {
            run_proxy(proxy, tracer);
        }));
    }

    for t in proxy_threads {
        let _ = t.join();
    }
}

fn run_proxy(proxy: ProxyDef, tracer: Option<Tracer>) {
    let listener = TcpListener::bind(&proxy.local_addr).unwrap();
    if proxy.remote_addr.is_empty() {
        info!("Proxying {} -> <original dst>", proxy.local_addr);
    } else {
        info!("Proxying {} -> {}", proxy.local_addr, proxy.remote_addr);
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let client_addr = format_client_address(&stream.peer_addr().unwrap());

                let server_addr = if proxy.remote_addr.is_empty() {
                    if let Some(sockaddr) = dstaddr::orig_dst_addr(&stream) {
                        // This only assumes that NATd connections are received
                        // and thus always have a valid target address. We expect
                        // iptables rules to be in place to block direct access
                        // to the proxy port.
                        debug!("Original destination address: {:?}", sockaddr);
                        sockaddr.to_string()
                    } else {
                        error!("Host not set and destination address not found: {}",
                            &stream.peer_addr().unwrap());
                        // TODO: Increase a counter
                        continue;
                    }
                } else {
                    proxy.remote_addr.clone()
                };

                let tracer = tracer.clone();
                CONNECTION_COUNT_TOTAL.with_label_values(&[&client_addr.to_string()]).inc();

                thread::spawn(move || {
                    info!("new connection from {}", client_addr);
                    match handle_connection(&server_addr, TcpStream::from_stream(stream).unwrap(), tracer) {
                        Ok(_) => {
                            info!("{} closing connection.", client_addr);
                            DISCONNECTION_COUNT_TOTAL
                                .with_label_values(&[&client_addr.to_string()])
                                .inc();
                        },
                        Err(e) => {
                            warn!("{} connection error: {}", client_addr, e);
                            CONNECTION_ERRORS_TOTAL
                                .with_label_values(&[&client_addr.to_string()])
                                .inc();
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
fn handle_connection(server_addr: &str, mut client_stream: TcpStream, tracer: Option<Tracer>) -> std::io::Result<()> {
    const CLIENT: Token = Token(1);
    const SERVER: Token = Token(2);

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(16);

    info!("connecting to server: {}", server_addr);
    let timer = SERVER_CONNECT_TIME_SECONDS.with_label_values(&[server_addr]).start_timer();
    let server_addr = lookup_address(server_addr)?;
    let mut server_stream = TcpStream::connect(&server_addr)?;
    poll.register(&server_stream, SERVER, Ready::all(), PollOpt::edge()).unwrap();

    debug!("Waiting server connection to become ready.");
    'outer: loop {
        // TODO: This poll could also hang forever in case the server is not responding.
        // Consider timing out.
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

    let mut done = false;
    let client_addr = format_client_address(&client_stream.peer_addr()?);
    let mut tracker = MongoStatsTracker::new(&client_addr, &server_addr.to_string(), tracer);
    let mut last_time = Instant::now();

    while !done {
        poll.poll(&mut events, Some(Duration::from_millis(1000))).unwrap();
        debug!("Poll done.");

        let mut connection_was_idle = true;
        for event in events.iter() {
            match event.token() {
                CLIENT => {
                    debug!("Reading from client");
                    connection_was_idle = false;
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
                    connection_was_idle = false;
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

        let now = Instant::now();
        let duration = now.duration_since(last_time).as_secs_f64();

        CLIENT_CONNECT_TIME_SECONDS
            .with_label_values(&[&client_addr])
            .inc_by(duration);

        if connection_was_idle {
            CLIENT_IDLE_TIME_SECONDS
                .with_label_values(&[&client_addr])
                .inc_by(duration);
        }

        last_time = now;
    }

    Ok(())
}

// Copy bytes from one stream to another, passing the read bytes to a callback.
// Consumes all input until read would block. Assumes that we are always ready
// to send bytes.
//
// Returns false if EOF reached on the from_stream.
fn copy_stream_with_fn(from_stream: &mut TcpStream, mut to_stream: &mut TcpStream,
        process_bytes: &mut dyn FnMut(&[u8])) -> std::io::Result<bool> {

    let mut buf = [0; 1024];

    loop {
        match from_stream.read(&mut buf) {
            Ok(0) => {
                return Ok(false);   // EOF
            }
            Ok(len) => {
                write_to_stream(&mut to_stream, &buf[..len])?;
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

// Write the buffer to the stream, waiting out the EAGAIN errors
// TODO: remove this hack
fn write_to_stream(stream: &mut TcpStream, mut buf: &[u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        match stream.write(buf) {
            Ok(len) => {
                buf = &buf[len..];
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                warn!("{:?}: write would have blocked: {}", thread::current().id(), e);
                thread::sleep(time::Duration::from_millis(1));
            },
            Err(other) => {
                error!("{:?}: write error: {}", thread::current().id(), other);
                return Err(other);
            },
        }
    }
    Ok(())
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

pub fn start_admin_listener(endpoint: &str) {
    let server = Server::bind(&endpoint.parse().unwrap())
        .serve(router_service)
        .map_err(|e| eprintln!("Metrics server error: {}", e));

    thread::spawn(|| hyper::rt::run(server));
}

fn router_service() -> Result<RouterService, std::io::Error> {
    let router = RouterBuilder::new()
        .add(Route::get("/").using(root_handler))
        .add(Route::get("/health").using(health_handler))
        .add(Route::get("/metrics").using(metrics_handler))
        .build();

    Ok(RouterService::new(router))
}

fn root_handler(_: Request<Body>) -> Response<Body> {
    Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "text/html")
        .body(Body::from("<a href='/metrics'>/metrics</a>\n<br>\n<a href='/health'>/health</a>"))
        .expect("Failed to construct the response")
}

fn health_handler(_: Request<Body>) -> Response<Body> {
    Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "text/plain")
        .body(Body::from("OK"))
        .expect("Failed to construct the response")
}

fn metrics_handler(_: Request<Body>) -> Response<Body> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .expect("Failed to construct the response")
}
