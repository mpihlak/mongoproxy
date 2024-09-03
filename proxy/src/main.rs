use std::convert::Infallible;
use std::net::{SocketAddr,ToSocketAddrs};
use std::io;
use std::str;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener,TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinSet;

use prometheus::{CounterVec,HistogramVec,Encoder,TextEncoder};
use clap::{Arg, App, crate_version};
use tracing::{info, warn, error, debug, info_span, Instrument, Level};
use tracing_subscriber::{FmtSubscriber, EnvFilter};
use lazy_static::lazy_static;

#[macro_use] extern crate prometheus;

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};

use mongoproxy::jaeger_tracing;
use mongoproxy::dstaddr;
use mongoproxy::appconfig::AppConfig;
use mongoproxy::tracker::{MongoStatsTracker, TrackerMessage};

use mongo_protocol::{MongoMessage, MongoMessageProxy, MsgHeader, ProxyError};


const JAEGER_ADDR: &str = "127.0.0.1:6831";
const ADMIN_PORT: &str = "9898";
const SERVICE_NAME: &str = "mongoproxy";

// Max number of bytes to read from the network
const READ_BUFFER_SIZE: usize = 16384;

// The largest message we can expect from MongoDb (oversize to be safe)
const MAX_MONGO_MESSAGE_SIZE: usize = 64*1024*1024;

// Max number of events the client and server message channels can take.
// We ought to be able to buffer the maximum MongoDb message there.
const MAX_CHANNEL_EVENTS: usize = MAX_MONGO_MESSAGE_SIZE / READ_BUFFER_SIZE;

lazy_static! {
    static ref MONGOPROXY_RUNTIME_INFO: CounterVec =
        register_counter_vec!(
            "mongoproxy_runtime_info",
            "Runtime information about Mongoproxy",
            &["version", "proxy", "service_name", "log_mongo_messages", "enable_jaeger"]).unwrap();

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
}

#[tokio::main]
async fn main() {
    let matches = App::new("mongoproxy")
        .version(crate_version!())
        .about("Proxies MongoDb requests to obtain metrics")
        .arg(Arg::with_name("proxy")
            .long("proxy")
            .value_name("local-port[:remote-host:remote-port]")
            .help("Port the proxy listens on (sidecar) and optionally\na target hostport (for static proxy)")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("log_mongo_messages")
            .long("log-mongo-messages")
            .help("Log the contents of MongoDb messages (adds full BSON parsing)")
            .takes_value(false)
            .required(false))
        .arg(Arg::with_name("enable_jaeger")
            .long("enable-jaeger")
            .help("Enable distributed tracing with Jaeger")
            .takes_value(false)
            .required(false))
        .arg(Arg::with_name("jaeger_addr")
            .long("jaeger-addr")
            .value_name("Jaeger agent host:port")
            .help("Jaeger agent hostport to send traces to (compact thrift protocol)")
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
            .help(&format!("Port the admin endpoints listens on (metrics and health). Default {}", ADMIN_PORT))
            .takes_value(true))
        .get_matches();

    let admin_port = matches.value_of("admin_port").unwrap_or(ADMIN_PORT);
    let admin_addr = format!("0.0.0.0:{}", admin_port);
    let service_name = matches.value_of("service_name").unwrap_or(SERVICE_NAME);
    let log_mongo_messages = matches.occurrences_of("log_mongo_messages") > 0;
    let enable_jaeger = matches.occurrences_of("enable_jaeger") > 0;
    let jaeger_addr = lookup_address(matches.value_of("jaeger_addr").unwrap_or(JAEGER_ADDR)).unwrap();

    let (writer, _guard) = tracing_appender::non_blocking(std::io::stdout());
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_writer(writer)
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(isatty::stdout_isatty())
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default trace subscriber failed");

    info!("MongoProxy v{}", crate_version!());

    start_admin_listener(&admin_addr)
        .expect("failed to start admin listener");
    info!("Admin endpoint at http://{}", admin_addr);

    let proxy_spec = matches.value_of("proxy").unwrap();
    let (local_hostport, remote_hostport) = parse_proxy_addresses(proxy_spec).unwrap();

    let (tracer, _uninstall) = jaeger_tracing::init_tracer(enable_jaeger, service_name, jaeger_addr);

    let app = AppConfig::new(
        tracer,
        log_mongo_messages,
    );

    MONGOPROXY_RUNTIME_INFO.with_label_values(&[
        crate_version!(),
        proxy_spec,
        service_name,
        if log_mongo_messages { "true" } else { "false" },
        if enable_jaeger { "true" } else { "false" } ],
    ).inc();

    run_accept_loop(local_hostport, remote_hostport, app).await;
}

// Accept connections in a loop and spawn a task to proxy them. If remote address is not explicitly
// specified attempt to proxy to the original destination obtained with SO_ORIGINAL_DST socket
// option.
//
// Never returns.
async fn run_accept_loop(local_addr: String, remote_addr: String, app: AppConfig)
{
    if remote_addr.is_empty() {
        info!("Proxying {} -> <original dst>", local_addr);
    } else {
        info!("Proxying {} -> {}", local_addr, remote_addr);
    }

    let listener = TcpListener::bind(&local_addr).await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let client_ip_port = peer_addr.to_string();
                let client_addr = format_client_address(&peer_addr);

                let server_addr = if remote_addr.is_empty() {
                    if let Some(sockaddr) = dstaddr::orig_dst_addr(&stream) {
                        // This only assumes that NATd connections are received
                        // and thus always have a valid target address. We expect
                        // iptables rules to be in place to block direct access
                        // to the proxy port.
                        debug!("Original destination address: {:?}", sockaddr);
                        sockaddr.to_string()
                    } else {
                        error!("Host not set and destination address not found: {}", client_addr);
                        // TODO: Increase a counter
                        continue;
                    }
                } else {
                    remote_addr.clone()
                };

                let app = app.clone();
                let server_ip_port = server_addr.clone();

                CONNECTION_COUNT_TOTAL.with_label_values(&[&client_addr.to_string()]).inc();

                let conn_handler = async move {
                    info!("new connection from {}", client_addr);
                    match handle_connection(&server_addr, stream, app).await {
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
                };

                tokio::spawn(
                    conn_handler.instrument(
                        tracing::info_span!("handle_connection",
                            client_addr = client_ip_port.as_str(),
                            server_addr = server_ip_port.as_str()))
                );
            },
            Err(e) => {
                warn!("accept: {:?}", e)
            },
        }
    }
}

// Open a connection to the server and start passing bytes between the client and the server. Also
// split the traffic to MongoDb protocol parser, so that we can get some stats out of this.
//
// The philosophy here is that we will not change any of the bytes that are passed between the
// client and the server. Instead we fork off a stream and send it to a separate tracker task,
// which then parses the messages and collects metrics from it. Should the tracker fail, the
// proxy still remains operational.
//

async fn handle_connection(server_addr: &str, client_stream: TcpStream, app: AppConfig)
    -> Result<(), Box<dyn std::error::Error>>
{
    info!("connecting to server: {}", server_addr);
    let timer = SERVER_CONNECT_TIME_SECONDS.with_label_values(&[server_addr]).start_timer();
    let server_addr = lookup_address(server_addr)?;
    let server_stream = TcpStream::connect(&server_addr).await?;
    timer.observe_duration();

    let client_addr = format_client_address(&client_stream.peer_addr()?);

    let log_mongo_messages = app.log_mongo_messages;
    let tracing_enabled = app.tracer.is_some();

    client_stream.set_nodelay(true)?;
    server_stream.set_nodelay(true)?;

    // Start the tracker to parse and track MongoDb messages from the input stream. This works by
    // having the proxy tasks send a copy of the parsed Mongo message over a channel.

    let (client_tracker, tracker_rx): (mpsc::Sender<TrackerMessage>, mpsc::Receiver<TrackerMessage>) = mpsc::channel(MAX_CHANNEL_EVENTS);
    let server_tracker = client_tracker.clone();

    let mut task_set = JoinSet::new();

    let mut tracker = MongoStatsTracker::new(
        &client_addr,
        &server_addr.to_string(),
        server_addr,
        app,
        tracker_rx,
    );

    task_set.spawn(async move {
        tracker.run_message_loop().await;
        debug!("Tracker done");
    }.instrument(info_span!("tracker")));

    let (read_client, write_client) = client_stream.into_split();
    let (read_server, write_server) = server_stream.into_split();

    // Read messages from client and pass to server and client tracker.
    let client_proxy = MongoMessageProxy::new(READ_BUFFER_SIZE, log_mongo_messages, tracing_enabled);
    task_set.spawn(async move {
        proxy_loop(true, client_proxy, read_client, write_server, client_tracker).await;
        debug!("Client proxy done");
    }.instrument(info_span!("client proxy")));

    // Read messages from the server and pass to client and server tracker.
    // Note that it is possible for the server to send messages on its' own so this needs to be
    // a separate task.
    let server_proxy = MongoMessageProxy::new(READ_BUFFER_SIZE, log_mongo_messages, tracing_enabled);
    task_set.spawn(async move {
        proxy_loop(false, server_proxy, read_server, write_client, server_tracker).await;
        debug!("Server proxy done");
    }.instrument(info_span!("server proxy")));

    while let Some(res) = task_set.join_next().await {
        if let Err(e) = res {
            warn!("Task completed with error, shutting down this connection: {e}");
            task_set.shutdown().await;
            info!("all tasks shut down");
            return Err(Box::new(e));
        }
    }
    debug!("All tasks finished");

    Ok(())
}

async fn proxy_loop(
    is_client: bool,
    mut proxy: MongoMessageProxy,
    mut read_from: OwnedReadHalf,
    mut write_to: OwnedWriteHalf,
    tracker: Sender<(bool, MsgHeader, MongoMessage)>,
    )
{
    let read_source = if is_client { "client" } else { "server" };
    loop {
        match proxy.proxy_mongo_message(read_source, &mut read_from).await {
            Ok((hdr, msg, buf)) => {
                if let Err(e) = write_to.write_all(&buf).await {
                    warn!("error writing bytes to the other end: {e}");
                    // TODO: Increase a counter
                    break;
                }
                if let Err(e) = tracker.send((is_client, hdr, msg)).await {
                    warn!("error sending message to server tracker: {e}");
                    // TODO: Increase a counter
                    break;
                }
            },
            Err(ProxyError::EOF) => break,
            Err(e) => {
                warn!("error processing server message: {e}");
                break;
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
        let _ = addr_str.split_off(pos);
    }
    addr_str
}

// Parse the local and remote address pair from provided proxy definition
fn parse_proxy_addresses(proxy_def: &str) -> Result<(String,String), io::Error> {
    if let Some(pos) = proxy_def.find(':') {
        let (local_port, remote_hostport) = proxy_def.split_at(pos);
        let local_addr = format!("0.0.0.0:{}", local_port);

        Ok((local_addr, remote_hostport[1..].to_string()))
    } else {
        Ok((format!("0.0.0.0:{}", proxy_def), String::from("")))
    }
}

async fn serve_admin_req(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut response = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            *response.body_mut() = Body::from("/");
        },
        (&Method::GET, "/health") => {
            *response.body_mut() = Body::from("OK");
        },
        (&Method::GET, "/metrics") => {
            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();

            *response.body_mut() = Body::from(buffer);
        },
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        },
    };

    Ok(response)
}

pub fn start_admin_listener(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = endpoint.to_string();
    let addr: SocketAddr = endpoint.parse()?;

    tokio::spawn(async move {
        Server::bind(&addr)
            .serve(make_service_fn(|_conn| async {
                Ok::<_, Infallible>(service_fn(serve_admin_req))
            }))
            .await?;
        Ok::<(), hyper::Error>(())
    });

    Ok(())
}
