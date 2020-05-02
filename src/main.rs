use std::sync::{Arc,Mutex};
use std::net::{SocketAddr,ToSocketAddrs};
use std::io::{self};
use std::error::{Error};
use std::{thread, str};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener,TcpStream};
use tokio::net::tcp::{OwnedReadHalf,OwnedWriteHalf};

use prometheus::{CounterVec,HistogramVec,Encoder,TextEncoder};
use clap::{Arg, App, crate_version};
use log::{info,warn,error,debug};
use lazy_static::lazy_static;

#[macro_use] extern crate prometheus;
#[macro_use] extern crate rouille;

use mongoproxy::tracing;
use mongoproxy::dstaddr;
use mongoproxy::appconfig::{AppConfig};
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
            "mongoproxy_client_connected_time_seconds",
            "How long has this client been connected",
            &["client"]).unwrap();

    static ref CLIENT_IDLE_TIME_SECONDS: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_idle_time_seconds",
            "How long has this connection been idle",
            &["client"]).unwrap();
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

    let enable_jaeger = matches.occurrences_of("enable_jaeger") > 0;
    let jaeger_addr = lookup_address(matches.value_of("jaeger_addr").unwrap_or(JAEGER_ADDR)).unwrap();

    env_logger::init();

    info!("MongoProxy v{}", crate_version!());

    start_admin_listener(&admin_addr);
    info!("Admin endpoint at http://{}", admin_addr);

    let proxy_spec = matches.value_of("proxy").unwrap();
    let (local_hostport, remote_hostport) = parse_proxy_addresses(proxy_spec).unwrap();

    let app = AppConfig::new(
        tracing::init_tracer(enable_jaeger, &service_name, jaeger_addr),
        matches.occurrences_of("log_mongo_messages") > 0,
    );

    run_accept_loop(local_hostport, remote_hostport, &app).await;
}

// Accept connections in a loop and spawn a task to proxy them. If remote address is not explicitly
// specified attempt to proxy to the original destination obtained with SO_ORIGINAL_DST socket
// option.
//
// Never returns.
async fn run_accept_loop(local_addr: String, remote_addr: String, app: &AppConfig)
{
    let mut listener = TcpListener::bind(&local_addr).await.unwrap();
    if remote_addr.is_empty() {
        info!("Proxying {} -> <original dst>", local_addr);
    } else {
        info!("Proxying {} -> {}", local_addr, remote_addr);
    }

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
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

                CONNECTION_COUNT_TOTAL.with_label_values(&[&client_addr.to_string()]).inc();

                tokio::spawn(async move {
                    info!("{:?} new connection from {}", thread::current().id(), client_addr);
                    match handle_connection(&server_addr, stream, app).await {
                        Ok(_) => {
                            info!("{:?} {} closing connection.", thread::current().id(), client_addr);
                            DISCONNECTION_COUNT_TOTAL
                                .with_label_values(&[&client_addr.to_string()])
                                .inc();
                        },
                        Err(e) => {
                            warn!("{:?} {} connection error: {}", thread::current().id(), client_addr, e);
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

// Open a connection to the server and start passing bytes between the client and the server. Also
// split the traffic to MongoDb protocol parser, so that we can get some stats out of this.
//
async fn handle_connection(server_addr: &str, client_stream: TcpStream, app: AppConfig)
    -> Result<(), Box<dyn Error>>
{
    info!("{:?} connecting to server: {}", thread::current().id(), server_addr);
    let timer = SERVER_CONNECT_TIME_SECONDS.with_label_values(&[server_addr]).start_timer();
    let server_addr = lookup_address(server_addr)?;
    let server_stream = TcpStream::connect(&server_addr).await?;
    timer.observe_duration();

    let client_addr = format_client_address(&client_stream.peer_addr()?);

    let tracker = Arc::new(Mutex::new(
            MongoStatsTracker::new(
                &client_addr,
                &server_addr.to_string(),
                server_addr,
                app)));
    let mut client_tracker = tracker.clone();
    let mut server_tracker = tracker.clone();

    client_stream.set_nodelay(true)?;
    server_stream.set_nodelay(true)?;

    let (mut read_client, mut write_client) = client_stream.into_split();
    let (mut read_server, mut write_server) = server_stream.into_split();

    // Read from client, write to server
    let client_task = tokio::spawn(async move {
        proxy_bytes(&mut read_client, &mut write_server, &mut client_tracker, true).await?;
        Ok::<(), Box<dyn Error+Sync+Send>>(())
    });

    // Read from server, write to client
    let server_task = tokio::spawn(async move {
        proxy_bytes(&mut read_server, &mut write_client, &mut server_tracker, false).await?;
        Ok::<(), Box<dyn Error+Sync+Send>>(())
    });

    match tokio::try_join!(client_task, server_task) {
        Ok(_) => Ok(()),
        Err(e) => Err(Box::new(e))
    }
}

// Move bytes between sockets
async fn proxy_bytes(
    read_from: &mut OwnedReadHalf,
    write_to: &mut OwnedWriteHalf,
    tracker: &mut Arc<Mutex<MongoStatsTracker>>,
    is_client_tracker: bool,
) -> Result<(), Box<dyn Error+Sync+Send>> {
    loop {
        let mut buf = [0; 1024];
        let len = read_from.read(&mut buf).await?;

        if len > 0 {
            write_to.write_all(&buf[0..len]).await?;

            let mut tracker = tracker.lock().unwrap();
            if is_client_tracker {
                tracker.track_client_request(&buf[..len]);
            } else {
                tracker.track_server_response(&buf[..len]);
            }
        } else {
            // EOF on read
            return Ok(());
        }
    }
}

fn lookup_address(addr: &str) -> std::io::Result<SocketAddr> {
    if let Some(sockaddr) = addr.to_socket_addrs()?.next() {
        debug!("{:?} {} resolves to {}", thread::current().id(), addr, sockaddr);
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

// Parse the local and remote address pair from provided proxy definition
fn parse_proxy_addresses(proxy_def: &str) -> Result<(String,String), Box<dyn Error>> {
    if let Some(pos) = proxy_def.find(':') {
        let (local_port, remote_hostport) = proxy_def.split_at(pos);
        let local_addr = format!("0.0.0.0:{}", local_port);

        Ok((local_addr, remote_hostport[1..].to_string()))
    } else {
        Ok((format!("0.0.0.0:{}", proxy_def), String::from("")))
    }
}

pub fn start_admin_listener(endpoint: &str) {
    let endpoint = endpoint.to_owned();
    thread::spawn(||
        rouille::start_server(endpoint, move |request| {
            router!(request,
                (GET) (/) => {
                    rouille::Response::html(
                        "<a href='/metrics'>metrics</a>\n<br>\n\
                         <a href='/health'>health</a>\n")
                },
                (GET) (/health) => {
                    rouille::Response::text("OK")
                },
                (GET) (/metrics) => {
                    let encoder = TextEncoder::new();
                    let metric_families = prometheus::gather();
                    let mut buffer = vec![];
                    encoder.encode(&metric_families, &mut buffer).unwrap();
                    rouille::Response::from_data("text/plain", buffer)
                },
                _ => rouille::Response::empty_404()
            )
        })
    );
}
