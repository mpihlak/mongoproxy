use std::net::{TcpListener, TcpStream};
use std::io::{self, Read, Write};
use std::{thread, time, str};
use log::{info,warn};
use env_logger;

mod mongodb;

const BACKEND_ADDR: &str = "localhost:27017";

fn main() {
    env_logger::init();

    let listen_addr = "127.0.0.1:27111";
    let listener = TcpListener::bind(listen_addr).unwrap();

    info!("Listening on {}", listen_addr);
    info!("^C to exit");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    match handle_connection(stream) {
                        Ok(_) => info!("closing connection."),
                        Err(e) => warn!("connection error: {}", e),
                    };
                });
            },
            Err(e) => {
                warn!("accept: {:?}", e)
            },
        }
    }
}

// Main proxy logic. Open a connection to the backend and start passing bytes
// between the client and the backend. Also split the traffic to MongoDb protocol
// parser, so that we can get some stats out of this.
//
// TODO: Convert this to async IO or some form of epoll (mio?)
fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    info!("new connection from {:?}", client_stream.peer_addr()?);
    info!("connecting to backend: {}", BACKEND_ADDR);
    let mut backend_stream = TcpStream::connect(BACKEND_ADDR)?;

    client_stream.set_nonblocking(true)?;
    backend_stream.set_nonblocking(true)?;

    let mut done = false;
    let mut client_parser = mongodb::MongoProtocolParser::new();
    let mut backend_parser = mongodb::MongoProtocolParser::new();

    while !done {
        let mut data_from_client = Vec::new();
        if !copy_stream(&mut client_stream, &mut backend_stream, &mut data_from_client)? {
            info!("{} client EOF", client_stream.peer_addr()?);
            done = true;
        }

        client_parser.parse_buffer(&data_from_client);

        let mut data_from_backend = Vec::new();
        if !copy_stream(&mut backend_stream, &mut client_stream, &mut data_from_backend)? {
            info!("{} backend EOF", backend_stream.peer_addr()?);
            done = true;
        }

        backend_parser.parse_buffer(&data_from_backend);

        thread::sleep(time::Duration::from_millis(1));
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
        },
        Err(e) => {
            warn!("error: {}", e);
        },
    }
    Ok(true)
}
