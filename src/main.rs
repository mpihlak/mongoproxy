use std::net::{TcpListener, TcpStream};
use std::io::{self, Read, Write};
use std::{thread, time, str};

mod mongodb;


const BACKEND_ADDR: &str = "localhost:27017";


fn main() {
    let listen_addr = "127.0.0.1:27111";
    let listener = TcpListener::bind(listen_addr).unwrap();

    println!("Listening on {}", listen_addr);
    println!("^C to exit");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    match handle_connection(stream) {
                        Ok(_) => println!("closing connection."),
                        Err(e) => println!("connection error: {}", e),
                    };
                });
            },
            Err(e) => {
                println!("accept: {:?}", e)
            },
        }
    }
}

// Copy bytes from one stream to another.
// Return true if there is more work to do.
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
            println!("error: {}", e);
        },
    }
    Ok(true)
}

fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    println!("new connection from {:?}", client_stream.peer_addr()?);
    println!("connecting to backend: {}", BACKEND_ADDR);
    let mut backend_stream = TcpStream::connect(BACKEND_ADDR)?;

    client_stream.set_nonblocking(true)?;
    backend_stream.set_nonblocking(true)?;

    let mut done = false;
    let mut client_parser = mongodb::MongoProtocolParser::new();
    let mut backend_parser = mongodb::MongoProtocolParser::new();

    while !done {
        let mut data_from_client = Vec::new();
        if !copy_stream(&mut client_stream, &mut backend_stream, &mut data_from_client)? {
            println!("client ran out of bytes");
            done = true;
        }

        client_parser.parse_buffer(&data_from_client);

        let mut data_from_backend = Vec::new();
        if !copy_stream(&mut backend_stream, &mut client_stream, &mut data_from_backend)? {
            println!("backend ran out of bytes");
            done = true;
        }

        backend_parser.parse_buffer(&data_from_backend);

        thread::sleep(time::Duration::from_millis(1));
    }

    Ok(())
}

