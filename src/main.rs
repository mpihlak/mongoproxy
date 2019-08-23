use byteorder::{LittleEndian, ReadBytesExt};

use std::net::{TcpListener, TcpStream};
use std::io::{self, Read, Write};
use std::{thread, time};


const BACKEND_ADDR: &str = "localhost:27017";

#[derive(Debug)]
struct MsgHeader {
    message_length: u32,
    request_id:     u32,
    response_to:    u32,
    op_code:        u32,
}

impl MsgHeader {
    fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let message_length  = rdr.read_u32::<LittleEndian>()?;
        let request_id      = rdr.read_u32::<LittleEndian>()?;
        let response_to     = rdr.read_u32::<LittleEndian>()?;
        let op_code         = rdr.read_u32::<LittleEndian>()?;
        Ok(MsgHeader{message_length, request_id, response_to, op_code})
    }
}

fn main() {
    let listen_addr = "127.0.0.1:27111";
    let listener = TcpListener::bind(listen_addr).unwrap();

    println!("Listening on {}", listen_addr);
    println!("^C to exit");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                match handle_connection(stream) {
                    Ok(_) => println!("done with it"),
                    Err(e) => println!("err: {}", e),
                }
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
                // TODO: handle writes errors, eg. EWOULDBLOCK
                to_stream.write(&buf[0..len])?;
                output_buf.extend(&buf[0..len]);
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

// Check if there's some parseable data in that buffer.
fn process_buffer(buf: &Vec<u8>) {
    if buf.len() >= 16 {
        match MsgHeader::from_reader(&buf[0..16]) {
            Ok(header) => println!("got a header: {:?}", header),
            Err(e) => println!("failed to read a header: {}", e),
        }
    }
}

fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    println!("new connection from {:?}", client_stream.peer_addr()?);

    println!("connecting to backend: {}", BACKEND_ADDR);
    let mut backend_stream = TcpStream::connect(BACKEND_ADDR)?;

    client_stream.set_nonblocking(true)?;
    backend_stream.set_nonblocking(true)?;

    let mut done = false;

    while !done {
        // First, take everything the client has and send it to the
        // backend. Naturally the backend wants to send it's response
        // back to the client, so handle that next.
        //
        let mut data_from_client = Vec::new();
        if !copy_stream(&mut client_stream, &mut backend_stream, &mut data_from_client)? {
            println!("client ran out of bytes");
            done = true;
        }

        process_buffer(&data_from_client);

        let mut data_from_backend = Vec::new();
        if !copy_stream(&mut backend_stream, &mut client_stream, &mut data_from_backend)? {
            println!("backend ran out of bytes");
            done = true;
        }

        // Sleep, as not to hog all CPU.
        thread::sleep(time::Duration::from_millis(1));
    }

    Ok(())
}

