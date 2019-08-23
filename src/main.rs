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

fn copy_stream(&mut from: TcpStream, &mut to: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 64];

    match from.read(&mut buf) {
        Ok(len) => {
            if len > 0 {
                // TODO: handle EWOULDBLOCK
                to.write(&buf[0..len]);
            } else {
                break;
            }
        },
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
        },
        Err(e) => {
            println!("error: {}", e);
        },
    }
}

fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    println!("new connection from {:?}", client_stream.peer_addr()?);

    println!("connecting to backend: {}", BACKEND_ADDR);
    let mut backend_stream = TcpStream::connect(BACKEND_ADDR)?;

    client_stream.set_nonblocking(true)?;
    backend_stream.set_nonblocking(true)?;

    loop {
        copy_stream(&mut client_stream, &mut backend_stream)?;
        copy_stream(&mut backend_stream, &mut client_stream)?;

        thread::sleep(time::Duration::from_millis(1));
    }
        
    /*
    println!("reading the header ...");

    match MsgHeader::from_reader(stream) {
        Ok(header) => println!("got a header: {:?}", header),
        Err(e) => println!("failed to read a header: {}", e),
    }
    */

    Ok(())
}

