use byteorder::{LittleEndian, ReadBytesExt};

use std::net::{TcpListener, TcpStream};
use std::io::{self, Read, Write};
use std::{thread, time};


const BACKEND_ADDR: &str = "localhost:27017";
const HEADER_LENGTH: usize = 16;

#[derive(Debug)]
struct MsgHeader {
    message_length: usize,
    request_id:     u32,
    response_to:    u32,
    op_code:        u32,
}

impl MsgHeader {
    fn new() -> MsgHeader {
        MsgHeader{
            message_length: 0,
            request_id: 0,
            response_to: 0,
            op_code: 0
        }
    }

    fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let message_length  = rdr.read_u32::<LittleEndian>()? as usize;
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
                // TODO: handle write errors, eg. EWOULDBLOCK
                to_stream.write(&buf[0..len])?;
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

struct ParseState {
    header: MsgHeader,
    have_header: bool,
    want_bytes: usize,
    message_start: usize,
}

impl ParseState {

    fn new() -> ParseState {
        ParseState{
            header: MsgHeader::new(),
            have_header: false,
            want_bytes: HEADER_LENGTH,
            message_start: 0,
        }
    }

    // Parse the buffer and advance the internal state
    fn parse_buffer(mut self, buf: &Vec<u8>) -> ParseState {
        let have_bytes = buf.len() - self.message_start;
        let message_end = self.message_start + self.want_bytes;

        if have_bytes < self.want_bytes {
            return self
        }

        if !self.have_header {
            match MsgHeader::from_reader(&buf[self.message_start..message_end]) {
                Ok(header) => {
                    self.header = header;
                    self.have_header = true;
                    self.message_start += self.want_bytes;
                    self.want_bytes = self.header.message_length - HEADER_LENGTH;
                    println!("parse: got a header: {:?}, want {} more bytes",
                             self.header, self.want_bytes);
                },
                Err(e) => {
                    println!("parse: failed to read a header: {}", e);
                },
            }
        } else {
            println!("processing payload {} bytes", self.want_bytes);
            self.message_start = message_end;
            self.have_header = false;
            self.want_bytes = HEADER_LENGTH;
        } 
        self
    }
}

fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    println!("new connection from {:?}", client_stream.peer_addr()?);

    println!("connecting to backend: {}", BACKEND_ADDR);
    let mut backend_stream = TcpStream::connect(BACKEND_ADDR)?;

    client_stream.set_nonblocking(true)?;
    backend_stream.set_nonblocking(true)?;

    let mut done = false;
    let mut parser = ParseState::new();

    // Currently these buffers accumulate everything the stream has provided
    // since the creation of the connection. This means that we need to keep
    // some offset in the ParseState to signify where the next message begins.
    let mut data_from_client = Vec::new();
    let mut data_from_backend = Vec::new();

    while !done {
        // First, take everything the client has and send it to the
        // backend. Naturally the backend wants to send it's response
        // back to the client, so handle that next.
        //
        if !copy_stream(&mut client_stream, &mut backend_stream, &mut data_from_client)? {
            println!("client ran out of bytes");
            done = true;
        }

        parser = parser.parse_buffer(&data_from_client);

        if !copy_stream(&mut backend_stream, &mut client_stream, &mut data_from_backend)? {
            println!("backend ran out of bytes");
            done = true;
        }

        // Sleep, as not to hog all CPU.
        thread::sleep(time::Duration::from_millis(500));
    }

    Ok(())
}

