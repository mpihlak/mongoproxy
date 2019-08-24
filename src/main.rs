use std::net::{TcpListener, TcpStream};
use std::io::{self, Read, Write};
use std::{thread, time, str};

mod mongodb;


const BACKEND_ADDR: &str = "localhost:27017";
const HEADER_LENGTH: usize = 16;


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

struct ParseState {
    header: mongodb::MsgHeader,
    have_header: bool,
    want_bytes: usize,      // How many more bytes do we need for a complete message
    message_buf: Vec<u8>,   // Accumulated message bytes, parseable when we have all want_bytes
}

impl ParseState {

    fn new() -> ParseState {
        ParseState{
            header: mongodb::MsgHeader::new(),
            have_header: false,
            want_bytes: HEADER_LENGTH,
            message_buf: Vec::new(),
        }
    }

    // Parse the buffer and advance the internal state
    //
    // The buffer that is passed to parsing is a segment from a stream
    // of bytes, so we try to assemble this into a complete message and
    // parse that.
    //
    // The first message we always want to see is the MongoDb message header.
    // This header in turn contains the length of the message that follows. So
    // we try to read message length worth of bytes and parse the message. Once
    // the message is parsed we expect a header again and so it goes.
    //
    fn parse_buffer(&mut self, buf: &Vec<u8>) {
        self.message_buf.extend(buf.iter().take(self.want_bytes));
        
        if buf.len() < self.want_bytes {
            self.want_bytes -= buf.len();
            return;
        }

        let surplus_buf = &buf[self.want_bytes..];

        if !self.have_header {
            match mongodb::MsgHeader::from_reader(&self.message_buf[..]) {
                Ok(header) => {
                    self.header = header;
                    self.have_header = true;
                    self.want_bytes = self.header.message_length - HEADER_LENGTH;
                    println!("parse: got a header: {:?}, want {} more bytes",
                             self.header, self.want_bytes);
                },
                Err(e) => {
                    println!("parse: failed to read a header: {}", e);
                },
            }
        } else {
            println!("processing payload {} bytes", self.header.message_length - HEADER_LENGTH);

            match self.header.op_code {
                2004 => {
                    let op = mongodb::MsgOpQuery::from_reader(&self.message_buf[..]);
                    println!("OP_QUERY: {:?}", op);
                },
                2013 => {
                    let op = mongodb::MsgOpMsg::from_reader(&self.message_buf[..]);
                    println!("OP_MSG: {:?}", op);
                },
                op_code => {
                    println!("OP {}", op_code);
                },
            }

            self.have_header = false;
            self.want_bytes = HEADER_LENGTH;
        }

        // Now deal with the remainder of the buffer
        //
        // Note that surplus_buf may actually contain multiple messages when the input buffer is
        // large and the messages to be parsed are small.
        //
        // Not ready to deal with that just yet, so just assert that this doesn't happen.
        //
        assert!(surplus_buf.len() <= self.want_bytes);
        self.message_buf = surplus_buf.to_vec();
        self.want_bytes -= surplus_buf.len();
    }
}

fn handle_connection(mut client_stream: TcpStream) -> std::io::Result<()> {
    println!("new connection from {:?}", client_stream.peer_addr()?);
    println!("connecting to backend: {}", BACKEND_ADDR);
    let mut backend_stream = TcpStream::connect(BACKEND_ADDR)?;

    client_stream.set_nonblocking(true)?;
    backend_stream.set_nonblocking(true)?;

    let mut done = false;
    let mut client_parser = ParseState::new();
    let mut backend_parser = ParseState::new();

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

