use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Read, Error, ErrorKind};
use bson::{decode_document};
use std::fmt;


const HEADER_LENGTH: usize = 16;


#[derive(Debug)]
pub struct MsgHeader {
    pub message_length: usize,
    pub request_id:     u32,
    pub response_to:    u32,
    pub op_code:        u32,
}

impl MsgHeader {
    pub fn new() -> MsgHeader {
        MsgHeader{
            message_length: 0,
            request_id: 0,
            response_to: 0,
            op_code: 0
        }
    }

    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let message_length  = rdr.read_u32::<LittleEndian>()? as usize;
        let request_id      = rdr.read_u32::<LittleEndian>()?;
        let response_to     = rdr.read_u32::<LittleEndian>()?;
        let op_code         = rdr.read_u32::<LittleEndian>()?;
        Ok(MsgHeader{message_length, request_id, response_to, op_code})
    }
}

#[derive(Debug)]
pub struct MsgOpMsg {
    pub flag_bits:  u32,
    pub kind:       u8,
    pub doc:        bson::Document,
    pub checksum:   u32,
}

impl fmt::Display for MsgOpMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_MSG flags: {}, kind: {}, checksum: {}\n",
               self.flag_bits, self.kind, self.checksum).unwrap();
        write!(f, "doc: {}", self.doc)
    }
}

impl MsgOpMsg {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flag_bits   = rdr.read_u32::<LittleEndian>()?;
        let kind        = rdr.read_u8()?;

        if kind != 0 {
            let _section_size = rdr.read_i32::<LittleEndian>()?;
            let _seq_id = read_c_string(&mut rdr)?;
            println!("size={}, seq_id={}", _section_size, _seq_id);
        }

        let doc = decode_document(&mut rdr).unwrap();
        // TODO: handle multiple documents

        let checksum = if flag_bits & 0x01 == 0x01 { rdr.read_u32::<LittleEndian>()? } else { 0 };

        Ok(MsgOpMsg{flag_bits, kind, doc, checksum})
    }
}

#[derive(Debug)]
pub struct MsgOpQuery {
    pub flags:  u32,
    pub full_collection_name: String,
    pub number_to_skip: i32,
    pub number_to_return: i32,
}

impl MsgOpQuery {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flags  = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_c_string(&mut rdr)?;
        let number_to_skip = rdr.read_i32::<LittleEndian>()?;
        let number_to_return = rdr.read_i32::<LittleEndian>()?;
        Ok(MsgOpQuery{flags, full_collection_name, number_to_skip, number_to_return})
    }
}

fn read_c_string(rdr: impl Read) -> io::Result<String> {
    let mut bytes = Vec::new();
    for byte in rdr.bytes() {
        match byte {
            Ok(b) if b == 0 => break,
            Ok(b) => bytes.push(b),
            Err(e) => return Err(e),
        }
    }

    if let Ok(res) = String::from_utf8(bytes) {
        return Ok(res)
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

pub struct MongoProtocolParser {
    header: MsgHeader,
    have_header: bool,
    want_bytes: usize,      // How many more bytes do we need for a complete message
    message_buf: Vec<u8>,   // Accumulated message bytes, parseable when we have all want_bytes
}

impl MongoProtocolParser {

    pub fn new() -> MongoProtocolParser {
        MongoProtocolParser{
            header: MsgHeader::new(),
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
    pub fn parse_buffer(&mut self, buf: &Vec<u8>) {
        self.message_buf.extend(buf.iter().take(self.want_bytes));
        
        if buf.len() < self.want_bytes {
            self.want_bytes -= buf.len();
            return;
        }

        let surplus_buf = &buf[self.want_bytes..];

        if !self.have_header {
            match MsgHeader::from_reader(&self.message_buf[..]) {
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
                    let op = MsgOpQuery::from_reader(&self.message_buf[..]);
                    println!("OP_QUERY: {:?}", op);
                },
                2013 => {
                    let op = MsgOpMsg::from_reader(&self.message_buf[..]);
                    println!("OP_MSG: {}", op.unwrap());
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
        // TODO: implement this, as it really does happen.
        //
        assert!(surplus_buf.len() <= self.want_bytes);
        self.message_buf = surplus_buf.to_vec();
        self.want_bytes -= surplus_buf.len();
    }
}

