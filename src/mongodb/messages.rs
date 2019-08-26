use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Read, Error, ErrorKind};
use bson::{decode_document};
use std::fmt;
use log::{debug,info,warn};


pub const HEADER_LENGTH: usize = 16;


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
    flag_bits:  u32,
    kind:       u8,
    sections:   Vec<bson::Document>,
}

impl fmt::Display for MsgOpMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_MSG flags: {}, kind: {}\n",
               self.flag_bits, self.kind).unwrap();

        for (i, v)  in self.sections.iter().enumerate() {
            write!(f, "section {}: {}\n", i, v)?;
        }
        write!(f, ".")
    }
}

impl MsgOpMsg {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flag_bits   = rdr.read_u32::<LittleEndian>()?;
        let kind        = rdr.read_u8()?;

        if kind != 0 {
            let _section_size = rdr.read_i32::<LittleEndian>()?;
            let _seq_id = read_c_string(&mut rdr)?;
            info!("---------------------------------------------------------");
            info!("section_size={}, seq_id={}", _section_size, _seq_id);
            info!("---------------------------------------------------------");
            // XXX: is every section actually preceeded by this header?
        }

        let mut sections = Vec::new();
        while let Ok(doc) = decode_document(&mut rdr) {
            sections.push(doc);
        }

        // Note: there may be checksum following, but we've probably eaten
        // it's bytes while trying to decode the section list.

        Ok(MsgOpMsg{flag_bits, kind, sections})
    }
}

#[derive(Debug)]
pub struct MsgOpQuery {
    flags:  u32,
    full_collection_name: String,
    number_to_skip: i32,
    number_to_return: i32,
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
    // the message is parsed we expect a header again and the process repeats.
    //
    // TODO: Shut down the parser on parsing errors
    // TODO: Return parsing result
    //
    pub fn parse_buffer(&mut self, buf: &Vec<u8>) {
        let mut work_buf = &buf[..];

        loop {
            self.message_buf.extend(work_buf.iter().take(self.want_bytes));

            if work_buf.len() < self.want_bytes {
                self.want_bytes -= work_buf.len();
                return;
            }

            // We have some surplus data, we'll get to that in the next loops
            work_buf = &work_buf[self.want_bytes..];

            debug!("bytes in buffer: {}, bytes in message: {}, want: {}",
                     work_buf.len(), self.message_buf.len(), self.want_bytes);

            if !self.have_header {
                match MsgHeader::from_reader(&self.message_buf[..]) {
                    Ok(header) => {
                        debug!("parse: got a header: {:?}", header);
                        assert!(header.message_length >= HEADER_LENGTH);

                        self.header = header;
                        self.have_header = true;
                        self.want_bytes = self.header.message_length - HEADER_LENGTH;
                        debug!("want {} more bytes", self.want_bytes);
                    },
                    Err(e) => {
                        warn!("parse: failed to read a header: {}", e);
                    },
                }
            } else {
                debug!("processing payload {} bytes", self.header.message_length - HEADER_LENGTH);

                match self.header.op_code {
                    2004 => {
                        let op = MsgOpQuery::from_reader(&self.message_buf[..]);
                        info!("OP_QUERY: {:?}", op);
                    },
                    2013 => {
                        let op = MsgOpMsg::from_reader(&self.message_buf[..]);
                        info!("OP_MSG: {}", op.unwrap());
                    },
                    op_code => {
                        info!("OP {}", op_code);
                    },
                }

                if work_buf.len() > self.want_bytes {
                    work_buf = &work_buf[self.want_bytes..];
                }

                self.have_header = false;
                self.want_bytes = HEADER_LENGTH;
            }

            // At this point we've finished parsing of a complete buffer, but still
            // have some surplus to deal with. Start afresh with an empty buffer and
            // go for another loop.
            //
            self.message_buf.clear();
        }
    }
}

