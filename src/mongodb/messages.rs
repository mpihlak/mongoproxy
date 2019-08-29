use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Read, Error, ErrorKind};
use bson::{decode_document};
use std::fmt;
use log::{info};
use num_derive::FromPrimitive;


pub const HEADER_LENGTH: usize = 16;

#[derive(Debug)]
#[derive(FromPrimitive)]
pub enum OpCode{
    OpReply = 1,
    OpQuery = 2004,
    OpMsg = 2013,
    OpPing = 2010,
    OpPong = 2011,
}

#[derive(Debug)]
pub enum MongoMessage {
    Header(MsgHeader),
    Msg(MsgOpMsg),
    Query(MsgOpQuery),
    Reply(MsgOpReply),
    None,
}

impl MongoMessage {
    pub fn update_stats(&self, source_label: &str) {
        // Extract the stats from the message and update any counters
        match self {
            MongoMessage::Header(m) => info!("{}: hdr: {}", source_label, m),
            MongoMessage::Msg(m) => info!("{}: msg: {}", source_label, m),
            MongoMessage::Query(m) => info!("{}: msg: {}", source_label, m),
            MongoMessage::Reply(m) => info!("{}: msg: {}", source_label, m),
            MongoMessage::None => {},
        }
    }
}

#[derive(Debug,Clone)]
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

impl fmt::Display for MsgHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HDR op: {}, request_id: {}, response_to: {}, length: {}",
        self.op_code, self.request_id, self.response_to, self.message_length)
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
               self.flag_bits, self.kind)?;
        for (i, v)  in self.sections.iter().enumerate() {
            write!(f, "section {}: {}\n", i, v)?;
        }
        Ok(())
    }
}

impl MsgOpMsg {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flag_bits   = rdr.read_u32::<LittleEndian>()?;
        let kind        = rdr.read_u8()?;

        if kind != 0 {
            let _section_size = rdr.read_i32::<LittleEndian>()?;
            let _seq_id = read_c_string(&mut rdr)?;
            info!("section_size={}, seq_id={}", _section_size, _seq_id);
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

impl fmt::Display for MsgOpQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_QUERY flags: {}, collection: {}, to_skip: {}, to_return: {}",
               self.flags, self.full_collection_name, self.number_to_skip, self.number_to_return)
    }
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

#[derive(Debug)]
pub struct MsgOpReply {
    flags:              u32,
    cursor_id:          u64,
    starting_from:      u32,
    number_returned:    u32,
    documents:          Vec<bson::Document>,
}

impl fmt::Display for MsgOpReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_REPLY flags: {}, cursor_id: {}, starting_from: {}, number_returned: {}\n",
               self.flags, self.cursor_id, self.starting_from, self.number_returned)?;
        for (i, v)  in self.documents.iter().enumerate() {
            write!(f, "document {}: {}\n", i, v)?;
        }
        Ok(())
    }
}

impl MsgOpReply {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flags  = rdr.read_u32::<LittleEndian>()?;
        let cursor_id = rdr.read_u64::<LittleEndian>()?;
        let starting_from = rdr.read_u32::<LittleEndian>()?;
        let number_returned = rdr.read_u32::<LittleEndian>()?;
        let mut documents = Vec::new();
        while let Ok(doc) = decode_document(&mut rdr) {
            documents.push(doc);
        }
        Ok(MsgOpReply{flags, cursor_id, starting_from, number_returned, documents})
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

#[test]
fn test_read_cstring() {
    let buf = b"kala\0";
    let res = read_c_string(&buf[..]).unwrap();
    assert_eq!(res, "kala");

    let buf = b"\0";
    let res = read_c_string(&buf[..]).unwrap();
    assert_eq!(res, "");
}
