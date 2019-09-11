use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::io::{self, Read, Write, Error, ErrorKind};
use bson::{decode_document};
use std::fmt;
use log::{warn,debug};
use num_derive::FromPrimitive;


pub const HEADER_LENGTH: usize = 16;

#[derive(Debug)]
#[derive(FromPrimitive)]
pub enum OpCode{
    OpReply = 1,
    OpUpdate = 2001,
    OpInsert = 2002,
    OpQuery = 2004,
    OpDelete = 2006,
    OpMsg = 2013,
    OpPing = 2010,
    OpPong = 2011,
}

#[derive(Debug)]
pub enum MongoMessage {
    Msg(MsgOpMsg),
    Query(MsgOpQuery),
    Reply(MsgOpReply),
    Update(MsgOpUpdate),
    Delete(MsgOpDelete),
    Insert(MsgOpInsert),
    None,
}

impl fmt::Display for MongoMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return match self {
            // TODO: Surely this can be handled more elegantly, perhaps with trait impl
            MongoMessage::Msg(m) => m.fmt(f),
            MongoMessage::Query(m) => m.fmt(f),
            MongoMessage::Reply(m) => m.fmt(f),
            MongoMessage::Update(m) => m.fmt(f),
            MongoMessage::Delete(m) => m.fmt(f),
            MongoMessage::Insert(m) => m.fmt(f),
            MongoMessage::None => "(None)".fmt(f),
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

    #[allow(dead_code)]
    pub fn write(&self, mut writer: impl Write) -> io::Result<()> {
        writer.write_u32::<LittleEndian>(self.message_length as u32)?;
        writer.write_u32::<LittleEndian>(self.request_id)?;
        writer.write_u32::<LittleEndian>(self.response_to)?;
        writer.write_u32::<LittleEndian>(self.op_code)?;
        Ok(())
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
    pub flag_bits:  u32,
    pub sections:   Vec<bson::Document>,
}

impl fmt::Display for MsgOpMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_MSG flags: {}\n",
               self.flag_bits)?;
        for (i, v)  in self.sections.iter().enumerate() {
            write!(f, "section {}: {}\n", i, v)?;
        }
        Ok(())
    }
}

impl MsgOpMsg {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flag_bits   = rdr.read_u32::<LittleEndian>()?;
        debug!("flag_bits={:04x}", flag_bits);

        let mut sections = Vec::new();

        loop {
            let kind = match rdr.read_u8() {
                Ok(r)   => r,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // This is OK if we've already read at least one doc
                    break;
                },
                Err(e)  => {
                    warn!("error on read: {}", e);
                    break;
                },
            };

            if kind != 0 {
                let section_size = rdr.read_u32::<LittleEndian>()? as usize;
                let seq_id = read_c_string(&mut rdr)?;

                // So looks like the section_size is actually the BSON document size
                // plus the length of the Cstring and the 4 bytes for the size.
                // We're going to ignore it, as the BSON doc has it's own size bytes.
                debug!("section_size={}, seq_id={}", section_size, seq_id);
            }

            match decode_document(&mut rdr) {
                Ok(doc) => {
                    debug!("doc: {}", doc);
                    sections.push(doc);
                },
                Err(e) => {
                    warn!("BSON decoder error: {:?}", e);
                    break;
                }
            }
        }

        // Note: there may be checksum following, but we've probably eaten
        // it's bytes while trying to decode the section list.

        Ok(MsgOpMsg{flag_bits, sections})
    }

    #[allow(dead_code)]
    pub fn write(&self, mut writer: impl Write) -> io::Result<()> {
        writer.write_u32::<LittleEndian>(self.flag_bits)?;
        for section in self.sections.iter() {
            let mut buf = Vec::new();
            bson::encode_document(&mut buf, section).unwrap();

            let seq_id = "documents";
            let seq_len = 1 + seq_id.len() + buf.len();

            writer.write_u8(1)?;    // "kind" byte
            writer.write_u32::<LittleEndian>(seq_len as u32)?;
            writer.write(seq_id.as_bytes())?;
            writer.write_u8(0)?;    // terminator for the cstring
            writer.write_all(&buf[..])?;
        }
        Ok(())
    }

}

#[derive(Debug)]
pub struct MsgOpQuery {
    flags:  u32,
    pub full_collection_name: String,
    number_to_skip: i32,
    number_to_return: i32,
    pub query: bson::Document,
    // There's also optional "returnFieldsSelector" but we ignore it
}

impl fmt::Display for MsgOpQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_QUERY flags: {}, collection: {}, to_skip: {}, to_return: {}\n",
               self.flags, self.full_collection_name, self.number_to_skip, self.number_to_return)?;
        write!(f, "query: {}", self.query)
    }
}

impl MsgOpQuery {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flags  = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_c_string(&mut rdr)?;
        let number_to_skip = rdr.read_i32::<LittleEndian>()?;
        let number_to_return = rdr.read_i32::<LittleEndian>()?;
        let query = match decode_document(&mut rdr) {
            Ok(doc) => doc,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };
        Ok(MsgOpQuery{flags, full_collection_name, number_to_skip, number_to_return, query})
    }
}

#[derive(Debug)]
pub struct MsgOpUpdate {
    pub full_collection_name: String,
    flags: u32,
    selector: bson::Document,
    update: bson::Document,
}

impl fmt::Display for MsgOpUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_UPDATE flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpUpdate {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let _zero = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_c_string(&mut rdr)?;
        let flags = rdr.read_u32::<LittleEndian>()?;
        let selector = match decode_document(&mut rdr) {
            Ok(doc) => doc,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };
        let update = match decode_document(&mut rdr) {
            Ok(doc) => doc,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };
        Ok(MsgOpUpdate{flags, full_collection_name, selector, update})
    }
}

#[derive(Debug)]
pub struct MsgOpDelete {
    pub full_collection_name: String,
    flags: u32,
    selector: bson::Document,
}

impl fmt::Display for MsgOpDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_DELETE flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpDelete {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let _zero = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_c_string(&mut rdr)?;
        let flags = rdr.read_u32::<LittleEndian>()?;
        let selector = match decode_document(&mut rdr) {
            Ok(doc) => doc,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };
        Ok(MsgOpDelete{flags, full_collection_name, selector})
    }
}

#[derive(Debug)]
pub struct MsgOpInsert {
    flags: u32,
    pub full_collection_name: String,
}

impl fmt::Display for MsgOpInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_INSERT flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpInsert {
    pub fn from_reader(mut rdr: impl Read) -> io::Result<Self> {
        let flags = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_c_string(&mut rdr)?;
        // Ignore the documents, we don't need anything from there
        Ok(MsgOpInsert{flags, full_collection_name})
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_cstring() {
        let buf = b"kala\0";
        let res = read_c_string(&buf[..]).unwrap();
        assert_eq!(res, "kala");

        let buf = b"\0";
        let res = read_c_string(&buf[..]).unwrap();
        assert_eq!(res, "");
    }
}