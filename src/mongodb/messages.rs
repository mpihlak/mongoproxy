use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::io::{self, Read, BufRead, Write, Cursor};
use std::fmt;
use log::{warn,info,debug};
use crate::bson_lite::{self,FieldSelector,BsonLiteDocument,read_cstring};

extern crate bson;


pub const HEADER_LENGTH: usize = 16;


lazy_static! {
    static ref MONGO_BSON_FIELD_SELECTOR: FieldSelector<'static> =
        FieldSelector::build()
            .with("op", "/#1")                                // first field name
            .with("op_value", "/@1")                          // first field value
            .with("db", "/$db")
            .with("collection", "/collection")
            .with("ok", "/ok")
            // The following are returned as a response to "isMaster"
            .with("replicaset", "/setName")
            .with("server_host", "/me")
            // TODO: Handle comments for "aggregate" command
            // { aggregate: "records", pipeline: [{ $match: { x: { $gt: 0 }, $comment: "foo" }
            .with("comment", "/comment")
            .with("comment", "/q/$comment")
            .with("comment", "/query/$comment")
            // The first isMaster response also contains connection metadata, we want the appname from there:
            // { isMaster: 1, client: { application: { name: "kala" },
            //   driver: { name: "MongoDB Internal Client", version: "4.0.2" },
            //   os: { type: "Darwin", name: "Mac OS X", architecture: "x86_64", version: "18.7.0" } } }
            .with("app_name", "/client/application/name")
            .with("cursor_id", "/cursor/id")
            .with("docs_returned", "/cursor/firstBatch/[]")     // array length
            .with("docs_returned", "/cursor/nextBatch/[]")      // array length
            .with("n", "/n")
            .with("n", "/lastErrorObject/n")                    // findAndModify returns this
            .with("n_modified", "/nModified");
}

#[derive(Debug)]
pub enum OpCode{
    OpReply = 1,
    OpUpdate = 2001,
    OpInsert = 2002,
    OpQuery = 2004,
    OpGetMore = 2005,
    OpDelete = 2006,
    OpMsg = 2013,
    OpPing = 2010,
    OpPong = 2011,
}

#[derive(Debug)]
pub enum MongoMessage {
    Msg(MsgOpMsg),
    Query(MsgOpQuery),
    GetMore(MsgOpGetMore),
    Reply(MsgOpReply),
    Update(MsgOpUpdate),
    Delete(MsgOpDelete),
    Insert(MsgOpInsert),
    None,
}

impl fmt::Display for MongoMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MongoMessage::Msg(m) => m.fmt(f),
            MongoMessage::Query(m) => m.fmt(f),
            MongoMessage::GetMore(m) => m.fmt(f),
            MongoMessage::Reply(m) => m.fmt(f),
            MongoMessage::Update(m) => m.fmt(f),
            MongoMessage::Delete(m) => m.fmt(f),
            MongoMessage::Insert(m) => m.fmt(f),
            MongoMessage::None => "(None)".fmt(f),
        }
    }
}

#[derive(Debug,Clone,Default)]
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

    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
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
        write!(f, "op: {}, request_id: {}, response_to: {}, length: {}",
        self.op_code, self.request_id, self.response_to, self.message_length)
    }
}

#[derive(Debug)]
pub struct MsgOpMsg {
    pub flag_bits:  u32,
    pub documents:   Vec<BsonLiteDocument>,
}

impl fmt::Display for MsgOpMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "OP_MSG flags: {}", self.flag_bits)?;
        for (i, v)  in self.documents.iter().enumerate() {
            writeln!(f, "section {}: {}", i, v)?;
        }
        Ok(())
    }
}

impl MsgOpMsg {
    pub fn from_reader<R: Read+BufRead>(mut rdr: &mut R) -> io::Result<Self> {
        let flag_bits = rdr.read_u32::<LittleEndian>()?;
        debug!("flag_bits={:04x}", flag_bits);

        let mut documents = Vec::new();

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

            // We're reading the individual BSON documents into Vec so that we can dump the
            // contents for debugging.

            let mut buf = Vec::new();
            if kind != 0 {
                let section_size = rdr.read_u32::<LittleEndian>()? as usize;
                let seq_id = read_cstring(&mut rdr)?;

                debug!("section_size={}, seq_id={}", section_size, seq_id);

                // Section size includes the size of the cstring and the length bytes
                let bson_size = section_size - seq_id.len() - 1 - 4;
                rdr.take(bson_size as u64).read_to_end(&mut buf)?;
            } else {
                rdr.read_to_end(&mut buf)?;
            }

            if cfg!(feature = "log_mongodb_messages") {
                if let Ok(doc) = bson::decode_document(&mut &buf[..]) {
                    info!("OP_MSG BSON: {}", doc);
                } else {
                    warn!("OP_MSG BSON parsing failed");
                }
            }

            match bson_lite::decode_document(&mut &buf[..], &MONGO_BSON_FIELD_SELECTOR) {
                Ok(doc) => {
                    debug!("doc: {}", doc);
                    documents.push(doc);
                },
                Err(e) => {
                    warn!("BSON decoder error: {:?}", e);
                    break;
                }
            }
        }

        // Note: there may be checksum following, but we've probably eaten
        // it's bytes while trying to decode the section list.

        Ok(MsgOpMsg{flag_bits, documents})
    }

    #[allow(dead_code)]
    // An incomplete write function for basic testing. Takes a single section
    // document in the buffer.
    pub fn write(&self, mut writer: impl Write, doc_buf: &[u8]) -> io::Result<()> {
        writer.write_u32::<LittleEndian>(self.flag_bits)?;

        let seq_id = "documents";
        let seq_len = 1 + seq_id.len() + doc_buf.len() + 4;

        writer.write_u8(1)?;    // "kind" byte
        writer.write_u32::<LittleEndian>(seq_len as u32)?;
        writer.write_all(seq_id.as_bytes())?;
        writer.write_u8(0)?;    // terminator for the cstring
        writer.write_all(&doc_buf[..])?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct MsgOpQuery {
    flags:  u32,
    pub full_collection_name: String,
    number_to_skip: i32,
    number_to_return: i32,
    pub query: BsonLiteDocument,
    // There's also optional "returnFieldsSelector" but we ignore it
}

impl fmt::Display for MsgOpQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "OP_QUERY flags: {}, collection: {}, to_skip: {}, to_return: {}",
               self.flags, self.full_collection_name, self.number_to_skip, self.number_to_return)?;
        writeln!(f, "query: {}", self.query)
    }
}

impl MsgOpQuery {
    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
        let flags  = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_cstring(&mut rdr)?;
        let number_to_skip = rdr.read_i32::<LittleEndian>()?;
        let number_to_return = rdr.read_i32::<LittleEndian>()?;

        let mut buf = Vec::new();
        rdr.read_to_end(&mut buf)?;
        let query = match bson_lite::decode_document(&mut &buf[..], &MONGO_BSON_FIELD_SELECTOR) {
            Ok(doc) => doc,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };

        if cfg!(feature = "log_mongodb_messages") {
            if let Ok(doc) = bson::decode_document(&mut &buf[..]) {
                info!("OP_QUERY BSON: {}", doc);
            } else {
                warn!("OP_QUERY BSON parsing failed");
            }
        }

        Ok(MsgOpQuery{flags, full_collection_name, number_to_skip, number_to_return, query})
    }
}

#[derive(Debug)]
pub struct MsgOpGetMore {
    pub full_collection_name: String,
    number_to_return: i32,
    cursor_id: i64,
}

impl fmt::Display for MsgOpGetMore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "OP_GET_MORE collection: {}, to_return: {}, cursor_id: {}",
               self.full_collection_name, self.number_to_return, self.cursor_id)
    }
}

impl MsgOpGetMore {
    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
        let _zero = rdr.read_i32::<LittleEndian>()?;
        let full_collection_name = read_cstring(&mut rdr)?;
        let number_to_return = rdr.read_i32::<LittleEndian>()?;
        let cursor_id = rdr.read_i64::<LittleEndian>()?;

        Ok(MsgOpGetMore{full_collection_name, number_to_return, cursor_id})
    }
}

#[derive(Debug)]
pub struct MsgOpUpdate {
    pub full_collection_name: String,
    flags: u32,
    selector: BsonLiteDocument,
    update: BsonLiteDocument,
}

impl fmt::Display for MsgOpUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_UPDATE flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpUpdate {
    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
        let _zero = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_cstring(&mut rdr)?;
        let flags = rdr.read_u32::<LittleEndian>()?;
        let selector = match bson_lite::decode_document(&mut rdr, &MONGO_BSON_FIELD_SELECTOR) {
            Ok(doc) => doc,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };
        let update = match bson_lite::decode_document(&mut rdr, &MONGO_BSON_FIELD_SELECTOR) {
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
    selector: BsonLiteDocument,
}

impl fmt::Display for MsgOpDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_DELETE flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpDelete {
    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
        let _zero = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_cstring(&mut rdr)?;
        let flags = rdr.read_u32::<LittleEndian>()?;
        let selector = match bson_lite::decode_document(&mut rdr, &MONGO_BSON_FIELD_SELECTOR) {
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
    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
        let flags = rdr.read_u32::<LittleEndian>()?;
        let full_collection_name = read_cstring(&mut rdr)?;
        // Ignore the documents, we don't need anything from there
        Ok(MsgOpInsert{flags, full_collection_name})
    }
}

#[derive(Debug)]
pub struct MsgOpReply {
    flags:                  u32,
    cursor_id:              u64,
    starting_from:          u32,
    pub number_returned:    u32,
    pub documents:          Vec<BsonLiteDocument>,
}

impl fmt::Display for MsgOpReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "OP_REPLY flags: {}, cursor_id: {}, starting_from: {}, number_returned: {}",
               self.flags, self.cursor_id, self.starting_from, self.number_returned)?;
        for (i, v)  in self.documents.iter().enumerate() {
            writeln!(f, "document {}: {}", i, v)?;
        }
        Ok(())
    }
}

impl MsgOpReply {
    pub fn from_reader(mut rdr: impl BufRead) -> io::Result<Self> {
        let flags  = rdr.read_u32::<LittleEndian>()?;
        let cursor_id = rdr.read_u64::<LittleEndian>()?;
        let starting_from = rdr.read_u32::<LittleEndian>()?;
        let number_returned = rdr.read_u32::<LittleEndian>()?;
        let mut documents = Vec::new();

        let mut buf = Vec::new();
        rdr.read_to_end(&mut buf)?;

        let mut c = Cursor::new(&buf);
        while let Ok(doc) = bson_lite::decode_document(&mut c, &MONGO_BSON_FIELD_SELECTOR) {
            documents.push(doc);
        }

        if cfg!(feature = "log_mongodb_messages") {
            let mut c = Cursor::new(&buf);
            while let Ok(doc) = bson::decode_document(&mut c) {
                info!("OP_REPLY BSON: {}", doc);
            }
        }

        Ok(MsgOpReply{flags, cursor_id, starting_from, number_returned, documents})
    }
}
