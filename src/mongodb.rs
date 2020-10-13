use std::fmt;
use log::{error,warn,info,debug};
use byteorder::{LittleEndian, WriteBytesExt};
use async_bson::{DocumentParser, Document, read_cstring};
use prometheus::{CounterVec};
use std::thread;
use bson;

use std::io::{Read, Write, Error, ErrorKind};
use tokio::io::{self, AsyncReadExt, Result};


pub const HEADER_LENGTH: usize = 16;

pub trait AsyncReadExtPlus: AsyncReadExt+Unpin+Send {}
impl <T>AsyncReadExtPlus for T where T: AsyncReadExt+Unpin+Send {}

lazy_static! {
    static ref MONGO_DOC_PARSER: DocumentParser<'static> =
        DocumentParser::new()
            .match_name_at("/", 1, "op")
            .match_value_at("/", 1, "op_value")
            .match_exact("/$db", "db")
            .match_exact("/collection", "collection")
            .match_exact("/ok", "ok")
            .match_exact("/setName", "replicaset")
            .match_exact("/me", "server_host")
            .match_exact("/comment", "comment")
            .match_exact("/q/$comment", "comment")
            .match_exact("/query/$comment", "comment")
            .match_exact("/filter/$comment", "comment")
            // TODO: support comments also in other pipeline steps
            .match_exact("/pipeline/0/$match/$comment", "comment")
            .match_exact("/client/application/name", "app_name")
            // Workaround for Elixir Mongo driver that has an extra nested "client"
            .match_exact("/client/client/application/name", "app_name")
            .match_exact("/cursor/id", "cursor_id")
            .match_array_len("/cursor/firstBatch", "docs_returned")
            .match_array_len("/cursor/nextBatch", "docs_returned")
            .match_exact("/n", "n")
            // findAndModify returns number of collection in lastErrorObject/n
            .match_exact("/lastErrorObject/n", "n")
            .match_exact("/nModified", "n_modified");

    static ref OPCODE_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_opcode_count_total",
            "Number of different opcodes encountered",
            &["op"]).unwrap();

    static ref UNSUPPORTED_OPCODE_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_unsupported_op_code_count_total",
            "Number of unrecognized opcodes in MongoDb header",
            &["op"]).unwrap();

    static ref MESSAGE_PARSE_ERRORS_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_message_parse_error_count_total",
            "Message body parse errors",
            &["error"]).unwrap();

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
    OpCompressed = 2012,
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
    Compressed(MsgOpCompressed),
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
            MongoMessage::Compressed(m) => m.fmt(f),
            MongoMessage::None => "(None)".fmt(f),
        }
    }
}

impl MongoMessage {

    pub async fn from_reader(
        mut rdr: impl AsyncReadExtPlus,
        log_mongo_messages: bool,
        collect_tracing_data: bool,
    ) -> Result<(MsgHeader, MongoMessage)> {
        let hdr = MsgHeader::from_reader(&mut rdr).await?;

        if hdr.message_length < HEADER_LENGTH {
            return Err(Error::new(ErrorKind::Other, "Invalid MongoDb header"));
        }

        debug!("have header: {}", hdr);

        // Take only as much as promised in the header.
        let mut rdr = rdr.take((hdr.message_length - HEADER_LENGTH) as u64);

        // XXX: Load the message into buffer for debugging
        let mut buf = Vec::new();
        let len = rdr.read_to_end(&mut buf).await?;
        let buf = &buf[..len];

        OPCODE_COUNTER.with_label_values(&[&hdr.op_code.to_string()]).inc();

        let msg = match MongoMessage::extract_message(
                hdr.op_code,
                //&mut rdr,
                &mut &buf[..],
                log_mongo_messages,
                collect_tracing_data).await {
            Ok(msg) => msg,
            Err(e) => {
                error!("Failed to parse MongoDb {} message: {}", hdr.op_code, e);
                error!("{} bytes:", len);
                debug_print(&buf[..]).unwrap();
                MESSAGE_PARSE_ERRORS_COUNTER.with_label_values(&[&e.to_string()]).inc();

                // In theory we could keep the parser active -- maybe it's just this message
                // that got garbled.
                return Err(e);
            }
        };

        // Sink the rest of the bytes in case there are leftovers.
        io::copy(&mut rdr, &mut tokio::io::sink()).await?;

        Ok((hdr, msg))
    }

    // Extract a message or return an error
    async fn extract_message(
        op: u32,
        mut rdr: impl AsyncReadExtPlus,
        log_mongo_messages: bool,
        collect_tracing_data: bool,
    ) -> Result<MongoMessage> {
        let msg = match op {
               1 => MongoMessage::Reply(MsgOpReply::from_reader(&mut rdr, log_mongo_messages).await?),
            2004 => MongoMessage::Query(MsgOpQuery::from_reader(&mut rdr, log_mongo_messages).await?),
            2005 => MongoMessage::GetMore(MsgOpGetMore::from_reader(&mut rdr).await?),
            2001 => MongoMessage::Update(MsgOpUpdate::from_reader(&mut rdr).await?),
            2006 => MongoMessage::Delete(MsgOpDelete::from_reader(&mut rdr).await?),
            2002 => MongoMessage::Insert(MsgOpInsert::from_reader(&mut rdr).await?),
            2012 => MongoMessage::Compressed(MsgOpCompressed::from_reader(&mut rdr).await?),
            2013 => MongoMessage::Msg(MsgOpMsg::from_reader(&mut rdr,
                        log_mongo_messages, collect_tracing_data).await?),
            2010 | 2011 => {
                // This is an undocumented legacy protocol that some clients (bad Robo3T)
                // still use. We ignore it.
                MongoMessage::None
            },
            _ => {
                UNSUPPORTED_OPCODE_COUNTER.with_label_values(&[&op.to_string()]).inc();
                warn!("Unhandled OP: {}", op);
                MongoMessage::None
            },
        };

        Ok(msg)
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
            op_code: 0,
        }
    }

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus) -> Result<Self> {
        let message_length  = rdr.read_u32_le().await? as usize;
        let request_id      = rdr.read_u32_le().await?;
        let response_to     = rdr.read_u32_le().await?;
        let op_code         = rdr.read_u32_le().await?;
        Ok(MsgHeader{message_length, request_id, response_to, op_code})
    }

    pub fn write(&self, mut writer: impl Write) -> Result<()> {
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

// Response objects implement this trait to be handled as server response
pub trait ResponseDocuments {
    fn get_documents(&self) -> &Vec<Document>;
}

#[derive(Debug)]
pub struct MsgOpMsg {
    pub flag_bits:          u32,
    pub documents:          Vec<Document>,
    pub section_bytes:      Vec<Vec<u8>>,
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

impl ResponseDocuments for MsgOpMsg {
    fn get_documents(&self) -> &Vec<Document> {
        &self.documents
    }
}

impl MsgOpMsg {

    pub async fn from_reader(
        mut rdr: impl AsyncReadExtPlus,
        log_mongo_messages: bool,
        collect_tracing_data: bool,
    ) -> Result<Self> {
        let flag_bits = rdr.read_u32_le().await?;
        debug!("flag_bits={:04x}", flag_bits);

        let mut documents = Vec::new();
        let mut section_bytes = Vec::new();

        loop {
            let kind = match rdr.read_u8().await {
                Ok(r)   => r,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // This is OK if we've already read at least one doc. In fact
                    // we're relying on reaching an EOF here to determine when to
                    // stop parsing.
                    if documents.len() == 0 {
                        return Err(Error::new(ErrorKind::Other, "EOF reached, but no sections were read"));
                    }
                    break;
                },
                Err(e)  => {
                    warn!("error on read: {}", e);
                    return Err(e);
                },
            };

            // This business with the sections is described at:
            // https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#id1
            // Anyway, we just eat the section headers here and leave the reader pointing
            // to the beginning of a document.

            if kind != 0 {
                let section_size = rdr.read_u32_le().await? as usize;
                let seq_id = read_cstring(&mut rdr).await?;
                debug!("kind=1: section_size={}, seq_id={}", section_size, seq_id);

                // Section size includes the size of the cstring and the length bytes, but
                // does not include the "kind" byte.
                let _bson_size = section_size - seq_id.len() - 1 - 4;
                // But we're not going to use it but instead just leave it to the
                // BSON parser to figure out.
            } else {
                debug!("kind=0");
                // Nothing to do here, reader already at the start of the document
            }

            let doc = MONGO_DOC_PARSER.parse_document_opt(
                    &mut rdr,
                    log_mongo_messages | collect_tracing_data).await?;
            debug!("doc: {}", doc);

            if log_mongo_messages {
                if let Some(bytes) = doc.get_raw_bytes() {
                    if let Ok(doc) = bson::Document::from_reader(&mut &bytes[..]) {
                        info!("{:?} OP_MSG BSON: {}", thread::current().id(), doc);
                    } else {
                        warn!("{:?} OP_MSG BSON parsing failed", thread::current().id());
                    }
                }
            }

            if collect_tracing_data && (doc.contains_key("comment")
                    || doc.get_str("op").unwrap_or("") == "killCursors") {
                if let Some(bytes) = doc.get_raw_bytes() {
                    section_bytes.push(bytes.clone());
                }
            }

            documents.push(doc);
        }

        if flag_bits & 1 == 1 {
            // Have checksum, eat it
            let _checksum = rdr.read_u32().await?;
        }

        Ok(MsgOpMsg{flag_bits, documents, section_bytes})
    }

    #[allow(dead_code)]
    // An incomplete write function for basic testing. Takes a single section
    // document in the buffer.
    pub fn write(&self, mut writer: impl Write, doc_buf: &[u8]) -> Result<()> {
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
    pub query: Document,
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

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus, log_mongo_messages: bool) -> Result<Self> {
        let flags  = rdr.read_u32_le().await?;
        let full_collection_name = read_cstring(&mut rdr).await?;
        let number_to_skip = rdr.read_i32_le().await?;
        let number_to_return = rdr.read_i32_le().await?;
        let query = MONGO_DOC_PARSER.parse_document_opt(&mut rdr, log_mongo_messages).await?;

        if log_mongo_messages {
            if let Some(bytes) = query.get_raw_bytes() {
                if let Ok(doc) = bson::Document::from_reader(&mut &bytes[..]) {
                    info!("{:?} OP_QUERY BSON: {}", thread::current().id(), doc);
                }
            } else {
                warn!("{:?} OP_QUERY BSON parsing failed", thread::current().id());
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

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus) -> Result<Self> {
        let _zero = rdr.read_i32_le().await?;
        let full_collection_name = read_cstring(&mut rdr).await?;
        let number_to_return = rdr.read_i32_le().await?;
        let cursor_id = rdr.read_i64_le().await?;

        Ok(MsgOpGetMore{full_collection_name, number_to_return, cursor_id})
    }
}

#[derive(Debug)]
pub struct MsgOpUpdate {
    pub full_collection_name: String,
    flags: u32,
    selector: Document,
    update: Document,
}

impl fmt::Display for MsgOpUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_UPDATE flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpUpdate {

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus) -> Result<Self> {
        let _zero = rdr.read_u32_le().await?;
        let full_collection_name = read_cstring(&mut rdr).await?;
        let flags = rdr.read_u32_le().await?;
        let selector = MONGO_DOC_PARSER.parse_document(&mut rdr).await?;
        let update = MONGO_DOC_PARSER.parse_document(&mut rdr).await?;

        Ok(MsgOpUpdate{flags, full_collection_name, selector, update})
    }
}

#[derive(Debug)]
pub struct MsgOpDelete {
    pub full_collection_name: String,
    flags: u32,
    selector: Document,
}

impl fmt::Display for MsgOpDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_DELETE flags: {}, collection: {}",
               self.flags, self.full_collection_name)
    }
}

impl MsgOpDelete {

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus) -> Result<Self> {
        let _zero = rdr.read_u32_le().await?;
        let full_collection_name = read_cstring(&mut rdr).await?;
        let flags = rdr.read_u32_le().await?;
        let selector = MONGO_DOC_PARSER.parse_document(&mut rdr).await?;

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
    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus) -> Result<Self> {
        let flags = rdr.read_u32_le().await?;
        let full_collection_name = read_cstring(&mut rdr).await?;

        // There's also a list of documents in the message, but we ignore it.

        Ok(MsgOpInsert{flags, full_collection_name})
    }
}

#[derive(Debug)]
pub struct MsgOpReply {
    flags:                  u32,
    cursor_id:              u64,
    starting_from:          u32,
    pub number_returned:    u32,
    pub documents:          Vec<Document>,
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

impl ResponseDocuments for MsgOpReply {
    fn get_documents(&self) -> &Vec<Document> {
        &self.documents
    }
}

impl MsgOpReply {

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus, log_mongo_messages: bool) -> Result<Self> {
        let flags  = rdr.read_u32_le().await?;
        let cursor_id = rdr.read_u64_le().await?;
        let starting_from = rdr.read_u32_le().await?;
        let number_returned = rdr.read_u32_le().await?;
        let mut documents = Vec::new();

        while let Ok(doc) = MONGO_DOC_PARSER.parse_document_opt(&mut rdr, log_mongo_messages).await {
            documents.push(doc);
        }

        if log_mongo_messages {
            for doc in documents.iter() {
                if let Some(bytes) = doc.get_raw_bytes() {
                    if let Ok(doc) = bson::Document::from_reader(&mut &bytes[..]) {
                        info!("{:?} OP_REPLY BSON: {}", thread::current().id(), doc);
                    }
                } else {
                    warn!("{:?} OP_REPLY BSON parsing failed", thread::current().id());
                }
            }
        }

        Ok(MsgOpReply{flags, cursor_id, starting_from, number_returned, documents})
    }
}

#[derive(Debug)]
pub struct MsgOpCompressed {
    original_op: i32,
    uncompressed_size: i32,
    compressor_id: u8,
}

impl fmt::Display for MsgOpCompressed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OP_COMPRESSED op: {}, uncompressed_size: {}, compressor: {}",
               self.original_op, self.uncompressed_size, self.compressor_id)
    }
}

impl MsgOpCompressed {

    pub async fn from_reader(mut rdr: impl AsyncReadExtPlus) -> Result<Self> {
        let original_op = rdr.read_i32_le().await?;
        let uncompressed_size = rdr.read_i32_le().await?;
        let compressor_id = rdr.read_u8().await?;

        // Ignore the actual compressed message, we are not going to use it

        Ok(MsgOpCompressed{original_op, uncompressed_size, compressor_id})
    }
}

// Debugging helper to print out xxd compatible byte dumps.
pub fn debug_print(mut rdr: impl Read) -> Result<()> {
    let mut buf = Vec::new();

    rdr.read_to_end(&mut buf)?;

    let mut hex_str = String::new();
    let mut txt_str = String::new();
    let mut offset = 0;
    for (i, b) in buf.iter().enumerate() {
        hex_str.push_str(&format!("{:02x} ", b));
        txt_str.push(if *b < 32 || *b > 127 { '.' } else { *b as char });
        if (i + 1) % 16 == 0 {
            println!("{:08x}: {}{}", offset, hex_str, txt_str);
            offset += 16;
            hex_str.clear();
            txt_str.clear();
        }
    }
    if !hex_str.is_empty() {
        let padding = " ".repeat(48 - hex_str.len());
        println!("{:08x}: {}{}{}", offset, hex_str, padding, txt_str);
    }

    Ok(())
}

#[cfg(test)]

mod tests {
    use super::*;
    use byteorder::{LittleEndian, WriteBytesExt};
    use bson::doc;

    #[tokio::test]
    async fn test_parse_header() {
        let hdr = MsgHeader {
            message_length: 100,
            request_id: 1,
            response_to: 2,
            op_code: 2013,
        };

        let mut buf = Vec::new();
        hdr.write(&mut buf).unwrap();

        let parsed_hdr = MsgHeader::from_reader(&buf[..]).await.unwrap();
        assert_eq!(hdr.message_length, parsed_hdr.message_length);
        assert_eq!(hdr.request_id, parsed_hdr.request_id);
        assert_eq!(hdr.response_to, parsed_hdr.response_to);
        assert_eq!(hdr.op_code, parsed_hdr.op_code);
    }

    fn msgop_to_buf(doc_id: u32, mut buf: impl Write) {
        buf.write_i32::<LittleEndian>(0i32).unwrap();   // flag bits

        // Section 0
        buf.write_u8(0).unwrap();
        let doc = doc! { format!("x{}", doc_id): "foo" };
        doc.to_writer(&mut buf).unwrap();
 
        // Section 1 - first document
        buf.write_u8(1).unwrap();                       // section type=1
        buf.write_i32::<LittleEndian>(1234).unwrap();   // section size (ignored in parser)
        buf.write(b"id0\0").unwrap();                   // id of the section
        let doc = doc! { format!("y{}", doc_id): "bar" };                  // first document
        doc.to_writer(&mut buf).unwrap();

        // Section 1 - second document
        buf.write_u8(1).unwrap();                       // section type=1
        buf.write_i32::<LittleEndian>(1234).unwrap();   // section size (ignored in parser)
        buf.write(b"id1\0").unwrap();                   // id of the section
        let doc = doc! { format!("z{}", doc_id): "baz" };                  // first document
        doc.to_writer(&mut buf).unwrap();
    }

    #[tokio::test]
    async fn test_parse_op_msg() {
        let mut buf = Vec::new();
        msgop_to_buf(0, &mut buf);

        let msg = MsgOpMsg::from_reader(&buf[..], false, false).await.unwrap();

        assert_eq!(0, msg.flag_bits);
        assert_eq!(3, msg.documents.len());
        assert_eq!(0, msg.section_bytes.len());
        assert_eq!("x0", msg.documents[0].get_str("op").unwrap());
        assert_eq!("y0", msg.documents[1].get_str("op").unwrap());
        assert_eq!("z0", msg.documents[2].get_str("op").unwrap());
    }

    #[tokio::test]
    async fn test_parse_op_msg_raw() {
        let buf = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x00, 0x02, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65,
            0x00, 0x08, 0x00, 0x00, 0x00, 0x6b, 0x69, 0x74, 0x74, 0x65, 0x6e, 0x73, 0x00, 0x08, 0x6f, 0x72,
            0x64, 0x65, 0x72, 0x65, 0x64, 0x00, 0x01, 0x03, 0x6c, 0x73, 0x69, 0x64, 0x00, 0x1e, 0x00, 0x00,
            0x00, 0x05, 0x69, 0x64, 0x00, 0x10, 0x00, 0x00, 0x00, 0x04, 0x9a, 0xe6, 0x20, 0x9f, 0x8b, 0xdf,
            0x4d, 0x1a, 0x9c, 0xd9, 0xe8, 0x61, 0x37, 0xf9, 0x8f, 0x37, 0x00, 0x02, 0x24, 0x64, 0x62, 0x00,
            0x05, 0x00, 0x00, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x03, 0x24, 0x72, 0x65, 0x61, 0x64, 0x50,
            0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x00, 0x17, 0x00, 0x00, 0x00, 0x02, 0x6d,
            0x6f, 0x64, 0x65, 0x00, 0x08, 0x00, 0x00, 0x00, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79, 0x00,
            0x00, 0x00, 0x01, 0x75, 0x00, 0x00, 0x00, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x73, 0x00, 0x69,
            0x00, 0x00, 0x00, 0x03, 0x71, 0x00, 0x56, 0x00, 0x00, 0x00, 0x02, 0x24, 0x63, 0x6f, 0x6d, 0x6d,
            0x65, 0x6e, 0x74, 0x00, 0x43, 0x00, 0x00, 0x00, 0x75, 0x62, 0x65, 0x72, 0x2d, 0x74, 0x72, 0x61,
            0x63, 0x65, 0x2d, 0x69, 0x64, 0x3a, 0x61, 0x31, 0x36, 0x36, 0x33, 0x35, 0x65, 0x61, 0x38, 0x63,
            0x39, 0x62, 0x35, 0x33, 0x34, 0x39, 0x3a, 0x36, 0x31, 0x36, 0x66, 0x31, 0x33, 0x37, 0x62, 0x33,
            0x66, 0x39, 0x35, 0x37, 0x38, 0x36, 0x65, 0x3a, 0x31, 0x37, 0x33, 0x31, 0x36, 0x35, 0x31, 0x32,
            0x38, 0x34, 0x65, 0x39, 0x38, 0x37, 0x35, 0x66, 0x3a, 0x31, 0x00, 0x00, 0x10, 0x6c, 0x69, 0x6d,
            0x69, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let msg = MsgOpMsg::from_reader(&buf[..], true, true).await.unwrap();

        assert_eq!(0, msg.flag_bits);
        assert_eq!(2, msg.documents.len());
        assert_eq!(1, msg.section_bytes.len());
        assert_eq!(0x69, msg.section_bytes[0].len());
    }

    #[tokio::test]
    async fn test_parse_op_query() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(0i32).unwrap();   // flag bits
        buf.write(b"tribbles\0").unwrap();
        buf.write_i32::<LittleEndian>(9i32).unwrap();           // num to skip
        buf.write_i32::<LittleEndian>(6i32).unwrap();           // num to return
        let doc = doc! { "a": 1, "q": { "$comment": "ok" } };   // query text
        doc.to_writer(&mut buf).unwrap();

        let msg = MsgOpQuery::from_reader(&buf[..], false).await.unwrap();
        assert_eq!(0, msg.flags);
        assert_eq!("tribbles", msg.full_collection_name);
        assert_eq!(9, msg.number_to_skip);
        assert_eq!(6, msg.number_to_return);
        assert_eq!("a", msg.query.get_str("op").unwrap());
        assert_eq!("ok", msg.query.get_str("comment").unwrap());
    }

    #[tokio::test]
    async fn test_parse_op_getmore() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(0).unwrap();      // zero
        buf.write(b"tribbles\0").unwrap();              // collection name
        buf.write_i32::<LittleEndian>(10).unwrap();     // number to return
        buf.write_i64::<LittleEndian>(123456).unwrap(); // cursor id

        let msg = MsgOpGetMore::from_reader(&buf[..]).await.unwrap();
        assert_eq!("tribbles", msg.full_collection_name);
        assert_eq!(10, msg.number_to_return);
        assert_eq!(123456, msg.cursor_id);
    }

    #[tokio::test]
    async fn test_parse_op_update() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(0).unwrap();      // zero
        buf.write(b"tribbles\0").unwrap();              // collection name
        buf.write_i32::<LittleEndian>(123).unwrap();    // flags
        let doc = doc! { "a": 1 };                      // selector
        doc.to_writer(&mut buf).unwrap();
        let doc = doc! { "b": 2 };                      // update
        doc.to_writer(&mut buf).unwrap();

        let msg = MsgOpUpdate::from_reader(&buf[..]).await.unwrap();
        assert_eq!("tribbles", msg.full_collection_name);
        assert_eq!(123, msg.flags);
        assert_eq!("a", msg.selector.get_str("op").unwrap());
        assert_eq!("b", msg.update.get_str("op").unwrap());
    }

    #[tokio::test]
    async fn test_parse_op_delete() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(0).unwrap();      // zero
        buf.write(b"tribbles\0").unwrap();              // collection name
        buf.write_i32::<LittleEndian>(123).unwrap();    // flags
        let doc = doc! { "a": 1 };                      // selector
        doc.to_writer(&mut buf).unwrap();

        let msg = MsgOpDelete::from_reader(&buf[..]).await.unwrap();
        assert_eq!("tribbles", msg.full_collection_name);
        assert_eq!(123, msg.flags);
        assert_eq!("a", msg.selector.get_str("op").unwrap());
    }

    #[tokio::test]
    async fn test_parse_op_insert() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(123).unwrap();    // flags
        buf.write(b"tribbles\0").unwrap();              // collection name

        let msg = MsgOpInsert::from_reader(&buf[..]).await.unwrap();
        assert_eq!(123, msg.flags);
        assert_eq!("tribbles", msg.full_collection_name);
    }

    #[tokio::test]
    async fn test_parse_op_reply() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(123).unwrap();    // flags
        buf.write_i64::<LittleEndian>(123456).unwrap(); // cursor id
        buf.write_i32::<LittleEndian>(555).unwrap();    // starting from
        buf.write_i32::<LittleEndian>(666).unwrap();    // number returned

        for (i, v) in vec!["a", "b", "c"].iter().enumerate() {
            let doc = doc! { v.to_string(): i as i32 };
            doc.to_writer(&mut buf).unwrap();
        }

        let msg = MsgOpReply::from_reader(&buf[..], false).await.unwrap();
        assert_eq!(123, msg.flags);
        assert_eq!(123456, msg.cursor_id);
        assert_eq!(555, msg.starting_from);
        assert_eq!(666, msg.number_returned);
        assert_eq!(3, msg.documents.len());
    }

    // Test parsing multiple back to back messages
    #[tokio::test]
    async fn test_parse_multiple_message() {
        let mut buf = Vec::new();

        for i in 0..3u32 {
            let mut msg_buf = Vec::new();
            msgop_to_buf(i, &mut msg_buf);

            let hdr = MsgHeader {
                message_length: HEADER_LENGTH + msg_buf.len(),
                request_id: i+1,
                response_to: i,
                op_code: 2013,
            };

            hdr.write(&mut buf).unwrap();
            buf.extend(&msg_buf);
        }

        let mut cur = std::io::Cursor::new(buf);
        let mut messages = Vec::new();
        while let Ok((_, msg)) = MongoMessage::from_reader(&mut cur, false, false).await {
            messages.push(msg);
        }

        assert_eq!(3, messages.len());
        for (i, msg) in messages.iter().enumerate() {
            match msg {
                MongoMessage::Msg(op_msg) => {
                    assert_eq!(format!("x{}", i), op_msg.documents[0].get_str("op").unwrap());
                },
                _ => {
                    panic!("expecting MsgOpMsg");
                },
            }
        }
    }
}
