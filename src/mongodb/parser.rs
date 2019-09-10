use super::messages::{self,MsgHeader,MsgOpMsg,MsgOpQuery,MsgOpReply,MsgOpUpdate,MsgOpDelete,MsgOpInsert,MongoMessage,OpCode};
use std::io::{self,Read};
use log::{debug,warn,error};
use prometheus::{CounterVec};


lazy_static! {
    static ref OPCODE_COUNTER: CounterVec =
        register_counter_vec!(
            "opcode_count_total",
            "Number of different opcodes encountered",
            &["op_code"]).unwrap();

    static ref UNSUPPORTED_OPCODE_COUNTER: CounterVec =
        register_counter_vec!(
            "unsupported_op_code_count_total",
            "Number of unrecognized opcodes in MongoDb header",
            &["op_code"]).unwrap();

    static ref HEADER_PARSE_ERRORS_COUNTER: CounterVec =
        register_counter_vec!(
            "header_parse_error_count_total",
            "Header parse errors",
            &["error"]).unwrap();

}

pub struct MongoProtocolParser {
    pub header:     messages::MsgHeader,
    have_header:    bool,
    want_bytes:     usize,
    message_buf:    Vec<u8>,
    have_message:   bool,
    parser_active:  bool,
}

impl MongoProtocolParser {

    pub fn new() -> MongoProtocolParser {
        MongoProtocolParser{
            header: MsgHeader::new(),
            have_header: false,
            want_bytes: messages::HEADER_LENGTH,
            message_buf: Vec::new(),
            parser_active: true,
            have_message: false,
        }
    }

    // Parse the buffer and return the parsed object when ready.
    //
    // parse_buffer expects that it is being fed chunks of the incoming
    // stream. It tries to assemble MongoDb messages and returns the parsed
    // message.
    //
    // The first message we always want to see is the MongoDb message header.
    // This header in turn contains the length of the message that follows. So
    // we try to read message length worth of bytes and parse the message. Once
    // the message is parsed we expect a header again and the process repeats.
    //
    pub fn parse_buffer(&mut self, buf: &Vec<u8>) -> Option<MongoMessage> {
        if !self.parser_active {
            return None;
        }

        self.message_buf.extend(buf);

        let mut result = MongoMessage::None;
        let mut loop_counter = 0;
        while self.want_bytes > 0 && self.message_buf.len() >= self.want_bytes && loop_counter < 2 {
            // Make a note of how many bytes we got as we're going to
            // overwrite it later.
            let new_buffer_start = self.want_bytes;

            if !self.have_header {
                match MsgHeader::from_reader(&self.message_buf[..self.want_bytes]) {
                    Ok(header) => {
                        assert!(header.message_length >= messages::HEADER_LENGTH);
                        self.header = header;
                        self.have_header = true;
                        self.have_message = false;
                        self.want_bytes = self.header.message_length - messages::HEADER_LENGTH;
                        debug!("parser: have header {:?}, want {} more bytes", self.header, self.want_bytes);
                    },
                    Err(e) => {
                        warn!("parser: failed to read a header, stopping: {}", e);
                        self.parser_active = false;
                        HEADER_PARSE_ERRORS_COUNTER.with_label_values(&[&e.to_string()]).inc();
                    },
                }
            } else {
                match extract_message(self.header.op_code, &self.message_buf[..self.want_bytes]) {
                    Ok(res) => {
                        result = res;
                    },
                    Err(e) => {
                        error!("Error extracting message: {}", e);
                        self.parser_active = false;
                        return None;
                    }
                }
                // We got the payload, time to ask for a header again
                self.have_header = false;
                self.have_message = true;
                self.want_bytes = messages::HEADER_LENGTH;
            }

            // Point the message_buf to the bytes that we haven't yet processed
            // And don't worry about performance, yet
            self.message_buf = self.message_buf[new_buffer_start..].to_vec();
            debug!("loop {}: {} bytes in buffer, want {}", loop_counter, self.message_buf.len(), self.want_bytes);
            loop_counter += 1;
        }

        if self.message_buf.len() >= self.want_bytes {
            // Warn if we could've immediately returned another message
            warn!("parser: {} surplus bytes in buf and I want {}.",
                self.message_buf.len(), self.want_bytes);
        }

        debug!("result={:?}", result);
        Some(result)
    }
}

fn extract_message(op_code: u32, mut rdr: impl Read) -> io::Result<MongoMessage> {
    OPCODE_COUNTER.with_label_values(&[&op_code.to_string()]).inc();

    match num_traits::FromPrimitive::from_u32(op_code) {
        Some(OpCode::OpReply) => {
            return Ok(MongoMessage::Reply(MsgOpReply::from_reader(&mut rdr)?));
        }
        Some(OpCode::OpQuery) => {
            return Ok(MongoMessage::Query(MsgOpQuery::from_reader(&mut rdr)?));
        },
        Some(OpCode::OpUpdate) => {
            return Ok(MongoMessage::Update(MsgOpUpdate::from_reader(&mut rdr)?));
        },
        Some(OpCode::OpDelete) => {
            return Ok(MongoMessage::Delete(MsgOpDelete::from_reader(&mut rdr)?));
        },
        Some(OpCode::OpInsert) => {
            return Ok(MongoMessage::Insert(MsgOpInsert::from_reader(&mut rdr)?));
        },
        Some(OpCode::OpMsg) => {
            return Ok(MongoMessage::Msg(MsgOpMsg::from_reader(&mut rdr)?));
        },
        Some(OpCode::OpPing) => {},
        Some(OpCode::OpPong) => {},
        None => {
            UNSUPPORTED_OPCODE_COUNTER.with_label_values(&[&op_code.to_string()]).inc();
            warn!("Unhandled OP: {}", op_code);
        },
    }
    return Ok(MongoMessage::None);
}

#[cfg(test)]
mod tests {

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_parse_buffer_header() {
        init();

        let hdr = MsgHeader {
            message_length: messages::HEADER_LENGTH,
            request_id: 1234,
            response_to: 5678,
            op_code: 1,
        };

        let mut buf = [0 as u8; messages::HEADER_LENGTH];
        hdr.write(&mut buf[..]).unwrap();

        let mut parser = MongoProtocolParser::new();
        let _result = parser.parse_buffer(&buf.to_vec());

        assert_eq!(parser.have_header, true);
        assert_eq!(parser.have_message, false);
        assert_eq!(parser.want_bytes, 0);
    }

    #[test]
    fn test_parse_msg() {
        init();

        let mut msg = MsgOpMsg{ flag_bits: 0, sections: Vec::new() };

        // Craft a MongoDb OP_MSG. Something like: { insert: "kala", $db: "test" }
        let mut doc = bson::Document::new();
        doc.insert("insert".to_owned(), bson::Bson::String("foo".to_owned()));
        msg.sections.push(doc);

        // Write the document to get the encoded length
        let mut msg_buf = Vec::new();
        msg.write(&mut msg_buf).unwrap();

        // Now that we know the message lengthg, consruct the header and write the
        // whole message out.
        let mut buf = Vec::new();
        let hdr = MsgHeader {
            message_length: messages::HEADER_LENGTH + msg_buf.len(),
            request_id: 1234,
            response_to: 5678,
            op_code: 2013,
        };

        hdr.write(&mut buf).unwrap();
        buf.extend(msg_buf);

        let mut parser = MongoProtocolParser::new();
        match parser.parse_buffer(&buf) {
            Some(MongoMessage::Msg(m)) => {
                assert_eq!(m.sections.len(), 1);
                let doc = &m.sections[0];
                assert_eq!(doc.get_str("insert").unwrap(), "foo");
            },
            other => panic!("Instead of MsgOpMsg, got this: {:?}", other),
        }

        assert_eq!(parser.have_header, false);
        assert_eq!(parser.have_message, true);
        assert_eq!(parser.parser_active, true);
        assert_eq!(parser.want_bytes, messages::HEADER_LENGTH);
    }
}