use super::messages::{self,MsgHeader,MsgOpMsg,MsgOpQuery,MsgOpReply,MsgOpUpdate,MsgOpDelete,MsgOpInsert,MongoMessage,OpCode};
use std::io::{self,Read};
use std::{thread};
use log::{debug,warn,error};
use prometheus::{CounterVec,GaugeVec};


lazy_static! {
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

    static ref HEADER_PARSE_ERRORS_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_header_parse_error_count_total",
            "Header parse errors",
            &["error"]).unwrap();

    static ref MESSAGE_PARSE_ERRORS_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_message_parse_error_count_total",
            "Message body parse errors",
            &["error"]).unwrap();

    static ref THREAD_MESSAGE_BUF_SIZE: GaugeVec =
        register_gauge_vec!(
            "mongoproxy_parser_buf_size",
            "Current size of the parser buffer",
            &["thread"]).unwrap();
}

pub struct MongoProtocolParser {
    pub header:     messages::MsgHeader,
    have_header:    bool,
    want_bytes:     usize,
    message_buf:    Vec<u8>,
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
        }
    }

    // Parse the buffer and return the parsed objects.
    //
    // parse_buffer expects that it is being fed chunks of the incoming stream. It tries to
    // assemble MongoDb messages and returns the parsed messages.
    //
    // Since MongoDb may send multiple messages in one go, we need to try and consume all the
    // messages from the parser buffer. Otherwise we might leave some unparsed messages in the
    // buffer and mess up the response/request sequence.
    //
    // The first message we always want to see is the MongoDb message header.  This header in turn
    // contains the length of the message that follows. So we try to read message length worth of
    // bytes and parse the message. Once the message is parsed we expect a header again and the
    // process repeats.
    //
    pub fn parse_buffer(&mut self, buf: &[u8]) -> Vec<MongoMessage> {
        if !self.parser_active {
            return vec![];
        }

        self.message_buf.extend(buf);
        THREAD_MESSAGE_BUF_SIZE
            .with_label_values(&[&format!("{:?}", thread::current().id())])
            .set(self.message_buf.len() as f64);

        let mut result = Vec::new();
        let mut loop_counter = 0;

        while self.want_bytes > 0 && self.message_buf.len() >= self.want_bytes {
            // Since we entered the loop we have either a header or a message body.
            // Make a note of the next packet starts and consume the bytes.
            let next_buffer = &self.message_buf[self.want_bytes..];

            if !self.have_header {
                match MsgHeader::from_reader(&self.message_buf[..self.want_bytes]) {
                    Ok(header) => {
                        assert!(header.message_length >= messages::HEADER_LENGTH);
                        self.header = header;
                        self.have_header = true;
                        self.want_bytes = self.header.message_length - messages::HEADER_LENGTH;
                        debug!("parser: have header {:?}, want {} more bytes", self.header, self.want_bytes);
                    },
                    Err(e) => {
                        error!("Failed to read a header, stopping: {}", e);
                        self.parser_active = false;
                        HEADER_PARSE_ERRORS_COUNTER.with_label_values(&[&e.to_string()]).inc();
                        break;
                    },
                }
            } else {
                match extract_message(self.header.op_code, &self.message_buf[..self.want_bytes]) {
                    Ok(res) => {
                        result.push(res);
                    },
                    Err(e) => {
                        error!("Error extracting message, stopping: {}", e);
                        self.parser_active = false;
                        MESSAGE_PARSE_ERRORS_COUNTER.with_label_values(&[&e.to_string()]).inc();
                        break;
                    }
                }
                // We got the payload, time to ask for a header again
                self.have_header = false;
                self.want_bytes = messages::HEADER_LENGTH;
            }

            // Advance the message buf to the unprocessed bytes
            self.message_buf = next_buffer.to_vec();
            debug!("loop {}: {} bytes in buffer, want {}",
                   loop_counter, self.message_buf.len(), self.want_bytes);
            loop_counter += 1;
        }

        result
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
    Ok(MongoMessage::None)
}

#[cfg(test)]
mod tests {

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn create_header(request_id: u32, response_to: u32, msg_buf: &Vec<u8>) -> MsgHeader {
        MsgHeader {
            message_length: messages::HEADER_LENGTH + msg_buf.len(),
            request_id: request_id,
            response_to: response_to,
            op_code: 2013,
        }
    }

    fn create_message(key: &str, val: &str) -> Vec<u8> {
        let mut msg = MsgOpMsg{ flag_bits: 0, documents: Vec::new() };
        let mut doc = bson::Document::new();
        doc.insert(key.to_owned(), bson::Bson::String(val.to_owned()));
        msg.documents.push(doc);

        let mut doc_buf = Vec::new();
        msg.write(&mut doc_buf).unwrap();
        doc_buf
    }

    #[test]
    fn test_parse_buffer_header() {
        init();

        let hdr = create_header(1234, 5678, &vec![]);
        let mut buf = [0 as u8; messages::HEADER_LENGTH];
        hdr.write(&mut buf[..]).unwrap();

        let mut parser = MongoProtocolParser::new();
        let result = parser.parse_buffer(&buf.to_vec());

        assert_eq!(result.len(), 0);
        assert_eq!(parser.have_header, true);
        assert_eq!(parser.want_bytes, 0);
    }

    #[test]
    fn test_parse_msg() {
        init();

        let msg_buf = create_message("insert", "foo");
        let hdr = create_header(1234, 5678, &msg_buf);
        let mut buf = Vec::new();

        hdr.write(&mut buf).unwrap();
        buf.extend(msg_buf);

        let mut parser = MongoProtocolParser::new();
        let result  = parser.parse_buffer(&buf);
        assert_eq!(result.len(), 1);

        match result.iter().next().unwrap() {
            MongoMessage::Msg(m) => {
                assert_eq!(m.documents.len(), 1);
                let doc = &m.documents[0];
                assert_eq!(doc.get_str("insert").unwrap(), "foo");
            },
            other => panic!("Instead of MsgOpMsg, got this: {:?}", other),
        }

        assert_eq!(parser.have_header, false);
        assert_eq!(parser.parser_active, true);
        assert_eq!(parser.want_bytes, messages::HEADER_LENGTH);
    }

    #[test]
    fn test_parse_partial_msg_sequence() {
        init();

        let mut parser = MongoProtocolParser::new();
        let mut buf = Vec::new();

        let first_msg_buf = create_message("insert", "foo");
        let hdr = create_header(1234, 5678, &first_msg_buf);

        // Write the header of the first message and try parse. This must parse
        // the header but return nothing because it doesn't have a message body yet.
        hdr.write(&mut buf).unwrap();
        let result = parser.parse_buffer(&buf);
        if result.len() == 0 {
            assert_eq!(parser.have_header, true);
            assert_eq!(parser.header.request_id, 1234);
            assert_eq!(parser.header.response_to, 5678);
        } else {
            panic!("wasn't expecting to parse anything but a header: {:?}", result);
        }

        // Now "write" the remainder of the first message
        buf = first_msg_buf.to_vec();

        // And construct and write the header of the second message. NB! We don't write
        // the second message body just yet, because we want to verify that the parser
        // doesn't get confused.

        let second_msg_buf = create_message("delete", "bar");
        let hdr = create_header(5678, 1234, &second_msg_buf);
        hdr.write(&mut buf).unwrap();

        // Now the parser must return the parsed first message. It also should have
        // started to parse the bytes for the header of the second message.
        match parser.parse_buffer(&buf).iter().next().unwrap() {
            MongoMessage::Msg(m) => {
                assert_eq!(m.documents.len(), 1);
                let doc = &m.documents[0];
                assert_eq!(doc.get_str("insert").unwrap(), "foo");
            },
            other => panic!("Couldn't parse the first message, got something else: {:?}", other),
        }

        // Now, the next call with empty buffer must parse the second message header
        // but not return the message itself.
        match parser.parse_buffer(&[]).is_empty() {
            true => {
                assert_eq!(parser.have_header, true);
                assert_eq!(parser.header.request_id, 5678);
                assert_eq!(parser.header.response_to, 1234);
            }
            false => panic!("Expected nothing, got something"),
        }

        // Finally write the seconds message body and expect to parse the full message.
        // Also check that the header matches the second message.
        buf = second_msg_buf.to_vec();
        match parser.parse_buffer(&buf).iter().next().unwrap() {
            MongoMessage::Msg(m) => {
                assert_eq!(m.documents.len(), 1);
                let doc = &m.documents[0];
                assert_eq!(doc.get_str("delete").unwrap(), "bar");
            },
            other => panic!("Instead of MsgOpMsg, got this: {:?}", other),
        }

        assert_eq!(parser.have_header, false);
        assert_eq!(parser.header.request_id, 5678);
        assert_eq!(parser.header.response_to, 1234);
        assert_eq!(parser.parser_active, true);
        assert_eq!(parser.want_bytes, messages::HEADER_LENGTH);
    }

    #[test]
    fn test_parse_complete_msg_sequence() {
        init();

        let mut parser = MongoProtocolParser::new();
        let mut buf = Vec::new();

        // Write the first message
        let msg_buf = create_message("insert", "foo");
        let hdr = create_header(1234, 5678, &msg_buf);
        hdr.write(&mut buf).unwrap();
        buf.extend(&msg_buf);

        // Now write the second message
        let msg_buf = create_message("delete", "bar");
        let hdr = create_header(5678, 1234, &msg_buf);
        hdr.write(&mut buf).unwrap();
        buf.extend(&msg_buf);

        // Parse and validate the messages
        let result = parser.parse_buffer(&buf);
        assert_eq!(result.len(), 2);

        let mut it = result.iter();

        match it.next().unwrap() {
            MongoMessage::Msg(m) => {
                assert_eq!(m.documents.len(), 1);
                let doc = &m.documents[0];
                assert_eq!(doc.get_str("insert").unwrap(), "foo");
            },
            other => panic!("Couldn't parse the first message, got something else: {:?}", other),
        }

        // Now, parse and validate the second message.
        match it.next().unwrap() {
            MongoMessage::Msg(m) => {
                assert_eq!(m.documents.len(), 1);
                let doc = &m.documents[0];
                assert_eq!(doc.get_str("delete").unwrap(), "bar");
            },
            other => panic!("Couldn't parse the second message, got something else: {:?}", other),
        }
    }

}
