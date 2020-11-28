use std::error::Error;
use std::sync::Once;

use mongodb::{MsgHeader, MongoMessage};
use tracing::{trace};


#[derive(Default)]
pub struct MongoProtocolDecoder {
    header:             Option<mongodb::MsgHeader>,
    want_bytes:         usize,
    message_buf:        Vec<u8>,
    message_buf_pos:    usize,
}

impl MongoProtocolDecoder {

    pub fn new() -> MongoProtocolDecoder {
        MongoProtocolDecoder {
            header: None,
            want_bytes: mongodb::HEADER_LENGTH,
            message_buf: Vec::new(),
            message_buf_pos: 0,
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
    // The first message we always want to see is the MongoDb message header. This header in turn
    // contains the length of the message that follows. So we try to read message length worth of
    // bytes and parse the message. Once the message is parsed we expect a header again and the
    // process repeats.
    //
    pub fn parse_buffer(&mut self, buf: &[u8]) -> Result<Vec<(MsgHeader, MongoMessage)>, Box<dyn Error>> {
        self.message_buf.extend(buf);

        let mut result = Vec::new();
        let mut work_buf = &self.message_buf[self.message_buf_pos..];

        while self.want_bytes > 0 && work_buf.len() >= self.want_bytes {
            trace!("want {} bytes, have {} in buffer.", self.want_bytes, work_buf.len());

            // Since we entered the loop we have either a header or a message body.
            // Make a note of the next packet starts and consume the bytes.
            self.message_buf_pos += self.want_bytes;

            if self.header.is_none() {
                let hdr = MsgHeader::from_reader(&work_buf[..self.want_bytes])?;
                assert!(hdr.message_length >= mongodb::HEADER_LENGTH);
                trace!("obtained header: {:?}", hdr);

                self.want_bytes = hdr.message_length - mongodb::HEADER_LENGTH;
                self.header = Some(hdr);
            } else {
                let hdr = self.header.take().unwrap();
                let msg = MongoMessage::extract_message(
                    hdr.op_code,
                    &work_buf[..self.want_bytes],
                    false,
                    false,
                    (hdr.message_length - mongodb::HEADER_LENGTH) as u64)?;
                trace!("obtained message: {:?}", msg);
                result.push((hdr, msg));

                // We got the payload, time to ask for a header again
                self.want_bytes = mongodb::HEADER_LENGTH;
            }

            // Advance the message buf to the unprocessed bytes
            work_buf = &self.message_buf[self.message_buf_pos..];
        }

        if work_buf.is_empty() {
            trace!("want more data");
            self.message_buf_pos = 0;
            self.message_buf.clear();
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use bson::doc;
    use mongodb::{MsgOpMsg};
    use tracing_subscriber::{FmtSubscriber, EnvFilter};

    static INIT: Once = Once::new();

    fn init_tracing() {
        INIT.call_once(|| {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(tracing::Level::TRACE)
                .with_env_filter(EnvFilter::from_default_env())
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        });
    }

    fn create_header(request_id: u32, response_to: u32, len: usize) -> MsgHeader {
        MsgHeader {
            message_length: len + mongodb::HEADER_LENGTH,
            request_id,
            response_to,
            op_code: 2013,
        }
    }

    fn create_message(key: &str, val: &str) -> Vec<u8> {
        let msg = MsgOpMsg{ flag_bits: 0, documents: Vec::new(), section_bytes: Vec::new() };

        let doc = doc! {
            key.to_owned(): val.to_owned(),
        };

        let mut doc_buf = Vec::new();
        doc.to_writer(&mut doc_buf).unwrap();

        let mut buf = Vec::new();
        msg.write(&mut buf, &doc_buf).unwrap();
        buf
    }

    #[test]
    fn test_parse_buffer_header() {
        init_tracing();

        let hdr = create_header(1234, 5678, 0);
        let mut buf = [0 as u8; mongodb::HEADER_LENGTH];
        hdr.write(&mut buf[..]).unwrap();

        let mut parser = MongoProtocolDecoder::new();
        let result = parser.parse_buffer(&buf.to_vec()).unwrap();

        assert_eq!(result.len(), 0);
        assert!(parser.header.is_some());
        assert_eq!(parser.want_bytes, 0);
    }

    #[test]
    fn test_parse_msg() {
        init_tracing();

        let msg_buf = create_message("insert", "foo");
        let hdr = create_header(1234, 5678, msg_buf.len());
        let mut buf = Vec::new();

        hdr.write(&mut buf).unwrap();
        buf.extend(msg_buf);

        let mut parser = MongoProtocolDecoder::new();
        let result = parser.parse_buffer(&buf).unwrap();
        assert_eq!(result.len(), 1);

        match result.iter().next().unwrap() {
            (h, MongoMessage::Msg(m)) => {
                assert_eq!(m.documents.len(), 1);
                assert_eq!(h.request_id, 1234);
                assert_eq!(h.response_to, 5678);
                assert_eq!(m.documents[0].get_str("op").unwrap(), "insert");
                assert_eq!(m.documents[0].get_str("op_value").unwrap(), "foo");
            },
            other => panic!("Instead of MsgOpMsg, got this: {:?}", other),
        }

        assert!(parser.header.is_none());
        assert_eq!(parser.want_bytes, mongodb::HEADER_LENGTH);
    }

    #[test]
    fn test_parse_partial_msg_sequence() {
        init_tracing();

        let mut parser = MongoProtocolDecoder::new();
        let mut buf = Vec::new();

        let first_msg_buf = create_message("insert", "foo");
        let hdr = create_header(1234, 5678, first_msg_buf.len());

        // Write the header of the first message and try parse. This must parse
        // the header but return nothing because it doesn't have a message body yet.
        hdr.write(&mut buf).unwrap();
        let result = parser.parse_buffer(&buf).unwrap();
        if result.len() == 0 {
            let hdr = parser.header.as_ref().unwrap();
            assert_eq!(hdr.request_id, 1234);
            assert_eq!(hdr.response_to, 5678);
        } else {
            panic!("wasn't expecting to parse anything but a header: {:?}", result);
        }

        // Now "write" the remainder of the first message
        buf = first_msg_buf.to_vec();

        // And construct and write the header of the second message. NB! We don't write
        // the second message body just yet, because we want to verify that the parser
        // doesn't get confused.

        let second_msg_buf = create_message("delete", "bar");
        let hdr = create_header(5678, 1234, second_msg_buf.len());
        hdr.write(&mut buf).unwrap();

        // Now the parser must return the parsed first message. It also should have
        // started to parse the bytes for the header of the second message.
        match parser.parse_buffer(&buf).unwrap().iter().next().unwrap() {
            (_, MongoMessage::Msg(m)) => {
                assert_eq!(m.documents.len(), 1);
                assert_eq!(m.documents[0].get_str("op").unwrap(), "insert");
                assert_eq!(m.documents[0].get_str("op_value").unwrap(), "foo");
            },
            other => panic!("Couldn't parse the first message, got something else: {:?}", other),
        }

        // Now, the next call with empty buffer must parse the second message header
        // but not return the message itself.
        match parser.parse_buffer(&[]).unwrap().is_empty() {
            true => {
                assert!(parser.header.is_some());
                let hdr = parser.header.as_ref().unwrap();
                assert_eq!(hdr.request_id, 5678);
                assert_eq!(hdr.response_to, 1234);
            }
            false => panic!("Expected nothing, got something"),
        }

        // Finally write the seconds message body and expect to parse the full message.
        // Also check that the header matches the second message.
        buf = second_msg_buf.to_vec();
        match parser.parse_buffer(&buf).unwrap().iter().next().unwrap() {
            (h, MongoMessage::Msg(m)) => {
                assert_eq!(m.documents.len(), 1);
                assert_eq!(h.request_id, 5678);
                assert_eq!(h.response_to, 1234);
                assert_eq!(m.documents[0].get_str("op").unwrap(), "delete");
                assert_eq!(m.documents[0].get_str("op_value").unwrap(), "bar");
            },
            other => panic!("Instead of MsgOpMsg, got this: {:?}", other),
        }

        assert!(parser.header.is_none());
        assert_eq!(parser.want_bytes, mongodb::HEADER_LENGTH);
    }

    #[test]
    fn test_parse_complete_msg_sequence() {
        init_tracing();

        let mut parser = MongoProtocolDecoder::new();
        let mut buf = Vec::new();

        // Write the first message
        let msg_buf = create_message("insert", "foo");
        let hdr = create_header(1234, 5678, msg_buf.len());
        hdr.write(&mut buf).unwrap();
        buf.extend(&msg_buf);

        // Now write the second message
        let msg_buf = create_message("delete", "bar");
        let hdr = create_header(5678, 1234, msg_buf.len());
        hdr.write(&mut buf).unwrap();
        buf.extend(&msg_buf);

        // Parse and validate the messages
        let result = parser.parse_buffer(&buf).unwrap();
        assert_eq!(result.len(), 2);

        let mut it = result.iter();

        match it.next().unwrap() {
            (_, MongoMessage::Msg(m)) => {
                assert_eq!(m.documents.len(), 1);
                assert_eq!(m.documents[0].get_str("op").unwrap(), "insert");
                assert_eq!(m.documents[0].get_str("op_value").unwrap(), "foo");
            },
            other => panic!("Couldn't parse the first message, got something else: {:?}", other),
        }

        // Now, parse and validate the second message.
        match it.next().unwrap() {
            (_, MongoMessage::Msg(m)) => {
                assert_eq!(m.documents.len(), 1);
                assert_eq!(m.documents[0].get_str("op").unwrap(), "delete");
                assert_eq!(m.documents[0].get_str("op_value").unwrap(), "bar");
            },
            other => panic!("Couldn't parse the second message, got something else: {:?}", other),
        }
    }

}
