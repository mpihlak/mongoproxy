use super::messages::{self,MsgHeader,MsgOpMsg,MsgOpQuery,MongoMessage};
use std::io::Read;
use log::{debug,warn};


pub struct MongoProtocolParser {
    header: messages::MsgHeader,
    have_header: bool,
    want_bytes: usize,
    message_buf: Vec<u8>,
    parser_active: bool,
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
    pub fn parse_buffer(&mut self, buf: &Vec<u8>) -> MongoMessage {
        if !self.parser_active {
            return MongoMessage::None;
        }

        self.message_buf.extend(buf);

        let mut result = MongoMessage::None;

        if self.message_buf.len() >= self.want_bytes {
            // Make a note of how many bytes we got as we're going to
            // overwrite it later.
            let new_buffer_start = self.want_bytes;

            if !self.have_header {
                match MsgHeader::from_reader(&self.message_buf[..]) {
                    Ok(header) => {
                        assert!(header.message_length >= messages::HEADER_LENGTH);
                        self.header = header;
                        self.have_header = true;
                        self.want_bytes = self.header.message_length - messages::HEADER_LENGTH;
                        debug!("parser: have header {:?}, want {} more bytes", self.header, self.want_bytes);
                    },
                    Err(e) => {
                        warn!("parser: failed to read a header, stopping: {}", e);
                        self.parser_active = false;
                    },
                }
            } else {
                result = get_message_from_reader(self.header.op_code, &self.message_buf[..]);

                // We got the payload, time to ask for a header again
                self.have_header = false;
                self.want_bytes = messages::HEADER_LENGTH;
            }

            // Point the message_buf to the bytes that we haven't yet processed
            // And don't worry about performance, yet
            self.message_buf = self.message_buf[new_buffer_start..].to_vec();
            debug!("message_buf capacity={}", self.message_buf.capacity());

            return result;
        }

        messages::MongoMessage::None
    }
}

fn get_message_from_reader(op_code: u32, mut rdr: impl Read) -> MongoMessage {
    match op_code {
        2004 => {
            let op = MsgOpQuery::from_reader(&mut rdr).unwrap();
            return MongoMessage::Query(op);
        },
        2013 => {
            let op = MsgOpMsg::from_reader(&mut rdr).unwrap();
            return MongoMessage::Msg(op);
        },
        op_code => {
            warn!("Unhandled OP: {}", op_code);
        },
    }
    return MongoMessage::None;
}