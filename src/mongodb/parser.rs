use super::*;

use log::{debug,info,warn};


pub struct MongoProtocolParser {
    header: messages::MsgHeader,
    have_header: bool,
    want_bytes: usize,      // How many more bytes do we need for a complete message
    message_buf: Vec<u8>,   // Accumulated message bytes, parseable when we have all want_bytes
}

impl MongoProtocolParser {

    pub fn new() -> MongoProtocolParser {
        MongoProtocolParser{
            header: messages::MsgHeader::new(),
            have_header: false,
            want_bytes: messages::HEADER_LENGTH,
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
                match messages::MsgHeader::from_reader(&self.message_buf[..]) {
                    Ok(header) => {
                        debug!("parse: got a header: {:?}", header);
                        assert!(header.message_length >= messages::HEADER_LENGTH);

                        self.header = header;
                        self.have_header = true;
                        self.want_bytes = self.header.message_length - messages::HEADER_LENGTH;
                        debug!("want {} more bytes", self.want_bytes);
                    },
                    Err(e) => {
                        warn!("parse: failed to read a header: {}", e);
                    },
                }
            } else {
                debug!("processing payload {} bytes", self.header.message_length - messages::HEADER_LENGTH);

                match self.header.op_code {
                    2004 => {
                        let op = messages::MsgOpQuery::from_reader(&self.message_buf[..]);
                        info!("OP_QUERY: {:?}", op);
                    },
                    2013 => {
                        let op = messages::MsgOpMsg::from_reader(&self.message_buf[..]);
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
                self.want_bytes = messages::HEADER_LENGTH;
            }

            // At this point we've finished parsing of a complete buffer, but still
            // have some surplus to deal with. Start afresh with an empty buffer and
            // go for another loop.
            //
            self.message_buf.clear();
        }
    }
}
