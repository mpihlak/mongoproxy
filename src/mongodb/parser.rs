use super::messages::{self,MsgHeader,MsgOpMsg,MsgOpQuery,MsgOpReply,MongoMessage,OpCode};
use std::io::Read;
use std::time::{Instant};
use log::{debug,info,warn};
use prometheus::{CounterVec, HistogramVec};
use crate::metrics::{Metrics};


lazy_static! {
    static ref OPCODE_COUNTER: CounterVec =
        register_counter_vec!(
            "opcode_count_total",
            "Number of different opcodes encountered",
            &["opcode"]).unwrap();

    static ref UNSUPPORTED_OPCODE_COUNTER: CounterVec =
        register_counter_vec!(
            "unsupported_opcode_count_total",
            "Number of unrecognized opcodes in MongoDb header",
            &["opcode"]).unwrap();

    static ref HEADER_PARSE_ERRORS_COUNTER: CounterVec =
        register_counter_vec!(
            "header_parse_error_count_total",
            "Header parse errors",
            &["error"]).unwrap();

    static ref TIME_TO_FIRST_BYTE_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "time_to_first_byte_seconds",
            "Time to first byte from the backend",
            &["op", "collection"]).unwrap();

    static ref TIME_TO_LAST_BYTE_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "time_to_last_byte_seconds",
            "Time to last byte from the backend",
            &["op", "collection"]).unwrap();
}

pub struct MongoStatsTracker {
    client_parser: MongoProtocolParser,
    server_parser: MongoProtocolParser,
    client_addr: String,
}

impl MongoStatsTracker {
    pub fn new(client_addr: &String) -> Self {
        MongoStatsTracker {
            client_parser: MongoProtocolParser::new(),
            server_parser: MongoProtocolParser::new(),
            client_addr: client_addr.clone(),
        }
    }

    pub fn track_client_request(&mut self, metrics: &Metrics, buf: &Vec<u8>) {
        let msg = self.client_parser.parse_buffer(buf);
        msg.log("client");
        metrics.client_bytes_recv.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

    }

    pub fn track_server_response(&mut self, metrics: &Metrics, buf: &Vec<u8>) {
        let msg = self.server_parser.parse_buffer(buf);
        msg.log("backend");
        self.server_parser.track_stats(&self.client_parser);

        metrics.client_bytes_sent.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

    }
}

pub struct MongoProtocolParser {
    header: messages::MsgHeader,
    have_header: bool,
    header_time: Instant,
    want_bytes: usize,
    message_buf: Vec<u8>,
    message_time: Instant,
    have_message: bool,
    parser_active: bool,
}

impl MongoProtocolParser {

    pub fn new() -> MongoProtocolParser {
        MongoProtocolParser{
            header: MsgHeader::new(),
            have_header: false,
            header_time: Instant::now(),
            want_bytes: messages::HEADER_LENGTH,
            message_buf: Vec::new(),
            message_time: Instant::now(),
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
    // TODO: Stop the parser if any of the internal routines returns an error
    //
    pub fn parse_buffer(&mut self, buf: &Vec<u8>) -> MongoMessage {
        let mut result = MongoMessage::None;

        if !self.parser_active {
            return MongoMessage::None;
        }

        self.message_buf.extend(buf);

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
                        self.have_message = false;
                        self.header_time = Instant::now();
                        self.want_bytes = self.header.message_length - messages::HEADER_LENGTH;
                        debug!("parser: have header {:?}, want {} more bytes", self.header, self.want_bytes);
                        result = MongoMessage::Header(self.header.clone());
                    },
                    Err(e) => {
                        warn!("parser: failed to read a header, stopping: {}", e);
                        self.parser_active = false;
                        HEADER_PARSE_ERRORS_COUNTER.with_label_values(&[&e.to_string()]).inc();
                    },
                }
            } else {
                result = extract_message(self.header.op_code, &self.message_buf[..]);

                // We got the payload, time to ask for a header again
                self.message_time = Instant::now();
                self.have_header = false;
                self.have_message = true;
                self.want_bytes = messages::HEADER_LENGTH;
            }

            // Point the message_buf to the bytes that we haven't yet processed
            // And don't worry about performance, yet
            self.message_buf = self.message_buf[new_buffer_start..].to_vec();
            debug!("message_buf capacity={}", self.message_buf.capacity());
        }

        result
    }

    pub fn track_stats(&self, client: &MongoProtocolParser) {
        let request_header = &client.header;

        if !self.have_message {
            return;
        }

        if self.header.response_to != request_header.request_id {
            warn!("response id {} does not match last request {}",
                self.header.response_to, request_header.request_id);
            return;
        }

        info!("Updating counters");

        let time_to_first_byte = self.header_time.
            duration_since(client.header_time).as_millis() as f64;
        TIME_TO_FIRST_BYTE_HISTOGRAM
            .with_label_values(&[&"foo", &"bar"])
            .observe(time_to_first_byte * 1000.0);

        let time_to_last_byte = self.message_time.
            duration_since(client.header_time).as_millis() as f64;
        TIME_TO_LAST_BYTE_HISTOGRAM
            .with_label_values(&[&"foo", &"bar"])
            .observe(time_to_last_byte * 1000.0);
    }
}

fn extract_message(op_code: u32, mut rdr: impl Read) -> MongoMessage {
    OPCODE_COUNTER.with_label_values(&[&op_code.to_string()]).inc();

    match num_traits::FromPrimitive::from_u32(op_code) {
        Some(OpCode::OpReply) => {
            let msg = MsgOpReply::from_reader(&mut rdr).unwrap();
            return MongoMessage::Reply(msg);
        }
        Some(OpCode::OpQuery) => {
            let msg = MsgOpQuery::from_reader(&mut rdr).unwrap();
            return MongoMessage::Query(msg);
        },
        Some(OpCode::OpMsg) => {
            let msg = MsgOpMsg::from_reader(&mut rdr).unwrap();
            return MongoMessage::Msg(msg);
        },
        Some(OpCode::OpPing) => {},
        Some(OpCode::OpPong) => {},
        None => {
            UNSUPPORTED_OPCODE_COUNTER.with_label_values(&[&op_code.to_string()]).inc();
            warn!("Unhandled OP: {}", op_code);
        },
    }
    return MongoMessage::None;
}