use super::messages::{self,MsgHeader,MsgOpMsg,MsgOpQuery,MsgOpReply,MsgOpUpdate,MsgOpDelete,MsgOpInsert,MongoMessage,OpCode};
use std::io::{self,Read};
use std::time::{Instant};
use std::collections::{HashMap, HashSet};
use log::{debug,info,warn,error};
use prometheus::{CounterVec, HistogramVec};


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

    static ref SERVER_RESPONSE_TIME_SECONDS: HistogramVec =
        register_histogram_vec!(
            "server_response_time_seconds",
            "Backend response latency",
            &["client", "op", "collection", "db"]).unwrap();

    static ref CLIENT_BYTES_SENT_TOTAL: CounterVec =
        register_counter_vec!(
            "client_bytes_sent_total",
            "Total number of bytes sent by the client",
            &["client"]).unwrap();

    static ref SERVER_BYTES_SENT_TOTAL: CounterVec =
        register_counter_vec!(
            "sever_bytes_sent_total",
            "Total number of bytes sent by the server",
            &["client"]).unwrap();
}

pub struct MongoStatsTracker {
    client:                 MongoProtocolParser,
    server:                 MongoProtocolParser,
    client_addr:            String,
    client_request_time:    Instant,
    client_message:         MongoMessage,
}

impl MongoStatsTracker{
    pub fn new(client_addr: &String) -> Self {
        MongoStatsTracker {
            client: MongoProtocolParser::new(),
            server: MongoProtocolParser::new(),
            client_addr: client_addr.clone(),
            client_request_time: Instant::now(),
            client_message: MongoMessage::None,
        }
    }

    pub fn track_client_request(&mut self, buf: &Vec<u8>) {
        CLIENT_BYTES_SENT_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        if let Some(msg) = self.client.parse_buffer(buf) {
            if let MongoMessage::None = msg {
                return;
            }
            self.client_message = msg;
            info!("client: hdr: {}", self.client.header);
            info!("client: msg: {}", self.client_message);
            self.client_request_time = Instant::now();
        }
    }

    pub fn track_server_response(&mut self, buf: &Vec<u8>) {
        SERVER_BYTES_SENT_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        for msg in self.server.parse_buffer(buf) {
            if let MongoMessage::None = msg {
                continue;
            }

            info!("server: hdr: {}", self.server.header);
            info!("server: msg: {}", msg);

            let mut labels = self.extract_labels();
            let time_to_response = self.client_request_time.elapsed().as_millis();
            labels.insert("client", self.client_addr.as_str());
            SERVER_RESPONSE_TIME_SECONDS
                .with(&labels)
                .observe(time_to_response as f64 / 1000.0);
        }
    }

    pub fn extract_labels(&self) -> HashMap<&str, &str> {
        let mut result = HashMap::new();

        // Put in some defaults, so that we dont crash on metrics
        result.insert("op", "");
        result.insert("collection", "");
        result.insert("db", "");

        let known_ops: HashSet<&'static str> =
            ["find", "insert", "delete"].iter().cloned().collect();

        match &self.client_message {
            MongoMessage::Msg(m) => {
                for s in m.sections.iter() {
                    for elem in s.iter().take(1) {
                        if known_ops.contains(elem.0.as_str()) {
                            result.insert("op", elem.0.as_str());
                            if let Some(collection) = elem.1.as_str() {
                                result.insert("collection", collection);
                            }
                            info!("known op: {} coll: {:?}", elem.0, elem.1.as_str());
                        } else {
                            info!("op: {:?}", elem);
                        }
                    }
                    if let Ok(db) = s.get_str("$db") {
                        result.insert("db", db);
                    }
                }
            },
            MongoMessage::Query(m) => {
                add_collection_labels(&mut result, "query", &m.full_collection_name);
            },
            MongoMessage::Insert(m) => {
                add_collection_labels(&mut result, "insert", &m.full_collection_name);
            },
            MongoMessage::Update(m) => {
                add_collection_labels(&mut result, "update", &m.full_collection_name);
            },
            MongoMessage::Delete(m) => {
                add_collection_labels(&mut result, "delete", &m.full_collection_name);
            },
            other => {
                warn!("Labels not implemented for {}", other);
            },
        }

        result
    }
}

fn add_collection_labels<'a>(
        labels: &mut HashMap<&'a str, &'a str>,
        op_name: &'a str,
        full_collection_name: &'a String) {
    labels.insert("op", op_name);
    if let Some(pos) = full_collection_name.find('.') {
        let (db, collection) = full_collection_name.split_at(pos);
        labels.insert("db", db);
        labels.insert("collection", &collection[1..]);
    } else {
        labels.insert("db", "");
        labels.insert("collection", "");
    }
}

pub struct MongoProtocolParser {
    header:         messages::MsgHeader,
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

        if self.message_buf.len() > 0 {
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

}