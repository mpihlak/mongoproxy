use super::messages::{MongoMessage};
use super::parser::MongoProtocolParser;
use std::time::{Instant};
use std::collections::{HashMap, HashSet};
use log::{debug,info,warn};
use prometheus::{CounterVec, HistogramVec};


lazy_static! {
    static ref UNSUPPORTED_OPNAME_COUNTER: CounterVec =
        register_counter_vec!(
            "unsupported_op_name_count_total",
            "Number of unrecognized op names in MongoDb response",
            &["op_name"]).unwrap();

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
            debug!("client: hdr: {}", self.client.header);
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

            debug!("server: hdr: {}", self.server.header);
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
            ["find", "insert", "delete", "update"].iter().cloned().collect();

        match &self.client_message {
            MongoMessage::Msg(m) => {
                // Go and loop through all the sections and see if we find an
                // operation that we know. This should be the first key of the
                // doc so we only look at first key of each section.
                for s in m.sections.iter() {
                    for elem in s.iter().take(1) {
                        if known_ops.contains(elem.0.as_str()) {
                            result.insert("op", elem.0.as_str());
                            if let Some(collection) = elem.1.as_str() {
                                result.insert("collection", collection);
                            }
                            debug!("known op: {} coll: {:?}", elem.0, elem.1.as_str());
                        } else {
                            UNSUPPORTED_OPNAME_COUNTER.with_label_values(&[&elem.0.as_str()]).inc();
                            warn!("unrecognized op: {:?}", elem);
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
