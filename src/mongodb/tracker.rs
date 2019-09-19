use super::messages::{MongoMessage};
use super::parser::MongoProtocolParser;
use std::time::{Instant};
use std::{thread};
use std::collections::{HashMap, HashSet};
use log::{debug,info,warn};
use prometheus::{Counter,CounterVec,HistogramVec,GaugeVec};


lazy_static! {
    static ref UNSUPPORTED_OPNAME_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_unsupported_op_name_count_total",
            "Number of unrecognized op names in MongoDb response",
            &["op"]).unwrap();

    static ref THREAD_TIMING_HBUF_SIZE: GaugeVec =
        register_gauge_vec!(
            "mongoproxy_timing_htab_keys",
            "Number of current keys in the client timing HashMap",
            &["thread"]).unwrap();

    static ref RESPONSE_TO_REQUEST_MISMATCH: Counter =
        register_counter!(
            "mongoproxy_response_to_request_id_mismatch",
            "Number of occurrences where we don't have a matching client request for the response"
            ).unwrap();

    static ref SERVER_RESPONSE_FIRST_BYTE_SECONDS: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_response_first_byte_latency_seconds",
            "Backend response latency to first byte",
            &["client", "app", "op", "collection", "db"]).unwrap();

    static ref DOCUMENTS_RETURNED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_returned_total",
            "Number of documents returned in the response",
            &["client", "app", "op", "collection", "db"]).unwrap();

    static ref DOCUMENTS_CHANGED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_changed_total",
            "Number of documents matched by insert, update or delete operations",
            &["client", "app", "op", "collection", "db"]).unwrap();

    static ref SERVER_RESPONSE_SIZE_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_server_response_bytes_total",
            "Size of the server response",
            &["client", "app", "op", "collection", "db"]).unwrap();

    static ref CLIENT_BYTES_SENT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_bytes_sent_total",
            "Total number of bytes sent by the client",
            &["client"]).unwrap();

    static ref CLIENT_BYTES_RECV_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_client_bytes_received_total",
            "Total number of bytes sent by the server",
            &["client"]).unwrap();
}

pub struct MongoStatsTracker {
    client:                 MongoProtocolParser,
    server:                 MongoProtocolParser,
    client_addr:            String,
    client_request_times:   HashMap<u32, Instant>,
    client_application:     String,
    client_message:         MongoMessage,
}

impl MongoStatsTracker{
    pub fn new(client_addr: &str) -> Self {
        MongoStatsTracker {
            client: MongoProtocolParser::new(),
            server: MongoProtocolParser::new(),
            client_addr: client_addr.to_string(),
            client_request_times: HashMap::new(),
            client_application: String::from(""),
            client_message: MongoMessage::None,
        }
    }

    pub fn track_client_request(&mut self, buf: &[u8]) {
        CLIENT_BYTES_SENT_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        if let Some(msg) = self.client.parse_buffer(buf) {
            if let MongoMessage::None = msg {
                return;
            }
            self.client_message = msg;
            info!("{:?}: {} client: hdr: {} msg: {}", thread::current().id(), self.client_addr,
                self.client.header, self.client_message);

            // We're always removing these entries if we get a server response to
            // the request. But what if some requests never get a response ...
            self.client_request_times.insert(self.client.header.request_id, Instant::now());

            // For isMaster requests we  make an attempt to obtain connection metadata
            // from the payload. This will be sent on the first isMaster request and
            // contains a document such as this:
            // { isMaster: 1, client: { application: { name: "kala" },
            //   driver: { name: "MongoDB Internal Client", version: "4.0.2" },
            //   os: { type: "Darwin", name: "Mac OS X", architecture: "x86_64", version: "18.7.0" } } }
            // But sometimes it's called "ismaster" (mongoose) instead of "isMaster" so we need to
            // handle that as well.
            if let MongoMessage::Query(m) = &self.client_message {
                if m.query.contains_key("isMaster") || m.query.contains_key("ismaster") {
                    if let Some(bson::Bson::Document(client)) = m.query.get("client") {
                        if let Some(bson::Bson::Document(application)) = client.get("application") {
                            if let Ok(name) = application.get_str("name") {
                                self.client_application = String::from(name);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn track_server_response(&mut self, buf: &[u8]) {
        CLIENT_BYTES_RECV_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        if let Some(msg) = self.server.parse_buffer(buf) {
            if let MongoMessage::None = msg {
                return;
            }

            info!("{:?}: {} server: hdr: {} msg: {}", thread::current().id(), self.client_addr,
                self.server.header, msg);

            let mut labels = extract_client_labels(&self.client_message);
            labels.insert("client", self.client_addr.as_str());
            labels.insert("app", &self.client_application);

            SERVER_RESPONSE_SIZE_TOTAL
                .with(&labels)
                .observe(self.server.header.message_length as f64);

            THREAD_TIMING_HBUF_SIZE
                .with_label_values(&[&format!("{:?}", thread::current().id())])
                .set(self.client_request_times.len() as f64);

            // Note that we're only getting time to first byte here, since we immediately
            // remove the request id from the HashMap. Time to last byte would be more
            // complicated since we'd have to keep the client request id around until
            // the cursor is exhausted (though we could detect this by batch length probably)
            if let Some(client_request_time) = self.client_request_times.remove(&self.server.header.response_to) {
                let time_to_response = client_request_time.elapsed().as_millis();
                SERVER_RESPONSE_FIRST_BYTE_SECONDS
                    .with(&labels)
                    .observe(time_to_response as f64 / 1000.0);
            } else {
                RESPONSE_TO_REQUEST_MISMATCH.inc();
                warn!("client request not found for server response to {}",
                    self.server.header.response_to);
            }

            // Look into the server response and try to exract some counters from it.
            // Things like number of documents returned, inserted, updated, deleted.
            // TODO: For completeness we also need to look into OP_QUERY, OP_INSERT etc.
            //
            // This should be valid even if we don't know the matching client request
            if let MongoMessage::Msg(m) = msg {
                for section in m.sections {
                    // Calculate number of documents returned from cursor response
                    if let Ok(cursor) = section.get_document("cursor") {
                        let mut batch_found = false;
                        for key in ["firstBatch", "nextBatch"].iter() {
                            if let Ok(batch) = cursor.get_array(key) {
                                debug!("documents returned={}", batch.len());
                                DOCUMENTS_RETURNED_TOTAL
                                    .with(&labels)
                                    .observe(batch.len() as f64);
                                batch_found = true;
                                break;
                            }
                        }
                        if !batch_found {
                            warn!("did not find a batch in the response cursor");
                        }
                    } else if section.contains_key("n") && section.contains_key("ok") {
                        // This should happen in response to insert,update,delete but
                        // don't bother to check.
                        // TODO: for update ops we also need to check nModified, "n" is not enough
                        // TODO: check for "writeErrors" array in the response
                        // TODO: Observation is that this counter doesn't get increased
                        let num_rows = section.get_i32("n").unwrap_or(0);
                        DOCUMENTS_CHANGED_TOTAL
                            .with(&labels)
                            .observe(f64::from(num_rows.abs()));
                    }
                }
            }
        }
    }
}

// Extract metric labels from the client message.
pub fn extract_client_labels(client_message: &MongoMessage) -> HashMap<&str, &str> {
    let mut result = HashMap::new();

    // Put in some defaults, so that we dont crash on metrics
    result.insert("op", "");
    result.insert("collection", "");
    result.insert("db", "");

    let ignore_ops: HashSet<&'static str> =
        ["isMaster", "ismaster", "whatsmyuri", "buildInfo", "buildinfo",
        "saslStart", "saslContinue", "getLog", "getFreeMonitoringStatus",
        "listDatabases", "listIndexes", "listCollections", "replSetGetStatus",
        "endSessions", "dropDatabase", "_id", "q",
            ].iter().cloned().collect();

    let collection_ops: HashSet<&'static str> =
        ["find", "findAndModify", "insert", "delete", "update", "count",
        "getMore", "aggregate", "distinct"].iter().cloned().collect();

    match client_message {
        MongoMessage::Msg(m) => {
            // Go and loop through all the sections and see if we find an
            // operation that we know. This should be the first key of the
            // doc so we only look at first key of each section.
            for s in m.sections.iter() {
                for elem in s.iter().take(1) {
                    // Always track the operation, even if we're unable to get any
                    // additional details for it.
                    result.insert("op", elem.0.as_str());

                    if collection_ops.contains(elem.0.as_str()) {
                        if let Some(collection) = elem.1.as_str() {
                            result.insert("collection", collection);
                        }
                    } else if !ignore_ops.contains(elem.0.as_str()) {
                        // Track all unrecognized ops that we explicitly don't ignore
                        warn!("unsupported op: {}", elem.0.as_str());
                        UNSUPPORTED_OPNAME_COUNTER.with_label_values(&[&elem.0.as_str()]).inc();
                    }
                }
                if let Ok(collection) = s.get_str("collection") {
                    // getMore has collection as an explicit field, support that
                    result.insert("collection", collection);
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

fn add_collection_labels<'a>(
        labels: &mut HashMap<&'a str, &'a str>,
        op_name: &'a str,
        full_collection_name: &'a str) {
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
