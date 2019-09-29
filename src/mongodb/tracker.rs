use super::messages::{MsgHeader,MongoMessage};
use super::parser::MongoProtocolParser;
use crate::tracing;

use std::time::{Instant};
use std::{thread};
use std::collections::{HashMap, HashSet};
use log::{debug,info,warn};
use prometheus::{Counter,CounterVec,HistogramVec,GaugeVec};
use rustracing::span::{Span};
use rustracing_jaeger::span::{SpanContextState};

lazy_static! {
    static ref UNSUPPORTED_OPNAME_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_unsupported_op_name_count_total",
            "Number of unrecognized op names in MongoDb response",
            &["op"]).unwrap();

    static ref REQUEST_MATCH_HASHMAP_SIZE: GaugeVec =
        register_gauge_vec!(
            "mongoproxy_request_match_hashmap_size",
            "Number of current keys in the request/response mapping HashMap",
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
            &["client", "app", "op", "collection", "db"],
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 10.0]).unwrap();

    static ref DOCUMENTS_RETURNED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_returned_total",
            "Number of documents returned in the response",
            &["client", "app", "op", "collection", "db"],
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100_000.0 ]).unwrap();

    static ref DOCUMENTS_CHANGED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_changed_total",
            "Number of documents matched by insert, update or delete operations",
            &["client", "app", "op", "collection", "db"],
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100_000.0 ]).unwrap();

    static ref SERVER_RESPONSE_SIZE_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_server_response_bytes_total",
            "Size of the server response",
            &["client", "app", "op", "collection", "db"],
            vec![128.0, 1024.0, 16384.0, 131_072.0, 1_048_576.0]).unwrap();

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

    static ref IGNORE_MONGODB_OPS: HashSet<&'static str> =
        ["isMaster", "ismaster", "whatsmyuri", "buildInfo", "buildinfo",
        "saslStart", "saslContinue", "getLog", "getFreeMonitoringStatus",
        "listDatabases", "listIndexes", "listCollections", "replSetGetStatus",
        "endSessions", "dropDatabase", "_id", "q"].iter().cloned().collect();

    static ref MONGODB_COLLECTION_OPS: HashSet<&'static str> =
        ["find", "findAndModify", "insert", "delete", "update", "count",
        "getMore", "aggregate", "distinct"].iter().cloned().collect();
}

// Stripped down version of the client request. We need this mostly for timing
// stats and metric labels.
struct ClientRequest {
    message_time: Instant,
    op: String,
    db: String,
    coll: String,
    _span: Span<SpanContextState>,
}

impl ClientRequest {
    fn from(msg: MongoMessage) -> Self {
        let message_time = Instant::now();
        let mut op = String::from("");
        let mut db = String::from("");
        let mut coll = String::from("");
        let mut span = Span::inactive();

        match msg {
            MongoMessage::Msg(m) => {
                // Go and loop through all the documents in the msg and see if we
                // find an operation that we know. This should be the first key of
                // the doc so we only look at first key of each section.
                for s in m.documents.iter() {
                    for elem in s.iter().take(1) {
                        // Always track the operation, even if we're unable to get any
                        // additional details for it.
                        op = elem.0.as_str().to_owned();

                        if MONGODB_COLLECTION_OPS.contains(elem.0.as_str()) {
                            if let Some(collection) = elem.1.as_str() {
                                coll = collection.to_owned();
                            }
                        } else if !IGNORE_MONGODB_OPS.contains(elem.0.as_str()) {
                            // Track all unrecognized ops that we explicitly don't ignore
                            warn!("unsupported op: {}", elem.0.as_str());
                            UNSUPPORTED_OPNAME_COUNTER.with_label_values(&[&elem.0.as_str()]).inc();
                        }
                    }
                    if let Ok(collection) = s.get_str("collection") {
                        // getMore has collection as an explicit field, support that
                        coll = collection.to_owned();
                    }
                    if let Ok(have_db) = s.get_str("$db") {
                        db = have_db.to_string();
                    }
                    if let Ok(comm) = s.get_str("comment") {
                        if let Ok(Some(parent_span)) = tracing::extract_from_text(comm) {
                            span = tracing::global_tracer()
                                .span(op.to_owned())
                                .child_of(&parent_span)
                                .start();
                        }
                    }
                }
            },
            MongoMessage::Query(m) => {
                op = String::from("query");
                ClientRequest::parse_collname(&m.full_collection_name, &mut db, &mut coll)
            },

            // There is no response to OP_INSERT, DELETE, UPDATE so don't bother
            // processing labels for these.
            MongoMessage::Insert(_) |
            MongoMessage::Update(_) |
            MongoMessage::Delete(_) => {
                warn!("Not processing labels for obsolete INSERT, UPDATE or DELETE messages");
            },
            other => {
                warn!("Labels not implemented for {}", other);
            },
        }

        ClientRequest {
            coll,
            db,
            op,
            message_time,
            _span: span,
        }
    }

    // Parse a fully qualified collection name into "db" and "coll".
    // Both db and coll are expected to be empty before.
    fn parse_collname(full_collection_name: &str, db: &mut String, coll: &mut String) {
        let pos = full_collection_name.find('.').unwrap_or_else(|| full_collection_name.len());
        let (_db, _coll) = full_collection_name.split_at(pos);
        db.push_str(_db);
        coll.push_str(_coll);
    }

}

pub struct MongoStatsTracker {
    client:                 MongoProtocolParser,
    server:                 MongoProtocolParser,
    client_addr:            String,
    client_application:     String,
    client_request_map:     HashMap<u32, ClientRequest>,
}

impl MongoStatsTracker{
    pub fn new(client_addr: &str) -> Self {
        MongoStatsTracker {
            client: MongoProtocolParser::new(),
            server: MongoProtocolParser::new(),
            client_addr: client_addr.to_string(),
            client_request_map: HashMap::new(),
            client_application: String::from(""),
        }
    }

    pub fn track_client_request(&mut self, buf: &[u8]) {
        CLIENT_BYTES_SENT_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        for (hdr, msg) in self.client.parse_buffer(buf) {
            info!("{:?}: {} client: hdr: {} msg: {}", thread::current().id(), self.client_addr, hdr, msg);

            // For isMaster requests we  make an attempt to obtain connection metadata
            // from the payload. This will be sent on the first isMaster request and
            // contains a document such as this:
            // { isMaster: 1, client: { application: { name: "kala" },
            //   driver: { name: "MongoDB Internal Client", version: "4.0.2" },
            //   os: { type: "Darwin", name: "Mac OS X", architecture: "x86_64", version: "18.7.0" } } }
            // But sometimes it's called "ismaster" (mongoose) instead of "isMaster" so we need to
            // handle that as well.
            if let MongoMessage::Query(m) = &msg {
                if self.client_application.is_empty() && m.query.contains_key("isMaster") || m.query.contains_key("ismaster") {
                    if let Some(bson::Bson::Document(client)) = m.query.get("client") {
                        if let Some(bson::Bson::Document(application)) = client.get("application") {
                            if let Ok(name) = application.get_str("name") {
                                self.client_application = String::from(name);
                            }
                        }
                    }
                }
            }

            // We're always removing these entries if we get a server response to
            // the request. TODO: But what if some requests never get a response ...
            //
            // A a better approach would be to use a small circular buffer,
            // So that we're adding entries until the buffer is full and then start
            // replacing older entries. For lookup we'd just scan the whole buffer,
            // and probably be still better off than with a HashMap, if the buf is small.
            let req = ClientRequest::from(msg);
            self.client_request_map.insert(hdr.request_id, req);
        }
    }

    fn label_values<'a>(&'a self, req: &'a ClientRequest) -> [&'a str; 5] {
        [ &self.client_addr, &self.client_application, &req.op, &req.coll, &req.db]
    }

    pub fn track_server_response(&mut self, buf: &[u8]) {
        CLIENT_BYTES_RECV_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        for (hdr, msg) in self.server.parse_buffer(buf) {
            info!("{:?}: {} server: hdr: {} msg: {}", thread::current().id(), self.client_addr, hdr, msg);

            REQUEST_MATCH_HASHMAP_SIZE
                .with_label_values(&[&format!("{:?}", thread::current().id())])
                .set(self.client_request_map.len() as f64);

            if let Some(client_request) = self.client_request_map.remove(&hdr.response_to) {
                self.observe_server_response_to(&hdr, msg, &client_request);
                // If the client_request had a span, it will be automatically sent down the
                // channel here.
                // TODO: Add any tags and log before that happens.
            } else {
                RESPONSE_TO_REQUEST_MISMATCH.inc();
                warn!("{:?}: response {} not mapped to request", thread::current().id(), hdr.response_to);
            }
        }
    }

    fn observe_server_response_to(&self, hdr: &MsgHeader, msg: MongoMessage, client_request: &ClientRequest) {
        let time_to_response = client_request.message_time.elapsed().as_millis();

        SERVER_RESPONSE_FIRST_BYTE_SECONDS
            .with_label_values(&self.label_values(&client_request))
            .observe(time_to_response as f64 / 1000.0);
        SERVER_RESPONSE_SIZE_TOTAL
            .with_label_values(&self.label_values(&client_request))
            .observe(hdr.message_length as f64);

        // Look into the server response and exract some counters from it.
        // Things like number of documents returned, inserted, updated, deleted.
        // The only interesting messages here are OP_MSG and OP_REPLY.
        match msg {
            MongoMessage::Msg(m) => {
                for section in m.documents {
                    // Calculate number of documents returned from cursor response
                    if let Ok(cursor) = section.get_document("cursor") {
                        let mut batch_found = false;
                        for key in ["firstBatch", "nextBatch"].iter() {
                            if let Ok(batch) = cursor.get_array(key) {
                                debug!("documents returned={}", batch.len());
                                DOCUMENTS_RETURNED_TOTAL
                                    .with_label_values(&self.label_values(&client_request))
                                    .observe(batch.len() as f64);
                                batch_found = true;
                                break;
                            }
                        }
                        if !batch_found {
                            warn!("did not find a batch in the response cursor");
                        }
                    } else if section.contains_key("n") && client_request.op != "count" {
                        // This should happen in response to insert,update,delete but
                        // don't bother to check.
                        let num_rows = if client_request.op == "update" {
                            section.get_i32("nModified").unwrap_or(0)
                        } else {
                            section.get_i32("n").unwrap_or(0)
                        };
                        DOCUMENTS_CHANGED_TOTAL
                            .with_label_values(&self.label_values(&client_request))
                            .observe(f64::from(num_rows.abs()));
                    }
                }
            },
            MongoMessage::Reply(r) => {
                DOCUMENTS_RETURNED_TOTAL
                    .with_label_values(&self.label_values(&client_request))
                    .observe(f64::from(r.number_returned));
            },
            other => {
                warn!("Unrecognized message_type: {:?}", other);
            },
        }
    }
}
