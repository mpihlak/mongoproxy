use super::messages::{MsgHeader,MongoMessage};
use super::parser::MongoProtocolParser;
use crate::bson_lite;
use crate::tracing;

use std::time::{Instant};
use std::{thread};
use std::collections::{HashMap, HashSet};
use log::{debug,info,warn};
use prometheus::{Counter,CounterVec,HistogramVec};

use rustracing::span::Span;
use rustracing::tag::Tag;
use rustracing_jaeger::span::{SpanContextState};
use rustracing_jaeger::{Tracer};


// Common labels for all op metrics
const OP_LABELS: &[&str] = &["client", "app", "op", "collection", "db", "replicaset", "server"];


lazy_static! {
    static ref APP_CONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_app_connections_established_total",
            "Total number of client connections established",
            &["app"]).unwrap();

    static ref APP_DISCONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_app_disconnections_total",
            "Total number of client disconnections",
            &["app"]).unwrap();

    static ref UNSUPPORTED_OPNAME_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_unsupported_op_name_count_total",
            "Number of unrecognized op names in MongoDb response",
            &["op"]).unwrap();

    static ref RESPONSE_TO_REQUEST_MISMATCH: Counter =
        register_counter!(
            "mongoproxy_response_to_request_id_mismatch",
            "Number of occurrences where we don't have a matching client request for the response"
            ).unwrap();

    static ref SERVER_RESPONSE_LATENCY_SECONDS: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_response_latency_seconds",
            "Backend response latency to first byte",
            OP_LABELS,
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]).unwrap();

    static ref DOCUMENTS_RETURNED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_returned_total",
            "Number of documents returned in the response",
            OP_LABELS,
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100_000.0 ]).unwrap();

    static ref DOCUMENTS_CHANGED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_changed_total",
            "Number of documents matched by insert, update or delete operations",
            OP_LABELS,
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100_000.0 ]).unwrap();

    static ref SERVER_RESPONSE_SIZE_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_server_response_bytes_total",
            "Size of the server response",
            OP_LABELS,
            vec![128.0, 1024.0, 16384.0, 131_072.0, 1_048_576.0]).unwrap();

    static ref SERVER_RESPONSE_ERRORS_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_server_response_errors_total",
            "Number of non-ok server responses",
            OP_LABELS).unwrap();

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
        ["isMaster", "ismaster", "ping", "whatsmyuri", "buildInfo", "buildinfo",
        "saslStart", "saslContinue", "getLog", "getFreeMonitoringStatus",
        "listDatabases", "listIndexes", "createIndexes", "listCollections", "replSetGetStatus",
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
    span: Span<SpanContextState>,
}

impl ClientRequest {
    fn from(tracker: &MongoStatsTracker, msg: MongoMessage) -> Self {
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
                    if let Some(_op) = s.get_str("op") {
                        op = _op;
                        if MONGODB_COLLECTION_OPS.contains(op.as_str()) {
                            if let Some(collection) = s.get_str("op_value") {
                                coll = collection.to_owned();
                            }
                        } else if !IGNORE_MONGODB_OPS.contains(op.as_str()) {
                            // Track all unrecognized ops that we explicitly don't ignore
                            warn!("unsupported op: {}", op);
                            UNSUPPORTED_OPNAME_COUNTER.with_label_values(&[&op.as_str()]).inc();
                        }
                    }
                    if let Some(collection) = s.get_str("collection") {
                        // getMore has collection as an explicit field, support that
                        coll = collection.to_owned();
                    }
                    if let Some(have_db) = s.get_str("db") {
                        db = have_db.to_string();
                    }
                    if let Some(comm) = s.get_str("comment") {
                        debug!("Have a comment field: {}", comm);
                        match tracing::extract_from_text(comm.as_str()) {
                            Ok(Some(parent_span)) => {
                                debug!("Extracted trace header: {:?}", parent_span);
                                if let Some(tracer) = &tracker.tracer {
                                    span = tracer
                                        .span(op.to_owned())
                                        .child_of(&parent_span)
                                        .tag(Tag::new("appName", tracker.client_application.clone()))
                                        .tag(Tag::new("client", tracker.client_addr.clone()))
                                        .tag(Tag::new("server", tracker.server_addr.clone()))
                                        .tag(Tag::new("collection", coll.to_owned()))
                                        .tag(Tag::new("db", db.to_owned()))
                                        .start();
                                }
                            },
                            other => {
                                debug!("No trace id found in the comment: {:?}", other);
                            },
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
            span,
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
    server_addr:            String,
    client_addr:            String,
    client_application:     String,
    client_request_map:     HashMap<u32, ClientRequest>,
    replicaset:             String,
    server_host:            String,
    tracer:                 Option<Tracer>,
}

impl Drop for MongoStatsTracker {
    fn drop(&mut self) {
        if !self.client_application.is_empty() {
            APP_DISCONNECTION_COUNT_TOTAL
                .with_label_values(&[&self.client_application])
                .inc();
        }
    }
}

impl MongoStatsTracker{
    pub fn new(client_addr: &str, server_addr: &str, tracer: Option<Tracer>) -> Self {
        MongoStatsTracker {
            client: MongoProtocolParser::new(),
            server: MongoProtocolParser::new(),
            client_addr: client_addr.to_string(),
            server_addr: server_addr.to_string(),
            client_request_map: HashMap::new(),
            client_application: String::from(""),
            replicaset: String::from(""),
            server_host: String::from(""),
            tracer,
        }
    }

    pub fn track_client_request(&mut self, buf: &[u8]) {
        CLIENT_BYTES_SENT_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        for (hdr, msg) in self.client.parse_buffer(buf) {
            info!("{:?}: {} client: hdr: {} msg: {}", thread::current().id(), self.client_addr, hdr, msg);

            // Ignore useless messages
            if let MongoMessage::None = msg {
                continue;
            }

            if self.client_application.is_empty() {
                if let Some(app_name) = extract_app_name(&msg) {
                    self.client_application = app_name;
                    APP_CONNECTION_COUNT_TOTAL
                        .with_label_values(&[&self.client_application])
                        .inc();
                }
                // TODO: The other interesting things here are replicaset name
                // and possibly also server name (if we have it). This becomes
                // especially relevant where running the proxy in a sidecar mode
                // where a single server may serve any number of MongoDb databases.
            }

            // Keep the client request so that we can keep track to which request
            // a server response belongs to.
            let req = ClientRequest::from(&self, msg);
            self.client_request_map.insert(hdr.request_id, req);
        }
    }

    fn label_values<'a>(&'a self, req: &'a ClientRequest) -> [&'a str; 7] {
        [ &self.client_addr, &self.client_application, &req.op, &req.coll, &req.db, &self.replicaset, &self.server_host]
    }

    pub fn track_server_response(&mut self, buf: &[u8]) {
        CLIENT_BYTES_RECV_TOTAL.with_label_values(&[&self.client_addr])
            .inc_by(buf.len() as f64);

        for (hdr, msg) in self.server.parse_buffer(buf) {
            info!("{:?}: {} server: hdr: {} msg: {}", thread::current().id(), self.client_addr, hdr, msg);

            if let Some(mut client_request) = self.client_request_map.remove(&hdr.response_to) {
                self.observe_server_response_to(&hdr, msg, &mut client_request);
                // If the client_request had a span, it will be automatically sent down the
                // channel as it goes out of scope here.
            } else {
                RESPONSE_TO_REQUEST_MISMATCH.inc();
                warn!("{:?}: response {} not mapped to request", thread::current().id(), hdr.response_to);
            }
        }
    }

    fn observe_server_response_to(&mut self, hdr: &MsgHeader, msg: MongoMessage, client_request: &mut ClientRequest) {
        SERVER_RESPONSE_LATENCY_SECONDS
            .with_label_values(&self.label_values(&client_request))
            .observe(client_request.message_time.elapsed().as_secs_f64());
        SERVER_RESPONSE_SIZE_TOTAL
            .with_label_values(&self.label_values(&client_request))
            .observe(hdr.message_length as f64);

        // Look into the server response and exract some counters from it.
        // Things like number of documents returned, inserted, updated, deleted.
        // The only interesting messages here are OP_MSG and OP_REPLY.
        match msg {
            MongoMessage::Msg(m) => {
                for section in m.documents {
                    self.try_parsing_replicaset(&section);

                    if let Some(ok) = section.get_float("ok") {
                        if ok == 0.0 {
                            client_request.span.set_tag(|| {
                                Tag::new("error", true)
                            });
                            SERVER_RESPONSE_ERRORS_TOTAL
                                .with_label_values(&self.label_values(&client_request))
                                .inc();
                        }
                    }
                    // Calculate number of documents returned from cursor response
                    if let Some(docs_returned) = section.get_i32("docs_returned") {
                        debug!("documents returned={}", docs_returned);
                        client_request.span.set_tag(|| {
                            Tag::new("documents_returned", docs_returned as i64)
                        });
                        DOCUMENTS_RETURNED_TOTAL
                            .with_label_values(&self.label_values(&client_request))
                            .observe(docs_returned as f64);
                    } else if section.contains_key("n") && client_request.op != "count" {
                        // This should happen in response to insert,update,delete but
                        // don't bother to check.
                        let num_rows = if client_request.op == "update" {
                            section.get_i32("n_modified").unwrap_or(0)
                        } else {
                            section.get_i32("n").unwrap_or(0)
                        };
                        client_request.span.set_tag(|| {
                            Tag::new("documents_changed", num_rows as i64)
                        });
                        DOCUMENTS_CHANGED_TOTAL
                            .with_label_values(&self.label_values(&client_request))
                            .observe(f64::from(num_rows.abs()));
                    }
                }
            },
            MongoMessage::Reply(r) => {
                for doc in &r.documents {
                    // The first isMaster response is an OP_REPLY so we need to look at it
                    self.try_parsing_replicaset(&doc);
                }
                client_request.span.set_tag(|| {
                    Tag::new("documents_returned", r.number_returned as i64)
                });
                DOCUMENTS_RETURNED_TOTAL
                    .with_label_values(&self.label_values(&client_request))
                    .observe(f64::from(r.number_returned));
            },
            other => {
                warn!("Unrecognized message_type: {:?}", other);
            },
        }
    }

    fn try_parsing_replicaset(&mut self, doc: &bson_lite::BsonLiteDocument) {
        if let Some(op) = doc.get_str("op") {
            if op == "hosts" {
                if let Some(replicaset) = doc.get_str("replicaset") {
                    self.replicaset = replicaset;
                }
                if let Some(server_host) = doc.get_str("server_host") {
                    self.server_host = server_host;
                }
            }
        }
    }

}

/// Extract `appname` from MongoDb `isMaster` query
fn extract_app_name(msg: &MongoMessage) -> Option<String> {
    if let MongoMessage::Query(m) = msg {
        if let Some(op) = m.query.get_str("op") {
            if op == "isMaster" || op == "ismaster" {
                return m.query.get_str("app_name");
            }
        }
    }
    None
}