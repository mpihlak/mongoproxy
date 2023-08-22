use mongo_protocol::{MsgHeader,MongoMessage,MsgOpMsg,ResponseDocuments};
use crate::jaeger_tracing;
use crate::appconfig::{AppConfig};

use std::time::{Instant};
use std::collections::{HashMap, HashSet};

use tracing::{debug, warn, info_span};
use prometheus::{Counter, CounterVec, HistogramVec, Gauge};

use opentelemetry::trace::{Tracer, SpanKind, TraceContextExt};
use opentelemetry::trace::Span as _Span;
use opentelemetry::sdk::trace::Span;
use opentelemetry::{KeyValue};

use async_bson::Document;

// Common labels for all op metrics
const OP_LABELS: &[&str] = &["client", "app", "op", "collection", "db", "replicaset", "server", "username"];

lazy_static! {
    static ref APP_CONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_app_connections_established_total",
            "Total number of client connections established",
            &["app"]).unwrap();

    // This is a separate counter because the app and user label values arrive at different
    // times (if at all). So there is no good way to determine if we have both.
    static ref USER_CONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_user_connections_established_total",
            "Total number of client connections established",
            &["username"]).unwrap();

    static ref APP_DISCONNECTION_COUNT_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_app_disconnections_total",
            "Total number of client disconnections",
            &["app", "username"]).unwrap();

    static ref UNSUPPORTED_OPNAME_COUNTER: CounterVec =
        register_counter_vec!(
            "mongoproxy_unsupported_op_name_count_total",
            "Number of unrecognized op names in MongoDb response",
            &["op"]).unwrap();

    static ref CURSOR_TRACE_PARENT_HASHMAP_CAPACITY: Gauge =
        register_gauge!(
            "mongoproxy_cursor_trace_hashmap_capacity_total",
            "Cursor trace parent mapping HashMap size"
            ).unwrap();

    static ref SERVER_RESPONSE_LATENCY_SECONDS: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_response_latency_seconds",
            "Backend response latency to first byte",
            OP_LABELS,
            vec![0.001, 0.01, 0.1, 1.0, 10.0, 60.0 ]).unwrap();

    static ref DOCUMENTS_RETURNED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_returned_total",
            "Number of documents returned in the response",
            OP_LABELS,
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0 ]).unwrap();

    static ref DOCUMENTS_CHANGED_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_documents_changed_total",
            "Number of documents changed by insert, update or delete operations",
            OP_LABELS,
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0 ]).unwrap();

    static ref SERVER_RESPONSE_SIZE_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_server_response_bytes_total",
            "Size of the server response",
            OP_LABELS,
            vec![128.0, 1024.0, 16384.0, 131_072.0, 1_048_576.0]).unwrap();

    static ref CLIENT_REQUEST_SIZE_TOTAL: HistogramVec =
        register_histogram_vec!(
            "mongoproxy_client_request_bytes_total",
            "Size of the client request",
            OP_LABELS,
            vec![128.0, 1024.0, 16384.0, 131_072.0, 1_048_576.0]).unwrap();

    static ref SERVER_RESPONSE_ERRORS_TOTAL: CounterVec =
        register_counter_vec!(
            "mongoproxy_server_response_errors_total",
            "Number of non-ok server responses",
            OP_LABELS).unwrap();

    static ref SERVER_RESPONSE_REQUEST_MISMATCH: Counter =
        register_counter!(
            "mongoproxy_server_response_request_mismatch_total",
            "Number of times mongoproxy was unable to match server response to a client request"
        ).unwrap();

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

    static ref OTHER_MONGODB_OPS: HashSet<&'static str> =
        ["isMaster", "ismaster", "ping", "whatsmyuri", "buildInfo", "buildinfo", "drop",
        "saslStart", "saslContinue", "getLog", "getFreeMonitoringStatus", "killCursors",
        "listDatabases", "listIndexes", "createIndexes", "listCollections", "replSetGetStatus",
        "endSessions", "dropDatabase", "_id", "q", "getMore", "hello"].iter().cloned().collect();

    // Operations that have collection name as op value
    static ref MONGODB_COLLECTION_OPS: HashSet<&'static str> =
        ["find", "findAndModify", "findandmodify", "insert", "delete", "update", "count",
        "aggregate", "distinct"].iter().cloned().collect();
}

// Since "getMore" doesn't have an attached trace id we need a way to look up the parent trace for
// them. So we need to keep around the trace Contexts for the initial operation and look them up
// by the server hostport and cursor id.
//
// XXX: If the cursor id's are not unique within a MongoDb instance then there's
// a risk of collision if there are multiple databases on the same server.
pub type CursorTraceMapper = HashMap<(std::net::SocketAddr,i64), opentelemetry::Context>;


// Stripped down version of the client request. We need this mostly for timing
// stats and metric labels.
struct ClientRequest {
    message_time: Instant,
    op: String,
    db: String,
    coll: String,
    cursor_id: i64,
    span: Option<Span>,
    message_length: usize,
}

impl ClientRequest {
    fn from(tracker: &MongoStatsTracker, message_length: usize, msg: &MongoMessage) -> Self {
        let message_time = Instant::now();
        let mut op = String::from("");
        let mut db = String::from("");
        let mut coll = String::from("");
        let mut cursor_id = 0;
        let mut span = None;

        match msg {
            MongoMessage::Msg(m) => {
                // First determine the op, db and collection name used for metrics and tracing.
                // Usually these should be in the "Type 0" document (which of there MUST be only
                // one). However we don't have that information here and must go through all of
                // them and make a prioritized guess.
                //
                let mut collection_op = None;
                let mut other_op = None;
                let mut unknown_op = None;
                let mut coll_name_from_op = None;
                let mut coll_name_from_param = None;

                for s in m.documents.iter() {
                    let opname = s.get_str("op").unwrap_or("");

                    if MONGODB_COLLECTION_OPS.contains(opname) {
                        collection_op = Some(opname);
                        coll_name_from_op = Some(s.get_str("op_value").unwrap_or(""));
                    } else if OTHER_MONGODB_OPS.contains(opname) {
                        other_op = Some(opname);
                        coll_name_from_param = Some(s.get_str("collection").unwrap_or(""));
                    } else {
                        unknown_op = Some(opname);
                    }

                    if let Some(have_db) = s.get_str("db") {
                        db = have_db.to_string();
                    }
                }

                op = if let Some(opname) = collection_op {
                    opname.to_owned()
                } else if let Some(opname) = other_op {
                        opname.to_owned()
                } else {
                    let opname = unknown_op.unwrap_or("<no-op>");
                    warn!("unsupported op: {}", opname);
                    UNSUPPORTED_OPNAME_COUNTER.with_label_values(&[opname]).inc();
                    opname.to_owned()
                };

                coll = if let Some(coll_name) = coll_name_from_op {
                    coll_name.to_owned()
                } else {
                    coll_name_from_param.unwrap_or("").to_owned()
                };

                // Once we have the opname, collection and db, see if we can create a tracing span
                // out of one of the documents.
                for s in m.documents.iter() {
                    if let Some((ok_cursor_id, ok_span)) = ClientRequest::maybe_create_span(
                            tracker, m, &db, &coll, &op, s) {
                        cursor_id = ok_cursor_id;
                        span = Some(ok_span);
                        break;
                    }
                }
            },
            MongoMessage::Query(m) => {
                // Despite the name, QUERY can also be insert, update or delete.
                // Or a ping, so handle these as well.
                op = String::from(m.query.get_str("op").unwrap_or("query"));

                // The database name can be obtained from the message itself, however the collection name
                // is *not* actually in the full_collection_name, but needs to be obtained from the payload
                // query. There too are multiple options (op_value or collection)
                let pos = m.full_collection_name.find('.').unwrap_or(m.full_collection_name.len());
                db = m.full_collection_name[..pos].to_owned();

                if let Some(val) = m.query.get_str("collection") {
                    coll = val.to_owned();
                } else if let Some(val) = m.query.get_str("op_value") {
                    coll = val.to_owned();
                }
            },
            MongoMessage::GetMore(m) => {
                op = String::from("getMore");
                if let Some(pos) = m.full_collection_name.find('.') {
                    let (_db, _coll) = m.full_collection_name.split_at(pos);
                    db = _db.to_owned();
                    coll = _coll[1..].to_owned();
                }
            },
            // There is no response to OP_INSERT, DELETE, UPDATE so don't bother
            // processing labels for these.
            MongoMessage::Insert(_) |
            MongoMessage::Update(_) |
            MongoMessage::Delete(_) => {
                warn!("Not processing labels for obsolete INSERT, UPDATE or DELETE messages");
            },
            MongoMessage::Compressed(_) => {
                // There's not much we can know about the compressed message unless we
                // uncompress it. Don't make noise about it.
            }
            other => {
                warn!("Labels not implemented for {}", other);
            },
        }

        ClientRequest {
            coll,
            db,
            op,
            cursor_id,
            message_time,
            span,
            message_length,
        }
    }

    // For OP_MSG messages try creating distributed tracing spans. For the initial span the parent
    // trace id is extracted from the $comment field of the query. For following cursor fetches we
    // need to keep store the trace id in a hashmap, keyed by cursor id.
    //
    fn maybe_create_span(
        tracker: &MongoStatsTracker,
        msg: &MsgOpMsg,
        db: &str,
        coll: &str,
        op: &str,
        doc: &Document,
    ) -> Option<(i64, Span)> {

        let tracer = match &tracker.app.tracer {
            Some(t) => t,
            _ => return None,
        };

        if op == "getMore" {
            // getMore operations will not have a client provided trace id. Instead we need follow
            // from the span that was created by the initial "find" or "aggregate" operation. So
            // we look that up from a table and follow from that.

            if let Some(cursor) = doc.get_i64("op_value") {
                let trace_mapper = tracker.app.trace_mapper.lock().unwrap();

                // Look up the text representation of the parent span
                let parent_span_ctx = match trace_mapper.get(&(tracker.server_addr_sa, cursor)) {
                    Some(parent_trace_id) => parent_trace_id,
                    _ => {
                        debug!("Parent span not found for cursor_id={}", cursor);
                        return None;
                    },
                };

                // Because we don't have a cursor id here, we can't store the span in the
                // trace_mapper just yet. Unfortunately we only get the cursor id in the response
                // document of the first "find", so that's where we put add it to the trace_mapper.

                let span = tracer.span_builder(op)
                    .with_parent_context(parent_span_ctx.clone())
                    .with_kind(SpanKind::Server)
                    .start(tracer);
                debug!("Started getMore span: {:?}", span.span_context());

                return Some((cursor, span));
            }
        } else if let Some(comment) = doc.get_str("comment") {
            // Otherwise we look up the parent trace id from the $comment field of
            // the query.

            let parent = match jaeger_tracing::extract_from_text(comment) {
                Some(parent) => parent,
                _ => {
                    debug!("No trace id found in $comment");
                    return None
                },
            };

            debug!("Extracted trace header: {:?}", parent);
            let span = tracer
                .span_builder(op)
                .with_parent_context(parent)
                .with_kind(SpanKind::Server)
                .with_attributes(vec![
                    KeyValue::new("db.mongodb.collection", coll.to_owned()),
                    KeyValue::new("db.name", db.to_owned()),
                    KeyValue::new("db.operation", op.to_owned()),
                    KeyValue::new("db.client.addr", tracker.client_addr.clone()),
                    KeyValue::new("db.server.addr", tracker.server_addr.clone()),
                    KeyValue::new("db.client.app", tracker.client_application.clone()),
                ])
                .start(tracer);
            debug!("Started initial span: {:?}", span.span_context());

            // Tag the span with all the documents in the message. This will give
            // us the query payload, delete query, etc.
            for bytes in msg.section_bytes.iter() {
                if let Ok(doc) = bson::Document::from_reader(&mut &bytes[..]) {
                    // Use the first key in the document as key name
                    if let Some(doc_first_key) = doc.keys().next() {
                        span.set_attribute(KeyValue::new(
                                format!("db.operation.{}", doc_first_key),
                                format!("{:.8192}", doc.to_string())));
                    }
                }
            }

            return Some((0, span));
        }
        None
    }

    fn is_collection_op(&self) -> bool {
        !self.coll.is_empty()
    }
}

pub struct MongoStatsTracker {
    server_addr:            String,
    server_addr_sa:         std::net::SocketAddr,
    client_addr:            String,
    client_application:     String,
    client_username:        String,
    client_request_hdr:     Option<(ClientRequest, MsgHeader)>,
    replicaset:             String,
    server_host:            String,
    app:                    AppConfig,
}

impl Drop for MongoStatsTracker {
    fn drop(&mut self) {
        if !self.client_application.is_empty() {
            APP_DISCONNECTION_COUNT_TOTAL
                .with_label_values(&[&self.client_application, &self.client_username])
                .inc();
        }
    }
}

impl MongoStatsTracker{
    pub fn new(client_addr: &str,
               server_addr: &str,
               server_addr_sa: std::net::SocketAddr,
               app: AppConfig) -> Self {
        MongoStatsTracker {
            client_addr: client_addr.to_string(),
            server_addr: server_addr.to_string(),
            server_addr_sa,
            client_request_hdr: None,
            client_application: String::from(""),
            client_username: String::from(""),
            replicaset: String::from(""),
            server_host: String::from(""),
            app,
        }
    }

    fn is_tracing_enabled(&self) -> bool {
        self.app.tracer.is_some()
    }

    pub fn track_client_request(&mut self, hdr: &MsgHeader, msg: &MongoMessage) {
        CLIENT_BYTES_SENT_TOTAL.with_label_values(&[&self.client_addr]).inc_by(hdr.message_length as f64);

        let span = info_span!("track_client_request");
        let _ = span.enter();

        match msg {
            MongoMessage::Query(m) => {
                self.maybe_extract_connection_metadata(&m.query);
            },
            MongoMessage::Msg(m) if !m.documents.is_empty() => {
                self.maybe_extract_connection_metadata(&m.documents[0]);
            }
            MongoMessage::None => return, // Ignore useless messages
            _ => {}
        }

        let req = ClientRequest::from(self, hdr.message_length, msg);

        // If we're tracking cursors for tracing purposes then also handle
        // the cleanup.
        self.maybe_kill_cursors(&req.op, msg);

        self.client_request_hdr = Some((req, hdr.clone()))
    }

    // The first isMaster message from client contains the connection metadata including the
    // optional application name and user. These do not appear in subsequent "hello" or "isMaster"
    // messages so grab them in the first message they appear in and use throughout the rest of the
    // connection.
    fn maybe_extract_connection_metadata(&mut self, doc: &Document) {
        if let Some(op) = doc.get_str("op") {
            if op == "isMaster" || op == "ismaster" || op == "hello" {
                if self.client_application.is_empty() {
                    if let Some(app_name) = doc.get_str("app_name") {
                        self.client_application = app_name.to_owned();
                        APP_CONNECTION_COUNT_TOTAL
                            .with_label_values(&[&self.client_application])
                            .inc();
                    }
                }
                if self.client_username.is_empty() {
                    if let Some(username) = doc.get_str("username") {
                        self.client_username = username.to_owned();
                        USER_CONNECTION_COUNT_TOTAL
                            .with_label_values(&[&self.client_username])
                            .inc();
                    }
                }
            }
        }
    }

    // Handle "killCursors" to clean up the trace parent hash map
    fn maybe_kill_cursors(&mut self, op: &str, msg: &MongoMessage) {
        if let MongoMessage::Msg(msg) = msg {
            if op == "killCursors" && self.is_tracing_enabled() && !msg.section_bytes.is_empty() {
                let bytes = &msg.section_bytes[0];
                if let Ok(doc) = bson::Document::from_reader(&mut &bytes[..]) {
                    if let Ok(cursor_ids) = doc.get_array("cursors") {
                        debug!("Killing cursors: {:?}", cursor_ids);
                        for cur_id in cursor_ids.iter() {
                            if let bson::Bson::Int64(cur_id) = cur_id {
                                let mut trace_mapper = self.app.trace_mapper.lock().unwrap();

                                trace_mapper.remove(&(self.server_addr_sa, *cur_id));
                                CURSOR_TRACE_PARENT_HASHMAP_CAPACITY.set(trace_mapper.capacity() as f64);
                            }
                        }
                    }
                }
            }
        }
    }

    // Label values for common metrics
    fn label_values<'a>(&'a self, req: &'a ClientRequest) -> [&'a str; 8] {
        [
            &self.client_addr,
            &self.client_application,
            &req.op,
            &req.coll,
            &req.db,
            &self.replicaset,
            &self.server_host,
            &self.client_username,
        ]
    }

    pub fn track_server_response(&mut self, hdr: MsgHeader, msg: MongoMessage) {
        CLIENT_BYTES_RECV_TOTAL.with_label_values(&[&self.client_addr]).inc_by(hdr.message_length as f64);

        let span = info_span!("track_server_response");
        let _ = span.enter();

        // Ignore useless messages
        if let MongoMessage::None = msg {
            return;
        }

        if let Some((mut client_request, req_hdr)) = self.client_request_hdr.take() {
            if hdr.response_to != req_hdr.request_id {
                // And if this starts to happen, then we need to go back to the HashMap of requests ...
                warn!("Server response to {} does not match client request {}",
                    hdr.response_to, req_hdr.request_id);
                SERVER_RESPONSE_REQUEST_MISMATCH.inc();
            } else {
                self.observe_server_response_to(&hdr, &msg, &mut client_request);
            }
        } else {
            warn!("No client request found for {:?}", hdr);
            SERVER_RESPONSE_REQUEST_MISMATCH.inc();
        }
    }

    fn observe_server_response_to(
        &mut self,
        hdr: &MsgHeader,
        msg: &MongoMessage,
        client_request: &mut ClientRequest,
    ) {
        if client_request.is_collection_op() {
            SERVER_RESPONSE_LATENCY_SECONDS
                .with_label_values(&self.label_values(client_request))
                .observe(client_request.message_time.elapsed().as_secs_f64());
            SERVER_RESPONSE_SIZE_TOTAL
                .with_label_values(&self.label_values(client_request))
                .observe(hdr.message_length as f64);
            CLIENT_REQUEST_SIZE_TOTAL
                .with_label_values(&self.label_values(client_request))
                .observe(client_request.message_length as f64);
        }

        // Look into the server response and exract some counters from it.
        // Things like number of documents returned, inserted, updated, deleted.
        // The only interesting messages here are OP_MSG and OP_REPLY.
        match msg {
            MongoMessage::Msg(m) => {
                self.process_response_documents(client_request, m.get_documents());
            },
            MongoMessage::Reply(r) => {
                for doc in &r.documents {
                    // The first isMaster response is an OP_REPLY so we need to look at it
                    self.try_parsing_replicaset(doc);
                }
                self.process_response_documents(client_request, r.get_documents());
            },
            MongoMessage::Compressed(m) => {
                debug!("Compressed message: {:?}", m);
            },
            other => {
                warn!("Unrecognized message_type: {:?}", other);
            },
        }
    }

    fn process_response_documents(&mut self, client_request: &mut ClientRequest, documents: &[Document]) {
        for section in documents {
            self.try_parsing_replicaset(section);

            if let Some(ok) = section.get_float("ok") {
                if ok == 0.0 {
                    if let Some(span) = &mut client_request.span {
                        span.set_attribute(KeyValue::new("error", true));
                    }
                    SERVER_RESPONSE_ERRORS_TOTAL
                        .with_label_values(&self.label_values(client_request))
                        .inc();
                }
            }

            let mut n_docs_returned = None;
            let mut n_docs_changed = None;
            let mut n_docs_matched = None;

            if let Some(n) = section.get_i32("docs_returned") {
                // Number of documents returned from a cursor operation (find, getMore, etc)
                n_docs_returned = Some(n);
            } else if client_request.op == "count" {
                // Count also kind of returns documents, record these
                n_docs_returned = Some(section.get_i32("n").unwrap_or(0));
            } else if client_request.op.to_ascii_lowercase() == "findandmodify" {
                // findAndModify always returns at most 1 row, the same as the num of changed rows
                n_docs_returned = Some(section.get_i32("n").unwrap_or(0));
                n_docs_changed = n_docs_returned;
            } else if client_request.op == "update" {
                // Update uses n_modified to indicate number of docs changed
                // n_upserted for number of new documents inserted
                // n for number of documents matches by the update
                let n_modified = section.get_i32("n_modified").unwrap_or(0);
                let n_upserted = section.get_i32("n_upserted").unwrap_or(0);
                n_docs_matched = section.get_i32("n");
                n_docs_changed = Some(n_modified + n_upserted);
            } else if section.contains_key("n") {
                // Lump the rest of the update operations together
                n_docs_changed = Some(section.get_i32("n").unwrap_or(0));
            }

            debug!("client_request: op={} coll={} n_docs_returned={:?} n_docs_changed={:?} n_docs_matched={:?}",
                client_request.op, client_request.coll, n_docs_returned, n_docs_changed, n_docs_matched);

            if let Some(n) = n_docs_returned {
                if let Some(span) = &mut client_request.span {
                    span.set_attribute(KeyValue::new("db.documents_returned", n as i64));
                }
                if client_request.is_collection_op() {
                    DOCUMENTS_RETURNED_TOTAL
                        .with_label_values(&self.label_values(client_request))
                        .observe(n as f64);
                }
            }

            if let Some(n) = n_docs_matched {
                if let Some(span) = &mut client_request.span {
                    span.set_attribute(KeyValue::new("db.documents_matched", n as i64));
                    if n_docs_changed.is_none() {
                        // This must have been a NOP update operation so let's make it explicit
                        // that nothing was actually updated so that it gets recording in the
                        // traces. We don't probably care about this as a metric, so leave it out
                        // for now.
                        n_docs_changed = Some(0);
                    }
                }
            }

            if let Some(n) = n_docs_changed {
                if let Some(span) = &mut client_request.span {
                    span.set_attribute(KeyValue::new("db.documents_changed", n as i64));
                }
                if client_request.is_collection_op() {
                    DOCUMENTS_CHANGED_TOTAL
                        .with_label_values(&self.label_values(client_request))
                        .observe(f64::from(n.abs()));
                }
            }

            // Span management for the cursor operations.
            if let Some(cursor_id) = section.get_i64("cursor_id") {
                if cursor_id == 0 {
                    // So this is the last batch in this cursor, we need to remove the parent trace
                    // from the parent trace map to prevent leaks.
                    //
                    // Note: To be on the safe side we're always removing, even though not all
                    // getMore's actually have a span
                    if self.is_tracing_enabled() && client_request.cursor_id != 0 {
                        debug!("Removing parent trace for exhausted cursor server_addr={}, cursor_id={}",
                            self.server_addr_sa, client_request.cursor_id);
                        let mut trace_mapper = self.app.trace_mapper.lock().unwrap();

                        trace_mapper.remove(&(self.server_addr_sa, client_request.cursor_id));
                        CURSOR_TRACE_PARENT_HASHMAP_CAPACITY.set(trace_mapper.capacity() as f64);
                    }
                } else if client_request.op == "find" || client_request.op == "aggregate" {
                    // This is a response to the first call of a cursor operation. If it was traced
                    // we take the requests' span context and associate it with cursor id so that
                    // subsequent getMore operations can follow spans from it.
                    //
                    // Note that we will let the initial span to go out of scope after observing it
                    // so that the span gets reported promptly. The subsequent getMore operations
                    // will each be reported in their own span that is the child of the initial
                    // "find" operation.
                    //
                    // Note: For a find() operation without limit, MongoDb will not immediately
                    // close the cursor even if the find immediately returns all the documents.
                    // Instead it expects the app to do a "getMore" and this is when we remove
                    // the entry from the "trace parent" HashMap.
                    //
                    // XXX: If the application never does a getMore we will be leaking memory.
                    //
                    if let Some(span) = &client_request.span {
                        debug!("Saving parent trace for cursor_id={}", cursor_id);
                        let mut trace_mapper = self.app.trace_mapper.lock().unwrap();

                        let cx = opentelemetry::Context::current_with_span(span.clone());

                        trace_mapper.insert((self.server_addr_sa, cursor_id), cx);
                        CURSOR_TRACE_PARENT_HASHMAP_CAPACITY.set(trace_mapper.capacity() as f64);
                    }
                } else if client_request.op != "getMore" {
                    warn!("operation={}, but cursor_id is set: {}", client_request.op, cursor_id);
                }
            }
        }
    }

    fn try_parsing_replicaset(&mut self, doc: &Document) {
        if let Some(op) = doc.get_str("op") {
            if (op == "hosts") || (op == "helloOk") {
                if let Some(replicaset) = doc.get_str("replicaset") {
                    self.replicaset = replicaset.to_owned();
                }
                if let Some(server_host) = doc.get_str("server_host") {
                    self.server_host = server_host.to_owned();
                }
            }
        }
    }

}
