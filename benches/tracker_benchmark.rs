#[macro_use]
extern crate criterion;
extern crate bson;
extern crate mongoproxy;

use std::io::Write;

use mongoproxy::mongodb::tracker::{MongoStatsTracker};
use mongoproxy::mongodb::parser::MongoProtocolParser;
use mongoproxy::mongodb::messages::{self,MsgHeader,MsgOpMsg};
use criterion::Criterion;


criterion_group!(benches, bench_tracker, bench_bson_parser);
criterion_main!(benches);

fn create_message(op: &str, op_value: &str, mut buf: impl Write) {
    let msg = MsgOpMsg{ flag_bits: 0, documents: Vec::new() };
    let mut doc = bson::Document::new();
    doc.insert(op.to_string(), bson::Bson::String(op_value.to_string()));

    // Add some meat to the request
    let comment_text = "X".repeat(4096);
    doc.insert("comment".to_string(), bson::Bson::String(comment_text));

    let mut doc_buf = Vec::new();
    bson::encode_document(&mut doc_buf, &doc).unwrap();
    let mut msg_buf = Vec::new();
    msg.write(&mut msg_buf, &doc_buf).unwrap();

    let hdr = MsgHeader {
        message_length: messages::HEADER_LENGTH + msg_buf.len(),
        request_id: 1234,
        response_to: 0,
        op_code: 2013,
    };

    hdr.write(&mut buf).unwrap();
    buf.write_all(&msg_buf).unwrap();
}

fn bench_tracker(c: &mut Criterion) {
    let mut client_buf = Vec::new();
    create_message("insert", "test.foo", &mut client_buf);

    let mut server_buf = Vec::new();
    create_message("n", "1", &mut server_buf);

    let mut parser = MongoProtocolParser::new();

    // Try 2 parses to validate that we have a complete message
    assert!(parser.parse_buffer(&client_buf).len() == 1);
    assert!(parser.parse_buffer(&client_buf).len() == 1);

    c.bench_function("parse_mongodb_message",
        |b| b.iter(|| { parser.parse_buffer(&client_buf) } ));

    let mut tracker = MongoStatsTracker::new("127.0.0.1", "127.0.0.2", None);

    c.bench_function("track_client_request",
        |b| b.iter(|| tracker.track_client_request(&client_buf)));

    c.bench_function("track_server_response",
        |b| b.iter(|| {
            tracker.track_client_request(&client_buf);
            tracker.track_server_response(&server_buf);
        })
    );
}

use bson::{Array,Bson,oid};
use mongoproxy::bson_lite::{FieldSelector,decode_document};

fn bench_bson_parser(c: &mut Criterion) {
    let mut doc = bson::Document::new();

    doc.insert("first".to_string(), Bson::String("foo".to_string()));
    doc.insert("ignore_this".to_string(), Bson::String("a".repeat(1024)));

    let mut subdoc = bson::Document::new();
    subdoc.insert("_field1".to_string(), Bson::String("a".repeat(1024)));
    subdoc.insert("_field2".to_string(), Bson::String("b".repeat(1024)));
    doc.insert("subdoc".to_string(), subdoc);

    let mut arr = Array::new();
    arr.push(Bson::String("blah".to_string()));
    arr.push(Bson::ObjectId(oid::ObjectId::with_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])));

    doc.insert("array".to_string(), Bson::Array(arr));
    doc.insert("last".to_owned(), bson::Bson::FloatingPoint(2.7));

    let mut buf = Vec::new();
    bson::encode_document(&mut buf, &doc).unwrap();

    let selector = FieldSelector::build()
        .with("first", "/@1")
        .with("array_len", "/array/[]")
        .with("last", "/last");

    c.bench_function("parse_bson_message",
        |b| b.iter(|| {
            let _doc = decode_document(&buf[..], &selector).unwrap();
        }
    ));
}
