#!/usr/bin/env python3

import os
import sys
import logging
import random
import time
import pymongo
import requests

from prometheus_client.parser import text_string_to_metric_families

APPNAME = "mongoproxy-i9n-test"
NUM_KITTENS = 110
TRACE_WAIT_SECONDS = 30

MONGODB_URL = os.getenv('MONGODB_URL', f"mongodb://localhost:27111/?appName={APPNAME}")
METRICS_URL = os.getenv('METRICS_URL', "http://localhost:9898/metrics")
JAEGER_URL = os.getenv('JAEGER_URL', "http://localhost:16686")

failures = []


def fail(msg):
    logging.error(f"FAIL {msg}")
    failures.append(msg)

def get_metrics_snapshot():
    """Get a snapshot of Prometheus metrics from Mongoproxy. Return a dict of Metric's, keyed by name"""
    res = requests.get(METRICS_URL)
    metric_families = text_string_to_metric_families(res.text)
    metrics_by_name = dict()
    for m in metric_families:
        metrics_by_name[m.name] = m
    return metrics_by_name

def find_sample(metric, suffix, labels):
    """Find a sample where all the provided labels match"""
    full_sample_name = f"{metric.name}{suffix}"
    for sample in metric.samples:
        found = sample.name == full_sample_name
        for k, v in labels.items():
            if sample.labels.get(k, None) != v:
                found = False
                break
        if found:
            return sample
    return None

def find_sample_by_labels(name, label_suffix, labels, metrics):
    """Find a sample for the specified metric that matches the labels"""
    if name not in metrics:
        return None
    m = metrics[name]
    return find_sample(m, label_suffix, labels)

def get_metrics_delta(metric_name, label_suffix, labels, before_metrics, after_metrics):
    """Calculate the difference between 2 samples"""
    s1 = find_sample_by_labels(metric_name, label_suffix, labels, before_metrics)
    s2 = find_sample_by_labels(metric_name, label_suffix, labels, after_metrics)

    if not s2:
        logging.error(f"Required metric/label combination not found: {metric_name}/{labels}")
        return None

    #logging.info(f"Before: {s1}")
    #logging.info(f"After: {s2}")

    # The labels first appear with the first matching operation
    return s2.value - (s1.value if s1 else 0)

class MetricsDelta(object):

    def __init__(self, name, metric_name, suffix):
        self.name = name
        self.metric_name = metric_name
        self.suffix = suffix
        self.m_before = None
        self.m_after = None

    def __enter__(self):
        self.m_before = get_metrics_snapshot()
        return self
    def __exit__(self, *exc_args):
        pass
    def assert_metric_value(self, labels, expected_val):
        if not self.m_after:
            self.m_after = get_metrics_snapshot()

        val = get_metrics_delta(self.metric_name, self.suffix, labels, self.m_before, self.m_after)
        if expected_val != val:
            fail(f"{self.name}: val={val}, expected={expected_val}")
        else:
            logging.info(f"{self.name}: OK")

class TraceParent(object):
    """A client side span whose id is passed to Mongoproxy in the $comment field"""

    def __init__(self, fmt, sampled=True):
        self.fmt = fmt
        self.sampled = sampled
        if fmt == "uber-trace-id":
            # Legacy Jaeger clients print the ids without leading zeros. Keep the top bits zero
            # so that Mongoproxy always has to pad them.
            trace_id, span_id = random.getrandbits(60), random.getrandbits(60)
            self.comment = f"uber-trace-id:{trace_id:x}:{span_id:x}:0:{1 if sampled else 0}"
        else:
            trace_id, span_id = random.getrandbits(128), random.getrandbits(64)
            self.comment = f"traceparent:00-{trace_id:032x}-{span_id:016x}-{'01' if sampled else '00'}"
        self.trace_id = f"{trace_id:032x}"
        self.span_id = f"{span_id:016x}"

def get_trace_spans(trace_id):
    """Return the spans of a trace from Jaeger as a list of (service_name, span) tuples"""
    res = requests.get(f"{JAEGER_URL}/api/v3/traces/{trace_id}")
    if res.status_code == 404:
        return []
    res.raise_for_status()
    spans = []
    for rs in res.json()["result"]["resourceSpans"]:
        attrs = {a["key"]: a["value"].get("stringValue") for a in rs["resource"]["attributes"]}
        for ss in rs["scopeSpans"]:
            for span in ss["spans"]:
                spans.append((attrs.get("service.name"), span))
    return spans

def wait_for_trace_spans(trace_id, count, deadline):
    while True:
        spans = get_trace_spans(trace_id)
        if len(spans) >= count or time.time() > deadline:
            return spans
        time.sleep(1)

def check_traces(expected_traces):
    """Check that Mongoproxy created a span for every sampled parent, and none for unsampled"""
    deadline = time.time() + TRACE_WAIT_SECONDS

    for name, parent, ops in expected_traces:
        if not parent.sampled:
            continue
        spans = wait_for_trace_spans(parent.trace_id, len(ops), deadline)
        span_ids = {}
        for op in ops:
            found = [s for svc, s in spans if svc == "mongoproxy" and s["name"] == op]
            if len(found) != 1:
                fail(f"{name} ({parent.fmt}): expected one '{op}' span in trace {parent.trace_id}, got {len(found)}")
                continue
            span_ids[op] = found[0]["spanId"]
            # getMore follows from the span of the initial find
            expected_parent = span_ids.get("find") if op == "getMore" else parent.span_id
            if found[0].get("parentSpanId") != expected_parent:
                fail(f"{name} ({parent.fmt}): '{op}' parent={found[0].get('parentSpanId')}, expected {expected_parent}")
        if len(spans) > len(ops):
            fail(f"{name} ({parent.fmt}): expected {len(ops)} spans in trace {parent.trace_id}, got {len(spans)}")
        elif len(span_ids) == len(ops):
            logging.info(f"{name} ({parent.fmt}) trace: OK")

    # By now the batches containing the sampled spans have been exported
    for name, parent, _ in expected_traces:
        if parent.sampled:
            continue
        spans = get_trace_spans(parent.trace_id)
        if spans:
            fail(f"{name} ({parent.fmt}): unsampled parent produced {len(spans)} spans")
        else:
            logging.info(f"{name} ({parent.fmt}) not traced: OK")

def run_queries(kittens, fmt, expected_traces):
    """Run queries with the trace parent in $comment, record the spans Mongoproxy should create"""

    # Generate some seed data
    kittens.delete_many({})
    for i in range(NUM_KITTENS):
        kittens.insert_one({"name": "Purry", "id": i})

    # XXX: Make this a cmdline option
    test_big_documents = False

    if test_big_documents:
        # Insert one that is close to MongoDb limit
        kittens.insert_one({"name": "Spot", "s": 'Meow'*1024*1023*4})
    else:
        kittens.insert_one({"name": "Spot", "s": 'Meow'})

    kitten_labels = {
        'app': 'mongoproxy-i9n-test',
        'collection': 'kittens',
        'db': 'test',
    }

    def trace_parent(name, ops, sampled=True):
        parent = TraceParent(fmt, sampled)
        expected_traces.append((name, parent, ops))
        return parent.comment

    # Fetch one document
    with MetricsDelta('FindOne', 'mongoproxy_documents_returned_total', '_sum') as md:
        for c in kittens.find({"name": "Spot"}).comment(trace_parent(md.name, ["find"])):
            pass
        md.assert_metric_value({'op': 'find', **kitten_labels}, 1)

    # Simple "find" operation that returns nothing
    with MetricsDelta('FindNone', 'mongoproxy_documents_returned_total', '_sum') as md:
        for c in kittens.find({"name": "Spotty"}).comment(trace_parent(md.name, ["find"])):
            pass
        md.assert_metric_value({'op': 'find', **kitten_labels}, 0)

    # Not sampled by the client, so Mongoproxy should not create spans either
    with MetricsDelta('FindNotSampled', 'mongoproxy_documents_returned_total', '_sum') as md:
        for c in kittens.find({"name": "Spot"}).comment(trace_parent(md.name, ["find"], sampled=False)):
            pass
        md.assert_metric_value({'op': 'find', **kitten_labels}, 1)

    # "find" with a following "getMore"
    with MetricsDelta('FindAndGetMore', 'mongoproxy_documents_returned_total', '_sum') as md:
        count = 0
        comment = trace_parent(md.name, ["find", "getMore"])
        for c in kittens.find({"name": "Purry"}).limit(1000).comment(comment):
            count += 1

        # expect to have a single 101 row find followed by a getMore for the remainder
        md.assert_metric_value({'op': 'find', **kitten_labels}, 101)
        md.assert_metric_value({'op': 'getMore', **kitten_labels}, NUM_KITTENS - 101)

    # Update one document
    with MetricsDelta('UpdateOne', 'mongoproxy_documents_changed_total', '_sum') as md:
        kittens.update_one(
            {"name": "Purry", "id": 0, "$comment": trace_parent(md.name, ["update"]) },
            {"$set": { "name": "Furry" }}
        )
        md.assert_metric_value({'op': 'update', **kitten_labels}, 1)

    # Update one document - should be a NOP
    with MetricsDelta('UpdateNone', 'mongoproxy_documents_changed_total', '_sum') as md:
        kittens.update_one(
            {"name": "Purry", "id": 0, "$comment": trace_parent(md.name, ["update"]) },
            {"$set": { "name": "Furry" }}
        )
        md.assert_metric_value({'op': 'update', **kitten_labels}, 0)

    # Delete one document
    with MetricsDelta('DeleteOne', 'mongoproxy_documents_changed_total', '_sum') as md:
        kittens.delete_one({"name": "Furry", "$comment": trace_parent(md.name, ["delete"]) })
        md.assert_metric_value({'op': 'delete', **kitten_labels}, 1)

def main():
    log_level = logging.INFO
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    con = pymongo.MongoClient(MONGODB_URL)
    db = con['test']
    kittens = db['kittens']

    expected_traces = []
    for fmt in ["uber-trace-id", "traceparent"]:
        logging.info(f"Running queries with {fmt} in $comment")
        run_queries(kittens, fmt, expected_traces)

    check_traces(expected_traces)

    if failures:
        logging.error(f"{len(failures)} checks failed")
        sys.exit(1)
    logging.info("All checks passed")

if __name__ == "__main__":
    main()
