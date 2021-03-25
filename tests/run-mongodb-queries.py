#!/usr/bin/env python3

import os
import logging
import time
import pymongo
import requests

import jaeger_client
from opentracing.propagation import Format
from prometheus_client.parser import text_string_to_metric_families

APPNAME = "mongoproxy-i9n-test"
NUM_KITTENS = 110
JAEGER_COLLECTION_DELAY_SECONDS = 3

MONGODB_URL = os.getenv('MONGODB_URL', f"mongodb://localhost:27111/?appName={APPNAME}")
METRICS_URL = os.getenv('METRICS_URL', "http://localhost:9898/metrics")


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

    if not s1 or not s2:
        logging.error(f"Required metric/label combination not found: {metric_name}/{labels}")
        return None

    #logging.info(f"Before: {s1}")
    #logging.info(f"After: {s2}")

    return s2.value - s1.value

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
            logging.error(f"{self.name}: FAIL (val={val}, expected={expected_val})")
        else:
            logging.info(f"{self.name}: OK")

def main():
    log_level = logging.INFO
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    config = jaeger_client.Config(
        config={
            'sampler': { 'type': 'const', 'param': 1, },
            'logging': False,
            'reporter_batch_size': 1,
        },
        service_name=APPNAME,
        validate=True,
    )

    tracer = config.initialize_tracer()

    con = pymongo.MongoClient(MONGODB_URL)
    db = con['test']
    kittens = db['kittens']

    # Generate some seed data
    kittens.delete_many({})
    for i in range(NUM_KITTENS):
        kittens.insert_one({"name": "Purry", "i": i})

    def span_as_text(span):
        """Format the span as "uber-trace-id" text map that Mongoproxy understands"""
        text_map = {}
        tracer.inject(span_context=span, format=Format.TEXT_MAP, carrier=text_map)
        return ''.join(["%s:%s" % (k,v) for k, v in text_map.items()])

    common_labels = {
        'app': 'mongoproxy-i9n-test',
        'collection': 'kittens',
        'db': 'test',
    }

    with tracer.start_span('Trace those cats') as root_span:

        # Simple "find" operation that returns nothing
        with MetricsDelta('FetchOne', 'mongoproxy_documents_returned_total', '_sum') as md:
            with tracer.start_span(md.name, root_span) as span:
                kittens.find({"name": "Spotty"}).comment(span_as_text(span))
            md.assert_metric_value({'op': 'find', **common_labels}, 0)

        # "find" with a following "getMore"
        with MetricsDelta('FetchAll', 'mongoproxy_documents_returned_total', '_sum') as md:
            with tracer.start_span(md.name, root_span) as span:
                count = 0
                for c in kittens.find({"name": "Purry"}).limit(1000).comment(span_as_text(span)):
                    count += 1
                    # XXX: Sleep some to increase the chances of find() to be processed before getMore()
                    time.sleep(0.01)
                print(f"Found {count} kittens.")

            # expect to have a single 101 row find followed by a getMore for the remainder
            md.assert_metric_value({'op': 'find', **common_labels}, 101)
            md.assert_metric_value({'op': 'getMore', **common_labels}, NUM_KITTENS - 101)

        # Update one document
        with MetricsDelta('UpdateOne', 'mongoproxy_documents_changed_total', '_sum') as md:
            with tracer.start_span(md.name, root_span) as span:
                kittens.update_one(
                    {"name": "Purry", "$comment": span_as_text(span) },
                    {"$set": { "name": "Furry" }}
                )
            md.assert_metric_value({'op': 'update', **common_labels}, 1)

        # Delete one document
        with MetricsDelta('DeleteOne', 'mongoproxy_documents_changed_total', '_sum') as md:
            with tracer.start_span(md.name, root_span) as span:
                kittens.delete_one({"name": "Furry", "$comment": span_as_text(span) })
            md.assert_metric_value({'op': 'delete', **common_labels}, 1)

    tracer.close()
    # TODO: Validate the Jaeger traces, once uploaded to collector

if __name__ == "__main__":
    main()
