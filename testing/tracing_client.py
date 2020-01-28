import logging
import time
import pymongo

from opentracing.propagation import Format
from jaeger_client import Config

if __name__ == "__main__":
    log_level = logging.DEBUG
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    config = Config(
        config={
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'logging': True,
        },
        service_name='tracing_client',
        validate=True,
    )

    tracer = config.initialize_tracer()

    con = pymongo.MongoClient("mongodb://localhost:27113/?appName=tracing_client")
    coll = con['test']['kittens']

    def span_as_text(span):
        text_map = {}
        tracer.inject(span_context=span, format=Format.TEXT_MAP, carrier=text_map)
        return ''.join(["%s:%s" % (k,v) for k, v in text_map.iteritems()])

    coll = con['test']['kittens']

    with tracer.start_span('Trace operations') as root_span:
        print("Cleaning up")
        with tracer.start_span('Delete data', root_span) as span:
            span_text = span_as_text(span)
            coll.delete_many({ "$comment": span_text })

        print("Inserting")
        with tracer.start_span('Insert data', root_span) as span:
            span_text = span_as_text(span)
            for i in range(110):
                # Insert does not actually take a $comment so we just skip
                # it's not that interesting op anyway.
                coll.insert({"name": "Purry", "number": i })

        print("Fetching exactly 101")
        with tracer.start_span('Fetching 101', root_span) as span:
            span_text = span_as_text(span)
            # Note: this uses limit() which is not exactly the same as fetching
            # from a collection that has exactly 101 documents. In that case the
            # "find" would actually be followed by empty "getMore"
            for r in coll.find({}).comment(span_text).limit(101):
                pass

        print("Fetching All")
        with tracer.start_span('Fetching All', root_span) as span:
            span_text = span_as_text(span)
            for r in coll.find({}).comment(span_text):
                pass

        print("Fetching with batch size")
        with tracer.start_span('Small batch fetch', root_span) as span:
            span_text = span_as_text(span)
            for r in coll.find({}).batch_size(50).comment(span_text):
                pass

        print("Enormous query")
        with tracer.start_span('Enormous query', root_span) as span:
            span_text = span_as_text(span)
            for r in coll.find({"name": "x" * 1024*16}).comment(span_text):
                pass

        print("Aggregate query")
        with tracer.start_span('Aggregate fetch', root_span) as span:
            span_text = span_as_text(span)
            for r in coll.aggregate([
                { "$match": { "name": "Purry", "$comment": span_text } },
                { "$group": { "_id": "$number", "total": { "$sum": "$number" } } },
                { "$sort": { "total": -1 } } ]):
                pass

    time.sleep(2)
    tracer.close()
