import logging
import time
import pymongo

from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opentelemetry import trace, baggage
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator


if __name__ == "__main__":
    log_level = logging.DEBUG
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    resource = Resource(attributes={
        SERVICE_NAME: "python-tracing-client"
    })

    traceProvider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="localhost:4317", insecure=True))
    traceProvider.add_span_processor(processor)
    trace.set_tracer_provider(traceProvider)

    tracer = trace.get_tracer("tracing.client")

    con = pymongo.MongoClient("mongodb://localhost:27111/?appName=tracing_client")
    db = con.test
    coll = db.kittens

    def span_as_text(span):
        headers = {}
        TraceContextTextMapPropagator().inject(headers)
        print(headers)
        return ''.join(["%s:%s" % (k,v) for k, v in headers.items()])

    coll = con['test']['kittens']

    with tracer.start_as_current_span('Trace operations') as root_span:
        print("Cleaning up")
        with tracer.start_as_current_span('Delete data') as span:
            span_text = span_as_text(span)
            coll.delete_many({ "$comment": span_text })

        print("Inserting")
        with tracer.start_as_current_span('Insert data') as span:
            span_text = span_as_text(span)
            for i in range(110):
                # Insert does not actually take a $comment so we just skip
                # it's not that interesting op anyway.
                coll.insert_one({"name": "Purry", "number": i })

        print("Fetching exactly 101")
        with tracer.start_as_current_span('Fetching 101') as span:
            span_text = span_as_text(span)
            # Note: this uses limit() which is not exactly the same as fetching
            # from a collection that has exactly 101 documents. In that case the
            # "find" would actually be followed by empty "getMore"
            for r in coll.find({}).comment(span_text).limit(101):
                pass

        print("Fetching All")
        with tracer.start_as_current_span('Fetching All') as span:
            span_text = span_as_text(span)
            for r in coll.find({}).comment(span_text):
                pass

        print("Fetching with batch size")
        with tracer.start_as_current_span('Small batch fetch') as span:
            span_text = span_as_text(span)
            for r in coll.find({}).batch_size(50).comment(span_text):
                pass

        print("Enormous query")
        with tracer.start_as_current_span('Enormous query') as span:
            span_text = span_as_text(span)
            for r in coll.find({"name": "x" * 1024*16}).comment(span_text):
                pass

        print("Update")
        with tracer.start_as_current_span('Update') as span:
            span_text = span_as_text(span)
            coll.update_many({ "name": "Purry", "$comment": span_text },
                            { "$set": { "foo": 1 } })

        print("Aggregate query")
        with tracer.start_as_current_span('Aggregate fetch') as span:
            span_text = span_as_text(span)
            for r in coll.aggregate([
                { "$match": { "name": "Purry", "$comment": span_text } },
                { "$group": { "_id": "$number", "total": { "$sum": "$number" } } },
                { "$sort": { "total": -1 } } ]):
                pass
