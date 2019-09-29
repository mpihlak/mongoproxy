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
        service_name='python_tracing_client',
        validate=True,
    )

    tracer = config.initialize_tracer()

    con = pymongo.MongoClient("mongodb://localhost:27113/?appName=tracing_client")
    bigcollection = con['test']['bigcollection']

    def span_as_text(span):
        text_map = {}
        tracer.inject(span_context=span, format=Format.TEXT_MAP, carrier=text_map)
        return ''.join(["%s:%s" % (k,v) for k, v in text_map.iteritems()])

    print("Deleting")
    with tracer.start_span('Deleting data') as span:
        span_text = span_as_text(span)
        print("Injected span: %s" % span_text)
        for i in range(10):
            bigcollection.remove({
                 "a": "bbbbbbbbbbbbbbbbbbbb",
                 "$comment": span_text
            })

    print("Inserting")
    with tracer.start_span('Inserting data') as span:
        span_text = span_as_text(span)
        print("Injected span: %s" % span_text)
        for i in range(10):
            bigcollection.insert_one({
                 "a": "bbbbbbbbbbbbbbbbbbbb",
                 "b": "CCCCCCCCCCCCCCCCCC",
                 # Insert does not take $comment
            })

    print("Fetching")
    with tracer.start_span('Fetching data') as span:
        span_text = span_as_text(span)
        print("Injected span: %s" % span_text)
        for i in range(3):
            list(bigcollection.find({}).limit(1000).comment(span_text))

    time.sleep(2)
    tracer.close()
