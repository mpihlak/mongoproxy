use std::thread;
use std::collections::HashMap;
use log::{warn,info,debug};

use rustracing::{self,sampler::AllSampler,span::SpanContext,carrier::ExtractFromTextMap};
use rustracing_jaeger::{Tracer,reporter::JaegerCompactReporter};


lazy_static! {

    // TODO: This can't be thread safe ... can it
    static ref GLOBAL_TRACER: Tracer = {
        let (span_tx, span_rx) = crossbeam_channel::unbounded();

        let reporter = JaegerCompactReporter::new("mongoproxy").unwrap();

        thread::spawn(move || {
            for span in span_rx {
                info!("# SPAN: {:?}", span);
                match reporter.report(&[span]) {
                    Ok(_) => {
                        info!("Sent to collector");
                    },
                    Err(e) => {
                        warn!("Failed to report span: {}", e);
                    },
                }
            }
        });

        // TODO: make the sampling strategy configurable.
        Tracer::with_sender(AllSampler, span_tx)
    };
}

// Initialize the global tracer and start the thread that writes the spans to Jaeger
pub fn initialize(service_name: &str, jaeger_addr: &str) {
    info!("Initializing tracer with service name {}, agent address: {}",
        service_name, jaeger_addr);

    // TODO: Actually initialize the global tracer
}

pub fn global_tracer() -> Option<&'static Tracer> {
    lazy_static::initialize(&GLOBAL_TRACER);
    Some(&GLOBAL_TRACER)
}

// Extract the span from a text map
pub fn extract_from_text<T>(span_text: &str) -> rustracing::Result<Option<SpanContext<T>>>
    where T: ExtractFromTextMap<HashMap<String,String>>
{
    const TRACE_ID_PREFIX: &str = "uber-trace-id";

    // For now expect that the trace is something like "uber-trace-id:1232132132:323232:1"
    // No spaces, quotation marks or other funny stuff.
    if span_text.starts_with(TRACE_ID_PREFIX) && span_text.len() > TRACE_ID_PREFIX.len() {
        let mut text_map = HashMap::new();
        debug!("trace-id-prefix: {}", TRACE_ID_PREFIX);
        debug!("trace-id: {}", &span_text[TRACE_ID_PREFIX.len()+1..]);
        text_map.insert(
            TRACE_ID_PREFIX.to_string(),
            span_text[TRACE_ID_PREFIX.len()+1..].to_string()
        );
        SpanContext::extract_from_text_map(&text_map)
    } else {
        Ok(None)
    }
}