use std::thread;
use std::collections::HashMap;
use log::{warn,info,debug};

use rustracing::{self,sampler::AllSampler,span::SpanContext,carrier::ExtractFromTextMap};
use rustracing_jaeger::{Tracer,reporter::JaegerCompactReporter};


lazy_static! {
    static ref GLOBAL_TRACER: Tracer = {
        let (span_tx, span_rx) = crossbeam_channel::unbounded();

        // TODO:
        // * Only do this if Jaeger was actually requested
        // * Get the service name and tags from parameters
        // * Consider startup race with Jaeger agent and other transient init failures
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

        Tracer::with_sender(AllSampler, span_tx)
    };
}

/// Default registry (global static).
pub fn global_tracer() -> &'static Tracer {
    lazy_static::initialize(&GLOBAL_TRACER);
    &GLOBAL_TRACER
}

// Extract the span from a text map
pub fn extract_from_text<T>(span_text: &str) -> rustracing::Result<Option<SpanContext<T>>>
    where T: ExtractFromTextMap<HashMap<String,String>>
{
    const TRACE_ID_PREFIX: &str = "uber-trace-id";

    // For now expect that the trace is something like "uber-trace-id:1232132132:323232:1"
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