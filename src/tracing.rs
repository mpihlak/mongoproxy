use std::thread;
use std::collections::HashMap;
use log::{info,debug};

use rustracing::{self,sampler::AllSampler,span::SpanContext};
use rustracing::carrier::ExtractFromTextMap;
use rustracing_jaeger::{Tracer};


lazy_static! {
    static ref GLOBAL_TRACER: Tracer = {
        let (span_tx, span_rx) = crossbeam_channel::unbounded();

        thread::spawn(move || {
            for span in span_rx {
                info!("# SPAN: {:?}", span);
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