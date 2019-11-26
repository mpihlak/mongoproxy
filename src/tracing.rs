use std::thread;
use std::collections::HashMap;
use std::net::{SocketAddr};

use log::{warn,info,debug};

use rustracing::{self,sampler::AllSampler,span::SpanContext,carrier::ExtractFromTextMap};
use rustracing_jaeger::{Tracer,reporter::JaegerCompactReporter};


// Initialize the tracer and start the thread that writes the spans to Jaeger.
// The tracer then needs to be cloned and passed to each thread.
pub fn init_tracer(enable_tracer: bool, service_name: &str, jaeger_addr: SocketAddr) -> Option<Tracer> {
    if !enable_tracer {
        info!("Tracing not enabled.");
        return None;
    }

    info!("Initializing tracer with service name {}, agent address: {}",
        service_name, jaeger_addr);

    let (span_tx, span_rx) = crossbeam_channel::unbounded();
    let service_name = service_name.to_owned();

    let mut reporter = JaegerCompactReporter::new(&service_name).unwrap();
    reporter.set_agent_addr(jaeger_addr).unwrap();

    thread::spawn(move || {
        for span in span_rx {
            debug!("# SPAN: {:?}", span);
            match reporter.report(&[span]) {
                Ok(_) => {
                    debug!("Sent to collector");
                },
                Err(e) => {
                    warn!("Failed to report span: {}", e);
                },
            }
        }
    });

    Some(Tracer::with_sender(AllSampler, span_tx))
}

// Extract the span from a text map
//
// This only returns Some if the span is sampled (flag bits 1 & 2 set). Otherwise
// we just ignore it as not to generate useless orphaned spans.
pub fn extract_from_text<T>(span_text: &str) -> rustracing::Result<Option<SpanContext<T>>>
    where T: ExtractFromTextMap<HashMap<String,String>>
{
    const TRACE_ID_PREFIX: &str = "uber-trace-id";

    // For now expect that the trace is something like "uber-trace-id:1232132132:323232:1"
    // No spaces, quotation marks or other funny stuff.
    //
    // TODO: Can we not look into the trace flags here and handle the trace/no-trace decision
    // at a higher level ...
    if span_text.starts_with(TRACE_ID_PREFIX) && span_text.len() > TRACE_ID_PREFIX.len() {
        let trace_data = &span_text[TRACE_ID_PREFIX.len()+1..];

        debug!("trace-id-prefix: {}", TRACE_ID_PREFIX);
        debug!("trace-id: {}", trace_data);

        let flags = trace_data.chars().last().unwrap_or('0');

        if flags == '1' || flags == '3' {
            let mut text_map = HashMap::new();
            text_map.insert(
                TRACE_ID_PREFIX.to_string(),
                trace_data.to_string()
            );
            SpanContext::extract_from_text_map(&text_map)
        } else {
            debug!("Trace not sampled, flags={}, ignoring", flags);
            Ok(None)
        }
    } else {
        Ok(None)
    }
}