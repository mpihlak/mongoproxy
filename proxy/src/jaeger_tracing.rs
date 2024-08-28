use std::collections::HashMap;
use std::net::SocketAddr;

use tracing::{info,debug};

pub use opentelemetry::sdk::trace::Tracer;
pub use opentelemetry::trace::SpanContext;

use opentelemetry::global;
use opentelemetry_jaeger::{Uninstall, Propagator};

pub const TRACE_ID_PREFIX: &str = "uber-trace-id";

pub fn init_tracer(
    enable_tracer: bool,
    service_name: &str,
    jaeger_addr: SocketAddr
) -> (Option<Tracer>, Option<Uninstall>) {
    if !enable_tracer {
        info!("Tracing not enabled.");
        return (None, None);
    }

    info!("Initializing tracer with service name {}, agent address: {}",
        service_name, jaeger_addr);

    global::set_text_map_propagator(Propagator::new());

    let (tracer, uninstall) = opentelemetry_jaeger::new_pipeline()
        .from_env()
        .with_agent_endpoint(jaeger_addr)
        .with_service_name(service_name)
        .install()
        .unwrap();

    debug!("Initialized tracer: {:?} provider={:?}", tracer, tracer.provider());

    (Some(tracer), Some(uninstall))
}

// Extract the span from a text map
//
// This only returns Some if the span is sampled. Otherwise we just ignore it as not to generate
// useless orphaned spans.
pub fn extract_from_text(span_text: &str) -> Option<opentelemetry::Context>
{
    // For now expect that the trace is something like "uber-trace-id:1232132132:323232:1"
    // No spaces, quotation marks or other funny stuff.
    //
    if !span_text.starts_with(TRACE_ID_PREFIX) || span_text.len() < TRACE_ID_PREFIX.len()+1 {
        return None;
    }

    let trace_data = &span_text[TRACE_ID_PREFIX.len()+1..];
    debug!("trace-data: {}", trace_data);

    let mut text_map = HashMap::new();
    text_map.insert(
        TRACE_ID_PREFIX.to_string(),
        trace_data.to_string()
    );

    let parent_ctx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&text_map)
    });

    Some(parent_ctx)
}
