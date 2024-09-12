use std::collections::HashMap;

use tracing::debug;

pub use opentelemetry_sdk::trace::Tracer;
pub use opentelemetry::trace::SpanContext;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;

pub const JAEGER_TRACE_HEADER: &str = "uber-trace-id";
pub const W3C_TRACE_HEADER: &str = "traceparent";


// Extract the span from a text map
//
// For now expect that only minimal trace information is provided in the comment, just
// "traceparent:<ID>" or "uber-trace-id:<ID>"
//
// This only returns Some if the span is sampled. Otherwise we just ignore it as not to generate
// useless orphaned spans.
pub fn extract_from_text(span_text: &str) -> Option<opentelemetry::Context>
{
    debug!("Extracting trace from: {span_text}");

    if let Some((header, value)) = span_text.split_once(':') {
        let propagator: Box<dyn TextMapPropagator> = match header {
            JAEGER_TRACE_HEADER => {
                debug!("Jaeger trace incoming.");
                Box::new(opentelemetry_jaeger_propagator::Propagator::new())
            },
            W3C_TRACE_HEADER => {
                debug!("W3C compatible trace incoming");
                Box::new(TraceContextPropagator::new())
            },
            _ => return None,
        };

        let mut text_map = HashMap::new();
        text_map.insert(
            header.to_string(),
            value.to_string()
        );

        let parent_ctx = propagator.extract(&text_map);
        Some(parent_ctx)
    } else {
        None
    }
}
