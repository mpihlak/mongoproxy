use std::collections::HashMap;

use tracing::{info, debug};

pub use opentelemetry_sdk::trace::SdkTracer as Tracer;

use opentelemetry::Context;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;

pub const JAEGER_TRACE_HEADER: &str = "uber-trace-id";
pub const W3C_TRACE_HEADER: &str = "traceparent";

// Without an explicit endpoint the exporter uses OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, then
// OTEL_EXPORTER_OTLP_ENDPOINT, then http://localhost:4317.
pub fn init_tracer(
    enable_tracer: bool,
    service_name: &str,
    otlp_endpoint: Option<&str>,
) -> Result<Option<Tracer>, opentelemetry_otlp::ExporterBuildError> {
    if !enable_tracer {
        info!("Tracing not enabled.");
        return Ok(None);
    }

    info!("Initializing OTLP tracer with service name {}, endpoint: {}",
        service_name, otlp_endpoint.unwrap_or("<default>"));

    let mut exporter = opentelemetry_otlp::SpanExporter::builder().with_tonic();
    if let Some(endpoint) = otlp_endpoint {
        exporter = exporter.with_endpoint(with_default_scheme(endpoint));
    }

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter.build()?)
        .with_resource(Resource::builder().with_service_name(service_name.to_owned()).build())
        .build();

    Ok(Some(provider.tracer("mongoproxy")))
}

// The exporter assumes https for an endpoint without a scheme, and the proxy is built without
// TLS support. Treat "host:port" as plain http, the same as the old --jaeger-addr.
fn with_default_scheme(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

// Extract the parent context from a $comment string: "uber-trace-id:<id>" or "traceparent:<id>".
//
// Returns None for unrecognized or malformed ids, so that we don't start orphaned root spans.
pub fn extract_from_text(span_text: &str) -> Option<Context> {
    let (header, value) = span_text.split_once(':')?;
    debug!("Extracting trace from {header}: {value}");

    let span_context = match header {
        JAEGER_TRACE_HEADER => parse_uber_trace_id(value)?,
        W3C_TRACE_HEADER => {
            let carrier = HashMap::from([(header.to_string(), value.to_string())]);
            let cx = TraceContextPropagator::new().extract(&carrier);
            cx.span().span_context().clone()
        },
        _ => return None,
    };

    if !span_context.is_valid() {
        return None;
    }
    Some(Context::new().with_remote_span_context(span_context))
}

// Jaeger propagation format: {trace-id}:{span-id}:{parent-span-id}:{flags}, all hex.
// Jaeger clients print the ids without leading zeros, so shorter ids are zero padded. The
// parent span id is deprecated and ignored. Only the sampled bit of the flags maps to OTel.
fn parse_uber_trace_id(value: &str) -> Option<SpanContext> {
    let value = if value.contains(':') { value.to_string() } else { value.replace("%3A", ":") };
    let parts: Vec<&str> = value.split(':').collect();
    let [trace_id, span_id, _parent_span_id, flags] = parts[..] else {
        return None;
    };

    if trace_id.len() > 32 || span_id.len() > 16 {
        return None;
    }
    let trace_id = TraceId::from(u128::from_str_radix(trace_id, 16).ok()?);
    let span_id = SpanId::from(u64::from_str_radix(span_id, 16).ok()?);
    let flags = u8::from_str_radix(flags, 16).ok()?;
    let trace_flags = if flags & 0x01 != 0 { TraceFlags::SAMPLED } else { TraceFlags::default() };

    Some(SpanContext::new(trace_id, span_id, trace_flags, true, TraceState::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(text: &str) -> Option<SpanContext> {
        extract_from_text(text).map(|cx| cx.span().span_context().clone())
    }

    #[test]
    fn test_uber_trace_id() {
        let cases = [
            ("uber-trace-id:4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7:0:1",
                0x4bf92f3577b34da6a3ce929d0e0e4736u128, 0x00f067aa0ba902b7u64, true),
            ("uber-trace-id:6d697c0f076183c:6d697c0f076183c:0:1",
                0x6d697c0f076183c, 0x6d697c0f076183c, true),
            ("uber-trace-id:6d697c0f076183c%3A6d697c0f076183c%3A0%3A1",
                0x6d697c0f076183c, 0x6d697c0f076183c, true),
            ("uber-trace-id:6d697c0f076183c:6d697c0f076183c:0:3",
                0x6d697c0f076183c, 0x6d697c0f076183c, true),
            ("uber-trace-id:6d697c0f076183c:6d697c0f076183c:0:0",
                0x6d697c0f076183c, 0x6d697c0f076183c, false),
        ];

        for (text, trace_id, span_id, sampled) in cases {
            let sc = extract(text).unwrap_or_else(|| panic!("no context from {}", text));
            assert_eq!(sc.trace_id(), TraceId::from(trace_id), "{}", text);
            assert_eq!(sc.span_id(), SpanId::from(span_id), "{}", text);
            assert_eq!(sc.is_sampled(), sampled, "{}", text);
            assert!(sc.is_remote());
        }
    }

    #[test]
    fn test_traceparent() {
        let sc = extract("traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        assert_eq!(sc.trace_id(), TraceId::from(0x4bf92f3577b34da6a3ce929d0e0e4736u128));
        assert_eq!(sc.span_id(), SpanId::from(0x00f067aa0ba902b7u64));
        assert!(sc.is_sampled());

        let sc = extract("traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00").unwrap();
        assert!(!sc.is_sampled());
    }

    #[test]
    fn test_with_default_scheme() {
        assert_eq!(with_default_scheme("collector:4317"), "http://collector:4317");
        assert_eq!(with_default_scheme("http://collector:4317"), "http://collector:4317");
        assert_eq!(with_default_scheme("https://collector:4317"), "https://collector:4317");
    }

    #[test]
    fn test_invalid() {
        let cases = [
            "",
            "just a comment",
            "uber-trace-id",
            "uber-trace-id:",
            "uber-trace-id:6d697c0f076183c:6d697c0f076183c:0",
            "uber-trace-id:xyz:6d697c0f076183c:0:1",
            "uber-trace-id:0:6d697c0f076183c:0:1",
            "uber-trace-id:6d697c0f076183c:0:0:1",
            "uber-trace-id:14bf92f3577b34da6a3ce929d0e0e4736:6d697c0f076183c:0:1",
            "traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-01",
            "traceparent:00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "b3:4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
        ];

        for text in cases {
            assert!(extract(text).is_none(), "{}", text);
        }
    }
}
