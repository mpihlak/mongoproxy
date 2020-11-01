use std::sync::{Arc,Mutex};

use crate::jaeger_tracing::Tracer;
use crate::tracker::CursorTraceMapper;

#[derive(Clone,Debug)]
pub struct AppConfig {
    pub tracer: Option<Tracer>,
    pub trace_mapper: Arc<Mutex<CursorTraceMapper>>,
    pub log_mongo_messages: bool,
}

impl AppConfig {

    pub fn new(tracer: Option<Tracer>, log_mongo_messages: bool) -> Self {
        AppConfig {
            tracer,
            trace_mapper: Arc::new(Mutex::new(CursorTraceMapper::new())),
            log_mongo_messages,
        }
    }
}
