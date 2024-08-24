use std::sync::{Arc,Mutex};

use crate::tracker::CursorTraceMapper;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub tracing_enabled: bool,
    pub trace_mapper: Arc<Mutex<CursorTraceMapper>>,
    pub log_mongo_messages: bool,
}

impl AppConfig {

    pub fn new(tracing_enabled: bool, log_mongo_messages: bool) -> Self {
        AppConfig {
            tracing_enabled,
            trace_mapper: Arc::new(Mutex::new(CursorTraceMapper::new())),
            log_mongo_messages,
        }
    }
}
