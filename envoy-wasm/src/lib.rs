mod codec;

use log::info;
use proxy_wasm::{self, traits::{Context, StreamContext}};
use proxy_wasm::types::{LogLevel, Action, PeerType};
use mongodb::debug_fmt;


#[no_mangle]
pub fn _start() {
    proxy_wasm::set_log_level(LogLevel::Trace);
    proxy_wasm::set_stream_context(|context_id, root_context_id| -> Box<dyn StreamContext> {
        info!("_start for context {}", context_id);
        Box::new(MongoDbFilter{
            context_id,
            root_context_id,
        })
    });
}

struct MongoDbFilter {
    context_id: u32,
    root_context_id: u32,
}

impl Context for MongoDbFilter {}

impl StreamContext for MongoDbFilter {

    fn on_new_connection(&mut self) -> Action {
        info!("ctx {}: new connection: root={}", self.context_id, self.root_context_id);
        Action::Continue
    }

    fn on_downstream_data(&mut self, data_size: usize, end_of_stream: bool) -> Action {
        info!("ctx {}: Downstream data: data_size={}, end={}", self.context_id, data_size, end_of_stream);
        if let Some(data) = self.get_downstream_data(0, data_size) {
            info!("ctx {}: data\n{}\n", self.context_id, debug_fmt(&data));
        } else {
            info!("ctx {}: no data :(", self.context_id);
        }
        Action::Continue
    }

    fn on_downstream_close(&mut self, _peer_type: PeerType) {
        info!("ctx {}: Downstream closed", self.context_id);
    }

    fn on_upstream_data(&mut self, data_size: usize, end_of_stream: bool) -> Action {
        info!("ctx {}: Upstream data: data_size={}, end={}", self.context_id, data_size, end_of_stream);
        if let Some(data) = self.get_upstream_data(0, data_size) {
            info!("ctx {}: data\n{}\n", self.context_id, debug_fmt(&data));
        } else {
            info!("ctx {}: no data :(", self.context_id);
        }
        Action::Continue
    }

    fn on_upstream_close(&mut self, _peer_type: PeerType) {
        info!("ctx {}: Upstream connection closed", self.context_id);
    }

    fn on_log(&mut self) {
        info!("ctx {}: on_log called", self.context_id);
    }
}
