use std::error::Error;
use mongodb::{MsgHeader, MongoMessage};

pub struct MongoDecoder {
}

impl MongoDecoder {

    // Add this bunch of bytes to our buffer and return all the Mongo messages
    // we can parse from that.
    //
    // Need to support multiple messages as we might get a bunch of them in the
    // same buffer.
    //
    fn get_messages(&mut self, buf: &[u8]) -> Result<Vec<(MsgHeader, MongoMessage)>, Box<dyn Error>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decoder() {
    }
}
