use mongodb::{MsgHeader, MongoMessage};

enum CodecState {
    Initial,
    HaveHeader,
    Broken,
}

pub struct MongoDecoder {
    state: CodecState,
    buf: Vec<u8>,
    hdr: Option<MsgHeader>,
}

impl MongoDecoder {

    fn next_message(&mut self, buf: &[u8]) -> Option<(MsgHeader, MongoMessage)> {
        self.buf.extend_from_slice(&buf);
        
        match self.state {
            CodecState::Initial => {
                if self.buf.len() >= mongodb::HEADER_LENGTH {
                    self.state = CodecState::HaveHeader;
                    // TODO: read the header
                    None
                } else {
                    None
                }
            },
            CodecState::HaveHeader => {
                if let Some(ref hdr) = self.hdr {
                    if self.buf.len() >= hdr.message_length {
                        // TODO: Advance the buffer
                        self.state = CodecState::Initial;
                    }
                } else {
                    self.state = CodecState::Broken;
                }
                None
            },
            CodecState::Broken => {
                return None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decoder() {
    }
}
