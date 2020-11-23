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
                    None
                } else {
                    None
                }
            },
            CodecState::HaveHeader => {
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
