pub const MESSAGE_BUFFER_SIZE: usize = 2048;
pub const CHANNEL_BUFFER_SIZE: usize = 2048;


pub const SIGNATURE_SIZE: usize = 64;
pub const PRIVATE_KEY_SIZE: usize = 32;
pub const DIGEST_SIZE: usize = 32;
pub const IDENTIFIER_SIZE: usize = 10;
pub const HEADER_SIZE: usize = IDENTIFIER_SIZE + 2;
pub const MIN_MESSAGE_SIZE: usize = HEADER_SIZE + SIGNATURE_SIZE;
