use std::{collections::HashSet, io};

use crate::Identifier;


#[derive(PartialEq, Eq)]
pub struct Instance {
    pub id: Identifier,
    pub my_id: u16,
    pub message: Option<Vec<u8>>,
    pub digest: Option<[u8; 32]>,
    pub echo_messages: HashSet<u16>,
    pub ready_messages: HashSet<u16>,
}


impl Instance {
    pub fn new(id: Identifier, my_id: u16) -> Self {
        Self {
            id, my_id, message: None,
            digest: None,
            echo_messages: HashSet::new(),
            ready_messages: HashSet::new(),
        }
    }
}



// Protocol Identifier
pub const RBC_IDENTIFIER: u8 = 0;

// Message Types
const MSG_BROADCAST: u8 = 0;
const MSG_SEND: u8 = 1;
const MSG_ECHO: u8 = 2;
const MSG_READY: u8 = 3;
const MSG_REQUEST: u8 = 4;
const MSG_ANSWER: u8 = 5;

const DIGEST_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq)]
pub enum ReliableBroadcastMessage {
    Broadcast(Vec<u8>),
    Send(Vec<u8>),
    Echo([u8; DIGEST_SIZE]),
    Ready([u8; DIGEST_SIZE]),
    Request,
    Answer(Vec<u8>),
} 


impl ReliableBroadcastMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.push(RBC_IDENTIFIER);
        
        match self {
            Self::Broadcast(msg) => {
                result.push(MSG_BROADCAST);
                result.extend_from_slice(msg);
            }
            Self::Send(msg) => {
                result.push(MSG_SEND);
                result.extend_from_slice(msg);
            }
            Self::Echo(digest) => {
                result.push(MSG_ECHO);
                result.extend_from_slice(digest);
            }
            Self::Ready(digest) => {
                result.push(MSG_READY);
                result.extend_from_slice(digest);
            }
            Self::Request => {
                result.push(MSG_REQUEST);
            }
            Self::Answer(msg) => {
                result.push(MSG_ANSWER);
                result.extend_from_slice(msg);
            }
        }
        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "Invalid message: length < 2"
            ));
        }
        
        if bytes[0] != RBC_IDENTIFIER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "Invalid message: not a Reliable Broadcast message"
            ));
        }

        match bytes[1] {
            MSG_BROADCAST => Ok(Self::Broadcast(bytes[2..].to_vec())),
            MSG_SEND => Ok(Self::Send(bytes[2..].to_vec())),
            MSG_ECHO => {
                if bytes.len() != 2 + DIGEST_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        format!("Invalid ECHO message: expected {} bytes", 2 + DIGEST_SIZE)
                    ));
                }
                let digest = bytes[2..2 + DIGEST_SIZE]
                    .try_into()
                    .map_err(|_| io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "Failed to parse ECHO digest"
                    ))?;
                Ok(Self::Echo(digest))
            }
            MSG_READY => {
                if bytes.len() != 2 + DIGEST_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        format!("Invalid READY message: expected {} bytes", 2 + DIGEST_SIZE)
                    ));
                }
                let digest = bytes[2..2 + DIGEST_SIZE]
                    .try_into()
                    .map_err(|_| io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "Failed to parse READY digest"
                    ))?;
                Ok(Self::Ready(digest))
            }
            MSG_REQUEST => {
                if bytes.len() != 2 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "Invalid REQUEST message: expected 2 bytes"
                    ));
                }
                Ok(Self::Request)
            }
            MSG_ANSWER => Ok(Self::Answer(bytes[2..].to_vec())),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                format!("Unknown message type: {}", bytes[1])
            )),
        }
    }
}
