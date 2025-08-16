use std::{io, sync::Arc};
use tokio::net::UdpSocket;

pub mod reliable_broadcast;


#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Identifier {
    sender: u16,  // j
    sequence: u64  // s
}


impl Identifier {
    pub fn new(sender: u16, sequence: u64) -> Self {
        Self { sender, sequence }
    }

    fn to_bytes(&self) -> [u8; 10] {
        let mut result: [u8; 10] = [0; 10];
        result[0..2].copy_from_slice(&self.sender.to_be_bytes());
        result[2..10].copy_from_slice(&self.sequence.to_be_bytes());
        result
    }

    fn from_bytes(bytes: &[u8; 10]) -> Self {
        Self {
            sender: u16::from_be_bytes(bytes[0..2].try_into().unwrap()),
            sequence: u64::from_be_bytes(bytes[2..10].try_into().unwrap()),
        }
    }
}


pub struct Instance {
    pub reliable_broadcast_instances: reliable_broadcast::Instance
}


impl Instance {
    pub fn new(id: Identifier) -> Self {
        Self {
            reliable_broadcast_instances: reliable_broadcast::Instance::new(id)
        }
    }
}



#[derive(Debug, Clone)]
pub enum MessageType {
    ReliableBroadcast(reliable_broadcast::types::ReliableBroadcastMessage),
}

impl MessageType {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            MessageType::ReliableBroadcast(msg) => msg.to_bytes(),
        }
    }
    
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid message: length < 1"))
        }
        match bytes[0] {
            reliable_broadcast::RBC_IDENTIFIER => Ok(MessageType::ReliableBroadcast(
                reliable_broadcast::types::ReliableBroadcastMessage::from_bytes(bytes)?
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                format!("Unknown protocol ID: {}", bytes[0])
            )),
        }
    }
}


#[derive(Debug, Clone)]
pub struct Message {
    pub id: Identifier,
    pub payload: MessageType,
}

impl Message {
    pub fn new(id: Identifier, payload: MessageType) -> Self {
        Self { id, payload }
    }
    
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.id.to_bytes());
        result.extend_from_slice(&self.payload.to_bytes());
        result
    }
    
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < 10 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "Message too short"
            ));
        }

        let id = Identifier::from_bytes(bytes[0..10].try_into().unwrap());
        let payload = MessageType::from_bytes(&bytes[10..])?;
        
        Ok(Message::new(id, payload))
    }
}


pub async fn broadcast(destinations: &[String], message: &Message, socket: Arc<UdpSocket>) {
    let message_bytes = message.to_bytes();
    for dest in destinations {
        _ = socket.send_to(&message_bytes, dest).await
    }
}
