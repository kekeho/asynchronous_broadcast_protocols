use std::{io, sync::Arc};
use ed25519::signature::SignerMut;
use tokio::net::UdpSocket;
use serde::Deserialize;

pub mod reliable_broadcast;
pub mod constants;

use constants::*;


#[derive(Deserialize, Clone)]
pub struct Config {
    pub my_id: u16,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Deserialize, Clone)]
pub struct NodeConfig {
    pub id: u16,
    pub address: String,
    pub privkey: [u8; 32],
}


impl Config {
    pub async fn load(filename: &str) -> io::Result<Config> {
        let config_data = tokio::fs::read_to_string(filename).await?;
        let config: Config = serde_json::from_str(&config_data)?;
        Ok(config)
    }

    /// 指定されたIDのノード設定を取得
    pub fn get_node(&self, id: u16) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// 自分のノード設定を取得
    pub fn get_my_node(&self) -> Option<&NodeConfig> {
        self.get_node(self.my_id)
    }

    /// 全ノードのアドレスリストを取得
    pub fn get_all_addresses(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.address.clone()).collect()
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Identifier {
    sender: u16,  // j
    sequence: u64  // s
}


impl Identifier {
    pub fn new(sender: u16, sequence: u64) -> Self {
        Self { sender, sequence }
    }

    fn to_bytes(&self) -> [u8; IDENTIFIER_SIZE] {
        let mut result: [u8; IDENTIFIER_SIZE] = [0; IDENTIFIER_SIZE];
        result[0..2].copy_from_slice(&self.sender.to_be_bytes());
        result[2..IDENTIFIER_SIZE].copy_from_slice(&self.sequence.to_be_bytes());
        result
    }

    pub fn from_bytes(bytes: &[u8; IDENTIFIER_SIZE]) -> Self {
        Self {
            sender: u16::from_be_bytes(bytes[0..2].try_into().unwrap()),
            sequence: u64::from_be_bytes(bytes[2..IDENTIFIER_SIZE].try_into().unwrap()),
        }
    }
}


pub struct Instance {
    pub reliable_broadcast_instance: reliable_broadcast::Instance
}


impl Instance {
    pub fn new(id: Identifier, my_id: u16) -> Self {
        Self {
            reliable_broadcast_instance: reliable_broadcast::Instance::new(id, my_id)
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
    pub sender: u16,
    pub payload: MessageType,
    pub signature: [u8; 64],
}

impl Message {
    pub fn new(id: Identifier, sender: u16, payload: MessageType, privkey: &[u8; 32]) -> Self {
        let mut signing_key = ed25519_dalek::SigningKey::from_bytes(privkey);
        let mut message =  Self { id, sender, payload, signature: [0; 64] };
        let sig = signing_key.sign(&message.to_header_and_payload_bytes());
        message.signature = sig.to_bytes();
        return message;
    }
    
    fn to_header_and_payload_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.id.to_bytes());
        result.extend_from_slice(&self.sender.to_be_bytes());
        result.extend_from_slice(&self.payload.to_bytes());
        result
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = self.to_header_and_payload_bytes();
        result.extend_from_slice(&self.signature);
        result
    }
    
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < MIN_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                format!("Message too short: {} bytes, expected at least {}", bytes.len(), MIN_MESSAGE_SIZE)
            ));
        }

        let id = Identifier::from_bytes(bytes[0..IDENTIFIER_SIZE].try_into().unwrap());
        let sender: u16 = u16::from_be_bytes(bytes[IDENTIFIER_SIZE..IDENTIFIER_SIZE+2].try_into().unwrap());
        let payload = MessageType::from_bytes(&bytes[HEADER_SIZE..bytes.len()-SIGNATURE_SIZE])?;
        let signature: [u8; SIGNATURE_SIZE] = bytes[bytes.len()-SIGNATURE_SIZE..].try_into().unwrap();
        
        Ok(Self {id, sender, payload, signature})
    }

    pub fn verify(&self, pubkey: ed25519_dalek::VerifyingKey) -> bool {
        let data = self.to_header_and_payload_bytes();
        let signature = ed25519::Signature::from_bytes(&self.signature);
        pubkey.verify_strict(&data, &signature).is_ok()
    }
}


pub async fn broadcast(destinations: &[String], message: &Message, socket: Arc<UdpSocket>) -> Result<(), io::Error> {
    let message_bytes = message.to_bytes();
    for dest in destinations {
        socket.send_to(&message_bytes, dest).await
            .map_err(|e| io::Error::new(io::ErrorKind::NetworkDown, format!("Failed to send to {}: {}", dest, e)))?;
    }
    Ok(())
}
