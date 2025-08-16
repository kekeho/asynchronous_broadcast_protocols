use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::net::UdpSocket;

use crate::{Identifier, Message, MessageType};

use super::types::{Instance, ReliableBroadcastMessage};

pub fn handler(instance: &mut Instance, message: &ReliableBroadcastMessage, from: u16, dests: &[u16]) -> Vec<(u16, Message)> {
    let n = dests.len();
    let t = calc_t(n);

    let mut result: Vec<(u16, Message)> = vec![];
    match (instance.requesting, message) {
        (false, ReliableBroadcastMessage::Broadcast(m)) => {
            let message = Message::new(instance.id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Send(m.clone())));
            for dest in dests {
                result.push((*dest, message.clone()));
            }
        }

        (false, ReliableBroadcastMessage::Send(m)) => {
            // TODO: Authenticationが必要? それはmainでやる?
            if instance.id.sender == from && instance.message.is_none() {
                instance.message = Some(m.clone());
                let digest: [u8; 32] = Sha256::digest(&m).into();
                let message = Message::new(instance.id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Echo(digest)));
                for dest in dests {
                    result.push((*dest, message.clone()));
                }
            }
        }

        (false, ReliableBroadcastMessage::Echo(d)) => {
            if !instance.echo_messages.insert(from) {
                return result;
            }
            if instance.echo_messages.len() == n-t && instance.ready_messages.len() <= t {
                let message = Message::new(instance.id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Ready(d.clone())));
                for dest in dests {
                    result.push((*dest, message.clone()));
                }
            }
        }

        (false, ReliableBroadcastMessage::Ready(d)) => {
            if !instance.ready_messages.insert(from) {
                return result;
            }
            if instance.ready_messages.len() == t+1 && instance.echo_messages.len() < n-t {
                let message = Message::new(instance.id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Ready(d.clone())));
                for dest in dests {
                    result.push((*dest, message.clone()));
                }
            } else if instance.ready_messages.len() == 2*t+1 {
                instance.digest = Some(d.clone());
                let m_digest: [u8; 32] = Sha256::digest(instance.message.as_ref().unwrap()).into();
                if m_digest != instance.digest.unwrap() {
                    for dest in dests[..2*t+1].iter() {
                        let message = Message::new(instance.id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Request));
                        result.push((*dest, message))
                    }
                } else {
                    instance.output_flag = true;  // done
                }
            }
        }

        (_, ReliableBroadcastMessage::Request) => {
            if let Some(m) = &instance.message {
                let message = Message::new(instance.id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Answer(m.clone())));
                result.push((from, message));
            }
            return result;
        }

        (true, ReliableBroadcastMessage::Answer(m)) => {
            let digest: [u8; 32] = Sha256::digest(&m).into();
            if digest == instance.digest.unwrap() {
                instance.message = Some(m.clone());
                instance.output_flag = true;
            }
        }
        _ => {}
    }

    return result;
}


pub async fn broadcast(id: Identifier, message: Vec<u8>, address_map: Vec<(u16, String)>, socket: Arc<UdpSocket>) {
    let my_addr = address_map.iter().filter(|(i, _)| *i == id.sender).last().unwrap().clone().1;
    let message = Message::new(id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Broadcast(message)));
    _ = socket.send_to(&message.to_bytes(), my_addr).await;
}


fn calc_t(n: usize) -> usize {
    (n-1) / 3
}