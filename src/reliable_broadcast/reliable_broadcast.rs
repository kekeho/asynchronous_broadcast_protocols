use std::{io, sync::Arc};

use sha2::{Digest, Sha256};
use tokio::{net::UdpSocket, sync::mpsc};

use crate::{Config, Identifier, Message, MessageType};

use super::types::{Instance, ReliableBroadcastMessage};


async fn send_message_to_all(message: Message, config: &Config, socket: Arc<UdpSocket>) -> Result<(), io::Error> {
    let destinations = config.get_all_addresses();
    let message_bytes = message.to_bytes();
    for dest in &destinations {
        socket.send_to(&message_bytes, dest).await
            .map_err(|e| io::Error::new(io::ErrorKind::NetworkDown, format!("Failed to send to {}: {}", dest, e)))?;
    }
    Ok(())
}


async fn send_message_to_node(message: Message, target_id: u16, config: &Config, socket: Arc<UdpSocket>) -> Result<(), io::Error> {
    if let Some(node) = config.get_node(target_id) {
        let message_bytes = message.to_bytes();
        socket.send_to(&message_bytes, &node.address).await
            .map_err(|e| io::Error::new(io::ErrorKind::NetworkDown, format!("Failed to send to {}: {}", node.address, e)))?;
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::NotFound, format!("Node {} not found", target_id)))
    }
}


pub async fn broadcast(id: Identifier, message: Vec<u8>, config: Config, socket: Arc<UdpSocket>) -> Result<(), io::Error> {
    let my_node = config.get_my_node()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "My node not found in config"))?;
    
    let message = Message::new(
        id,
        id.sender,
        MessageType::ReliableBroadcast(ReliableBroadcastMessage::Broadcast(message)),
        &my_node.privkey
    );
    
    socket.send_to(&message.to_bytes(), &my_node.address).await
        .map_err(|e| io::Error::new(io::ErrorKind::NetworkDown, format!("Failed to send broadcast: {}", e)))?;
    
    Ok(())
}


pub async fn receive(mut instance: Instance, mut rx: mpsc::Receiver<Message>, config: &Config, socket: Arc<UdpSocket>) -> Result<Vec<u8>, io::Error> {
    let n = config.nodes.len();
    let t = calc_t(n);
    let my_node = config.get_my_node()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "My node not found in config"))?;

    while let Some(message) = rx.recv().await {
        let rbc_message = match message.payload {
            MessageType::ReliableBroadcast(m) => m,
        };

        match rbc_message {
            ReliableBroadcastMessage::Broadcast(m) => {
                let message = Message::new(
                    instance.id,
                    instance.my_id,
                    MessageType::ReliableBroadcast(ReliableBroadcastMessage::Send(m.clone())),
                    &my_node.privkey
                );
                send_message_to_all(message, config, socket.clone()).await?;
            }

            ReliableBroadcastMessage::Send(m) => {
                if instance.id.sender == message.sender && instance.message.is_none() {
                    instance.message = Some(m.clone());
                    let digest: [u8; 32] = Sha256::digest(&m).into();
                    let message = Message::new(
                        instance.id,
                        instance.my_id,
                        MessageType::ReliableBroadcast(ReliableBroadcastMessage::Echo(digest)),
                        &my_node.privkey,
                    );
                    send_message_to_all(message, config, socket.clone()).await?;
                }
            }

            ReliableBroadcastMessage::Echo(d) => {
                if !instance.echo_messages.insert(message.sender) {  // Not first time
                    continue;
                }
                if instance.echo_messages.len() == n-t && instance.ready_messages.len() <= t {
                    let message = Message::new(
                        instance.id,
                        instance.my_id,
                        MessageType::ReliableBroadcast(ReliableBroadcastMessage::Ready(d.clone())),
                        &my_node.privkey,
                    );
                    send_message_to_all(message, config, socket.clone()).await?;
                }
            }

            ReliableBroadcastMessage::Ready(d) => {
                if !instance.ready_messages.insert(message.sender) {  // Not first time
                    continue;
                }
                if instance.ready_messages.len() == t+1 && instance.echo_messages.len() < n-t {
                    let message = Message::new(
                        instance.id,
                        instance.my_id,
                        MessageType::ReliableBroadcast(ReliableBroadcastMessage::Ready(d.clone())),
                        &my_node.privkey,
                    );
                    send_message_to_all(message, config, socket.clone()).await?;
                } else if instance.ready_messages.len() == 2*t+1 {
                    instance.digest = Some(d.clone());
                    let m_digest: [u8; 32] = Sha256::digest(instance.message.as_ref().unwrap()).into();
                    if m_digest != instance.digest.unwrap() {
                        let message = Message::new(
                            instance.id,
                            instance.my_id,
                            MessageType::ReliableBroadcast(ReliableBroadcastMessage::Request),
                            &my_node.privkey,
                        );
                        
                        // Send to first 2*t+1 nodes for request
                        let request_targets: Vec<String> = config.nodes.iter()
                            .take(2*t+1)
                            .map(|n| n.address.clone())
                            .collect();
                        let message_bytes = message.to_bytes();
                        for dest in request_targets {
                            socket.send_to(&message_bytes, &dest).await?;
                        }

                        // Wait for answers
                        while let Some(message) = rx.recv().await {
                            let rbc_message = match message.payload {
                                MessageType::ReliableBroadcast(m) => m,
                            };
                            match rbc_message {
                                ReliableBroadcastMessage::Answer(m) => {
                                    let d: [u8; 32] = Sha256::digest(&m).into();
                                    if instance.digest == Some(d) {
                                        instance.message = Some(m);
                                        return Ok(instance.message.as_ref().unwrap().clone());
                                    }
                                }
                                _ => { continue; }
                            }
                        }

                    } else {
                        return Ok(instance.message.as_ref().clone().unwrap().to_vec());
                    }
                }
            }

            ReliableBroadcastMessage::Request => {
                if let Some(m) = &instance.message {
                    let from = message.sender;
                    let message = Message::new(
                        instance.id,
                        instance.my_id,
                        MessageType::ReliableBroadcast(ReliableBroadcastMessage::Answer(m.clone())),
                        &my_node.privkey,
                    );
                    send_message_to_node(message, from, config, socket.clone()).await?;
                }
            }
            ReliableBroadcastMessage::Answer(_) => {
                // Handled in the request response loop above
            }
        }
    }
    Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Channel closed"))
}


fn calc_t(n: usize) -> usize {
    (n-1) / 3
}