use std::{io, sync::Arc};

use sha2::{Digest, Sha256};
use tokio::{net::UdpSocket, sync::mpsc};

use crate::{Identifier, Message, MessageType};

use super::types::{Instance, ReliableBroadcastMessage};


pub async fn broadcast(id: Identifier, message: Vec<u8>, address_map: Vec<(u16, String)>, socket: Arc<UdpSocket>) {
    let my_addr = address_map.iter().filter(|(i, _)| *i == id.sender).last().unwrap().clone().1;
    let message = Message::new(id, id.sender,MessageType::ReliableBroadcast(ReliableBroadcastMessage::Broadcast(message)));
    _ = socket.send_to(&message.to_bytes(), my_addr).await;
}


pub async fn receive(mut instance: Instance, mut rx: mpsc::Receiver<Message>, dests: Vec<(u16, String)>, socket: Arc<UdpSocket>) -> Result<Vec<u8>, io::Error> {
    let n = dests.len();
    let t = calc_t(n);
    while let Some(message) = rx.recv().await {
        let rbc_message = match message.payload {
            MessageType::ReliableBroadcast(m) => m,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown message")),
        };

        match rbc_message {
            ReliableBroadcastMessage::Broadcast(m) => {
                let message = Message::new(instance.id, instance.my_id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Send(m.clone())));
                let message_bytes = message.to_bytes();
                let dest_addrs: Vec<String> = dests.iter().map(|(_, a)| a.clone()).collect();
                let cloned_socket = socket.clone();
                tokio::spawn(async move {
                    for dest in dest_addrs {
                        _ = cloned_socket.send_to(&message_bytes, dest).await;
                    }
                });
                
            }

            ReliableBroadcastMessage::Send(m) => {
                if instance.id.sender == message.sender && instance.message.is_none() {
                    instance.message = Some(m.clone());
                    let digest: [u8; 32] = Sha256::digest(&m).into();
                    let message = Message::new(instance.id, instance.my_id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Echo(digest)));
                    let message_bytes = message.to_bytes();
                    let dest_addrs: Vec<String> = dests.iter().map(|(_, a)| a.clone()).collect();
                    let cloned_socket = socket.clone();
                    tokio::spawn(async move {
                        for dest in dest_addrs {
                            _ = cloned_socket.send_to(&message_bytes, dest).await;
                        }
                    });
                }
            }

            ReliableBroadcastMessage::Echo(d) => {
                if !instance.echo_messages.insert(message.sender) {  // Not first time
                    continue;
                }
                if instance.echo_messages.len() == n-t && instance.ready_messages.len() <= t {
                    let message = Message::new(instance.id, instance.my_id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Ready(d.clone())));
                    let message_bytes = message.to_bytes();
                    let dest_addrs: Vec<String> = dests.iter().map(|(_, a)| a.clone()).collect();
                    let cloned_socket = socket.clone();
                    tokio::spawn(async move {
                        for dest in dest_addrs {
                            _ = cloned_socket.send_to(&message_bytes, dest).await;
                        }
                    });
                }
            }

            ReliableBroadcastMessage::Ready(d) => {
                if !instance.ready_messages.insert(message.sender) {  // Not first time
                    continue;
                }
                if instance.ready_messages.len() == t+1 && instance.echo_messages.len() < n-t {
                    let message = Message::new(instance.id, instance.my_id,MessageType::ReliableBroadcast(ReliableBroadcastMessage::Ready(d.clone())));
                    let message_bytes = message.to_bytes();
                    let dest_addrs: Vec<String> = dests.iter().map(|(_, a)| a.clone()).collect();
                    let cloned_socket = socket.clone();
                    tokio::spawn(async move {
                        for dest in dest_addrs {
                            _ = cloned_socket.send_to(&message_bytes, dest).await;
                        }
                    });
                } else if instance.ready_messages.len() == 2*t+1 {
                    instance.digest = Some(d.clone());
                    let m_digest: [u8; 32] = Sha256::digest(instance.message.as_ref().unwrap()).into();
                    if m_digest != instance.digest.unwrap() {
                        let message = Message::new(instance.id, instance.my_id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Request));
                        let message_bytes = message.to_bytes();
                        let dest_addrs: Vec<String> = dests[..2*t+1].iter().map(|(_, a)| a.clone()).collect();
                        let cloned_socket = socket.clone();
                        tokio::spawn(async move {
                            for dest in dest_addrs {
                                _ = cloned_socket.send_to(&message_bytes, dest).await;
                            }
                        });

                        // Wait for answers
                        while let Some(message) = rx.recv().await {
                            let rbc_message = match message.payload {
                                MessageType::ReliableBroadcast(m) => m,
                                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown message")),
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
                    let message = Message::new(instance.id, instance.my_id, MessageType::ReliableBroadcast(ReliableBroadcastMessage::Answer(m.clone())));
                    let message_bytes = message.to_bytes();
                    let dest_addrs: Vec<String> = dests[..2*t+1].iter().map(|(_, a)| a.clone()).collect();
                    let cloned_socket = socket.clone();
                    tokio::spawn(async move {
                        for dest in dest_addrs {
                            _ = cloned_socket.send_to(&message_bytes, dest).await;
                        }
                    });
                }
            }
            _ => {}
        }
    }
    return Err(io::Error::new(io::ErrorKind::NetworkDown, ""));
}


fn calc_t(n: usize) -> usize {
    (n-1) / 3
}