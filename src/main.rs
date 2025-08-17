use asynchronous_broadcast_protocols::{reliable_broadcast::{self}, Config, Identifier, Message, constants::*};
use futures::io;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};
use std::{collections::HashMap, env::args, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = args().collect();
    if args.len() < 2 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Please provide a config file"));
    }
    
    let config = Config::load(&args[1]).await?;
    let my_node = config.get_my_node()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "My node not found in config"))?
        .clone();
    
    let socket: Arc<UdpSocket> = Arc::new(
        UdpSocket::bind(&my_node.address).await
            .map_err(|e| io::Error::new(io::ErrorKind::AddrInUse, format!("Failed to bind to {}: {}", my_node.address, e)))?
    );

    // recv
    let cloned_config = config.clone();
    let handle = tokio::spawn(receiver(socket.clone(), cloned_config));

    tokio::time::sleep(Duration::from_secs(5)).await;
    // RBC (send)
    for sequence in 0..10 {
        let identifier = Identifier::new(my_node.id, sequence);
        let message = format!("Message {} from {}", sequence, my_node.id);
        let cloned_config = config.clone();
        let _ = tokio::spawn(reliable_broadcast::broadcast(identifier, message.into_bytes(), cloned_config, socket.clone()));
    }
    handle.await.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Join error: {}", e)))?;
    Ok(())
}




async fn receiver(socket: Arc<UdpSocket>, config: Config) {
    let mut buffer: [u8; MESSAGE_BUFFER_SIZE] = [0; MESSAGE_BUFFER_SIZE];
    let mut instances: HashMap<Identifier, (mpsc::Sender<Message>, JoinHandle<()>)> = HashMap::new();
    
    loop {
        let result = socket.recv_from(&mut buffer).await;
        if result.is_err() { 
            eprintln!("Failed to receive message: {:?}", result.err());
            continue; 
        }
        let (len, _) = result.unwrap();
        
        let message = Message::from_bytes(&buffer[..len]);
        if message.is_err() { 
            eprintln!("Failed to parse message: {:?}", message.err());
            continue; 
        }
        let message = message.unwrap();

        let sender_config = config.get_node(message.sender);
        if sender_config.is_none() {
            eprintln!("Unknown sender: {}", message.sender);
            continue;
        }
        let sender_config = sender_config.unwrap();
        let sender_verify_key = ed25519_dalek::SigningKey::from_bytes(&sender_config.privkey).verifying_key();
        if !message.verify(sender_verify_key) {
            eprintln!("Failed to verify message signature from node {}", message.sender);
            continue;
        }

        if let Some((tx, handle)) = instances.get_mut(&message.id) {
            if !handle.is_finished() { 
                if let Err(e) = tx.send(message).await {
                    eprintln!("Failed to send message to instance: {}", e);
                }
            }
        } else {
            // New messages
            let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
            let cloned_socket = socket.clone();
            let cloned_config = config.clone();
            let message_id = message.id;
            
            let handle = tokio::spawn(async move {
                match reliable_broadcast::receive(
                    reliable_broadcast::Instance::new(message_id, cloned_config.my_id),
                    rx,
                    &cloned_config,
                    cloned_socket
                ).await {
                    Ok(msg) => {
                        match String::from_utf8(msg) {
                            Ok(str_msg) => println!("[RBC Received]: {}", str_msg),
                            Err(e) => eprintln!("Failed to decode received message: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Error in reliable broadcast receive: {}", e),
                }
            });
            
            if let Err(e) = tx.send(message).await {
                eprintln!("Failed to send initial message to new instance: {}", e);
            } else {
                instances.insert(message_id, (tx, handle));
            }
        }
    }
}