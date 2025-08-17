use asynchronous_broadcast_protocols::{reliable_broadcast::{self}, Config, Identifier, Message};
use futures::io;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};
use std::{collections::HashMap, env::args, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = args().collect();
    let config = Config::load(&args[1]).await?;
    let my_node = config.nodes.iter().filter(|n| n.id == config.my_id).last().unwrap().clone();
    let socket: Arc<UdpSocket> = Arc::new(
        UdpSocket::bind(my_node.address.clone()).await.unwrap()
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
        _ = tokio::spawn(reliable_broadcast::broadcast(identifier, message.into_bytes(), cloned_config, socket.clone()));
    }
    _ = handle.await;
    Ok(())
}




async fn receiver(socket: Arc<UdpSocket>, config: Config) {
    let mut buffer: [u8; 2048] = [0; 2048];
    let mut instances: HashMap<Identifier, (mpsc::Sender<Message>, JoinHandle<()>)> = HashMap::new();
    loop {
        let result = socket.recv_from(&mut buffer).await;
        if result.is_err() { continue; }
        let (len, _) = result.unwrap();
        let message = Message::from_bytes(&buffer[..len]);
        if message.is_err() { continue; }
        let message = message.unwrap();

        let sender_config = config.nodes.iter().filter(|n| n.id == message.sender).last();
        if sender_config.is_none() {
            continue;
        }
        let sender_config = sender_config.unwrap();
        let sender_verify_key = ed25519_dalek::SigningKey::from_bytes(&sender_config.privkey).verifying_key();
        if !message.verify(sender_verify_key) {
            continue;
        }

        if let Some((tx, handle)) = instances.get_mut(&message.id) {
            if !handle.is_finished() { _ = tx.send(message).await; }
        } else {
            // New messages
            let (tx, rx) = mpsc::channel(2048);
            let cloned_socket = socket.clone();
            let cloned_config = config.clone();
            let handle = tokio::spawn(async move {
                if let Ok(msg) = reliable_broadcast::receive(
                    reliable_broadcast::Instance::new(message.id, cloned_config.my_id),
                    rx,
                    &cloned_config,
                    cloned_socket
                ).await {
                    let str_msg: String = String::from_utf8(msg).unwrap();
                    println!("[RBC Received]: {}", str_msg);
                }
            });
            let id = message.id;
            _ = tx.send(message).await;
            instances.insert(id, (tx, handle));
        }
        
    }
}