use asynchronous_broadcast_protocols::{reliable_broadcast::{self}, Identifier, Message};
use futures::io;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};
use std::{collections::HashMap, env::args, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args: Vec<String> = args().collect();
    let my_id: u16 = args[1].parse().unwrap();
    let address_map: Vec<(u16, String)> = vec![
        (0, "127.0.0.1:8000".to_string()), (1, "127.0.0.1:8001".to_string()),
        (2, "127.0.0.1:8002".to_string()), (3, "127.0.0.1:8003".to_string()),
    ];  // TODO: args
    let addr: String = address_map.iter().filter(|(i, _)| *i == my_id).last().unwrap().1.clone();
    let socket: Arc<UdpSocket> = Arc::new(
        UdpSocket::bind(addr).await.unwrap()
    );

    // recv
    let handle = tokio::spawn(receiver(socket.clone(), my_id,  address_map.clone()));

    tokio::time::sleep(Duration::from_secs(5)).await;
    // RBC (send)
    for sequence in 0..10 {
        let identifier = Identifier::new(my_id, sequence);
        let message = format!("Message {} from {}", sequence, my_id);
        _ = tokio::spawn(reliable_broadcast::broadcast(identifier, message.into_bytes(), address_map.clone(), socket.clone()));
    }
    _ = handle.await;
    Ok(())
}



async fn receiver(socket: Arc<UdpSocket>, my_id: u16, address_map: Vec<(u16, String)>) {
    let mut buffer: [u8; 2048] = [0; 2048];
    let mut instances: HashMap<Identifier, (mpsc::Sender<Message>, JoinHandle<()>)> = HashMap::new();
    // let dests: Vec<u16> = address_map.iter().map(|(i, _)| *i).collect();
    loop {
        let result = socket.recv_from(&mut buffer).await;
        if result.is_err() { continue; }
        let (len, _) = result.unwrap();
        let message = Message::from_bytes(&buffer[..len]);
        if message.is_err() { continue; }
        let message = message.unwrap();

        // TODO: Authentication

        if let Some((tx, handle)) = instances.get_mut(&message.id) {
            if !handle.is_finished() { _ = tx.send(message).await; }
        } else {
            // New messages
            let (tx, rx) = mpsc::channel(2048);
            let cloned_address_map = address_map.clone();
            let cloned_socket = socket.clone();
            let handle = tokio::spawn(async move {
                if let Ok(msg) = reliable_broadcast::receive(
                    reliable_broadcast::Instance::new(message.id, my_id), rx, cloned_address_map, cloned_socket
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