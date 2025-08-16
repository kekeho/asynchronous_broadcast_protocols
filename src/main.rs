use asynchronous_broadcast_protocols::{reliable_broadcast::{self}, Identifier, Instance, Message, MessageType};
use futures::io;
use tokio::net::UdpSocket;
use std::{collections::HashMap, env::args, sync::Arc};

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
    tokio::spawn(receiver(socket.clone(), address_map.clone()));

    // RBC (send)
    let mut sequence: u64 = 0;
    loop {
        let mut user_input = String::new();
        _ = std::io::stdin().read_line(&mut user_input)?;
        sequence += 1;
        let identifier = Identifier::new(my_id, sequence);
        
        tokio::spawn(reliable_broadcast::broadcast(identifier, user_input.into_bytes(), address_map.clone(), socket.clone()));
    }
}



async fn receiver(socket: Arc<UdpSocket>, address_map: Vec<(u16, String)>) {
    let mut buffer: [u8; 2048] = [0; 2048];
    let mut instances: HashMap<Identifier, Instance> = HashMap::new();
    let dests: Vec<u16> = address_map.iter().map(|(i, _)| *i).collect();

    loop {
        let result = socket.recv_from(&mut buffer).await;
        if result.is_err() { continue; }
        let (len, from) = result.unwrap();
        let from = address_map.iter().filter(|a| a.1 == from.to_string()).last();
        if from.is_none() { continue; }
        let from = from.unwrap().0;
        let message = Message::from_bytes(&buffer[..len]);
        if message.is_err() { continue; }
        let message = message.unwrap();

        let instance: &mut Instance;
        let _instance = instances.get_mut(&message.id);
        if let Some(i) = _instance {
            instance = i;
        } else {
            instances.insert(message.id, Instance::new(message.id));
            instance = instances.get_mut(&message.id).unwrap()
        }

        let message_queue = match message.payload {
            MessageType::ReliableBroadcast(m) => {
                reliable_broadcast::handler(&mut instance.reliable_broadcast_instances, &m, from, &dests)
            }
        };

        for (d, m) in message_queue {
            let (_, dest) = address_map.iter().filter(|a| a.0 == d).last().unwrap().clone();
            let s = socket.clone();
            tokio::spawn(async move {
                _ = s.send_to(&m.to_bytes(), dest).await;
            });
        }

        if instance.reliable_broadcast_instances.output_flag == true {
            println!("[RBC Received]: {}", String::from_utf8(instance.reliable_broadcast_instances.message.as_ref().unwrap().clone()).unwrap())
        }
    }
}