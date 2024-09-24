use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

enum ActorMessage {
    GetUniqueId { respond_to: oneshot::Sender<u32> },
    SetId { id: u32 },
}

struct TxActor {
    receiver: mpsc::Receiver<ActorMessage>,
    next_id: u32,
}

impl TxActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        TxActor {
            receiver,
            next_id: 0,
        }
    }

    fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::GetUniqueId { respond_to } => {
                self.next_id += 1;
                println!("handle_message: GetUniqueId (next_id: {})", self.next_id);
                let _ = respond_to.send(self.next_id);
            }
            ActorMessage::SetId { id } => {
                // std::thread::sleep(Duration::from_millis(1));
                println!("handle_message: SetId (id: {})", id);
                self.next_id = id;
            }
        }
    }

    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            self.handle_message(message);
        }
    }
}

pub struct TxActorHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl TxActorHandle {
    pub fn new(bufsize: usize) -> Self {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver);
        tokio::task::spawn(async move {
            actor.run().await;
        });
        TxActorHandle { sender }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor = TxActorHandle::new(1000);
    let sender = Arc::new(actor.sender);
    let mut handles = vec![];
    for i in 0..1000 {
        let sender = sender.clone();
        let handle = tokio::task::spawn(async move {
            println!("sending SetId (id: {})", i);
            sender.send(ActorMessage::SetId { id: i }).await.unwrap();
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
    Ok(())
}
