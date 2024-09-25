use alloy::{
    hex::ToHexExt,
    primitives::Address,
    rpc::types::{TransactionReceipt, TransactionRequest},
};
use r2d2::{ManageConnection, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

enum ActorMessage {
    SendTransactionRequest {
        request: TransactionRequest,
        respond_to: oneshot::Sender<Option<TransactionReceipt>>,
    },
    DumpCache,
}

struct TxActor<M>
where
    M: ManageConnection<Connection = rusqlite::Connection>,
{
    receiver: mpsc::Receiver<ActorMessage>,
    pub db_pool: Pool<M>,
    cache: Vec<TransactionRequest>,
}

impl<M> TxActor<M>
where
    M: ManageConnection<Connection = rusqlite::Connection>,
{
    fn new(receiver: mpsc::Receiver<ActorMessage>, db_pool: Pool<M>) -> Self {
        let _ = db_pool.get().unwrap().execute(
            "CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                timestamp TEXT NOT NULL,
                tx_count INTEGER NOT NULL
            )",
            params![],
        );
        TxActor {
            receiver,
            db_pool,
            cache: vec![],
        }
    }

    fn export_cache(&mut self) {
        let db = self.db_pool.get().unwrap();
        let stmts = self.cache.iter().map(|request| {
            format!(
                "INSERT INTO runs (timestamp, tx_count) VALUES ('{}', {});",
                request.from.unwrap_or_default().encode_hex(),
                request.gas.unwrap_or_default() as u64
            )
        });
        db.execute_batch(&format!(
            "BEGIN;
            {}
            COMMIT;",
            stmts
                .reduce(|acc, x| format!("{}\n{}", acc, x))
                .unwrap_or_default(),
        ))
        .unwrap();
    }

    fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::SendTransactionRequest {
                request,
                respond_to,
            } => {
                self.cache.push(request);
                let _ = respond_to.send(None);
            }
            ActorMessage::DumpCache => {
                self.export_cache();
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
    pub fn new<M: ManageConnection<Connection = rusqlite::Connection>>(
        bufsize: usize,
        db: Pool<M>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db);
        tokio::task::spawn(async move {
            actor.run().await;
        });
        TxActorHandle { sender }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n = 1000_usize;
    let mgr = SqliteConnectionManager::file("test.db");
    let db = Pool::new(mgr).unwrap();
    let actor = TxActorHandle::new(24, db.clone());
    let sender = Arc::new(actor.sender);
    let mut handles = vec![];
    for _ in 0..n {
        let sender = sender.clone();
        let handle = tokio::task::spawn(async move {
            let receipt_handle = oneshot::channel();
            sender
                .send(ActorMessage::SendTransactionRequest {
                    request: TransactionRequest::default()
                        .from(
                            "0x1111111111111111111111111111111111111111"
                                .parse()
                                .unwrap(),
                        )
                        .gas_limit(100000)
                        .to(Address::ZERO),
                    respond_to: receipt_handle.0,
                })
                .await
                .unwrap();
            let receipt = receipt_handle.1.await.unwrap();
            println!("receipt: {:?}", receipt);
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }

    sender.send(ActorMessage::DumpCache).await.unwrap();

    let db = db.get().unwrap();
    let count: u64 = db.query_row("SELECT COUNT(*) FROM runs", params![], |row| row.get(0))?;
    println!("rows: {:?}", count);

    Ok(())
}
