use futures::lock::Mutex;
use serde_json::Value;
// use log::error;
// use serde_json::Value;
// use std::time::{Duration, SystemTime};
// use tokio::time::sleep;
use tokio_postgres::{Client, Config, Error, NoTls};

pub struct PgPool {
    config: Config,
    clients: Mutex<Vec<Client>>,
}

impl PgPool {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            clients: Mutex::new(Vec::new()),
        }
    }

    pub async fn get_client(&self) -> Result<Client, Error> {
        match self.clients.lock().await.pop() {
            Some(client) => Ok(client),
            None => match self.config.connect(NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move { connection.await });
                    Ok(client)
                }
                Err(error) => Err(error),
            },
        }
    }

    pub async fn put_client(&self, client: Client) {
        self.clients.lock().await.push(client);
    }

    pub async fn get_object(
        &self,
        _object_type: String,
        _object_id: String,
        _idx: Option<i64>,
    ) -> Result<Option<(i64, Option<Value>)>, Error> {
        todo!();
    }

    // async fn try_get_payload(&self, idx: i64) -> Result<Value, Error> {
    //     let client = self.get_client().await?;
    //     let row = client
    //         .query_one("select payload from events where idx = $1", &[&idx])
    //         .await?;
    //     self.put_client(client).await;
    //     Ok(row.get(0))
    // }

    // pub async fn get_payload(&self, idx: i64) -> Value {
    //     loop {
    //         match self.try_get_payload(idx).await {
    //             Ok(payload) => {
    //                 break payload;
    //             }
    //             Err(error) => {
    //                 error!("{}", error);
    //                 sleep(Duration::from_millis(200)).await;
    //             }
    //         }
    //     }
    // }

    // pub async fn try_get_last_index(&self) -> Result<i64, Error> {
    //     let client = self.get_client().await?;
    //     let row = client
    //         .query_one("select coalesce(max(idx), 0) from events", &[])
    //         .await?;
    //     self.put_client(client).await;
    //     Ok(row.get(0))
    // }

    // pub async fn try_insert(&self, idx: i64, payload: Value) -> Result<bool, Error> {
    //     let client = self.get_client().await?;
    //     let result = client
    //         .execute(
    //             "insert into events (idx, payload) values ($1, $2)",
    //             &[&idx, &payload],
    //         )
    //         .await;
    //     self.put_client(client).await;
    //     match result {
    //         Ok(_) => Ok(true),
    //         Err(error)
    //             if error
    //                 .as_db_error()
    //                 .is_some_and(|x| x.message() == "invalid idx") =>
    //         {
    //             Ok(false)
    //         }
    //         Err(error) => Err(error),
    //     }
    // }

    // pub async fn try_fetch(&self, idx: i64) -> Result<Option<(SystemTime, Value)>, Error> {
    //     let client = self.get_client().await?;
    //     let result = client
    //         .query_opt(
    //             "select inserted, payload from events where idx = $1",
    //             &[&idx],
    //         )
    //         .await?;
    //     self.put_client(client).await;
    //     match result {
    //         Some(row) => Ok(Some((row.get(0), row.get(1)))),
    //         None => Ok(None),
    //     }
    // }
}
