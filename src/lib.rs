pub use actions::{create, delete, fetch, update, Action, ObjectTypeName, NOOP};
use async_recursion::async_recursion;
use serde::de::DeserializeOwned;
use serde_json::{from_value, Value};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    future::Future,
};
use tokio_postgres::NoTls;

const SLEEP_SECS: u64 = 2;

async fn init_postgres() -> Result<i64, tokio_postgres::Error> {
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres dbname=postgres", NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let statements = vec![
        "create table if not exists event (
            idx bigint not null,
            data jsonb not null,
            primary key (idx)
          )",
        "create index if not exists event_idx on event(idx)",
        "create table if not exists object (
            idx bigint not null,
            type text not null,
            id text not null,
            data jsonb null,
            primary key (idx, type, id)
          )",
        "create index if not exists object_idx1 on object(idx, type, id)",
        "create index if not exists object_idx2 on object(type, id, idx)",
    ];
    for stmt in statements {
        client.execute(stmt, &[]).await?;
    }
    let result: i64 = client
        .query_one("select coalesce(max(idx), 0) as idx from event", &[])
        .await?
        .get("idx");
    Ok(result)
}

async fn forever<T, E, R, F>(f: F) -> T
where
    E: Display,
    R: Future<Output = Result<T, E>>,
    F: Fn() -> R,
{
    use tokio::time::{sleep, Duration};
    loop {
        match f().await {
            Err(err) => {
                eprintln!("Error {}", err);
                eprintln!("Retrying in {} secs", SLEEP_SECS);
                sleep(Duration::from_secs(SLEEP_SECS)).await;
            }
            Ok(value) => break value,
        }
    }
}

async fn poll_next_events(_idx: i64) -> Vec<Value> {
    todo!();
}

struct ValueBox {
    current_value: Option<Value>,
    modified: bool,
}

impl ValueBox {
    fn setvalue(&mut self, value: Option<Value>) {
        self.current_value = value;
        self.modified = true;
    }
}

struct Transaction<Res, Fetch>
where
    Res: Future<Output = Result<Option<Value>, tokio_postgres::Error>> + Send,
    Fetch: Fn(String, String) -> Res + Send,
{
    fetch: Fetch,
    hmp: HashMap<String, HashMap<String, ValueBox>>,
}

impl<Res, Fetch> Transaction<Res, Fetch>
where
    Res: Future<Output = Result<Option<Value>, tokio_postgres::Error>> + Send,
    Fetch: Fn(String, String) -> Res + Send,
{
    fn new(fetch: Fetch) -> Self {
        Self {
            fetch,
            hmp: HashMap::new(),
        }
    }

    #[async_recursion]
    async fn process_action(&mut self, action: Action) -> Result<(), tokio_postgres::Error> {
        match action {
            Action::Create(object_type, object_id, object_data) => {
                let b = self.get(object_type, &object_id).await?;
                match b.current_value {
                    None => b.setvalue(Some(object_data)),
                    Some(_) => {
                        panic!("Object {} {} already exists", object_type, object_id)
                    }
                }
            }
            Action::Update(object_type, object_id, object_data) => {
                let b = self.get(object_type, &object_id).await?;
                match b.current_value {
                    Some(_) => {
                        b.setvalue(Some(object_data));
                    }
                    None => {
                        panic!("Object {} {} does not exists", object_type, object_id);
                    }
                }
            }
            Action::Delete(object_type, object_id) => {
                let b = self.get(object_type, &object_id).await?;
                match b.current_value {
                    Some(_) => {
                        b.setvalue(None);
                    }
                    None => {
                        panic!("Object {} {} does not exists", object_type, object_id);
                    }
                }
            }
            Action::Fetch(object_type, object_id, handler) => {
                let b = self.get(object_type, &object_id).await?;
                let v = match b.current_value {
                    Some(ref value) => Some(value.clone()),
                    None => None,
                };
                self.process_action(handler(v)).await?;
            }
            Action::Sequence(actions) => {
                for action in actions {
                    self.process_action(action).await?;
                }
            }
        }
        Ok(())
    }

    async fn get(
        &mut self,
        object_type: &str,
        object_id: &str,
    ) -> Result<&mut ValueBox, tokio_postgres::Error> {
        let map = self
            .hmp
            .entry(object_type.to_string())
            .or_insert_with(HashMap::new);
        let entry = map.entry(object_id.to_string());
        match entry {
            Entry::Occupied(b) => Ok(b.into_mut()),
            Entry::Vacant(b) => {
                let fut = (self.fetch)(object_type.to_owned(), object_id.to_owned());
                let existing = fut.await?;
                let vbox: ValueBox = ValueBox {
                    current_value: existing,
                    modified: false,
                };
                Ok(b.insert(vbox))
            }
        }
    }

    fn get_changes(self) -> Vec<(String, String, Option<Value>)> {
        let mut v = Vec::new();
        for (object_type, h) in self.hmp.into_iter() {
            for (object_id, vbox) in h.into_iter() {
                if vbox.modified {
                    v.push((object_type.to_owned(), object_id, vbox.current_value));
                }
            }
        }
        v
    }
}

async fn process_events<F, E>(
    idx: i64,
    events: &Vec<Value>,
    f: &F,
) -> Result<(), tokio_postgres::Error>
where
    E: DeserializeOwned,
    F: Fn(E) -> Action,
{
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres dbname=postgres", NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
        .execute("begin transaction isolation level serializable", &[])
        .await?;
    let fetch = |object_type, object_id| async {
        let object_id = object_id;
        let object_type = object_type;
        let result = client
            .query_opt(
                "select data from object
                        where type = $1
                        and id = $2
                        and idx = (select max(idx) from object where type = $1 and id = $2)
                        and data is not null",
                &[&object_type, &object_id],
            )
            .await?;
        match result {
            None => Ok(None),
            Some(row) => Ok(Some(row.get("data"))),
        }
    };
    let mut transaction = Transaction::new(fetch);
    for event in events {
        let action = f(from_value(event.to_owned()).unwrap());
        transaction.process_action(action).await?;
    }
    let changes = transaction.get_changes();
    client
        .execute("insert into event (idx) values ($1)", &[&idx])
        .await?;
    for (object_type, object_id, object_data) in changes {
        client
            .execute(
                "insert into object (type, id, idx, value) values ($1, $2, $3, $4)",
                &[&object_type, &object_id, &idx, &object_data],
            )
            .await?;
    }
    client.execute("commit", &[]).await?;
    Ok(())
}

pub fn start_reducing<T: DeserializeOwned, F: Fn(T) -> Action>(f: F) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut idx = forever(init_postgres).await;
            println!("Done! idx={}", idx);
            loop {
                let events = poll_next_events(idx).await;
                forever(|| process_events(idx + 1, &events, &f)).await;
                idx += 1;
            }
        })
}

mod actions;
