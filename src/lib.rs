pub use actions::{create, delete, fetch, update, Action, ObjectTypeName, NOOP};
use async_recursion::async_recursion;
use change::Change;
use eventsapis_proto::{
    events_apis_client::EventsApisClient, PollEventsRequest, PollEventsResponse,
};
use pgpool::PgPool;
use serde::de::DeserializeOwned;
use serde_json::{from_value, Value};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    future::Future,
    sync::Arc,
};
use tokio::sync::broadcast;
use tokio_postgres::NoTls;
use tonic::Request;

mod change;
mod grpc_server;
mod pgpool;

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

async fn pause_for_error<E: Display>(err: E) {
    use tokio::time::{sleep, Duration};
    eprintln!("Error {}", err);
    eprintln!("Retrying in {} secs", SLEEP_SECS);
    sleep(Duration::from_secs(SLEEP_SECS)).await;
}

async fn forever<T, E, R, F>(f: F) -> T
where
    E: Display,
    R: Future<Output = Result<T, E>>,
    F: Fn() -> R,
{
    loop {
        match f().await {
            Err(err) => {
                pause_for_error(err).await;
            }
            Ok(value) => break value,
        }
    }
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

    fn get_changes(self) -> Vec<Change> {
        let mut v = Vec::new();
        for (object_type, h) in self.hmp.into_iter() {
            for (object_id, vbox) in h.into_iter() {
                if vbox.modified {
                    v.push(Change {
                        object_type: object_type.to_owned(),
                        object_id: object_id,
                        object_data: vbox.current_value,
                    });
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
    pool: &Arc<PgPool>,
) -> Result<Vec<Change>, tokio_postgres::Error>
where
    E: DeserializeOwned,
    F: Fn(E) -> Action,
{
    let client = pool.get_client().await?;
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
    for change in changes.iter() {
        let Change {
            object_type,
            object_id,
            object_data,
        } = change;
        client
            .execute(
                "insert into object (type, id, idx, value) values ($1, $2, $3, $4)",
                &[&object_type, &object_id, &idx, &object_data],
            )
            .await?;
    }
    client.execute("commit", &[]).await?;
    pool.put_client(client).await;
    Ok(changes)
}

// fn print_message(idx: i64, inserted: prost_types::Timestamp, events: Vec<Value>) {
//     use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};
//     let inserted = OffsetDateTime::from_unix_timestamp(inserted.seconds).unwrap()
//         + Duration::nanoseconds(inserted.nanos as i64);
//     let inserted = inserted.format(&Rfc3339).unwrap();
//     println!(
//         "{} {} {}",
//         idx,
//         inserted,
//         serde_json::to_string(&events).unwrap(),
//     );
// }

fn parse_events(payload: prost_types::Value) -> Vec<Value> {
    use anystruct::IntoJSON;
    let payload = payload.into_json();
    println!("parsing {}", serde_json::to_string(&payload).unwrap());
    let eventslist = match payload {
        Value::Object(mut map) => map.remove("events"),
        others => panic!("invalid payload: expected an object, found {:?}", others),
    };
    match eventslist {
        Some(Value::Array(events)) => events,
        others => panic!(
            "invalid payload.events: expected an array, found {:?}",
            others
        ),
    }
}

async fn start_polling<T: DeserializeOwned, F: Fn(T) -> Action>(
    last_idx: i64,
    f: F,
    tx: &broadcast::Sender<Arc<(i64, Vec<Change>)>>,
    pool: &Arc<PgPool>,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut client = EventsApisClient::connect("http://eventsapis:40001").await?;
    let mut stream = client
        .poll_events(Request::new(PollEventsRequest { last_idx }))
        .await?
        .into_inner();
    let mut last_idx = last_idx;
    loop {
        match stream.message().await {
            Ok(Some(message)) => {
                let PollEventsResponse {
                    idx,
                    inserted: _inserted,
                    payload,
                } = message;
                assert_eq!(idx, last_idx + 1);
                let events = parse_events(payload.unwrap());
                //print_message(idx, inserted.unwrap(), events);
                let changes = forever(|| process_events(idx, &events, &f, &pool)).await;
                tx.send(Arc::new((idx, changes))).unwrap();
                last_idx = idx;
            }
            Ok(None) => break,
            Err(err) => {
                eprintln!("an error occurred while polling a message: {}", err);
                break ();
            }
        }
    }
    Ok(last_idx)
}

pub fn start_reducing<T: DeserializeOwned, F: Fn(T) -> Action>(f: F) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let config = tokio_postgres::Config::new()
                .host("localhost")
                .user("postgres")
                .dbname("postgres")
                .clone();
            let pool = Arc::new(pgpool::PgPool::new(config));
            let mut last_idx = forever(init_postgres).await;
            println!("starting with last_idx={}", last_idx);
            let (tx, rx) = broadcast::channel(32);
            tokio::spawn({
                let pool = pool.clone();
                let last_idx = last_idx;
                async move { grpc_server::start(last_idx, rx, pool).await.unwrap() }
            });
            loop {
                match start_polling(last_idx, &f, &tx, &pool).await {
                    Ok(idx) => last_idx = idx,
                    Err(err) => {
                        pause_for_error(err).await;
                    }
                }
            }
        })
}

mod actions;
