use serde::{de::DeserializeOwned, Serialize};
use serde_json::{from_value, to_value, Value};

pub trait ObjectTypeName {
    fn type_name() -> &'static str;
}

pub enum Action {
    Create(&'static str, String, Value),
    Update(&'static str, String, Value),
    Delete(&'static str, String),
    Fetch(
        &'static str,
        String,
        Box<dyn FnOnce(Option<Value>) -> Action + Send>,
    ),
    Sequence(Vec<Action>),
}

pub fn create<T: Serialize + ObjectTypeName>(id: &str, obj: T) -> Action {
    Action::Create(T::type_name(), id.to_string(), to_value(obj).unwrap())
}

pub fn update<T: Serialize + ObjectTypeName>(id: &str, obj: T) -> Action {
    Action::Update(T::type_name(), id.to_string(), to_value(obj).unwrap())
}

pub fn delete<T: Serialize + ObjectTypeName>(id: &str) -> Action {
    Action::Delete(T::type_name(), id.to_string())
}

pub fn fetch<T: DeserializeOwned + ObjectTypeName, F: FnOnce(T) -> Action + 'static + Send>(
    id: &str,
    f: F,
) -> Action {
    Action::Fetch(
        T::type_name(),
        id.to_string(),
        Box::new(|x| match x {
            Some(v) => f(from_value(v).unwrap()),
            None => panic!(),
        }),
    )
}

// pub fn seq(actions: Vec<Action>) -> Action {
//     Action::Sequence(actions)
// }

pub const NOOP: Action = Action::Sequence(Vec::new());
