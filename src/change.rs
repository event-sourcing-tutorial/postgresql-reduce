use serde_json::Value;

#[derive(Debug)]
pub struct Change {
    pub object_type: String,
    pub object_id: String,
    pub object_data: Option<Value>,
}
