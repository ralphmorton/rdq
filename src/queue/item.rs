
pub trait Item: Sized {
    fn id(&self) -> Option<&str>;
    fn from_stream(stream_id: &redis::streams::StreamId) -> Option<Self>;
    fn to_stream(&self) -> Vec<(&str, String)>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct JsonItem<I> {
    pub id: Option<String>,
    pub item: I
}

impl<I> JsonItem<I> {
    pub fn new(item: I) -> Self {
        Self { id: None, item }
    }
}

impl<I: serde::de::DeserializeOwned + serde::Serialize + Sized> Item for JsonItem<I> {
    fn id(&self) -> Option<&str> {
        self.id.as_ref().map(|i| i.as_str())
    }

    fn from_stream(stream_id: &redis::streams::StreamId) -> Option<Self> {
        stream_id
            .get("json")
            .and_then(|json: String| serde_json::from_str(&json).ok())
            .and_then(|item| {
                let instance = Self {
                    id: Some(stream_id.id.clone()),
                    item
                };

                Some(instance)
            })
    }

    fn to_stream(&self) -> Vec<(&str, String)> {
        let json = serde_json::to_string(&self.item).unwrap();
        vec![("json", json)]
    }
}

#[cfg(test)]
mod tests {
    mod json_item {
        use crate::queue::{Item, JsonItem};

        #[test]
        fn serializes_json_correctly() {
            let item = JsonItem::new("123".to_string());
            let serialized = item.to_stream();

            let expected = vec![("json", "\"123\"".to_string())];
            assert_eq!(serialized, expected);
        }

        #[test]
        fn deserializes_correctly() {
            let sid = redis::streams::StreamId {
                id: "foo".to_string(),
                map: std::collections::HashMap::from(
                    [("json".to_string(), redis::Value::SimpleString("\"123\"".to_string()))]
                )
            };

            let item : JsonItem<String> = JsonItem::from_stream(&sid).unwrap();
            assert_eq!(item.id, Some("foo".to_string()));
            assert_eq!(item.item, "123".to_string());
        }

        #[test]
        fn deserialization_fails_gracefully() {
            let sid = redis::streams::StreamId {
                id: "foo".to_string(),
                map: std::collections::HashMap::from(
                    [("json".to_string(), redis::Value::SimpleString("\"123\"".to_string()))]
                )
            };

            let item : Option<JsonItem<i32>> = JsonItem::from_stream(&sid);
            assert_eq!(item.is_none(), true);
        }
    }
}
