use super::Value;
use bytes::Bytes;
use serde::Serialize;
use std::collections::HashMap;
use string_cache::DefaultAtom as Atom;

#[derive(Debug)]
enum MapValue {
    Value(Value),
    Map(HashMap<Atom, MapValue>),
    Array(Vec<Value>),
}

#[derive(Debug)]
pub struct Unflatten {
    map: HashMap<Atom, MapValue>,
}

impl From<HashMap<Atom, Value>> for Unflatten {
    fn from(log: HashMap<Atom, Value>) -> Self {
        let mut map: HashMap<Atom, Value> = HashMap::new();
        for (k, v) in log {
            let split = k.split(".");

            // TODO: there should always be one?
            let k = split.next().unwrap();
            if let Some(nested_k) = split.next() {
                insert_map(nested_k, v, &mut map);
            } else {
                map.insert(k, v.value.into_json_value());
            }
        }

        Unflatten { map }
    }
}

fn insert_map(k: Atom, v: Value, map: &mut HashMap<Atom, Value>) {
    let split = k.split(".");

    // TODO: there should always be one?
    let k = split.next().unwrap();

    if let Some(nested_map) = map.get_mut(k) {
        if let Some(nested_k) = split.next() {
            nested_map.insert(k, into(v.value));
        }

        // TODO: support multiple nested maps

        nested_map
    } else {
        let mut map = HashMap::new();
        map.insert(nested_k, into(v.value));
        map
    }
}

fn into(v: super::Value) -> Value {
    unimplemented!()
}

// // fn unflatten(map: HashMap<>)

// impl Serialize for Unflatten {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.collect_map(self.map.clone())
//     }
// }
