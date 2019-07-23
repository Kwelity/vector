use super::{Value, ValueKind};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use string_cache::DefaultAtom as Atom;

lazy_static! {
    static ref RE: Regex = Regex::new(r"(?P<key>\D+)\[(?P<index>\d+)\]").unwrap();
}

#[derive(Debug, Clone, PartialEq)]
enum MapValue {
    Value(ValueKind),
    Map(HashMap<Atom, MapValue>),
    Array(Vec<MapValue>),
    Null,
}

#[derive(Debug)]
pub struct Unflatten {
    map: HashMap<Atom, MapValue>,
}

impl From<HashMap<Atom, Value>> for Unflatten {
    fn from(log: HashMap<Atom, Value>) -> Self {
        let log = log
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().value))
            .collect();
        let map = unflatten(log);

        if let MapValue::Map(map) = map {
            Unflatten { map }
        } else {
            panic!("wrong type");
        }
    }
}

fn unflatten(map: HashMap<Atom, ValueKind>) -> MapValue {
    let mut m = MapValue::Map(HashMap::new());

    for (k, v) in map {
        let temp = uf(k, MapValue::Value(v));
        merge(&mut m, &temp);
    }

    m
}

fn uf(k: Atom, v: MapValue) -> MapValue {
    let mut s = k.rsplit(".").peekable();
    let mut map = HashMap::new();
    let mut v = Some(v);

    while let Some(k) = s.next() {
        let k = if let Some(cap) = RE.captures(&k) {
            match (cap.name("key"), cap.name("index")) {
                (Some(k), Some(i)) => {
                    let i = i.as_str().parse::<usize>().unwrap();

                    let mut array = if i > 0 {
                        (0..i)
                            .into_iter()
                            .map(|_| MapValue::Null)
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };

                    array.push(v.take().unwrap());
                    v = Some(MapValue::Array(array));

                    k.as_str()
                }
                _ => k,
            }
        } else {
            k
        };

        if let None = s.peek() {
            map.insert(k.into(), v.take().unwrap());
        } else {
            let mut m = HashMap::new();
            m.insert(k.into(), v.take().unwrap());
            v = Some(MapValue::Map(m));
        }
    }

    MapValue::Map(map)
}

fn merge(a: &mut MapValue, b: &MapValue) {
    match (a, b) {
        (&mut MapValue::Map(ref mut a), &MapValue::Map(ref b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(MapValue::Null), v);
            }
        }
        (&mut MapValue::Array(ref mut a), &MapValue::Array(ref b)) => {
            for (i, v) in b.iter().enumerate().filter(|(_, e)| e != &&MapValue::Null) {
                if i > 0 && i >= a.len() {
                    let extra_cap = i - a.len();
                    if extra_cap > 0 {
                        a.reserve(extra_cap);

                        for _ in 0..extra_cap {
                            a.push(MapValue::Null)
                        }
                    }
                }

                if i < a.len() {
                    a.remove(i);
                }

                a.insert(i, v.clone());
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

impl Serialize for Unflatten {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_map(self.map.clone())
    }
}

impl Serialize for MapValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            MapValue::Value(v) => v.serialize(serializer),
            MapValue::Map(m) => serializer.collect_map(m.clone()),
            MapValue::Array(a) => serializer.collect_seq(a.clone()),
            MapValue::Null => serializer.serialize_str("null"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{self, Event},
        transforms::{
            json_parser::{JsonParser, JsonParserConfig},
            Transform,
        },
    };
    use serde::Deserialize;

    #[test]
    fn nested() {
        let mut m = HashMap::new();
        m.insert("a.b.c".into(), "v1".into());
        m.insert("a.b.d".into(), "v2".into());

        let new_m = unflatten(m);

        let new_m = if let MapValue::Map(m) = new_m {
            m
        } else {
            panic!("wrong type");
        };

        let json = serde_json::to_string(&new_m).unwrap();
        let expected = serde_json::from_str::<Expected>(&json).unwrap();

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "snake_case")]
        struct Expected {
            a: A,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "snake_case")]
        struct A {
            b: B,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "snake_case")]
        struct B {
            c: String,
            d: String,
        }

        assert_eq!(&expected.a.b.c, "v1");
        assert_eq!(&expected.a.b.d, "v2");
    }

    #[test]
    fn array() {
        for _ in 0..100 {
            let mut m = HashMap::new();
            m.insert("a.b[0]".into(), "v1".into());
            m.insert("a.b[1]".into(), "v2".into());

            let new_m = unflatten(m);

            let new_m = if let MapValue::Map(m) = new_m {
                m
            } else {
                panic!("wrong type");
            };

            #[derive(Deserialize, Debug)]
            #[serde(rename_all = "snake_case")]
            struct Expected {
                a: A,
            }

            #[derive(Deserialize, Debug)]
            #[serde(rename_all = "snake_case")]
            struct A {
                b: Vec<String>,
            }

            let json = serde_json::to_string(&new_m).unwrap();
            let expected = serde_json::from_str::<Expected>(&json).unwrap();

            assert_eq!(expected.a.b, vec!["v1", "v2"]);
        }
    }

    proptest::proptest! {
        #[test]
        fn unflatten_abirtrary(json in prop::json()) {
            let s = serde_json::to_string(&json).unwrap();
            println!("Checking: {}", serde_json::to_string_pretty(&json).unwrap());
            let mut event = Event::new_empty_log();
            event.as_mut_log().insert_implicit(event::MESSAGE.clone().into(), s.into());
            let mut parser = JsonParser::from(JsonParserConfig::default());
            let event = parser.transform(event).unwrap().into_log().unflatten();
            let expected_value = serde_json::to_value(&event).unwrap();
            // let expected_value = serde_json::from_str::<serde_json::Value>(&event_json).unwrap();

            assert_eq!(expected_value, json, "{:?}", event);
        }
    }

    mod prop {
        use proptest::{
            arbitrary::any,
            collection::{hash_map, vec},
            prop_oneof,
            strategy::{Just, Strategy},
        };
        use serde_json::Value;

        pub fn json() -> impl Strategy<Value = Value> {
            let leaf = prop_oneof![
                // Just(Value::Null),
                any::<bool>().prop_map(Value::Bool),
                any::<i64>().prop_map(|n| Value::Number(n.into())),
                "[a-z]*".prop_map(Value::String),
            ];

            leaf.prop_recursive(8, 256, 10, |inner| {
                prop_oneof![
                    vec(inner.clone(), 1..10).prop_map(Value::Array),
                    hash_map("[a-z]*", inner, 1..10)
                        .prop_map(|m| Value::Object(m.into_iter().collect())),
                ]
            })
            .prop_map(|m| serde_json::json!({ "some": m }))
        }
    }
}
