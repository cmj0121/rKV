use std::collections::HashMap;
use std::io::Cursor;

use rmpv::Value as MsgValue;

use super::error::{Error, Result};

/// A property value in Knot.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    String(std::string::String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Binary(Vec<u8>),
    Geo(f64, f64),
}

/// A flat map of property names to values.
pub type Properties = HashMap<std::string::String, PropertyValue>;

/// A node: key + optional properties.
#[derive(Debug, Clone)]
pub struct Node {
    pub key: std::string::String,
    pub properties: Option<Properties>,
}

/// Encode properties to MessagePack bytes.
pub fn encode_properties(props: &Properties) -> Vec<u8> {
    let pairs: Vec<(MsgValue, MsgValue)> = props
        .iter()
        .map(|(k, v)| (MsgValue::String(k.clone().into()), prop_to_msgpack(v)))
        .collect();
    let value = MsgValue::Map(pairs);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &value).expect("msgpack encode");
    buf
}

/// Decode properties from MessagePack bytes.
pub fn decode_properties(bytes: &[u8]) -> Result<Properties> {
    let msg = rmpv::decode::read_value(&mut Cursor::new(bytes))
        .map_err(|e| Error::StorageError(format!("msgpack decode: {e}")))?;

    let map = msg
        .as_map()
        .ok_or_else(|| Error::StorageError("expected msgpack map".into()))?;

    let mut props = Properties::new();
    for (k, v) in map {
        let key = k
            .as_str()
            .ok_or_else(|| Error::StorageError("property name must be string".into()))?
            .to_owned();
        let value = msgpack_to_prop(v)?;
        props.insert(key, value);
    }
    Ok(props)
}

fn prop_to_msgpack(v: &PropertyValue) -> MsgValue {
    match v {
        PropertyValue::String(s) => MsgValue::String(s.clone().into()),
        PropertyValue::Integer(n) => MsgValue::Integer((*n).into()),
        PropertyValue::Float(f) => MsgValue::F64(*f),
        PropertyValue::Boolean(b) => MsgValue::Boolean(*b),
        PropertyValue::Binary(b) => MsgValue::Binary(b.clone()),
        PropertyValue::Geo(lat, lon) => {
            MsgValue::Array(vec![MsgValue::F64(*lat), MsgValue::F64(*lon)])
        }
    }
}

fn msgpack_to_prop(v: &MsgValue) -> Result<PropertyValue> {
    match v {
        MsgValue::String(s) => {
            let s = s
                .as_str()
                .ok_or_else(|| Error::StorageError("invalid utf-8 in property".into()))?;
            Ok(PropertyValue::String(s.to_owned()))
        }
        MsgValue::Integer(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PropertyValue::Integer(i))
            } else {
                Err(Error::StorageError("integer out of range".into()))
            }
        }
        MsgValue::F64(f) => Ok(PropertyValue::Float(*f)),
        MsgValue::F32(f) => Ok(PropertyValue::Float(*f as f64)),
        MsgValue::Boolean(b) => Ok(PropertyValue::Boolean(*b)),
        MsgValue::Binary(b) => Ok(PropertyValue::Binary(b.clone())),
        MsgValue::Array(arr) if arr.len() == 2 => {
            // Geo: [lat, lon]
            let lat = arr[0]
                .as_f64()
                .ok_or_else(|| Error::StorageError("geo lat must be float".into()))?;
            let lon = arr[1]
                .as_f64()
                .ok_or_else(|| Error::StorageError("geo lon must be float".into()))?;
            Ok(PropertyValue::Geo(lat, lon))
        }
        _ => Err(Error::StorageError(format!(
            "unsupported msgpack type: {v:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_properties() {
        let mut props = Properties::new();
        props.insert("name".into(), PropertyValue::String("alice".into()));
        props.insert("age".into(), PropertyValue::Integer(30));
        props.insert("score".into(), PropertyValue::Float(9.5));
        props.insert("active".into(), PropertyValue::Boolean(true));
        props.insert("data".into(), PropertyValue::Binary(vec![1, 2, 3]));
        props.insert("location".into(), PropertyValue::Geo(42.3, -71.1));

        let bytes = encode_properties(&props);
        let decoded = decode_properties(&bytes).unwrap();

        assert_eq!(props, decoded);
    }

    #[test]
    fn empty_properties() {
        let props = Properties::new();
        let bytes = encode_properties(&props);
        let decoded = decode_properties(&bytes).unwrap();
        assert!(decoded.is_empty());
    }
}
