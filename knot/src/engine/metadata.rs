use std::collections::HashMap;
use std::io::Cursor;

use rkv::{Key, DB};
use rmpv::Value as MsgValue;

use super::error::{self, Result};

/// In-memory catalog of tables, link tables, and indexes.
#[derive(Debug, Default)]
pub struct Metadata {
    pub tables: HashMap<String, TableDef>,
    pub links: HashMap<String, LinkDef>,
}

/// Definition of a data table.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
}

/// Definition of a link table.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LinkDef {
    pub name: String,
    pub source: String,
    pub target: String,
    pub bidirectional: bool,
    pub cascade: bool,
}

impl Metadata {
    /// Load metadata from the rKV metadata namespace, rebuilding the in-memory
    /// catalog. Returns an empty catalog if the namespace is new.
    pub fn load(db: &DB, namespace: &str) -> Result<Self> {
        let meta_ns = format!("knot.{namespace}.meta");
        let ns = db.namespace(&meta_ns, None).map_err(error::storage)?;

        let mut meta = Self::default();

        // Scan all keys in the metadata namespace
        let keys = ns
            .scan(&Key::Str(String::new()), 0, 0, false)
            .map_err(error::storage)?;

        for key in keys {
            let key_str = match key.as_str() {
                Some(s) => s,
                None => continue,
            };

            if let Some(table_name) = key_str.strip_prefix("table:") {
                meta.tables.insert(
                    table_name.to_owned(),
                    TableDef {
                        name: table_name.to_owned(),
                    },
                );
            } else if let Some(link_name) = key_str.strip_prefix("link:") {
                let value = ns.get(key_str).map_err(error::storage)?;
                if let Some(def) = Self::parse_link_def(link_name, &value) {
                    meta.links.insert(link_name.to_owned(), def);
                }
            }
        }

        Ok(meta)
    }

    fn parse_link_def(name: &str, value: &rkv::Value) -> Option<LinkDef> {
        let bytes = match value {
            rkv::Value::Data(b) => b,
            _ => return None,
        };

        let msg = rmpv::decode::read_value(&mut Cursor::new(bytes)).ok()?;
        let map = msg.as_map()?;

        let get_str = |key: &str| -> Option<String> {
            map.iter()
                .find(|(k, _)| k.as_str() == Some(key))
                .and_then(|(_, v)| v.as_str().map(|s| s.to_owned()))
        };
        let get_bool = |key: &str| -> bool {
            map.iter()
                .find(|(k, _)| k.as_str() == Some(key))
                .and_then(|(_, v)| v.as_bool())
                .unwrap_or(false)
        };

        let source = get_str("source")?;
        let target = get_str("target")?;
        let bidirectional = get_bool("bidirectional");
        let cascade = get_bool("cascade");

        Some(LinkDef {
            name: name.to_owned(),
            source,
            target,
            bidirectional,
            cascade,
        })
    }

    /// Encode a link definition as MessagePack bytes.
    #[allow(dead_code)]
    pub fn encode_link_def(def: &LinkDef) -> Vec<u8> {
        let map = vec![
            (
                MsgValue::String("source".into()),
                MsgValue::String(def.source.clone().into()),
            ),
            (
                MsgValue::String("target".into()),
                MsgValue::String(def.target.clone().into()),
            ),
            (
                MsgValue::String("bidirectional".into()),
                MsgValue::Boolean(def.bidirectional),
            ),
            (
                MsgValue::String("cascade".into()),
                MsgValue::Boolean(def.cascade),
            ),
        ];
        let value = MsgValue::Map(map);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &value).expect("msgpack encode");
        buf
    }
}
