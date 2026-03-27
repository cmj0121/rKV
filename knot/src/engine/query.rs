use rkv::Value;

use super::backend::Backend;
use super::condition::{self, Condition};
use super::error::Result;
use super::property::{self, Node, PropertyValue};

/// Sort direction.
#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Sort specification.
#[derive(Debug, Clone)]
pub struct Sort {
    pub field: String,
    pub order: SortOrder,
}

/// A page of query results.
#[derive(Debug)]
pub struct Page {
    pub items: Vec<Node>,
    pub has_more: bool,
    pub cursor: Option<String>,
}

/// Query all nodes in an rKV namespace.
pub fn query_nodes(
    backend: &dyn Backend,
    ns_name: &str,
    filter: Option<&Condition>,
    sort: Option<&Sort>,
    projection: Option<&[String]>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Page> {
    let keys = backend.scan(ns_name, "", usize::MAX)?;
    let empty_props = std::collections::HashMap::new();

    let mut nodes = Vec::new();
    for key_str in &keys {
        if let Some(c) = cursor {
            if key_str.as_str() <= c {
                continue;
            }
        }

        let value = match backend.get(ns_name, key_str)? {
            Some(v) => v,
            None => continue,
        };

        let node = value_to_node(key_str, &value)?;

        if let Some(cond) = filter {
            let props = node.properties.as_ref().map_or(&empty_props, |p| p);
            if !condition::evaluate(cond, props) {
                continue;
            }
        }

        nodes.push(node);
    }

    if let Some(sort_spec) = sort {
        nodes.sort_by(|a, b| {
            let va = a.properties.as_ref().and_then(|p| p.get(&sort_spec.field));
            let vb = b.properties.as_ref().and_then(|p| p.get(&sort_spec.field));
            let ord = cmp_option_prop(va, vb);
            match sort_spec.order {
                SortOrder::Asc => ord,
                SortOrder::Desc => ord.reverse(),
            }
        });
    }

    let has_more = nodes.len() > limit && limit > 0;
    if limit > 0 && nodes.len() > limit {
        nodes.truncate(limit);
    }

    let next_cursor = if has_more {
        nodes.last().map(|n| n.key.clone())
    } else {
        None
    };

    if let Some(fields) = projection {
        for node in &mut nodes {
            if let Some(props) = &mut node.properties {
                props.retain(|k, _| fields.iter().any(|f| f == k));
            }
        }
    }

    Ok(Page {
        items: nodes,
        has_more,
        cursor: next_cursor,
    })
}

/// Count nodes matching an optional filter.
pub fn count_nodes(
    backend: &dyn Backend,
    ns_name: &str,
    filter: Option<&Condition>,
) -> Result<u64> {
    if filter.is_none() {
        return backend.count(ns_name);
    }

    let keys = backend.scan(ns_name, "", usize::MAX)?;
    let empty_props = std::collections::HashMap::new();
    let mut count = 0u64;
    for key_str in &keys {
        let value = match backend.get(ns_name, key_str)? {
            Some(v) => v,
            None => continue,
        };
        let node = value_to_node(key_str, &value)?;
        if let Some(cond) = filter {
            let props = node.properties.as_ref().map_or(&empty_props, |p| p);
            if !condition::evaluate(cond, props) {
                continue;
            }
        }
        count += 1;
    }
    Ok(count)
}

fn value_to_node(key: &str, value: &Value) -> Result<Node> {
    match value {
        Value::Data(bytes) => {
            let props = property::decode_properties(bytes)?;
            Ok(Node {
                key: key.to_owned(),
                properties: Some(props),
            })
        }
        _ => Ok(Node {
            key: key.to_owned(),
            properties: None,
        }),
    }
}

fn cmp_option_prop(a: Option<&PropertyValue>, b: Option<&PropertyValue>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(va), Some(vb)) => cmp_prop_ord(va, vb),
    }
}

fn cmp_prop_ord(a: &PropertyValue, b: &PropertyValue) -> std::cmp::Ordering {
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}
