#![allow(dead_code)]

use knot::{Condition, Properties, PropertyValue};

/// Parsed REPL expression.
#[derive(Debug)]
pub enum Expr {
    /// +{table key}[props] or +{table key}
    InsertNode {
        table: String,
        key: String,
        properties: Option<Properties>,
    },
    /// +(link from -> to)[props] or +(link from <-> to)
    InsertLink {
        link: String,
        from: String,
        to: String,
        bidi: bool,
        properties: Option<Properties>,
    },
    /// -{table key}
    DeleteNode { table: String, key: String },
    /// -!{table key}
    CascadeDeleteNode { table: String, key: String },
    /// -(link from -> to)
    DeleteLink {
        link: String,
        from: String,
        to: String,
    },
    /// -!(link from -> to)
    CascadeDeleteLink {
        link: String,
        from: String,
        to: String,
    },
    /// ?{table key}
    GetNode { table: String, key: String },
    /// ?{table} or ?{table | condition}
    QueryNodes {
        table: String,
        filter: Option<Condition>,
        sort: Option<SortSpec>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    /// ?{table key} -> (link) [-> (link) ...]
    Traverse {
        table: String,
        key: String,
        hops: Vec<TraversalHop>,
    },
    /// ?{table key} -> (*:N)
    Discover {
        table: String,
        key: String,
        max_hops: usize,
        bidi: bool,
    },
}

#[derive(Debug)]
pub struct SortSpec {
    pub field: String,
    pub desc: bool,
}

#[derive(Debug)]
pub struct TraversalHop {
    pub link: String,
    pub bidi: bool,
}

/// Parse a REPL expression line.
pub fn parse(line: &str) -> Result<Expr, String> {
    let line = line.trim();

    if line.starts_with("+{") {
        parse_insert_node(line)
    } else if line.starts_with("+(") {
        parse_insert_link(line)
    } else if line.starts_with("-!{") {
        parse_cascade_delete_node(line)
    } else if line.starts_with("-!(") {
        parse_cascade_delete_link(line)
    } else if line.starts_with("-{") {
        parse_delete_node(line)
    } else if line.starts_with("-(") {
        parse_delete_link(line)
    } else if line.starts_with('?') {
        parse_query(line)
    } else {
        Err(format!("unrecognized expression: {line}"))
    }
}

fn parse_insert_node(line: &str) -> Result<Expr, String> {
    // +{table key}[props] or +{table key}
    let after_plus = &line[1..]; // skip +
    let (table, key) = parse_node_ref(after_plus)?;
    let rest = skip_past_brace(after_plus);
    let properties = if rest.starts_with('[') {
        Some(parse_properties(rest)?)
    } else {
        None
    };
    Ok(Expr::InsertNode {
        table,
        key,
        properties,
    })
}

fn parse_insert_link(line: &str) -> Result<Expr, String> {
    // +(link from -> to)[props] or +(link from <-> to)
    let after_plus = &line[1..]; // skip +
    let (link, from, to, bidi) = parse_link_ref(after_plus)?;
    let rest = skip_past_paren(after_plus);
    let properties = if rest.starts_with('[') {
        Some(parse_properties(rest)?)
    } else {
        None
    };
    Ok(Expr::InsertLink {
        link,
        from,
        to,
        bidi,
        properties,
    })
}

fn parse_delete_node(line: &str) -> Result<Expr, String> {
    let after_dash = &line[1..]; // skip -
    let (table, key) = parse_node_ref(after_dash)?;
    Ok(Expr::DeleteNode { table, key })
}

fn parse_cascade_delete_node(line: &str) -> Result<Expr, String> {
    let after_prefix = &line[2..]; // skip -!
    let (table, key) = parse_node_ref(after_prefix)?;
    Ok(Expr::CascadeDeleteNode { table, key })
}

fn parse_delete_link(line: &str) -> Result<Expr, String> {
    let after_dash = &line[1..]; // skip -
    let (link, from, to, _bidi) = parse_link_ref(after_dash)?;
    Ok(Expr::DeleteLink { link, from, to })
}

fn parse_cascade_delete_link(line: &str) -> Result<Expr, String> {
    let after_prefix = &line[2..]; // skip -!
    let (link, from, to, _bidi) = parse_link_ref(after_prefix)?;
    Ok(Expr::CascadeDeleteLink { link, from, to })
}

fn parse_query(line: &str) -> Result<Expr, String> {
    // Parse prefix: ?[:N[+M]]
    let (limit, offset, rest) = parse_query_prefix(line)?;

    // Check for traversal: ?{table key} -> ...
    if let Some(arrow_pos) = rest.find(" -> ") {
        let node_part = &rest[..arrow_pos];
        let (table, key) = parse_node_ref(node_part)?;
        let traverse_part = &rest[arrow_pos + 4..]; // skip " -> "
        return parse_traversal(table, key, traverse_part);
    }

    // Parse {table [key] [| condition]}
    let brace_content = extract_braces(rest)?;
    let after_brace = skip_past_brace(rest);

    // Check for sort: [field:asc|desc]
    let sort = if after_brace.starts_with('[') {
        Some(parse_sort(after_brace)?)
    } else {
        None
    };

    // Check if it's a get (table + key) or query (table + filter)
    if let Some(pipe_pos) = brace_content.find('|') {
        let table = brace_content[..pipe_pos].trim().to_owned();
        let cond_str = brace_content[pipe_pos + 1..].trim();
        let filter = parse_condition(cond_str)?;
        Ok(Expr::QueryNodes {
            table,
            filter: Some(filter),
            sort,
            limit,
            offset,
        })
    } else {
        let parts: Vec<&str> = brace_content.split_whitespace().collect();
        if parts.len() == 1 {
            // ?{table} — scan all
            Ok(Expr::QueryNodes {
                table: parts[0].to_owned(),
                filter: None,
                sort,
                limit,
                offset,
            })
        } else if parts.len() == 2 {
            // ?{table key} — get by key
            Ok(Expr::GetNode {
                table: parts[0].to_owned(),
                key: parts[1].to_owned(),
            })
        } else {
            Err("expected {table} or {table key} or {table | condition}".into())
        }
    }
}

fn parse_traversal(table: String, key: String, traverse_part: &str) -> Result<Expr, String> {
    let trimmed = traverse_part.trim();

    // Check for discovery: (*:N) or (<*:N>)
    if trimmed.starts_with("(*:") || trimmed.starts_with("(<*:") {
        let bidi = trimmed.starts_with("(<*:");
        let num_start = if bidi { 4 } else { 3 };
        let num_end = trimmed.find(')').ok_or("missing ) in discovery")?;
        let n: usize = trimmed[num_start..num_end]
            .parse()
            .map_err(|_| "invalid number in (*:N)")?;
        return Ok(Expr::Discover {
            table,
            key,
            max_hops: n,
            bidi,
        });
    }

    // Directed: (link) [-> (link) ...]
    let mut hops = Vec::new();
    let mut remaining = trimmed;

    loop {
        let (hop, rest) = parse_hop(remaining)?;
        hops.push(hop);

        let rest = rest.trim();
        if let Some(r) = rest.strip_prefix("-> ") {
            remaining = r;
        } else if let Some(r) = rest.strip_prefix("<-> ") {
            remaining = r;
        } else {
            break;
        }
    }

    Ok(Expr::Traverse { table, key, hops })
}

fn parse_hop(s: &str) -> Result<(TraversalHop, &str), String> {
    let s = s.trim();
    if !s.starts_with('(') {
        return Err(format!("expected (link) but got: {s}"));
    }
    let end = s.find(')').ok_or("missing ) in traversal hop")?;
    let link = s[1..end].trim().to_owned();
    let rest = &s[end + 1..];
    Ok((TraversalHop { link, bidi: false }, rest))
}

// --- Helper parsers ---

fn parse_query_prefix(line: &str) -> Result<(Option<usize>, Option<usize>, &str), String> {
    if let Some(rest) = line.strip_prefix("?:") {
        let brace_pos = rest.find('{').ok_or("expected { after ?:N")?;
        let num_part = &rest[..brace_pos];
        if let Some(plus_pos) = num_part.find('+') {
            let limit: usize = num_part[..plus_pos]
                .parse()
                .map_err(|_| "invalid limit number")?;
            let offset: usize = num_part[plus_pos + 1..]
                .parse()
                .map_err(|_| "invalid offset number")?;
            Ok((Some(limit), Some(offset), &rest[brace_pos..]))
        } else {
            let limit: usize = num_part.parse().map_err(|_| "invalid limit number")?;
            Ok((Some(limit), None, &rest[brace_pos..]))
        }
    } else if let Some(rest) = line.strip_prefix('?') {
        Ok((None, None, rest))
    } else {
        Err("expected ? prefix".into())
    }
}

fn extract_braces(s: &str) -> Result<String, String> {
    let s = s.trim();
    if !s.starts_with('{') {
        return Err(format!("expected {{ but got: {s}"));
    }
    let end = s.find('}').ok_or("missing }")?;
    Ok(s[1..end].trim().to_owned())
}

fn skip_past_brace(s: &str) -> &str {
    match s.find('}') {
        Some(pos) => &s[pos + 1..],
        None => "",
    }
}

fn skip_past_paren(s: &str) -> &str {
    match s.find(')') {
        Some(pos) => &s[pos + 1..],
        None => "",
    }
}

fn parse_node_ref(s: &str) -> Result<(String, String), String> {
    let content = extract_braces(s)?;
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(format!("expected {{table key}} but got: {{{content}}}"));
    }
    Ok((parts[0].to_owned(), parts[1].to_owned()))
}

fn parse_link_ref(s: &str) -> Result<(String, String, String, bool), String> {
    let s = s.trim();
    if !s.starts_with('(') {
        return Err(format!("expected ( but got: {s}"));
    }
    let end = s.find(')').ok_or("missing )")?;
    let content = &s[1..end];

    // link from -> to  OR  link from <-> to
    let bidi = content.contains("<->");
    let arrow = if bidi { "<->" } else { "->" };

    let arrow_pos = content
        .find(arrow)
        .ok_or("expected -> or <-> in link expression")?;

    let before_arrow = content[..arrow_pos].trim();
    let after_arrow = content[arrow_pos + arrow.len()..].trim();

    let parts: Vec<&str> = before_arrow.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(format!("expected (link from -> to) but got: ({content})"));
    }

    Ok((
        parts[0].to_owned(),
        parts[1].to_owned(),
        after_arrow.to_owned(),
        bidi,
    ))
}

fn parse_properties(s: &str) -> Result<Properties, String> {
    let s = s.trim();
    if !s.starts_with('[') {
        return Err("expected [".into());
    }
    let end = s.find(']').ok_or("missing ]")?;
    let content = &s[1..end];

    let mut props = Properties::new();
    for pair in content.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let eq_pos = pair
            .find('=')
            .ok_or(format!("expected key=value: {pair}"))?;
        let key = pair[..eq_pos].trim().to_owned();
        let value_str = pair[eq_pos + 1..].trim();
        let value = parse_value(value_str)?;
        props.insert(key, value);
    }
    Ok(props)
}

fn parse_sort(s: &str) -> Result<SortSpec, String> {
    let s = s.trim();
    if !s.starts_with('[') {
        return Err("expected [".into());
    }
    let end = s.find(']').ok_or("missing ]")?;
    let content = &s[1..end];

    // field:asc or field:desc
    if let Some(colon_pos) = content.find(':') {
        let field = content[..colon_pos].trim().to_owned();
        let dir = content[colon_pos + 1..].trim().to_lowercase();
        let desc = dir == "desc";
        Ok(SortSpec { field, desc })
    } else {
        Ok(SortSpec {
            field: content.trim().to_owned(),
            desc: false,
        })
    }
}

pub fn parse_value(s: &str) -> Result<PropertyValue, String> {
    let s = s.trim();

    // Quoted string
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Ok(PropertyValue::String(s[1..s.len() - 1].to_owned()));
    }

    // Boolean
    if s.eq_ignore_ascii_case("true") {
        return Ok(PropertyValue::Boolean(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(PropertyValue::Boolean(false));
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return Ok(PropertyValue::Integer(n));
    }

    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Ok(PropertyValue::Float(f));
    }

    // Unquoted string
    Ok(PropertyValue::String(s.to_owned()))
}

pub fn parse_condition(s: &str) -> Result<Condition, String> {
    let s = s.trim();

    // Handle OR (lowest precedence)
    if let Some(pos) = find_top_level(s, " OR ") {
        let left = parse_condition(&s[..pos])?;
        let right = parse_condition(&s[pos + 4..])?;
        return Ok(Condition::or(vec![left, right]));
    }

    // Handle AND / comma
    if let Some(pos) = find_top_level(s, " AND ") {
        let left = parse_condition(&s[..pos])?;
        let right = parse_condition(&s[pos + 5..])?;
        return Ok(Condition::and(vec![left, right]));
    }
    if let Some(pos) = find_top_level(s, ",") {
        let left = parse_condition(&s[..pos])?;
        let right = parse_condition(&s[pos + 1..])?;
        return Ok(Condition::and(vec![left, right]));
    }

    // Handle NOT
    if s.starts_with("NOT ") || s.starts_with("not ") {
        let inner = parse_condition(&s[4..])?;
        return Ok(Condition::not(inner));
    }

    // Single condition: field op value
    if let Some(pos) = s.find("!=") {
        let field = s[..pos].trim();
        let value_str = s[pos + 2..].trim();
        let value = parse_value(value_str)?;
        return Ok(Condition::ne(field, value));
    }
    if let Some(pos) = s.find(">=") {
        let field = s[..pos].trim();
        let value_str = s[pos + 2..].trim();
        let value = parse_value(value_str)?;
        return Ok(Condition::ge(field, value));
    }
    if let Some(pos) = s.find("<=") {
        let field = s[..pos].trim();
        let value_str = s[pos + 2..].trim();
        let value = parse_value(value_str)?;
        return Ok(Condition::le(field, value));
    }
    if let Some(pos) = s.find('>') {
        let field = s[..pos].trim();
        let value_str = s[pos + 1..].trim();
        let value = parse_value(value_str)?;
        return Ok(Condition::gt(field, value));
    }
    if let Some(pos) = s.find('<') {
        let field = s[..pos].trim();
        let value_str = s[pos + 1..].trim();
        let value = parse_value(value_str)?;
        return Ok(Condition::lt(field, value));
    }
    if let Some(pos) = s.find('=') {
        let field = s[..pos].trim();
        let value_str = s[pos + 1..].trim();
        let value = parse_value(value_str)?;
        return Ok(Condition::eq(field, value));
    }

    Err(format!("cannot parse condition: {s}"))
}

/// Find a delimiter at the top level (not inside parentheses).
fn find_top_level(s: &str, delim: &str) -> Option<usize> {
    let mut depth = 0;
    let bytes = s.as_bytes();
    let delim_bytes = delim.as_bytes();

    for i in 0..bytes.len() {
        if bytes[i] == b'(' {
            depth += 1;
        } else if bytes[i] == b')' {
            depth -= 1;
        } else if depth == 0
            && i + delim_bytes.len() <= bytes.len()
            && &bytes[i..i + delim_bytes.len()] == delim_bytes
        {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_insert_node_with_props() {
        let expr = parse("+{person alice}[role=teacher, age=30]").unwrap();
        match expr {
            Expr::InsertNode {
                table,
                key,
                properties,
            } => {
                assert_eq!(table, "person");
                assert_eq!(key, "alice");
                let props = properties.unwrap();
                assert_eq!(
                    props.get("role"),
                    Some(&PropertyValue::String("teacher".into()))
                );
                assert_eq!(props.get("age"), Some(&PropertyValue::Integer(30)));
            }
            _ => panic!("expected InsertNode"),
        }
    }

    #[test]
    fn parse_insert_node_set_mode() {
        let expr = parse("+{person alice}").unwrap();
        match expr {
            Expr::InsertNode { properties, .. } => assert!(properties.is_none()),
            _ => panic!("expected InsertNode"),
        }
    }

    #[test]
    fn parse_insert_link() {
        let expr = parse("+(attends alice -> mit)[year=2020]").unwrap();
        match expr {
            Expr::InsertLink {
                link,
                from,
                to,
                bidi,
                properties,
            } => {
                assert_eq!(link, "attends");
                assert_eq!(from, "alice");
                assert_eq!(to, "mit");
                assert!(!bidi);
                assert!(properties.is_some());
            }
            _ => panic!("expected InsertLink"),
        }
    }

    #[test]
    fn parse_insert_bidi_link() {
        let expr = parse("+(friends alice <-> bob)").unwrap();
        match expr {
            Expr::InsertLink { bidi, .. } => assert!(bidi),
            _ => panic!("expected InsertLink"),
        }
    }

    #[test]
    fn parse_delete_node() {
        let expr = parse("-{person alice}").unwrap();
        assert!(matches!(expr, Expr::DeleteNode { .. }));
    }

    #[test]
    fn parse_cascade_delete() {
        let expr = parse("-!{person alice}").unwrap();
        assert!(matches!(expr, Expr::CascadeDeleteNode { .. }));
    }

    #[test]
    fn parse_get_node() {
        let expr = parse("?{person alice}").unwrap();
        match expr {
            Expr::GetNode { table, key } => {
                assert_eq!(table, "person");
                assert_eq!(key, "alice");
            }
            _ => panic!("expected GetNode"),
        }
    }

    #[test]
    fn parse_query_all() {
        let expr = parse("?{person}").unwrap();
        match expr {
            Expr::QueryNodes { table, filter, .. } => {
                assert_eq!(table, "person");
                assert!(filter.is_none());
            }
            _ => panic!("expected QueryNodes"),
        }
    }

    #[test]
    fn parse_query_with_filter() {
        let expr = parse("?{person | role=teacher, age>30}").unwrap();
        match expr {
            Expr::QueryNodes { table, filter, .. } => {
                assert_eq!(table, "person");
                assert!(filter.is_some());
            }
            _ => panic!("expected QueryNodes"),
        }
    }

    #[test]
    fn parse_query_with_limit() {
        let expr = parse("?:10{person}").unwrap();
        match expr {
            Expr::QueryNodes { limit, offset, .. } => {
                assert_eq!(limit, Some(10));
                assert!(offset.is_none());
            }
            _ => panic!("expected QueryNodes"),
        }
    }

    #[test]
    fn parse_query_with_limit_offset() {
        let expr = parse("?:10+5{person}").unwrap();
        match expr {
            Expr::QueryNodes { limit, offset, .. } => {
                assert_eq!(limit, Some(10));
                assert_eq!(offset, Some(5));
            }
            _ => panic!("expected QueryNodes"),
        }
    }

    #[test]
    fn parse_query_with_sort() {
        let expr = parse("?{person}[age:desc]").unwrap();
        match expr {
            Expr::QueryNodes { sort, .. } => {
                let s = sort.unwrap();
                assert_eq!(s.field, "age");
                assert!(s.desc);
            }
            _ => panic!("expected QueryNodes"),
        }
    }

    #[test]
    fn parse_traverse() {
        let expr = parse("?{person alice} -> (attends)").unwrap();
        match expr {
            Expr::Traverse { table, key, hops } => {
                assert_eq!(table, "person");
                assert_eq!(key, "alice");
                assert_eq!(hops.len(), 1);
                assert_eq!(hops[0].link, "attends");
            }
            _ => panic!("expected Traverse"),
        }
    }

    #[test]
    fn parse_multi_hop_traverse() {
        let expr = parse("?{person alice} -> (attends) -> (located-in)").unwrap();
        match expr {
            Expr::Traverse { hops, .. } => {
                assert_eq!(hops.len(), 2);
                assert_eq!(hops[0].link, "attends");
                assert_eq!(hops[1].link, "located-in");
            }
            _ => panic!("expected Traverse"),
        }
    }

    #[test]
    fn parse_discovery() {
        let expr = parse("?{person alice} -> (*:3)").unwrap();
        match expr {
            Expr::Discover { max_hops, bidi, .. } => {
                assert_eq!(max_hops, 3);
                assert!(!bidi);
            }
            _ => panic!("expected Discover"),
        }
    }

    #[test]
    fn parse_value_types() {
        assert_eq!(parse_value("42").unwrap(), PropertyValue::Integer(42));
        assert_eq!(parse_value("3.14").unwrap(), PropertyValue::Float(3.14));
        assert_eq!(parse_value("true").unwrap(), PropertyValue::Boolean(true));
        assert_eq!(
            parse_value("\"hello\"").unwrap(),
            PropertyValue::String("hello".into())
        );
        assert_eq!(
            parse_value("world").unwrap(),
            PropertyValue::String("world".into())
        );
    }
}
