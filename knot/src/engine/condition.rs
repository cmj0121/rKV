use super::property::PropertyValue;

/// A filter condition on a property.
#[derive(Debug, Clone)]
pub enum Condition {
    /// Property equals value.
    Eq(String, PropertyValue),
    /// Property does not equal value.
    Ne(String, PropertyValue),
    /// Property greater than value.
    Gt(String, PropertyValue),
    /// Property greater than or equal to value.
    Ge(String, PropertyValue),
    /// Property less than value.
    Lt(String, PropertyValue),
    /// Property less than or equal to value.
    Le(String, PropertyValue),
    /// Property exists (is not null/missing).
    Exists(String),
    /// Property does not exist (is null/missing).
    NotExists(String),
    /// String property matches a pattern (% as wildcard).
    Like(String, String),
    /// Property value is one of the given values.
    In(String, Vec<PropertyValue>),
    /// Logical AND of conditions.
    And(Vec<Condition>),
    /// Logical OR of conditions.
    Or(Vec<Condition>),
    /// Logical NOT of a condition.
    Not(Box<Condition>),
}

impl Condition {
    // Convenience constructors.

    pub fn eq(field: &str, value: PropertyValue) -> Self {
        Self::Eq(field.to_owned(), value)
    }

    pub fn ne(field: &str, value: PropertyValue) -> Self {
        Self::Ne(field.to_owned(), value)
    }

    pub fn gt(field: &str, value: PropertyValue) -> Self {
        Self::Gt(field.to_owned(), value)
    }

    pub fn ge(field: &str, value: PropertyValue) -> Self {
        Self::Ge(field.to_owned(), value)
    }

    pub fn lt(field: &str, value: PropertyValue) -> Self {
        Self::Lt(field.to_owned(), value)
    }

    pub fn le(field: &str, value: PropertyValue) -> Self {
        Self::Le(field.to_owned(), value)
    }

    pub fn exists(field: &str) -> Self {
        Self::Exists(field.to_owned())
    }

    pub fn not_exists(field: &str) -> Self {
        Self::NotExists(field.to_owned())
    }

    pub fn like(field: &str, pattern: &str) -> Self {
        Self::Like(field.to_owned(), pattern.to_owned())
    }

    pub fn r#in(field: &str, values: Vec<PropertyValue>) -> Self {
        Self::In(field.to_owned(), values)
    }

    pub fn and(conditions: Vec<Condition>) -> Self {
        Self::And(conditions)
    }

    pub fn or(conditions: Vec<Condition>) -> Self {
        Self::Or(conditions)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(condition: Condition) -> Self {
        Self::Not(Box::new(condition))
    }
}

/// Evaluate a condition against a property map.
#[allow(dead_code)]
pub fn evaluate(
    cond: &Condition,
    props: &std::collections::HashMap<String, PropertyValue>,
) -> bool {
    match cond {
        Condition::Eq(field, value) => props.get(field) == Some(value),
        Condition::Ne(field, value) => props.get(field) != Some(value),
        Condition::Gt(field, value) => props
            .get(field)
            .is_some_and(|v| cmp_prop(v, value) == Some(std::cmp::Ordering::Greater)),
        Condition::Ge(field, value) => props.get(field).is_some_and(|v| {
            matches!(
                cmp_prop(v, value),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            )
        }),
        Condition::Lt(field, value) => props
            .get(field)
            .is_some_and(|v| cmp_prop(v, value) == Some(std::cmp::Ordering::Less)),
        Condition::Le(field, value) => props.get(field).is_some_and(|v| {
            matches!(
                cmp_prop(v, value),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            )
        }),
        Condition::Exists(field) => props.contains_key(field),
        Condition::NotExists(field) => !props.contains_key(field),
        Condition::Like(field, pattern) => props.get(field).is_some_and(|v| {
            if let PropertyValue::String(s) = v {
                match_like(s, pattern)
            } else {
                false
            }
        }),
        Condition::In(field, values) => props.get(field).is_some_and(|v| values.contains(v)),
        Condition::And(conds) => conds.iter().all(|c| evaluate(c, props)),
        Condition::Or(conds) => conds.iter().any(|c| evaluate(c, props)),
        Condition::Not(cond) => !evaluate(cond, props),
    }
}

/// Compare two property values. Returns None if types are incompatible.
fn cmp_prop(a: &PropertyValue, b: &PropertyValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => Some(a.cmp(b)),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => a.partial_cmp(b),
        (PropertyValue::Integer(a), PropertyValue::Float(b)) => (*a as f64).partial_cmp(b),
        (PropertyValue::Float(a), PropertyValue::Integer(b)) => a.partial_cmp(&(*b as f64)),
        (PropertyValue::String(a), PropertyValue::String(b)) => Some(a.cmp(b)),
        (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Simple LIKE matching: % matches any sequence of characters.
fn match_like(value: &str, pattern: &str) -> bool {
    let parts: Vec<&str> = pattern.split('%').collect();
    if parts.len() == 1 {
        return value == pattern;
    }

    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            // Must start with this part
            if !value.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Must end with this part
            if !value[pos..].ends_with(part) {
                return false;
            }
        } else {
            // Must contain this part after current position
            match value[pos..].find(part) {
                Some(idx) => pos += idx + part.len(),
                None => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn props(pairs: &[(&str, PropertyValue)]) -> HashMap<String, PropertyValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn eq_condition() {
        let p = props(&[("age", PropertyValue::Integer(30))]);
        assert!(evaluate(
            &Condition::eq("age", PropertyValue::Integer(30)),
            &p
        ));
        assert!(!evaluate(
            &Condition::eq("age", PropertyValue::Integer(25)),
            &p
        ));
    }

    #[test]
    fn ne_condition() {
        let p = props(&[("age", PropertyValue::Integer(30))]);
        assert!(evaluate(
            &Condition::ne("age", PropertyValue::Integer(25)),
            &p
        ));
        assert!(!evaluate(
            &Condition::ne("age", PropertyValue::Integer(30)),
            &p
        ));
    }

    #[test]
    fn gt_lt_conditions() {
        let p = props(&[("age", PropertyValue::Integer(30))]);
        assert!(evaluate(
            &Condition::gt("age", PropertyValue::Integer(25)),
            &p
        ));
        assert!(!evaluate(
            &Condition::gt("age", PropertyValue::Integer(30)),
            &p
        ));
        assert!(evaluate(
            &Condition::lt("age", PropertyValue::Integer(35)),
            &p
        ));
        assert!(evaluate(
            &Condition::ge("age", PropertyValue::Integer(30)),
            &p
        ));
        assert!(evaluate(
            &Condition::le("age", PropertyValue::Integer(30)),
            &p
        ));
    }

    #[test]
    fn exists_conditions() {
        let p = props(&[("name", PropertyValue::String("alice".into()))]);
        assert!(evaluate(&Condition::exists("name"), &p));
        assert!(!evaluate(&Condition::exists("age"), &p));
        assert!(evaluate(&Condition::not_exists("age"), &p));
    }

    #[test]
    fn like_condition() {
        let p = props(&[("name", PropertyValue::String("alice".into()))]);
        assert!(evaluate(&Condition::like("name", "ali%"), &p));
        assert!(evaluate(&Condition::like("name", "%ice"), &p));
        assert!(evaluate(&Condition::like("name", "%lic%"), &p));
        assert!(!evaluate(&Condition::like("name", "bob%"), &p));
        assert!(evaluate(&Condition::like("name", "alice"), &p));
    }

    #[test]
    fn in_condition() {
        let p = props(&[("role", PropertyValue::String("teacher".into()))]);
        let cond = Condition::r#in(
            "role",
            vec![
                PropertyValue::String("teacher".into()),
                PropertyValue::String("admin".into()),
            ],
        );
        assert!(evaluate(&cond, &p));
    }

    #[test]
    fn and_or_not() {
        let p = props(&[
            ("role", PropertyValue::String("teacher".into())),
            ("age", PropertyValue::Integer(30)),
        ]);

        let cond = Condition::and(vec![
            Condition::eq("role", PropertyValue::String("teacher".into())),
            Condition::gt("age", PropertyValue::Integer(25)),
        ]);
        assert!(evaluate(&cond, &p));

        let cond = Condition::or(vec![
            Condition::eq("role", PropertyValue::String("student".into())),
            Condition::gt("age", PropertyValue::Integer(25)),
        ]);
        assert!(evaluate(&cond, &p));

        let cond = Condition::not(Condition::eq(
            "role",
            PropertyValue::String("student".into()),
        ));
        assert!(evaluate(&cond, &p));
    }

    #[test]
    fn mixed_number_comparison() {
        let p = props(&[("score", PropertyValue::Float(9.5))]);
        assert!(evaluate(
            &Condition::gt("score", PropertyValue::Integer(9)),
            &p
        ));
        assert!(evaluate(
            &Condition::lt("score", PropertyValue::Integer(10)),
            &p
        ));
    }

    #[test]
    fn missing_field_evaluates_correctly() {
        let p = props(&[]);
        assert!(!evaluate(
            &Condition::eq("age", PropertyValue::Integer(30)),
            &p
        ));
        assert!(evaluate(
            &Condition::ne("age", PropertyValue::Integer(30)),
            &p
        ));
        assert!(!evaluate(
            &Condition::gt("age", PropertyValue::Integer(0)),
            &p
        ));
        assert!(evaluate(&Condition::not_exists("age"), &p));
    }
}
