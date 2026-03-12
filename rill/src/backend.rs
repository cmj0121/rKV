use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use rkv::{Key, DB};

use crate::msgid::MsgIdGen;

const NS_PREFIX: &str = "rill_";

/// Remote rKV HTTP client.
pub struct RkvClient {
    base_url: String,
    client: Client,
}

/// Backend enum — either an embedded DB or a remote HTTP client.
pub enum Backend {
    Embed(Box<DB>, Arc<MsgIdGen>),
    Remote(RkvClient),
}

impl RkvClient {
    pub fn new(url: &str) -> Self {
        Self {
            base_url: url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api/{}", self.base_url, path)
    }

    fn keys_url(&self, ns: &str) -> String {
        format!("{}/api/{}/keys", self.base_url, ns)
    }

    fn key_url(&self, ns: &str, key: &str) -> String {
        format!("{}/api/{}/keys/{}", self.base_url, ns, key)
    }
}

fn queue_ns(name: &str) -> String {
    format!("{NS_PREFIX}{name}")
}

fn filter_queue_names(namespaces: Vec<String>) -> Vec<String> {
    namespaces
        .into_iter()
        .filter_map(|ns| ns.strip_prefix(NS_PREFIX).map(|s| s.to_string()))
        .collect()
}

impl Backend {
    pub async fn list_queues(&self) -> Result<Vec<String>, String> {
        match self {
            Backend::Embed(db, _) => {
                let all = db.list_namespaces().map_err(|e| e.to_string())?;
                Ok(filter_queue_names(all))
            }
            Backend::Remote(client) => {
                let resp = client
                    .client
                    .get(client.api_url("namespaces"))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                let all: Vec<String> = resp.json().await.map_err(|e| e.to_string())?;
                Ok(filter_queue_names(all))
            }
        }
    }

    pub async fn create_queue(&self, name: &str) -> Result<(), String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db, _) => {
                db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                Ok(())
            }
            Backend::Remote(client) => {
                let resp = client
                    .client
                    .post(client.api_url("namespaces"))
                    .json(&serde_json::json!({ "name": ns_name }))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(format!("create namespace failed: {body}"));
                }
                Ok(())
            }
        }
    }

    pub async fn delete_queue(&self, name: &str) -> Result<(), String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db, _) => {
                // Ignore "does not exist" errors — deleting a non-existent queue is a no-op
                let _ = db.drop_namespace(&ns_name);
                Ok(())
            }
            Backend::Remote(client) => {
                // Delete all keys in the namespace using prefix delete
                let url = format!("{}?prefix=", client.keys_url(&ns_name));
                let resp = client
                    .client
                    .delete(&url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(format!("delete queue failed: {body}"));
                }
                Ok(())
            }
        }
    }

    pub async fn push_message(
        &self,
        name: &str,
        body: &str,
        ttl: Option<Duration>,
    ) -> Result<String, String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db, idgen) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let id = idgen.generate();
                ns.put(Key::Str(id.clone()), body.as_bytes(), ttl)
                    .map_err(|e| e.to_string())?;
                Ok(id)
            }
            Backend::Remote(client) => {
                let id = MsgIdGen::one();
                let mut put_url = client.key_url(&ns_name, &id);
                if let Some(d) = ttl {
                    put_url = format!("{put_url}?ttl={}s", d.as_secs());
                }
                let resp = client
                    .client
                    .put(&put_url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    let err_body = resp.text().await.unwrap_or_default();
                    return Err(format!("push failed: {err_body}"));
                }
                Ok(id)
            }
        }
    }

    pub async fn pop_message(&self, name: &str) -> Result<Option<String>, String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db, _) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let prefix = Key::Str(String::new());
                let entry = ns
                    .entries(&prefix)
                    .map_err(|e| e.to_string())?
                    .filter_map(|e| e.ok())
                    .next();
                match entry {
                    Some((key, value)) => {
                        let data = value
                            .as_bytes()
                            .map(|b| String::from_utf8_lossy(b).to_string());
                        let _ = ns.delete(key);
                        Ok(data)
                    }
                    _ => Ok(None),
                }
            }
            Backend::Remote(client) => {
                // Get first key (limit=1 to avoid downloading all keys)
                let list_url = format!("{}?offset=0&limit=1", client.keys_url(&ns_name));
                let resp = client
                    .client
                    .get(&list_url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    let err = resp.text().await.unwrap_or_default();
                    return Err(format!("list keys failed: {err}"));
                }
                let keys: Vec<String> = resp.json().await.map_err(|e| e.to_string())?;
                let first_key = match keys.first() {
                    Some(k) => k.clone(),
                    None => return Ok(None),
                };
                // Get the value
                let get_url = client.key_url(&ns_name, &first_key);
                let resp = client
                    .client
                    .get(&get_url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                let value = if resp.status().is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    // rKV returns JSON-encoded string, strip quotes
                    serde_json::from_str::<String>(&text).unwrap_or(text)
                } else {
                    return Ok(None);
                };
                // Delete the key
                let del_url = client.key_url(&ns_name, &first_key);
                let del_resp = client
                    .client
                    .delete(&del_url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !del_resp.status().is_success() {
                    let err = del_resp.text().await.unwrap_or_default();
                    return Err(format!("pop delete failed: {err}"));
                }
                Ok(Some(value))
            }
        }
    }

    pub async fn queue_length(&self, name: &str) -> Result<usize, String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db, _) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let count = ns.count().map_err(|e| e.to_string())? as usize;
                Ok(count)
            }
            Backend::Remote(client) => {
                let url = client.keys_url(&ns_name);
                let resp = client
                    .client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    return Ok(0);
                }
                let keys: Vec<String> = resp.json().await.map_err(|e| e.to_string())?;
                Ok(keys.len())
            }
        }
    }

    pub async fn peek_messages(
        &self,
        name: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(String, String)>, String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db, _) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let prefix = Key::Str(String::new());
                let entries: Vec<_> = ns
                    .entries(&prefix)
                    .map_err(|e| e.to_string())?
                    .filter_map(|e| e.ok())
                    .skip(offset)
                    .take(limit)
                    .map(|(key, value)| {
                        let k = match &key {
                            Key::Int(n) => n.to_string(),
                            Key::Str(s) => s.clone(),
                        };
                        let v = value
                            .as_bytes()
                            .map(|b| String::from_utf8_lossy(b).to_string())
                            .unwrap_or_default();
                        (k, v)
                    })
                    .collect();
                Ok(entries)
            }
            Backend::Remote(client) => {
                let list_url = format!(
                    "{}?offset={}&limit={}",
                    client.keys_url(&ns_name),
                    offset,
                    limit
                );
                let resp = client
                    .client
                    .get(&list_url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    return Ok(Vec::new());
                }
                let keys: Vec<String> = resp.json().await.map_err(|e| e.to_string())?;
                let mut results = Vec::new();
                for key in keys {
                    let get_url = client.key_url(&ns_name, &key);
                    let resp = client
                        .client
                        .get(&get_url)
                        .send()
                        .await
                        .map_err(|e| e.to_string())?;
                    let value = if resp.status().is_success() {
                        let text = resp.text().await.unwrap_or_default();
                        serde_json::from_str::<String>(&text).unwrap_or(text)
                    } else {
                        String::new()
                    };
                    results.push((key, value));
                }
                Ok(results)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkv::Config;

    fn embed_backend() -> Backend {
        let db = DB::open(Config::in_memory()).unwrap();
        Backend::Embed(Box::new(db), Arc::new(MsgIdGen::new()))
    }

    #[tokio::test]
    async fn list_queues_empty() {
        let b = embed_backend();
        let queues = b.list_queues().await.unwrap();
        assert!(queues.is_empty());
    }

    #[tokio::test]
    async fn create_and_list_queues() {
        let b = embed_backend();
        b.create_queue("tasks").await.unwrap();
        b.create_queue("events").await.unwrap();
        let mut queues = b.list_queues().await.unwrap();
        queues.sort();
        assert_eq!(queues, vec!["events", "tasks"]);
    }

    #[tokio::test]
    async fn list_queues_ignores_non_rill_namespaces() {
        let b = embed_backend();
        b.create_queue("myq").await.unwrap();
        if let Backend::Embed(db, _) = &b {
            db.namespace("other_ns", None).unwrap();
        }
        let queues = b.list_queues().await.unwrap();
        assert_eq!(queues, vec!["myq"]);
    }

    #[tokio::test]
    async fn push_and_pop_fifo_order() {
        let b = embed_backend();
        b.create_queue("q").await.unwrap();
        b.push_message("q", "first", None).await.unwrap();
        b.push_message("q", "second", None).await.unwrap();
        b.push_message("q", "third", None).await.unwrap();

        assert_eq!(b.pop_message("q").await.unwrap(), Some("first".into()));
        assert_eq!(b.pop_message("q").await.unwrap(), Some("second".into()));
        assert_eq!(b.pop_message("q").await.unwrap(), Some("third".into()));
        assert_eq!(b.pop_message("q").await.unwrap(), None);
    }

    #[tokio::test]
    async fn pop_empty_queue_returns_none() {
        let b = embed_backend();
        b.create_queue("empty").await.unwrap();
        assert_eq!(b.pop_message("empty").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_queue_clears_messages() {
        let b = embed_backend();
        b.create_queue("del").await.unwrap();
        b.push_message("del", "msg1", None).await.unwrap();
        b.push_message("del", "msg2", None).await.unwrap();
        b.delete_queue("del").await.unwrap();
        assert_eq!(b.pop_message("del").await.unwrap(), None);
    }

    #[tokio::test]
    async fn push_after_pop_maintains_fifo() {
        let b = embed_backend();
        b.create_queue("seq").await.unwrap();
        b.push_message("seq", "a", None).await.unwrap();
        b.pop_message("seq").await.unwrap();
        b.push_message("seq", "b", None).await.unwrap();
        assert_eq!(b.pop_message("seq").await.unwrap(), Some("b".into()));
    }

    #[tokio::test]
    async fn multiple_queues_are_isolated() {
        let b = embed_backend();
        b.create_queue("q1").await.unwrap();
        b.create_queue("q2").await.unwrap();
        b.push_message("q1", "from-q1", None).await.unwrap();
        b.push_message("q2", "from-q2", None).await.unwrap();
        assert_eq!(b.pop_message("q1").await.unwrap(), Some("from-q1".into()));
        assert_eq!(b.pop_message("q2").await.unwrap(), Some("from-q2".into()));
    }

    #[test]
    fn filter_queue_names_strips_prefix() {
        let names = vec![
            "rill_tasks".into(),
            "rill_events".into(),
            "other".into(),
            "rill_".into(),
        ];
        let mut result = filter_queue_names(names);
        result.sort();
        assert_eq!(result, vec!["", "events", "tasks"]);
    }

    #[test]
    fn queue_ns_adds_prefix() {
        assert_eq!(queue_ns("tasks"), "rill_tasks");
    }

    #[test]
    fn filter_queue_names_empty_input() {
        assert!(filter_queue_names(vec![]).is_empty());
    }

    #[test]
    fn filter_queue_names_no_matches() {
        let names = vec!["foo".into(), "bar".into()];
        assert!(filter_queue_names(names).is_empty());
    }

    #[test]
    fn rkv_client_url_builders() {
        let client = RkvClient::new("http://localhost:8321/");
        assert_eq!(
            client.api_url("namespaces"),
            "http://localhost:8321/api/namespaces"
        );
        assert_eq!(
            client.keys_url("rill_q1"),
            "http://localhost:8321/api/rill_q1/keys"
        );
        assert_eq!(
            client.key_url("rill_q1", "42"),
            "http://localhost:8321/api/rill_q1/keys/42"
        );
    }

    #[test]
    fn rkv_client_strips_trailing_slash() {
        let client = RkvClient::new("http://host:8321///");
        assert_eq!(client.base_url, "http://host:8321");
    }

    #[tokio::test]
    async fn queue_length_empty() {
        let b = embed_backend();
        b.create_queue("len").await.unwrap();
        assert_eq!(b.queue_length("len").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn queue_length_tracks_push_pop() {
        let b = embed_backend();
        b.create_queue("len").await.unwrap();
        b.push_message("len", "a", None).await.unwrap();
        b.push_message("len", "b", None).await.unwrap();
        assert_eq!(b.queue_length("len").await.unwrap(), 2);
        b.pop_message("len").await.unwrap();
        assert_eq!(b.queue_length("len").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn peek_messages_returns_without_consuming() {
        let b = embed_backend();
        b.create_queue("peek").await.unwrap();
        b.push_message("peek", "x", None).await.unwrap();
        b.push_message("peek", "y", None).await.unwrap();

        let msgs = b.peek_messages("peek", 0, 10).await.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].1, "x");
        assert_eq!(msgs[1].1, "y");

        // Messages still there
        assert_eq!(b.queue_length("peek").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn peek_messages_offset_and_limit() {
        let b = embed_backend();
        b.create_queue("page").await.unwrap();
        for i in 0..5 {
            b.push_message("page", &format!("m{i}"), None)
                .await
                .unwrap();
        }

        let page1 = b.peek_messages("page", 0, 2).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].1, "m0");
        assert_eq!(page1[1].1, "m1");

        let page2 = b.peek_messages("page", 2, 2).await.unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].1, "m2");
        assert_eq!(page2[1].1, "m3");

        let page3 = b.peek_messages("page", 4, 2).await.unwrap();
        assert_eq!(page3.len(), 1);
        assert_eq!(page3[0].1, "m4");
    }

    fn remote_backend() -> Backend {
        // Points at a port nothing listens on — all requests will fail with connection error
        Backend::Remote(RkvClient::new("http://127.0.0.1:1"))
    }

    #[tokio::test]
    async fn remote_list_queues_returns_error() {
        let b = remote_backend();
        assert!(b.list_queues().await.is_err());
    }

    #[tokio::test]
    async fn remote_create_queue_returns_error() {
        let b = remote_backend();
        assert!(b.create_queue("test").await.is_err());
    }

    #[tokio::test]
    async fn remote_delete_queue_returns_error() {
        let b = remote_backend();
        assert!(b.delete_queue("test").await.is_err());
    }

    #[tokio::test]
    async fn remote_push_message_returns_error() {
        let b = remote_backend();
        assert!(b.push_message("test", "msg", None).await.is_err());
    }

    #[tokio::test]
    async fn remote_pop_message_returns_error() {
        let b = remote_backend();
        assert!(b.pop_message("test").await.is_err());
    }

    #[tokio::test]
    async fn remote_queue_length_returns_error() {
        let b = remote_backend();
        assert!(b.queue_length("test").await.is_err());
    }

    #[tokio::test]
    async fn remote_peek_messages_returns_error() {
        let b = remote_backend();
        assert!(b.peek_messages("test", 0, 10).await.is_err());
    }

    #[tokio::test]
    async fn delete_nonexistent_queue_is_noop() {
        let b = embed_backend();
        b.delete_queue("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn queue_length_nonexistent_queue() {
        let b = embed_backend();
        assert_eq!(b.queue_length("nope").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn peek_empty_queue() {
        let b = embed_backend();
        b.create_queue("empty").await.unwrap();
        let msgs = b.peek_messages("empty", 0, 10).await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn peek_beyond_offset() {
        let b = embed_backend();
        b.create_queue("off").await.unwrap();
        b.push_message("off", "a", None).await.unwrap();
        let msgs = b.peek_messages("off", 100, 10).await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn push_returns_ulid_ids() {
        let b = embed_backend();
        b.create_queue("ids").await.unwrap();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let id = b.push_message("ids", "x", None).await.unwrap();
            assert_eq!(id.len(), 26, "ULID should be 26 chars: {id}");
            ids.push(id);
        }
        // IDs are monotonically increasing
        for w in ids.windows(2) {
            assert!(w[0] < w[1], "IDs not monotonic: {} >= {}", w[0], w[1]);
        }
        // Peek returns same IDs in order
        let msgs = b.peek_messages("ids", 0, 10).await.unwrap();
        let peek_ids: Vec<&str> = msgs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(peek_ids, ids);
    }

    #[tokio::test]
    async fn create_queue_idempotent() {
        let b = embed_backend();
        b.create_queue("idem").await.unwrap();
        b.create_queue("idem").await.unwrap();
        let queues = b.list_queues().await.unwrap();
        assert_eq!(queues.iter().filter(|q| *q == "idem").count(), 1);
    }

    #[tokio::test]
    async fn delete_then_recreate_queue() {
        let b = embed_backend();
        b.create_queue("rc").await.unwrap();
        b.push_message("rc", "old", None).await.unwrap();
        b.delete_queue("rc").await.unwrap();
        b.create_queue("rc").await.unwrap();
        assert_eq!(b.pop_message("rc").await.unwrap(), None);
        b.push_message("rc", "new", None).await.unwrap();
        let msgs = b.peek_messages("rc", 0, 10).await.unwrap();
        assert_eq!(msgs[0].1, "new");
    }

    #[tokio::test]
    async fn push_with_ttl() {
        let b = embed_backend();
        b.create_queue("ttl").await.unwrap();
        let id = b
            .push_message("ttl", "expires", Some(Duration::from_secs(3600)))
            .await
            .unwrap();
        assert_eq!(id.len(), 26);
        // Message is readable before expiry
        assert_eq!(b.pop_message("ttl").await.unwrap(), Some("expires".into()));
    }
}
