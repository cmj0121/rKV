use reqwest::Client;
use rkv::{Key, DB};

const NS_PREFIX: &str = "rill_";

/// Remote rKV HTTP client.
pub struct RkvClient {
    base_url: String,
    client: Client,
}

/// Backend enum — either an embedded DB or a remote HTTP client.
pub enum Backend {
    Embed(Box<DB>),
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
            Backend::Embed(db) => {
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
            Backend::Embed(db) => {
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
            Backend::Embed(db) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let prefix = Key::Str(String::new());
                let keys: Vec<_> = ns
                    .keys(&prefix)
                    .map_err(|e| e.to_string())?
                    .filter_map(|k| k.ok())
                    .collect();
                for key in keys {
                    let _ = ns.delete(key);
                }
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

    pub async fn push_message(&self, name: &str, body: &str) -> Result<(), String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let prefix = Key::Str(String::new());
                let next_id = match ns.rkeys(&prefix).map_err(|e| e.to_string())?.next() {
                    Some(Ok(Key::Int(n))) => n + 1,
                    _ => 0,
                };
                ns.put(Key::Int(next_id), body.as_bytes(), None)
                    .map_err(|e| e.to_string())?;
                Ok(())
            }
            Backend::Remote(client) => {
                // Find highest key via reverse scan
                let list_url = format!("{}?reverse=true&limit=1", client.keys_url(&ns_name));
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
                let next_id: i64 = match keys.first() {
                    Some(k) => k.parse::<i64>().unwrap_or(0) + 1,
                    None => 0,
                };
                // Put the new message
                let put_url = client.key_url(&ns_name, &next_id.to_string());
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
                Ok(())
            }
        }
    }

    pub async fn pop_message(&self, name: &str) -> Result<Option<String>, String> {
        let ns_name = queue_ns(name);
        match self {
            Backend::Embed(db) => {
                let ns = db.namespace(&ns_name, None).map_err(|e| e.to_string())?;
                let prefix = Key::Str(String::new());
                let mut entries = ns.entries(&prefix).map_err(|e| e.to_string())?;
                match entries.next() {
                    Some(Ok((key, value))) => {
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
}
