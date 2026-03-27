use base64::Engine as _;
use rkv::{RevisionID, Value};

use super::backend::Backend;
use super::error::{Error, Result};

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// Remote backend — connects to an rKV HTTP server.
pub struct RemoteBackend {
    base_url: String,
    client: reqwest::blocking::Client,
}

impl RemoteBackend {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            client: reqwest::blocking::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/{path}", self.base_url)
    }
}

impl Backend for RemoteBackend {
    fn ensure_namespace(&self, ns: &str) -> Result<()> {
        // rKV creates namespaces on first access — just access it
        let _ = self
            .client
            .get(self.url(&format!("{ns}/keys?limit=0")))
            .send();
        Ok(())
    }

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        let _ = self
            .client
            .delete(self.url(ns))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;
        Ok(())
    }

    fn get(&self, ns: &str, key: &str) -> Result<Option<Value>> {
        let res = self
            .client
            .get(self.url(&format!("{ns}/keys/{key}")))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        if res.status().as_u16() == 404 {
            return Ok(None);
        }
        if res.status().as_u16() == 204 {
            return Ok(Some(Value::Null));
        }

        let text = res.text().map_err(|e| Error::StorageError(e.to_string()))?;

        // rKV returns JSON — values stored by Knot are base64 strings or "null"
        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| Error::StorageError(format!("json parse: {e}")))?;

        match json {
            serde_json::Value::Null => Ok(Some(Value::Null)),
            serde_json::Value::String(s) => {
                // Decode base64 → msgpack bytes
                match B64.decode(&s) {
                    Ok(bytes) => Ok(Some(Value::Data(bytes))),
                    Err(_) => Ok(Some(Value::Data(s.into_bytes()))),
                }
            }
            _ => Ok(Some(Value::Null)),
        }
    }

    fn put(&self, ns: &str, key: &str, value: Value) -> Result<()> {
        let json_body = match &value {
            Value::Data(bytes) => {
                // Encode binary data as base64 JSON string
                let encoded = B64.encode(bytes);
                serde_json::Value::String(encoded)
            }
            Value::Null => serde_json::Value::Null,
            _ => serde_json::Value::Null,
        };

        self.client
            .put(self.url(&format!("{ns}/keys/{key}")))
            .header("Content-Type", "application/json")
            .json(&json_body)
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;
        Ok(())
    }

    fn delete(&self, ns: &str, key: &str) -> Result<()> {
        self.client
            .delete(self.url(&format!("{ns}/keys/{key}")))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;
        Ok(())
    }

    fn exists(&self, ns: &str, key: &str) -> Result<bool> {
        let res = self
            .client
            .head(self.url(&format!("{ns}/keys/{key}")))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;
        Ok(res.status().is_success())
    }

    fn scan(&self, ns: &str, prefix: &str, limit: usize) -> Result<Vec<String>> {
        let url = if prefix.is_empty() {
            format!("{ns}/keys?limit={limit}")
        } else {
            format!("{ns}/keys?prefix={prefix}&limit={limit}")
        };

        let res = self
            .client
            .get(self.url(&url))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        let keys: Vec<String> = res.json().map_err(|e| Error::StorageError(e.to_string()))?;
        Ok(keys)
    }

    fn count(&self, ns: &str) -> Result<u64> {
        let res = self
            .client
            .get(self.url(&format!("{ns}/count")))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        let count: u64 = res
            .text()
            .map_err(|e| Error::StorageError(e.to_string()))?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| Error::StorageError(e.to_string()))?;
        Ok(count)
    }

    fn rev_count(&self, ns: &str, key: &str) -> Result<u64> {
        let res = self
            .client
            .get(self.url(&format!("{ns}/keys/{key}/revisions")))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        if res.status().as_u16() == 404 {
            return Ok(0);
        }

        let count: u64 = res
            .text()
            .map_err(|e| Error::StorageError(e.to_string()))?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| Error::StorageError(e.to_string()))?;
        Ok(count)
    }

    fn rev_get(&self, ns: &str, key: &str, index: u64) -> Result<Option<Value>> {
        let res = self
            .client
            .get(self.url(&format!("{ns}/keys/{key}/revisions/{index}")))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        if res.status().as_u16() == 404 || res.status().as_u16() == 410 {
            return Ok(None);
        }

        let bytes = res
            .bytes()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        if bytes.is_empty() {
            Ok(Some(Value::Null))
        } else {
            Ok(Some(Value::Data(bytes.to_vec())))
        }
    }

    fn get_revision_id(&self, _ns: &str, _key: &str) -> Result<RevisionID> {
        // Remote backend cannot easily get revision IDs — return ZERO
        Ok(RevisionID::ZERO)
    }

    fn list_namespaces(&self, prefix: &str) -> Result<Vec<String>> {
        let res = self
            .client
            .get(format!("{}/api/namespaces", self.base_url))
            .send()
            .map_err(|e| Error::StorageError(e.to_string()))?;

        let all: Vec<String> = res.json().map_err(|e| Error::StorageError(e.to_string()))?;
        Ok(all.into_iter().filter(|n| n.starts_with(prefix)).collect())
    }
}
