use log::warn;
use reqwest::header::HeaderMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

const CF_API_URL: &str = "https://api.cloudflare.com/client/v4/";

async fn convert_string_to_error(s: &str) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::new(std::io::ErrorKind::Other, s))
}

async fn check_success(resp_json: Value) -> Result<bool, Box<dyn std::error::Error>> {
    match resp_json.get("success") {
        Some(success) => match success.as_bool() {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => Err(convert_string_to_error(
                "The returned 'success' field is not a boolean value.",
            )
            .await),
        },
        None => Err(convert_string_to_error(
            "The returned JSON does not contain the 'success' field.",
        )
        .await),
    }
}

#[derive(Clone)]
pub struct KvClient {
    pub account_id: String,
    pub api_key: String,
    client: Client,
    url: String,
    header_map: HeaderMap,
}

#[derive(Clone, Debug)]
pub struct Namespace {
    pub id: String,
    pub title: String,
}

impl KvClient {
    pub fn new(account_id: &str, api_key: &str) -> Self {
        let headers = HeaderMap::from_iter([
            (
                "Authorization".parse().unwrap(),
                format!("Bearer {}", api_key).parse().unwrap(),
            ),
            (
                "Content-Type".parse().unwrap(),
                "application/json".parse().unwrap(),
            ),
        ]);

        KvClient {
            account_id: account_id.to_string(),
            api_key: api_key.to_string(),
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            url: format!(
                "{}{}{}{}",
                CF_API_URL, "accounts/", account_id, "/storage/kv/namespaces"
            ),
            header_map: headers,
        }
    }

    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .get(self.url.clone())
            .headers(self.header_map.clone())
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        match resp_json.get("result") {
            Some(result) => match result.as_array() {
                Some(namespaces) => {
                    let mut namespace_list = Vec::new();
                    for namespace in namespaces {
                        let id = namespace["id"].as_str().unwrap().to_string();
                        let title = namespace["title"].as_str().unwrap().to_string();
                        namespace_list.push(Namespace { id, title });
                    }
                    Ok(namespace_list)
                }
                None => Err(convert_string_to_error(
                    "The 'results' field cannot be converted to an array.",
                )
                .await),
            },
            None => Err(convert_string_to_error(
                "The returned JSON does not contain the 'result' field.",
            )
            .await),
        }
    }

    pub async fn create_namespace(
        &self,
        title: &str,
    ) -> Result<Namespace, Box<dyn std::error::Error>> {
        let payload = json!({
            "title": title
        });
        let resp = self
            .client
            .post(self.url.clone())
            .headers(self.header_map.clone())
            .json(&payload)
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        match resp_json.get("result") {
            Some(result) => {
                let id = match result.get("id") {
                    Some(id) => match id.as_str() {
                        Some(id) => id,
                        None => {
                            return Err(convert_string_to_error(
                                "The 'id' field cannot be converted to a string.",
                            )
                            .await)
                        }
                    },
                    None => {
                        return Err(convert_string_to_error(
                            "The 'id' field cannot be found in the 'result' field.",
                        )
                        .await)
                    }
                };

                let title = match result.get("title") {
                    Some(title) => match title.as_str() {
                        Some(title) => title,
                        None => {
                            return Err(convert_string_to_error(
                                "The 'title' field cannot be converted to a string.",
                            )
                            .await)
                        }
                    },
                    None => {
                        return Err(convert_string_to_error(
                            "The 'title' field cannot be found in the'result' field.",
                        )
                        .await)
                    }
                };

                Ok(Namespace {
                    id: id.to_string(),
                    title: title.to_string(),
                })
            }
            None => Err(convert_string_to_error(
                "The returned JSON does not contain the 'result' field.",
            )
            .await),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KvNamespaceClient {
    pub account_id: String,
    pub api_key: String,
    pub namespace_id: String,
    client: Client,
    url: String,
    header_map: HeaderMap,
}

impl KvNamespaceClient {
    pub fn new(account_id: &str, api_key: &str, namespace_id: &str) -> Self {
        let headers = HeaderMap::from_iter([
            (
                "Authorization".parse().unwrap(),
                format!("Bearer {}", api_key).parse().unwrap(),
            ),
            (
                "Content-Type".parse().unwrap(),
                "application/json".parse().unwrap(),
            ),
        ]);

        KvNamespaceClient {
            account_id: account_id.to_string(),
            api_key: api_key.to_string(),
            namespace_id: namespace_id.to_string(),
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            url: format!(
                "{}{}{}{}{}",
                CF_API_URL, "accounts/", account_id, "/storage/kv/namespaces/", namespace_id
            ),
            header_map: headers,
        }
    }

    pub fn from_kvclient(kvclient: &KvClient, namespace_id: &str) -> Self {
        KvNamespaceClient {
            account_id: kvclient.account_id.clone(),
            api_key: kvclient.api_key.clone(),
            namespace_id: namespace_id.to_string(),
            client: kvclient.client.clone(),
            url: format!("{}/{}", kvclient.url.clone(), namespace_id),
            header_map: kvclient.header_map.clone(),
        }
    }

    pub async fn delete_namespace(&self) -> Result<(), Box<dyn std::error::Error>> {
        let resp = self
            .client
            .delete(self.url.clone())
            .headers(self.header_map.clone())
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }
        Ok(())
    }

    pub async fn rename_namespace(
        &self,
        new_title: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = json!({
            "title": new_title
        });

        let resp = self
            .client
            .put(self.url.clone())
            .headers(self.header_map.clone())
            .json(&payload)
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        Ok(())
    }
    pub async fn write(&self, payload: KvRequest) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/bulk", self.url);

        let payload_vec = vec![payload];

        let resp = self
            .client
            .put(url)
            .headers(self.header_map.clone())
            .json(&payload_vec)
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;
        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        Ok(())
    }

    pub async fn write_multiple(
        &self,
        payload: Vec<KvRequest>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/bulk", self.url);
        let resp = self
            .client
            .put(url)
            .headers(self.header_map.clone())
            .json(&payload)
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/bulk/delete", self.url);
        let payload = json!([key]);

        let resp = self
            .client
            .post(url)
            .headers(self.header_map.clone())
            .json(&payload)
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        Ok(())
    }

    pub async fn delete_multiple(&self, keys: Vec<&str>) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/bulk/delete", self.url);
        let payload = json!(keys);

        let resp = self
            .client
            .post(url)
            .headers(self.header_map.clone())
            .json(&payload)
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        Ok(())
    }

    pub async fn list_all_keys(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let url = format!("{}/keys", self.url);
        let mut keys = Vec::new();
        let mut cursor = "".to_string();
        loop {
            let url = format!("{}?cursor={}", url, cursor);
            let resp = self
                .client
                .get(url.clone())
                .headers(self.header_map.clone())
                .send()
                .await?;
            if resp.status().is_success() == false {
                warn!("Cloudflare returned an ERROR httpcode.")
            }
            let resp_json = resp.json::<Value>().await?;

            if check_success(resp_json.clone()).await? == false {
                return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
            }

            let results = match resp_json.get("result") {
                Some(result) => match result.as_array() {
                    Some(result) => result,
                    None => {
                        return Err(convert_string_to_error("No result found in response.").await);
                    }
                },
                None => {
                    return Err(convert_string_to_error("No result found in response.").await);
                }
            };

            for result in results {
                match result.get("name") {
                    Some(name) => {
                        let name = match name.as_str() {
                            Some(name) => name,
                            None => {
                                return Err(
                                    convert_string_to_error("No name found in response.").await
                                );
                            }
                        };
                        keys.push(name.to_string());
                    }
                    None => {
                        return Err(convert_string_to_error("No name found in response.").await);
                    }
                }
            }

            let (cursor_tmp, _cursor_count) = match resp_json.get("result_info") {
                Some(result_info) => {
                    let cursor_tmp = match result_info.get("cursor") {
                        Some(cursor) => match cursor.as_str() {
                            Some(cursor) => cursor.to_string(),
                            None => {
                                return Err(convert_string_to_error(
                                    "No cursor found in response.",
                                )
                                .await);
                            }
                        },
                        None => {
                            return Err(
                                convert_string_to_error("No cursor found in response.").await
                            );
                        }
                    };
                    let cursor_count = match result_info.get("count") {
                        Some(count) => match count.as_u64() {
                            Some(count) => count,
                            None => {
                                return Err(
                                    convert_string_to_error("No count found in response.").await
                                );
                            }
                        },
                        None => {
                            return Err(
                                convert_string_to_error("No count found in response.").await
                            );
                        }
                    };
                    (cursor_tmp, cursor_count)
                }
                None => {
                    return Err(convert_string_to_error("No result_info found in response.").await);
                }
            };


            if cursor_tmp.is_empty() {
                break;
            } else {
                cursor = cursor_tmp;
                continue;
            }
        }
        Ok(keys)
    }

    pub async fn read_metadata(&self, key: &str) -> Result<Value, Box<dyn std::error::Error>> {
        let url = format!("{}/metadata/{}", self.url, key);

        let resp = self
            .client
            .get(url)
            .headers(self.header_map.clone())
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        let resp_json = resp.json::<Value>().await?;

        if check_success(resp_json.clone()).await? == false {
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        match resp_json.get("result") {
            Some(result) => Ok(result.clone()),
            None => {
                Err(convert_string_to_error("No result found in response.").await)
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let url = format!("{}/values/{}", self.url, key);

        let resp = self
            .client
            .get(url)
            .headers(self.header_map.clone())
            .send()
            .await?;

        if resp.status().is_success() == false {
            warn!("Cloudflare returned an ERROR httpcode.")
        }

        if resp.status().as_u16() == 404 {
            let resp_json = resp.json::<Value>().await?;
            log::error!("Key: {} Not Found", key);
            return Err(convert_string_to_error(resp_json.to_string().as_str()).await);
        }

        let resp_value = resp.text().await?;

        Ok(resp_value)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KvRequest {
    key: String,
    value: String,
    base64: bool,
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
    metadata: Option<Value>,
}

impl KvRequest {
    pub fn new(key: &str, value: &str) -> Self {
        KvRequest {
            key: key.to_string(),
            value: value.to_string(),
            base64: false,
            expiration: None,
            expiration_ttl: None,
            metadata: None,
        }
    }

    pub fn enable_base64(&self) -> Self {
        KvRequest {
            base64: true,
            key: self.key.clone(),
            value: self.value.clone(),
            expiration: self.expiration,
            expiration_ttl: self.expiration_ttl,
            metadata: self.metadata.clone(),
        }
    }

    pub fn ttl_sec(&self, ttl_sec: u64) -> Self {
        KvRequest {
            base64: self.base64,
            key: self.key.clone(),
            value: self.value.clone(),
            expiration: self.expiration,
            expiration_ttl: Some(ttl_sec),
            metadata: self.metadata.clone(),
        }
    }

    pub fn ttl_timestemp(&self, ttl_timestemp: u64) -> Self {
        KvRequest {
            base64: self.base64,
            key: self.key.clone(),
            value: self.value.clone(),
            expiration: Some(ttl_timestemp),
            expiration_ttl: self.expiration_ttl,
            metadata: self.metadata.clone(),
        }
    }

    pub fn metadata(&self, metadata: Value) -> Self {
        KvRequest {
            base64: self.base64,
            key: self.key.clone(),
            value: self.value.clone(),
            expiration: self.expiration,
            expiration_ttl: self.expiration_ttl,
            metadata: Some(metadata),
        }
    }
}
