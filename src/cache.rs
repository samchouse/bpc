use crate::AppState;
use crate::posters::error_response;
use axum::{
    extract::{Path, State},
    http::{StatusCode, header},
    response::Response,
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    env,
    path::PathBuf,
    sync::{Arc, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;
const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const CACHE_MAX_BYTES: u64 = 10_000_000_000;

#[derive(Clone, Deserialize, serde::Serialize)]
pub(crate) struct CacheEntry {
    pub(crate) source_path: String,
    pub(crate) content_type: String,
    pub(crate) asset_name: String,
    pub(crate) byte_len: u64,
    pub(crate) refreshed_at: u64,
}

struct CacheState {
    entries: HashMap<String, CacheEntry>,
    last_access: HashMap<String, u64>,
    total_bytes: u64,
    initialized: bool,
}

pub(crate) struct DiskCache {
    directory: PathBuf,
    state: AsyncMutex<CacheState>,
    request_locks: AsyncMutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

pub(crate) struct CacheWriter {
    cache: Arc<DiskCache>,
    key: String,
    pub(crate) source_path: String,
    pub(crate) content_type: String,
    temp_path: PathBuf,
    pub(crate) file: Option<tokio::fs::File>,
    pub(crate) byte_len: u64,
}

impl DiskCache {
    pub(crate) fn new() -> Self {
        Self {
            directory: env::var_os("BPC_CACHE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("cache")),
            state: AsyncMutex::new(CacheState {
                entries: HashMap::new(),
                last_access: HashMap::new(),
                total_bytes: 0,
                initialized: false,
            }),
            request_locks: AsyncMutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn request_lock(&self, key: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.request_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(key.to_string(), Arc::downgrade(&lock));
        lock
    }

    pub(crate) async fn begin_stream(
        self: &Arc<Self>,
        key: &str,
        source_path: &str,
        content_type: &str,
    ) -> Option<CacheWriter> {
        let mut state = self.state.lock().await;
        self.initialize(&mut state).await;
        let (_, asset_name) = self.names(&format!("{key}:{source_path}"));
        let temp_path = self
            .directory
            .join(PathBuf::from(asset_name).with_extension("download.tmp"));
        drop(state);
        let file = tokio::fs::File::create(&temp_path).await.ok()?;
        Some(CacheWriter {
            cache: Arc::clone(self),
            key: key.to_string(),
            source_path: source_path.to_string(),
            content_type: content_type.to_string(),
            temp_path,
            file: Some(file),
            byte_len: 0,
        })
    }

    async fn commit_stream(&self, writer: &CacheWriter) {
        let mut state = self.state.lock().await;
        self.initialize(&mut state).await;
        let (index_name, _) = self.names(&writer.key);
        let (_, asset_name) = self.names(&format!("{}:{}", writer.key, writer.source_path));
        let map_key = index_name.trim_end_matches(".json").to_string();
        let old_entry = state.entries.get(&map_key).cloned();
        let old_len = old_entry.as_ref().map_or(0, |entry| entry.byte_len);
        if writer.byte_len > CACHE_MAX_BYTES {
            return;
        }
        state.total_bytes = state.total_bytes.saturating_sub(old_len);
        self.evict_to_fit(&mut state, writer.byte_len, &map_key)
            .await;
        let entry = CacheEntry {
            source_path: writer.source_path.clone(),
            content_type: writer.content_type.clone(),
            asset_name,
            byte_len: writer.byte_len,
            refreshed_at: unix_now(),
        };
        let Ok(metadata) = serde_json::to_vec(&entry) else {
            state.total_bytes = state.total_bytes.saturating_add(old_len);
            return;
        };
        let asset_path = self.directory.join(&entry.asset_name);
        let temp_index = self.directory.join(format!("{index_name}.tmp"));
        if tokio::fs::rename(&writer.temp_path, &asset_path)
            .await
            .is_err()
            || tokio::fs::write(&temp_index, metadata).await.is_err()
            || tokio::fs::rename(&temp_index, self.directory.join(&index_name))
                .await
                .is_err()
        {
            let _ = tokio::fs::remove_file(&temp_index).await;
            state.total_bytes = state.total_bytes.saturating_add(old_len);
            return;
        }
        if let Some(old_entry) = old_entry {
            if old_entry.asset_name != entry.asset_name {
                let _ = tokio::fs::remove_file(self.directory.join(old_entry.asset_name)).await;
            }
        }
        state.total_bytes = state.total_bytes.saturating_add(entry.byte_len);
        state.last_access.insert(map_key.clone(), unix_now());
        state.entries.insert(map_key, entry);
    }

    fn names(&self, key: &str) -> (String, String) {
        let encoded = key
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        (format!("{encoded}.json"), format!("{encoded}.bin"))
    }

    async fn initialize(&self, state: &mut CacheState) {
        if state.initialized {
            return;
        }
        state.initialized = true;
        if tokio::fs::create_dir_all(&self.directory).await.is_err() {
            return;
        }
        let Ok(mut directory) = tokio::fs::read_dir(&self.directory).await else {
            return;
        };
        while let Ok(Some(file)) = directory.next_entry().await {
            if file
                .path()
                .extension()
                .is_none_or(|extension| extension != "json")
            {
                continue;
            }
            let Ok(bytes) = tokio::fs::read(file.path()).await else {
                continue;
            };
            let Ok(entry) = serde_json::from_slice::<CacheEntry>(&bytes) else {
                continue;
            };
            let asset_path = self.directory.join(&entry.asset_name);
            if tokio::fs::metadata(&asset_path).await.is_ok() {
                let key = file
                    .file_name()
                    .to_string_lossy()
                    .trim_end_matches(".json")
                    .to_string();
                state.total_bytes = state.total_bytes.saturating_add(entry.byte_len);
                state.last_access.insert(key.clone(), entry.refreshed_at);
                state.entries.insert(key, entry);
            }
        }
        self.evict_oversized(state).await;
    }

    pub(crate) async fn read(&self, key: &str) -> Option<(CacheEntry, Vec<u8>, bool)> {
        let mut state = self.state.lock().await;
        self.initialize(&mut state).await;
        let (index_name, _) = self.names(key);
        let map_key = index_name.trim_end_matches(".json");
        let entry = state.entries.get(map_key)?.clone();
        state.last_access.insert(map_key.to_string(), unix_now());
        let fresh = unix_now().saturating_sub(entry.refreshed_at) < CACHE_TTL.as_secs();
        let bytes = if fresh {
            tokio::fs::read(self.directory.join(&entry.asset_name))
                .await
                .ok()?
        } else {
            Vec::new()
        };
        Some((entry, bytes, fresh))
    }

    pub(crate) async fn bust(&self, asset_type: &str) -> Option<usize> {
        let mut state = self.state.lock().await;
        self.initialize(&mut state).await;
        let keys = state
            .entries
            .keys()
            .filter(|key| {
                if asset_type == "all" {
                    return true;
                }
                let mut encoded = Vec::with_capacity(key.len() / 2);
                if key.len() % 2 != 0 {
                    return false;
                }
                for pair in key.as_bytes().chunks_exact(2) {
                    let Ok(pair) = std::str::from_utf8(pair) else {
                        return false;
                    };
                    let Ok(byte) = u8::from_str_radix(pair, 16) else {
                        return false;
                    };
                    encoded.push(byte);
                }
                let Ok(cache_key) = String::from_utf8(encoded) else {
                    return false;
                };
                cache_key.starts_with(&format!("{asset_type}:"))
            })
            .cloned()
            .collect::<Vec<_>>();
        let count = keys.len();
        for key in keys {
            self.remove_entry(&mut state, &key).await;
        }
        Some(count)
    }

    pub(crate) async fn renew_if_same(
        &self,
        key: &str,
        source_path: &str,
    ) -> Option<(Vec<u8>, String)> {
        let mut state = self.state.lock().await;
        self.initialize(&mut state).await;
        let (index_name, _) = self.names(key);
        let map_key = index_name.trim_end_matches(".json").to_string();
        let mut entry = state.entries.get(&map_key)?.clone();
        if entry.source_path != source_path {
            return None;
        }
        let bytes = tokio::fs::read(self.directory.join(&entry.asset_name))
            .await
            .ok()?;
        entry.refreshed_at = unix_now();
        let encoded = serde_json::to_vec(&entry).ok()?;
        tokio::fs::write(self.directory.join(&index_name), encoded)
            .await
            .ok()?;
        state.entries.insert(map_key, entry.clone());
        state
            .last_access
            .insert(index_name.trim_end_matches(".json").to_string(), unix_now());
        Some((bytes, entry.content_type))
    }

    pub(crate) async fn write(
        &self,
        key: &str,
        source_path: &str,
        content_type: &str,
        bytes: &[u8],
    ) {
        let mut state = self.state.lock().await;
        self.initialize(&mut state).await;
        let (index_name, _) = self.names(key);
        let (_, asset_name) = self.names(&format!("{key}:{source_path}"));
        let map_key = index_name.trim_end_matches(".json").to_string();
        let old_entry = state.entries.get(&map_key).cloned();
        let old_len = old_entry.as_ref().map_or(0, |entry| entry.byte_len);
        if bytes.len() as u64 > CACHE_MAX_BYTES {
            return;
        }
        state.total_bytes = state.total_bytes.saturating_sub(old_len);
        self.evict_to_fit(&mut state, bytes.len() as u64, &map_key)
            .await;
        let entry = CacheEntry {
            source_path: source_path.to_string(),
            content_type: content_type.to_string(),
            asset_name,
            byte_len: bytes.len() as u64,
            refreshed_at: unix_now(),
        };
        let Ok(metadata) = serde_json::to_vec(&entry) else {
            return;
        };
        let asset_path = self.directory.join(&entry.asset_name);
        let temp_asset = asset_path.with_extension("bin.tmp");
        let temp_index = self.directory.join(format!("{index_name}.tmp"));
        if tokio::fs::write(&temp_asset, bytes).await.is_err()
            || tokio::fs::rename(&temp_asset, &asset_path).await.is_err()
            || tokio::fs::write(&temp_index, metadata).await.is_err()
            || tokio::fs::rename(&temp_index, self.directory.join(&index_name))
                .await
                .is_err()
        {
            let _ = tokio::fs::remove_file(temp_asset).await;
            let _ = tokio::fs::remove_file(temp_index).await;
            state.total_bytes = state.total_bytes.saturating_add(old_len);
            return;
        }
        if let Some(old_entry) = old_entry {
            if old_entry.asset_name != entry.asset_name {
                let _ = tokio::fs::remove_file(self.directory.join(old_entry.asset_name)).await;
            }
        }
        state.total_bytes = state.total_bytes.saturating_add(entry.byte_len);
        state.last_access.insert(map_key.clone(), unix_now());
        state.entries.insert(map_key, entry);
    }

    async fn evict_oversized(&self, state: &mut CacheState) {
        let mut keys = state.entries.keys().cloned().collect::<Vec<_>>();
        keys.sort_by_key(|key| state.last_access.get(key).copied().unwrap_or(0));
        for key in keys {
            if state.total_bytes > CACHE_MAX_BYTES {
                self.remove_entry(state, &key).await;
            }
        }
    }

    async fn evict_to_fit(&self, state: &mut CacheState, incoming: u64, replacing: &str) {
        let mut keys = state
            .entries
            .keys()
            .filter(|key| key.as_str() != replacing)
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_by_key(|key| state.last_access.get(key).copied().unwrap_or(0));
        for key in keys {
            if state.total_bytes.saturating_add(incoming) <= CACHE_MAX_BYTES {
                break;
            }
            self.remove_entry(state, &key).await;
        }
    }

    async fn remove_entry(&self, state: &mut CacheState, key: &str) {
        if let Some(entry) = state.entries.remove(key) {
            state.last_access.remove(key);
            state.total_bytes = state.total_bytes.saturating_sub(entry.byte_len);
            let _ = tokio::fs::remove_file(self.directory.join(entry.asset_name)).await;
            let _ = tokio::fs::remove_file(self.directory.join(format!("{key}.json"))).await;
        }
    }
}

impl CacheWriter {
    pub(crate) async fn write_chunk(&mut self, chunk: &[u8]) {
        let Some(file) = self.file.as_mut() else {
            return;
        };
        let Some(next_len) = self.byte_len.checked_add(chunk.len() as u64) else {
            self.abort().await;
            return;
        };
        if next_len > CACHE_MAX_BYTES || file.write_all(chunk).await.is_err() {
            self.abort().await;
            return;
        }
        self.byte_len = next_len;
    }

    pub(crate) async fn finish(mut self) {
        if let Some(mut file) = self.file.take() {
            if file.flush().await.is_err() {
                self.abort().await;
                return;
            }
        }
        self.cache.commit_stream(&self).await;
        let _ = tokio::fs::remove_file(&self.temp_path).await;
    }

    async fn abort(&mut self) {
        self.file.take();
        let _ = tokio::fs::remove_file(&self.temp_path).await;
    }
}

impl Drop for CacheWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            let path = self.temp_path.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = tokio::fs::remove_file(path).await;
                });
            }
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) async fn bust_cache(
    State(state): State<AppState>,
    Path(asset_type): Path<String>,
) -> Response {
    if !matches!(
        asset_type.as_str(),
        "logo" | "backdrop" | "poster" | "cover" | "covers" | "all"
    ) {
        return error_response(StatusCode::NOT_FOUND, "Unknown cache type");
    }
    let asset_type = if asset_type == "covers" {
        "cover"
    } else {
        &asset_type
    };
    let Some(removed) = state.cache.bust(asset_type).await else {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Could not bust cache");
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(format!("Cleared {removed} cached {asset_type} assets").into())
        .unwrap()
}
