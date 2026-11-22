use ab_glyph::{FontRef, PxScale, VariableFont};
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::get,
};
use futures_util::StreamExt;
use image::{DynamicImage, ImageFormat, Rgba};
use imageproc::drawing::{draw_text_mut, text_size};
use rusto::{DetectTextBuilderExt, DetectTextResult, DetectionRunOptions, InitializeConfig, RustO};
use serde::Deserialize;
use std::{
    collections::HashMap,
    env, fs,
    io::Cursor,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    tmdb_api_key: Arc<str>,
    font: Arc<[u8]>,
    cover_font: Arc<[u8]>,
    ocr: Arc<Mutex<RustO>>,
    cache: Arc<DiskCache>,
}

const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const CACHE_MAX_BYTES: u64 = 10_000_000_000;
const HTTP_CACHE_CONTROL: &str = "public, max-age=86760";

#[derive(Clone, Deserialize, serde::Serialize)]
struct CacheEntry {
    source_path: String,
    content_type: String,
    asset_name: String,
    byte_len: u64,
    refreshed_at: u64,
}

struct CacheState {
    entries: HashMap<String, CacheEntry>,
    last_access: HashMap<String, u64>,
    total_bytes: u64,
    initialized: bool,
}

struct DiskCache {
    directory: PathBuf,
    state: AsyncMutex<CacheState>,
    request_locks: AsyncMutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

struct CacheWriter {
    cache: Arc<DiskCache>,
    key: String,
    source_path: String,
    content_type: String,
    temp_path: PathBuf,
    file: Option<tokio::fs::File>,
    byte_len: u64,
}

impl DiskCache {
    fn new() -> Self {
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

    async fn request_lock(&self, key: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.request_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(key.to_string(), Arc::downgrade(&lock));
        lock
    }

    async fn begin_stream(
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

    async fn read(&self, key: &str) -> Option<(CacheEntry, Vec<u8>, bool)> {
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

    async fn bust(&self, asset_type: &str) -> Option<usize> {
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

    async fn renew_if_same(&self, key: &str, source_path: &str) -> Option<(Vec<u8>, String)> {
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

    async fn write(&self, key: &str, source_path: &str, content_type: &str, bytes: &[u8]) {
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
    async fn write_chunk(&mut self, chunk: &[u8]) {
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

    async fn finish(mut self) {
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

fn cached_response(bytes: impl Into<Body>, content_type: &str) -> Response {
    let content_type = HeaderValue::from_str(content_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, HTTP_CACHE_CONTROL)
        .body(bytes.into())
        .unwrap()
}

#[derive(Default, Deserialize)]
struct TmdbImages {
    #[serde(default)]
    posters: Vec<TmdbPoster>,
    #[serde(default)]
    backdrops: Vec<TmdbPoster>,
    #[serde(default)]
    logos: Vec<TmdbPoster>,
}

#[derive(Clone, Deserialize)]
struct TmdbPoster {
    file_path: String,
    iso_639_1: Option<String>,
    #[serde(default)]
    width: u32,
    #[serde(default)]
    vote_count: u64,
    #[serde(default)]
    vote_average: f32,
}

#[derive(Deserialize)]
struct TmdbDetails {
    #[serde(default)]
    original_language: String,
}

fn preferred_language_images(images: Vec<TmdbPoster>, original_language: &str) -> Vec<TmdbPoster> {
    let preferred_language = if original_language == "fr" {
        "fr"
    } else {
        "en"
    };
    let preferred: Vec<_> = images
        .iter()
        .filter(|image| image.iso_639_1.as_deref() == Some(preferred_language))
        .cloned()
        .collect();
    if !preferred.is_empty() {
        return preferred;
    }
    let fallback_language = if preferred_language == "fr" {
        Some("en")
    } else if original_language != preferred_language {
        Some(original_language)
    } else {
        None
    };
    fallback_language.map_or_else(Vec::new, |language| {
        images
            .into_iter()
            .filter(|image| image.iso_639_1.as_deref() == Some(language))
            .collect()
    })
}

async fn fetch_tmdb_images(
    state: &AppState,
    tmdb_type: &str,
    id: u64,
    original_language: &str,
) -> Result<TmdbImages, StatusCode> {
    let url = format!("https://api.themoviedb.org/3/{tmdb_type}/{id}/images");
    let include_languages = format!("en,fr,{original_language},null");
    let response = state
        .http
        .get(url)
        .query(&[("include_image_language", include_languages.as_str())])
        .bearer_auth(&*state.tmdb_api_key)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err(StatusCode::NOT_FOUND);
    }
    if !response.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY);
    }
    response
        .json::<TmdbImages>()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)
}

pub fn app() -> Router {
    let tmdb_api_key = env::var("TMDB_API_KEY").expect("TMDB_API_KEY must be set");
    let font = fs::read("assets/Teko-wght.ttf")
        .or_else(|_| fs::read("assets/DejaVuSans-Bold.ttf"))
        .expect("Teko font or fallback font must be available");
    let cover_font = fs::read("assets/Inter-Variable.ttf")
        .or_else(|_| fs::read("assets/Teko-wght.ttf"))
        .expect("Inter font or fallback font must be available");
    let ocr = initialize_ocr().expect("Could not initialize English OCR engine");
    let state = AppState {
        http: reqwest::Client::new(),
        tmdb_api_key: tmdb_api_key.into(),
        font: font.into(),
        cover_font: cover_font.into(),
        ocr: Arc::new(Mutex::new(ocr)),
        cache: Arc::new(DiskCache::new()),
    };

    Router::new()
        .route("/bust/{asset_type}", get(bust_cache))
        .route("/posters/{media_type}/{id}", get(get_poster))
        .route("/backdrops/{media_type}/{id}", get(get_backdrop))
        .route("/logos/{media_type}/{id}", get(get_logo))
        .route("/covers/{service}/{media_type}", get(get_cover))
        .with_state(state)
}

async fn get_cover(
    State(state): State<AppState>,
    Path((service, media_type)): Path<(String, String)>,
) -> Response {
    if !matches!(media_type.as_str(), "movie" | "series") {
        return error_response(StatusCode::NOT_FOUND, "Unknown cover type");
    }
    let service = service.to_ascii_lowercase();
    let cache_key = format!("cover:v11:{service}:{media_type}");
    let request_lock = state.cache.request_lock(&cache_key).await;
    let _request_guard = request_lock.lock_owned().await;
    if let Some((entry, bytes, fresh)) = state.cache.read(&cache_key).await {
        if fresh {
            return cover_response(bytes, &entry.content_type);
        }
    }
    match render_cover(&service, &media_type, &state.cover_font) {
        Ok(bytes) => {
            state
                .cache
                .write(&cache_key, "generated:v11", "image/png", &bytes)
                .await;
            cover_response(bytes, "image/png")
        }
        Err(status) => error_response(status, "Could not render cover"),
    }
}

fn cover_response(bytes: impl Into<Body>, content_type: &str) -> Response {
    let mut response = cached_response(bytes, content_type);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    response
}

fn render_cover(service: &str, media_type: &str, font_data: &[u8]) -> Result<Vec<u8>, StatusCode> {
    let (accent, secondary, bloom_x, sweep, logo) = match service.to_ascii_lowercase().as_str() {
        "netflix" => (
            [229, 9, 20],
            [255, 80, 28],
            425.0,
            ((0.0, 680.0), (600.0, 360.0)),
            "netflix.png",
        ),
        "disney" | "disney-plus" | "disneyplus" => (
            [26, 78, 220],
            [0, 190, 255],
            175.0,
            ((0.0, 260.0), (600.0, 530.0)),
            "disney-plus-light.png",
        ),
        "prime" | "prime-video" | "amazon" => (
            [0, 168, 225],
            [24, 75, 207],
            425.0,
            ((35.0, 760.0), (565.0, 250.0)),
            "amazon-prime-video.png",
        ),
        "hulu" => (
            [28, 231, 131],
            [0, 125, 83],
            175.0,
            ((0.0, 350.0), (600.0, 650.0)),
            "hulu.png",
        ),
        "max" => (
            [140, 76, 255],
            [49, 20, 151],
            425.0,
            ((0.0, 780.0), (600.0, 340.0)),
            "max-light.png",
        ),
        "apple" | "apple-tv" | "appletv" => (
            [178, 87, 255],
            [39, 137, 255],
            175.0,
            ((0.0, 280.0), (600.0, 560.0)),
            "apple-tv-light.png",
        ),
        _ => return Err(StatusCode::NOT_FOUND),
    };
    let (sweep_start, sweep_end) = sweep;
    let sweep_dx = sweep_end.0 - sweep_start.0;
    let sweep_dy = sweep_end.1 - sweep_start.1;
    let sweep_length_squared = sweep_dx * sweep_dx + sweep_dy * sweep_dy;
    let mut image = image::RgbaImage::new(600, 900);
    for (x, y, pixel) in image.enumerate_pixels_mut() {
        let dx = (x as f32 - 300.0) / 250.0;
        let logo_dy = (y as f32 - 190.0) / 270.0;
        let title_dy = (y as f32 - 510.0) / 240.0;
        let logo_glow = (-(dx * dx + logo_dy * logo_dy) * 1.8).exp() * 0.30;
        let title_glow = (-(dx * dx + title_dy * title_dy) * 2.2).exp() * 0.15;
        let bloom_dx = (x as f32 - bloom_x) / 260.0;
        let bloom_dy = (y as f32 - 600.0) / 310.0;
        let color_bloom = (-(bloom_dx * bloom_dx + bloom_dy * bloom_dy) * 1.7).exp() * 0.24;
        let edge_dx = (x as f32 - 300.0) / 340.0;
        let edge_dy = (y as f32 - 930.0) / 145.0;
        let edge_glow = (-(edge_dx * edge_dx + edge_dy * edge_dy) * 1.5).exp() * 0.28;
        let to_x = x as f32 - sweep_start.0;
        let to_y = y as f32 - sweep_start.1;
        let projection = ((to_x * sweep_dx + to_y * sweep_dy) / sweep_length_squared)
            .clamp(0.0, 1.0);
        let nearest_x = sweep_start.0 + projection * sweep_dx;
        let nearest_y = sweep_start.1 + projection * sweep_dy;
        let distance_squared = (x as f32 - nearest_x).powi(2) + (y as f32 - nearest_y).powi(2);
        let light_sweep = (-(distance_squared) / (2.0 * 34.0_f32.powi(2))).exp() * 0.12;
        let mut grain = (x as u32)
            .wrapping_mul(374_761_393)
            .wrapping_add((y as u32).wrapping_mul(668_265_263));
        grain = (grain ^ (grain >> 13)).wrapping_mul(1_274_126_177);
        let grain = (grain & 7) as f32 - 3.0;
        *pixel = Rgba([
            (7.0 + accent[0] as f32 * (logo_glow + title_glow)
                + secondary[0] as f32 * color_bloom
                + secondary[0] as f32 * edge_glow
                + accent[0] as f32 * light_sweep
                + grain).clamp(0.0, 255.0) as u8,
            (10.0 + accent[1] as f32 * (logo_glow + title_glow)
                + secondary[1] as f32 * color_bloom
                + secondary[1] as f32 * edge_glow
                + accent[1] as f32 * light_sweep
                + grain).clamp(0.0, 255.0) as u8,
            (18.0 + accent[2] as f32 * (logo_glow + title_glow)
                + secondary[2] as f32 * color_bloom
                + secondary[2] as f32 * edge_glow
                + accent[2] as f32 * light_sweep
                + grain).clamp(0.0, 255.0) as u8,
            255,
        ]);
    }
    let logo = fs::read(PathBuf::from("assets/logos").join(logo))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let service_logo = image::load_from_memory(&logo)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .to_rgba8();
    let logo_scale = (320.0 / service_logo.width() as f32)
        .min(220.0 / service_logo.height() as f32);
    let logo_width = (service_logo.width() as f32 * logo_scale).round() as u32;
    let logo_height = (service_logo.height() as f32 * logo_scale).round() as u32;
    let service_logo = image::imageops::resize(
        &service_logo,
        logo_width,
        logo_height,
        image::imageops::FilterType::Lanczos3,
    );
    let logo_x = (image.width() - logo_width) / 2;
    image::imageops::overlay(&mut image, &service_logo, logo_x as i64, 76);
    let mut font = FontRef::try_from_slice(font_data).expect("loaded cover font is valid");
    font.set_variation(b"wght", 800.0);
    let mut title_size = 172.0_f32;
    let (initial_title_width, _) = text_size(PxScale::from(title_size), &font, "TOP 10");
    if initial_title_width > 400 {
        title_size *= 400.0 / initial_title_width as f32;
    }
    let title_scale = PxScale::from(title_size);
    let (title_width, _) = text_size(title_scale, &font, "TOP 10");
    let title_x = (image.width() as i32 - title_width as i32) / 2;
    draw_text_mut(&mut image, Rgba([255, 255, 255, 255]), title_x, 420, title_scale, &font, "TOP 10");
    let category = if media_type == "series" {
        "Series"
    } else {
        "Movies"
    };
    let category_scale = PxScale::from(82.0);
    let (category_width, _) = text_size(category_scale, &font, category);
    let category_x = (image.width() as i32 - category_width as i32) / 2;
    draw_text_mut(
        &mut image,
        Rgba([255, 255, 255, 255]),
        category_x,
        575,
        category_scale,
        &font,
        category,
    );
    let mut output = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(image)
        .write_to(&mut output, ImageFormat::Png)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(output.into_inner())
}

async fn bust_cache(State(state): State<AppState>, Path(asset_type): Path<String>) -> Response {
    if !matches!(asset_type.as_str(), "logo" | "backdrop" | "poster" | "cover" | "covers" | "all") {
        return error_response(StatusCode::NOT_FOUND, "Unknown cache type");
    }
    let asset_type = if asset_type == "covers" { "cover" } else { &asset_type };
    let Some(removed) = state.cache.bust(asset_type).await else {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Could not bust cache");
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(format!("Cleared {removed} cached {asset_type} assets").into())
        .unwrap()
}

async fn get_logo(
    State(state): State<AppState>,
    Path((media_type, id)): Path<(String, u64)>,
) -> Response {
    let request_started = Instant::now();
    let cache_key = format!("logo:v1:{media_type}:{id}");
    let request_lock = state.cache.request_lock(&cache_key).await;
    let request_guard = request_lock.lock_owned().await;
    if let Some((entry, bytes, fresh)) = state.cache.read(&cache_key).await {
        if fresh {
            log_image_request("logo", &media_type, id, true, request_started.elapsed());
            return cached_response(bytes, &entry.content_type);
        }
    }
    let tmdb_type = match media_type.as_str() {
        "movie" => "movie",
        "series" => "tv",
        _ => return error_response(StatusCode::NOT_FOUND, "Unknown media type"),
    };
    let details_url = format!("https://api.themoviedb.org/3/{tmdb_type}/{id}");
    let details = match state
        .http
        .get(details_url)
        .bearer_auth(&*state.tmdb_api_key)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<TmdbDetails>().await {
                Ok(details) => details,
                Err(_) => return error_response(StatusCode::BAD_GATEWAY, "Invalid TMDB response"),
            }
        }
        Ok(response) if response.status() == StatusCode::NOT_FOUND => {
            return error_response(StatusCode::NOT_FOUND, "Logo not found");
        }
        _ => return error_response(StatusCode::BAD_GATEWAY, "TMDB request failed"),
    };
    let images = match fetch_tmdb_images(&state, tmdb_type, id, &details.original_language).await {
        Ok(images) => images,
        Err(StatusCode::NOT_FOUND) => {
            return error_response(StatusCode::NOT_FOUND, "Logo not found");
        }
        Err(_) => return error_response(StatusCode::BAD_GATEWAY, "TMDB images request failed"),
    };
    let logos = preferred_language_images(images.logos, &details.original_language);
    // Preserve TMDB's ordering and take the first logo wide enough to stay
    // sharp at the w1280 delivery size. If none meet the threshold, use the
    // first available logo.
    const MIN_LOGO_WIDTH: u32 = 1000;
    let selected = logos
        .iter()
        .find(|logo| logo.width >= MIN_LOGO_WIDTH)
        .or_else(|| logos.first())
        .map(|logo| logo.file_path.clone());
    let Some(logo_path) = selected else {
        return error_response(StatusCode::NOT_FOUND, "Logo not found");
    };
    if let Some((bytes, content_type)) = state.cache.renew_if_same(&cache_key, &logo_path).await {
        log_image_request("logo", &media_type, id, true, request_started.elapsed());
        return cached_response(bytes, &content_type);
    }
    let response = match state
        .http
        .get(format!("https://image.tmdb.org/t/p/w1280{logo_path}"))
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return error_response(StatusCode::BAD_GATEWAY, "Could not fetch logo image"),
    };
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    log_image_request("logo", &media_type, id, false, request_started.elapsed());
    let writer = state
        .cache
        .begin_stream(&cache_key, &logo_path, &content_type)
        .await;
    let upstream = response.bytes_stream();
    let stream = futures_util::stream::unfold(
        (upstream, writer, Some(request_guard)),
        |(mut upstream, mut writer, request_guard)| async move {
            match upstream.next().await {
                Some(Ok(chunk)) => {
                    if let Some(cache_writer) = writer.as_mut() {
                        cache_writer.write_chunk(&chunk).await;
                        if cache_writer.file.is_none() {
                            writer = None;
                        }
                    }
                    Some((
                        Ok::<_, reqwest::Error>(chunk),
                        (upstream, writer, request_guard),
                    ))
                }
                Some(Err(error)) => Some((Err(error), (upstream, writer, request_guard))),
                None => {
                    if let Some(cache_writer) = writer {
                        cache_writer.finish().await;
                    }
                    None
                }
            }
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, HTTP_CACHE_CONTROL)
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn get_backdrop(
    State(state): State<AppState>,
    Path((media_type, id)): Path<(String, u64)>,
) -> Response {
    let request_started = Instant::now();
    let cache_key = format!("backdrop:v1:{media_type}:{id}");
    let request_lock = state.cache.request_lock(&cache_key).await;
    let request_guard = request_lock.lock_owned().await;
    if let Some((entry, bytes, fresh)) = state.cache.read(&cache_key).await {
        if fresh {
            log_image_request("backdrop", &media_type, id, true, request_started.elapsed());
            return cached_response(bytes, &entry.content_type);
        }
    }
    let tmdb_type = match media_type.as_str() {
        "movie" => "movie",
        "series" => "tv",
        _ => return error_response(StatusCode::NOT_FOUND, "Unknown media type"),
    };
    let details_url = format!("https://api.themoviedb.org/3/{tmdb_type}/{id}");
    let details = match state
        .http
        .get(details_url)
        .bearer_auth(&*state.tmdb_api_key)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<TmdbDetails>().await {
                Ok(details) => details,
                Err(_) => return error_response(StatusCode::BAD_GATEWAY, "Invalid TMDB response"),
            }
        }
        Ok(response) if response.status() == StatusCode::NOT_FOUND => {
            return error_response(StatusCode::NOT_FOUND, "Backdrop not found");
        }
        _ => return error_response(StatusCode::BAD_GATEWAY, "TMDB request failed"),
    };
    let images = match fetch_tmdb_images(&state, tmdb_type, id, &details.original_language).await {
        Ok(images) => images,
        Err(StatusCode::NOT_FOUND) => {
            return error_response(StatusCode::NOT_FOUND, "Backdrop not found");
        }
        Err(_) => return error_response(StatusCode::BAD_GATEWAY, "TMDB images request failed"),
    };
    let backdrops = preferred_language_images(images.backdrops, &details.original_language);
    let has_well_voted = backdrops.iter().any(|b| b.vote_count > 10);
    let backdrops: Vec<_> = if has_well_voted {
        backdrops
            .into_iter()
            .filter(|b| b.vote_count >= 5)
            .collect()
    } else {
        backdrops
    };
    let max_width = backdrops
        .iter()
        .map(|backdrop| backdrop.width)
        .max()
        .unwrap_or(0);
    let average_rating = if backdrops.is_empty() {
        0.0
    } else {
        backdrops.iter().map(|b| b.vote_average).sum::<f32>() / backdrops.len() as f32
    };
    let selected = backdrops
        .into_iter()
        .max_by(|a, b| {
            let score = |item: &TmdbPoster| {
                let votes = item.vote_count as f32;
                let rating = if has_well_voted {
                    (votes * item.vote_average + 5.0 * average_rating) / (votes + 5.0)
                } else {
                    item.vote_average
                };
                let resolution = if max_width == 0 {
                    0.0
                } else {
                    (item.width as f32 / max_width as f32).powi(2)
                };
                rating * resolution
            };
            score(a)
                .total_cmp(&score(b))
                .then_with(|| a.vote_count.cmp(&b.vote_count))
                .then_with(|| a.width.cmp(&b.width))
        })
        .map(|backdrop| backdrop.file_path);
    let Some(backdrop_path) = selected else {
        return error_response(StatusCode::NOT_FOUND, "Backdrop not found");
    };
    if let Some((bytes, content_type)) = state.cache.renew_if_same(&cache_key, &backdrop_path).await
    {
        log_image_request("backdrop", &media_type, id, true, request_started.elapsed());
        return cached_response(bytes, &content_type);
    }
    // Backdrops are commonly uploaded at 1920px wide. w1280 keeps them sharp
    // for display while avoiding the extra transfer and buffer cost of original.
    let response = match state
        .http
        .get(format!("https://image.tmdb.org/t/p/w1280{backdrop_path}"))
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return error_response(StatusCode::BAD_GATEWAY, "Could not fetch backdrop image"),
    };
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    log_image_request(
        "backdrop",
        &media_type,
        id,
        false,
        request_started.elapsed(),
    );
    let writer = state
        .cache
        .begin_stream(&cache_key, &backdrop_path, &content_type)
        .await;
    let upstream = response.bytes_stream();
    let stream = futures_util::stream::unfold(
        (upstream, writer, Some(request_guard)),
        |(mut upstream, mut writer, request_guard)| async move {
            match upstream.next().await {
                Some(Ok(chunk)) => {
                    if let Some(cache_writer) = writer.as_mut() {
                        cache_writer.write_chunk(&chunk).await;
                        if cache_writer.file.is_none() {
                            writer = None;
                        }
                    }
                    Some((
                        Ok::<_, reqwest::Error>(chunk),
                        (upstream, writer, request_guard),
                    ))
                }
                Some(Err(error)) => {
                    if let Some(mut cache_writer) = writer {
                        cache_writer.abort().await;
                    }
                    Some((Err(error), (upstream, None, request_guard)))
                }
                None => {
                    if let Some(cache_writer) = writer {
                        cache_writer.finish().await;
                    }
                    None
                }
            }
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, HTTP_CACHE_CONTROL)
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn get_poster(
    State(state): State<AppState>,
    Path((media_type, id)): Path<(String, u64)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let request_started = Instant::now();
    let raw_cache_key = format!("poster:v1:{media_type}:{id}");
    let request_lock = state.cache.request_lock(&raw_cache_key).await;
    let _request_guard = request_lock.lock().await;
    let ranked_cache_key = query
        .get("rank")
        .map(|rank| format!("poster:v1:{media_type}:{id}:r:{rank}"));
    if let Some(key) = ranked_cache_key.as_deref() {
        if let Some((entry, bytes, fresh)) = state.cache.read(key).await {
            if fresh {
                log_cached_poster_timing(&media_type, id, request_started.elapsed().as_millis());
                return cached_response(bytes, &entry.content_type);
            }
        }
    } else if let Some((entry, bytes, fresh)) = state.cache.read(&raw_cache_key).await {
        if fresh {
            log_cached_poster_timing(&media_type, id, request_started.elapsed().as_millis());
            return cached_response(bytes, &entry.content_type);
        }
    }
    if let Some(rank) = query.get("rank") {
        if let Some((entry, bytes, fresh)) = state.cache.read(&raw_cache_key).await {
            if fresh {
                let render_started = Instant::now();
                let rendered =
                    match render_ranked_poster(&bytes, &entry.content_type, rank, &state.font) {
                        Ok(image) => image,
                        Err(status) => {
                            return error_response(status, "Could not render ranked poster");
                        }
                    };
                if let Some(key) = ranked_cache_key.as_deref() {
                    state
                        .cache
                        .write(key, &entry.source_path, &entry.content_type, &rendered)
                        .await;
                }
                let render_ms = render_started.elapsed().as_millis();
                log_cached_rank_timing(
                    &media_type,
                    id,
                    render_ms,
                    request_started.elapsed().as_millis(),
                );
                return cached_response(rendered, &entry.content_type);
            }
        }
    }
    let tmdb_type = match media_type.as_str() {
        "movie" => "movie",
        "series" => "tv",
        _ => return error_response(StatusCode::NOT_FOUND, "Unknown media type"),
    };
    let tmdb_started = Instant::now();
    let response = state
        .http
        .get(format!("https://api.themoviedb.org/3/{tmdb_type}/{id}"))
        .bearer_auth(&*state.tmdb_api_key)
        .send()
        .await;
    let details = match response {
        Ok(response) if response.status().is_success() => {
            match response.json::<TmdbDetails>().await {
                Ok(details) => details,
                Err(_) => {
                    return error_response(StatusCode::BAD_GATEWAY, "Invalid TMDB response");
                }
            }
        }
        Ok(response) if response.status() == StatusCode::NOT_FOUND => {
            return error_response(StatusCode::NOT_FOUND, "Poster not found");
        }
        _ => return error_response(StatusCode::BAD_GATEWAY, "TMDB request failed"),
    };
    let images = match fetch_tmdb_images(&state, tmdb_type, id, &details.original_language).await {
        Ok(images) => images,
        Err(StatusCode::NOT_FOUND) => {
            return error_response(StatusCode::NOT_FOUND, "Poster not found");
        }
        Err(_) => return error_response(StatusCode::BAD_GATEWAY, "TMDB images request failed"),
    };
    let posters = preferred_language_images(images.posters, &details.original_language);
    let tmdb_ms = tmdb_started.elapsed().as_millis();
    let has_well_voted_poster = posters.iter().any(|poster| poster.vote_count > 10);
    let posters: Vec<_> = if has_well_voted_poster {
        posters
            .into_iter()
            .filter(|poster| poster.vote_count >= 5)
            .collect()
    } else {
        posters
    };
    let max_width = posters.iter().map(|poster| poster.width).max().unwrap_or(0);
    let average_rating = if posters.is_empty() {
        0.0
    } else {
        posters
            .iter()
            .map(|poster| poster.vote_average)
            .sum::<f32>()
            / posters.len() as f32
    };
    let no_well_voted_posters = !has_well_voted_poster;
    const PRIOR_VOTE_COUNT: f32 = 5.0;
    let mut ranked_posters: Vec<_> = posters.into_iter().collect();
    ranked_posters.sort_by(|a, b| {
        if no_well_voted_posters {
            return a
                .vote_average
                .total_cmp(&b.vote_average)
                .then_with(|| a.width.cmp(&b.width))
                .then_with(|| a.vote_count.cmp(&b.vote_count));
        }
        let score = |poster: &TmdbPoster| {
            let votes = poster.vote_count as f32;
            let bayesian_rating = (votes * poster.vote_average + PRIOR_VOTE_COUNT * average_rating)
                / (votes + PRIOR_VOTE_COUNT);
            // Nearly perfect ratings are often based on a tiny, unrepresentative
            // set of votes, so reduce their influence after Bayesian shrinkage.
            let perfect_rating_penalty = (poster.vote_average - 9.5).max(0.0) * 2.0;
            let resolution_factor = if max_width == 0 {
                0.0
            } else {
                (poster.width as f32 / max_width as f32).powi(2)
            };
            (bayesian_rating - perfect_rating_penalty) * resolution_factor
        };
        score(a)
            .total_cmp(&score(b))
            .then_with(|| a.vote_count.cmp(&b.vote_count))
            .then_with(|| a.width.cmp(&b.width))
    });
    ranked_posters.reverse();
    // Fetch the first two candidates concurrently, then fetch the third only
    // if OCR rejects both of them.
    let top_candidates: Vec<_> = ranked_posters.into_iter().take(3).collect();
    // Stale entries retain their previous image path. If TMDB still returns
    // that path among the current top candidates, renew its 24-hour lease and
    // serve the stored bytes without downloading or re-running OCR.
    for candidate in &top_candidates {
        let path = &candidate.file_path;
        if let Some(key) = ranked_cache_key.as_deref() {
            if let Some((bytes, content_type)) = state.cache.renew_if_same(key, path).await {
                log_cached_poster_timing(&media_type, id, request_started.elapsed().as_millis());
                return cached_response(bytes, &content_type);
            }
        }
        if let Some((bytes, content_type)) = state.cache.renew_if_same(&raw_cache_key, path).await {
            if ranked_cache_key.is_none() {
                log_cached_poster_timing(&media_type, id, request_started.elapsed().as_millis());
                return cached_response(bytes, &content_type);
            }
            if let Some(rank) = query.get("rank") {
                let render_started = Instant::now();
                let rendered = match render_ranked_poster(&bytes, &content_type, rank, &state.font)
                {
                    Ok(image) => image,
                    Err(status) => return error_response(status, "Could not render ranked poster"),
                };
                if let Some(key) = ranked_cache_key.as_deref() {
                    state.cache.write(key, path, &content_type, &rendered).await;
                }
                let render_ms = render_started.elapsed().as_millis();
                log_cached_rank_timing(
                    &media_type,
                    id,
                    render_ms,
                    request_started.elapsed().as_millis(),
                );
                return cached_response(rendered, &content_type);
            }
        }
    }
    let candidate_started = Instant::now();
    let mut downloads = tokio::task::JoinSet::new();
    let initial_download_count = top_candidates.len().min(2);
    for (index, candidate) in top_candidates
        .iter()
        .take(initial_download_count)
        .enumerate()
    {
        let http = state.http.clone();
        let url = format!("https://image.tmdb.org/t/p/w780{}", candidate.file_path);
        downloads.spawn(async move {
            let response = http.get(url).send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            let content_type = response
                .headers()
                .get(header::CONTENT_TYPE)
                .cloned()
                .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
            let bytes = response.bytes().await.ok()?;
            Some((index, bytes, content_type))
        });
    }
    let mut candidate_images = vec![None; top_candidates.len()];
    let mut downloads_pending = initial_download_count;
    let mut fetch_ms = 0_u128;

    // Run OCR in rank order and reject posters where OCR finds no readable text.
    // Fetch the third candidate only if both of the first two are rejected.
    let mut selected_poster = None;
    let mut selected_path = None;
    let mut checked_candidates = 0;
    let mut ocr_ms = 0_u128;
    for index in 0..top_candidates.len() {
        checked_candidates += 1;
        while index < initial_download_count
            && candidate_images[index].is_none()
            && downloads_pending > 0
        {
            let wait_started = Instant::now();
            let result = downloads.join_next().await;
            fetch_ms += wait_started.elapsed().as_millis();
            downloads_pending -= 1;
            if let Some(Ok(Some((downloaded_index, bytes, content_type)))) = result {
                candidate_images[downloaded_index] = Some((bytes, content_type));
            }
        }
        if index == 2 {
            let candidate = &top_candidates[index];
            let image_url = format!("https://image.tmdb.org/t/p/w780{}", candidate.file_path);
            let third_fetch_started = Instant::now();
            if let Ok(response) = state.http.get(image_url).send().await {
                if response.status().is_success() {
                    let content_type = response
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .cloned()
                        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
                    if let Ok(bytes) = response.bytes().await {
                        candidate_images[index] = Some((bytes, content_type));
                    }
                }
            }
            fetch_ms += third_fetch_started.elapsed().as_millis();
        }
        let Some((bytes, content_type)) = candidate_images[index].take() else {
            continue;
        };
        let image = match image::load_from_memory(&bytes) {
            Ok(image) => image,
            Err(_) => continue,
        };
        let rgb = image.to_rgb8();
        let width = rgb.width();
        let height = rgb.height();
        let ocr = Arc::clone(&state.ocr);
        let ocr_started = Instant::now();
        let recognized = match tokio::task::spawn_blocking(move || {
            let mut engine = ocr.lock().ok()?;
            let result = engine
                .detect_text_rgb_builder(rgb.as_raw(), width, height, width as usize * 3)
                // Use a larger TMDB rendition for more reliable text detection.
                .with_detection(DetectionRunOptions {
                    limit_side_len: Some(960),
                    limit_type: Some("max".to_string()),
                    ..Default::default()
                })
                .with_auto_unwarp(false)
                .with_layout_reading(false)
                .with_orientation_detection(false)
                .as_lines()
                .run()
                .ok()?;
            match result {
                DetectTextResult::Structured(lines) => {
                    Some(lines)
                }
                _ => Some(Vec::new()),
            }
        })
        .await
        {
            Ok(Some(lines)) => lines,
            _ => return error_response(StatusCode::BAD_GATEWAY, "Poster OCR failed"),
        };
        ocr_ms += ocr_started.elapsed().as_millis();
        let suitable = poster_text_suitability(&recognized, width, height);
        if !suitable {
            continue;
        }
        selected_poster = Some((bytes, content_type));
        selected_path = Some(top_candidates[index].file_path.clone());
        downloads.abort_all();
        break;
    }
    let candidate_ms = candidate_started.elapsed().as_millis();
    let mut render_ms = 0;
    let Some((bytes, content_type)) = selected_poster else {
        log_poster_timings(
            &media_type,
            id,
            tmdb_ms,
            fetch_ms,
            candidate_ms,
            ocr_ms,
            checked_candidates,
            render_ms,
            request_started.elapsed().as_millis(),
        );
        return error_response(StatusCode::NOT_FOUND, "Suitable poster not found");
    };
    let selected_path = selected_path.expect("selected poster has a TMDB path");
    let content_type_text = content_type
        .to_str()
        .unwrap_or("application/octet-stream")
        .to_string();
    state
        .cache
        .write(&raw_cache_key, &selected_path, &content_type_text, &bytes)
        .await;
    if let Some(rank) = query.get("rank") {
        let render_started = Instant::now();
        let output = match render_ranked_poster(&bytes, &content_type_text, rank, &state.font) {
            Ok(image) => image,
            Err(status) => return error_response(status, "Could not render ranked poster"),
        };
        if let Some(key) = ranked_cache_key.as_deref() {
            state
                .cache
                .write(key, &selected_path, &content_type_text, &output)
                .await;
        }
        let response = cached_response(output, &content_type_text);
        render_ms = render_started.elapsed().as_millis();
        log_poster_timings(
            &media_type,
            id,
            tmdb_ms,
            fetch_ms,
            candidate_ms,
            ocr_ms,
            checked_candidates,
            render_ms,
            request_started.elapsed().as_millis(),
        );
        return response;
    }
    let response = cached_response(bytes, &content_type_text);
    log_poster_timings(
        &media_type,
        id,
        tmdb_ms,
        fetch_ms,
        candidate_ms,
        ocr_ms,
        checked_candidates,
        render_ms,
        request_started.elapsed().as_millis(),
    );
    response
}

fn log_poster_timings(
    media_type: &str,
    id: u64,
    tmdb_ms: u128,
    fetch_ms: u128,
    candidate_ms: u128,
    ocr_ms: u128,
    checked_candidates: usize,
    render_ms: u128,
    total_ms: u128,
) {
    eprintln!(
        "poster_timing media_type={media_type} id={id} debug_build={} tmdb_ms={tmdb_ms} fetch_ms={fetch_ms} candidate_ms={candidate_ms} ocr_ms={ocr_ms} checked={checked_candidates} render_ms={render_ms} total_ms={total_ms}",
        cfg!(debug_assertions)
    );
}

fn log_cached_poster_timing(media_type: &str, id: u64, total_ms: u128) {
    eprintln!(
        "poster_timing media_type={media_type} id={id} debug_build={} cache_hit=true tmdb_ms=0 fetch_ms=0 candidate_ms=0 ocr_ms=0 checked=0 render_ms=0 total_ms={total_ms}",
        cfg!(debug_assertions)
    );
}

fn log_cached_rank_timing(media_type: &str, id: u64, render_ms: u128, total_ms: u128) {
    eprintln!(
        "poster_timing media_type={media_type} id={id} debug_build={} cache_hit=true tmdb_ms=0 fetch_ms=0 candidate_ms=0 ocr_ms=0 checked=0 render_ms={render_ms} total_ms={total_ms}",
        cfg!(debug_assertions)
    );
}

fn log_image_request(
    asset_type: &str,
    media_type: &str,
    id: u64,
    cache_hit: bool,
    elapsed: Duration,
) {
    eprintln!(
        "{asset_type}_request media_type={media_type} id={id} cache_hit={cache_hit} total_ms={}",
        elapsed.as_millis()
    );
}

fn render_ranked_poster(
    bytes: &[u8],
    content_type: &str,
    rank: &str,
    font_data: &[u8],
) -> Result<Vec<u8>, StatusCode> {
    let mime_type = content_type.split(';').next().unwrap_or("image/jpeg");
    let format = ImageFormat::from_mime_type(mime_type).unwrap_or(ImageFormat::Jpeg);
    let mut poster = image::load_from_memory(bytes)
        .map_err(|_| StatusCode::BAD_GATEWAY)?
        .to_rgba8();
    let mut font = FontRef::try_from_slice(font_data).expect("loaded font is valid");
    font.set_variation(b"wght", 700.0);
    let mut font_size = 356.0_f32;
    let (text_width, _) = text_size(PxScale::from(font_size), &font, rank);
    if text_width > poster.width().saturating_sub(36) {
        font_size *= poster.width().saturating_sub(36) as f32 / text_width.max(1) as f32;
    }
    draw_frosted_text(&mut poster, 40, 24, PxScale::from(font_size), &font, rank);
    let mut output = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(poster)
        .write_to(&mut output, format)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(output.into_inner())
}

fn draw_frosted_text(
    image: &mut image::RgbaImage,
    x: i32,
    y: i32,
    scale: PxScale,
    font: &FontRef<'_>,
    text: &str,
) {
    let source = image.clone();
    // Blur the full poster before clipping to the glyphs. This lets nearby
    // shapes and colors bleed into each letter, as they do through frosted glass.
    let mut mask = image::RgbaImage::new(image.width(), image.height());
    draw_text_mut(
        &mut mask,
        Rgba([255, 255, 255, 255]),
        x,
        y,
        scale,
        font,
        text,
    );

    // Font metrics describe advance size, but glyph outlines can extend beyond
    // that box. Find the actual painted glyph bounds to avoid clipping ascenders
    // or descenders when applying the glass treatment.
    let mut bounds = (image.width(), image.height(), 0, 0);
    for (px, py, pixel) in mask.enumerate_pixels() {
        if pixel[3] > 0 {
            bounds.0 = bounds.0.min(px);
            bounds.1 = bounds.1.min(py);
            bounds.2 = bounds.2.max(px);
            bounds.3 = bounds.3.max(py);
        }
    }
    if bounds.0 > bounds.2 || bounds.1 > bounds.3 {
        return;
    }
    let (left, top, right, bottom) = bounds;
    // Add a dark, blurred corner vignette that reaches the poster's top and
    // left edges and softly extends beyond the lettering to the right and below.
    let margin_x = (scale.x * 1.12).round() as u32;
    let margin_y = (scale.y * 1.05).round() as u32;
    let patch_right = right.saturating_add(margin_x).min(image.width() - 1);
    let patch_bottom = bottom.saturating_add(margin_y).min(image.height() - 1);
    // The frosted treatment only reads the top-left text/vignette patch. Blur
    // that region with a halo instead of blurring the entire poster twice.
    let blur_halo = 21;
    let blur_width = patch_right.saturating_add(1 + blur_halo).min(image.width());
    let blur_height = patch_bottom
        .saturating_add(1 + blur_halo)
        .min(image.height());
    let blur_input = image::imageops::crop_imm(&source, 0, 0, blur_width, blur_height).to_image();
    let backdrop_blur = image::imageops::blur(&blur_input, 7.0);
    let radius_x = patch_right.max(1) as f32;
    let radius_y = patch_bottom.max(1) as f32;
    for py in 0..=patch_bottom {
        for px in 0..=patch_right {
            let distance = ((px as f32 / radius_x).powi(2) + (py as f32 / radius_y).powi(2)).sqrt();
            let falloff = (1.0 - distance).clamp(0.0, 1.0);
            let falloff = falloff * falloff * (3.0 - 2.0 * falloff);
            let alpha = 0.78 * falloff;
            let original = source.get_pixel(px, py);
            let blurred = backdrop_blur.get_pixel(px, py);
            let pixel = image.get_pixel_mut(px, py);
            for channel in 0..3 {
                let dark_blur = f32::from(blurred[channel]) * 0.45;
                pixel[channel] =
                    (f32::from(original[channel]) * (1.0 - alpha) + dark_blur * alpha) as u8;
            }
        }
    }
    for py in top..=bottom {
        let t = (py - top) as f32 / (bottom - top).max(1) as f32;
        // Keep the upper letters crisp white, then ease the glass treatment
        // into the lower tips instead of frosting the entire glyph.
        let frost_t = ((t - 0.55) / 0.45).clamp(0.0, 1.0);
        let frost = frost_t * frost_t * (3.0 - 2.0 * frost_t);
        let tint = [226.0, 240.0, 255.0];
        for px in left..=right {
            let coverage = f32::from(mask.get_pixel(px, py)[3]) / 255.0;
            if coverage == 0.0 {
                continue;
            }

            let original = source.get_pixel(px, py);
            let refracted = backdrop_blur.get_pixel(px, py);
            let mut color = [0.0_f32; 3];
            for channel in 0..3 {
                // The blurred backdrop supplies the glass body; the translucent
                // gradient keeps it pale enough to read against varied artwork.
                let glass = refracted[channel] as f32 * 0.72 + tint[channel] * 0.28;
                let fill = 255.0 * (1.0 - frost) + glass * frost;
                color[channel] = original[channel] as f32 * (1.0 - coverage) + fill * coverage;
            }

            // Brighten the antialiased outer rim like the article's separate,
            // brighter glass-edge layer.
            let edge = px == 0
                || py == 0
                || px + 1 >= image.width()
                || py + 1 >= image.height()
                || mask.get_pixel(px - 1, py)[3] == 0
                || mask.get_pixel(px + 1, py)[3] == 0
                || mask.get_pixel(px, py - 1)[3] == 0
                || mask.get_pixel(px, py + 1)[3] == 0;
            if edge && frost > 0.0 {
                for value in &mut color {
                    *value = (*value * (1.0 + 0.12 * frost) + 16.0 * coverage * frost).min(255.0);
                }
            }
            let pixel = image.get_pixel_mut(px, py);
            for channel in 0..3 {
                pixel[channel] = color[channel] as u8;
            }
        }
    }
}

fn error_response(status: StatusCode, message: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(message))
        .unwrap()
}

fn poster_text_suitability(
    lines: &[rusto::TextResult],
    width: u32,
    height: u32,
) -> bool {
    let lines: Vec<_> = lines
        .iter()
        .filter(|line| !line.text.trim().is_empty())
        .collect();
    if lines.is_empty() {
        return false;
    }

    // Posters with copy scattered across multiple areas are usually billing-heavy
    // designs, even when each individual line is short.
    let occupied_areas: std::collections::HashSet<_> = lines
        .iter()
        .map(|line| {
            let x = line.box_points.iter().map(|point| point.0).sum::<f32>() / 4.0;
            let y = line.box_points.iter().map(|point| point.1).sum::<f32>() / 4.0;
            ((x / width.max(1) as f32 * 3.0).floor() as u8,
             (y / height.max(1) as f32 * 3.0).floor() as u8)
        })
        .collect();
    if lines.len() >= 4 && occupied_areas.len() >= 4 {
        return false;
    }

    // Treat the longest recognized line as the likely title. Permit up to four
    // additional words for a subtitle or minor incidental copy.
    let word_count = |text: &str| {
        text.split_whitespace()
            .filter(|word| word.chars().any(char::is_alphabetic))
            .count()
    };
    let total_words: usize = lines.iter().map(|line| word_count(&line.text)).sum();
    let title_words = lines
        .iter()
        .map(|line| word_count(&line.text))
        .max()
        .unwrap_or(0);
    if total_words > title_words + 4 {
        return false;
    }
    true
}

fn initialize_ocr() -> Result<RustO, Box<dyn std::error::Error>> {
    rusto::init();
    let cache_dir = rusto::models::get_cache_dir(None);
    rusto::download_model_pipeline(
        Some("ppocrv6-tiny"),
        Some("en"),
        false,
        false,
        false,
        None,
        false,
        &cache_dir,
    )?;
    let (det, rec, dict) = rusto::models::resolve_base_pipeline(
        Some("ppocrv6-tiny"),
        None,
        Some("en"),
        None,
        None,
        None,
        &cache_dir,
    )?;
    Ok(RustO::initialize(InitializeConfig::new(det, rec, dict))?)
}

#[tokio::main]
async fn main() {
    if let Err(error) = download_fonts().await {
        eprintln!("Could not download fonts: {error}");
    }
    if let Err(error) = download_service_logos().await {
        eprintln!("Could not download service logos: {error}");
    }
    let app = app();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn download_service_logos() -> Result<(), Box<dyn std::error::Error>> {
    // Logos are distributed by selfh.st/icons under CC BY 4.0.
    const FILES: &[&str] = &[
        "netflix.png",
        "disney-plus-light.png",
        "amazon-prime-video.png",
        "hulu.png",
        "max-light.png",
        "apple-tv-light.png",
    ];
    let directory = FsPath::new("assets/logos");
    fs::create_dir_all(directory)?;
    let client = reqwest::Client::new();
    for filename in FILES {
        let path = directory.join(filename);
        if path.exists() {
            continue;
        }
        let url = format!("https://cdn.jsdelivr.net/gh/selfhst/icons/png/{filename}");
        let bytes = client.get(url).send().await?.error_for_status()?.bytes().await?;
        fs::write(path, bytes)?;
    }
    Ok(())
}

async fn download_fonts() -> Result<(), Box<dyn std::error::Error>> {
    const FILES: &[(&str, &str)] = &[
        (
            "assets/Teko-wght.ttf",
            "https://raw.githubusercontent.com/google/fonts/main/ofl/teko/Teko%5Bwght%5D.ttf",
        ),
        (
            "assets/Teko-OFL.txt",
            "https://raw.githubusercontent.com/google/fonts/main/ofl/teko/OFL.txt",
        ),
        (
            "assets/Inter-Variable.ttf",
            "https://raw.githubusercontent.com/google/fonts/main/ofl/inter/Inter%5Bopsz%2Cwght%5D.ttf",
        ),
        (
            "assets/Inter-OFL.txt",
            "https://raw.githubusercontent.com/google/fonts/main/ofl/inter/OFL.txt",
        ),
    ];
    fs::create_dir_all("assets")?;
    let client = reqwest::Client::new();
    for (path, url) in FILES {
        if FsPath::new(path).exists() {
            continue;
        }
        let bytes = client
            .get(*url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        fs::write(path, bytes)?;
    }
    Ok(())
}
