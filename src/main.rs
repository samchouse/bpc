use axum::{
    Router,
    body::Body,
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::get,
};
use rusto::{InitializeConfig, RustO};
use std::{
    env, fs,
    path::Path as FsPath,
    sync::{Arc, Mutex},
};

mod cache;
mod covers;
mod media;
mod posters;
mod tmdb;
use cache::DiskCache;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    tmdb_access_token: Arc<str>,
    font: Arc<[u8]>,
    cover_font: Arc<[u8]>,
    ocr: Arc<Mutex<RustO>>,
    cache: Arc<DiskCache>,
}
const HTTP_CACHE_CONTROL: &str = "public, max-age=86760";

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

pub fn app() -> Router {
    let tmdb_access_token = env::var("TMDB_ACCESS_TOKEN").expect("TMDB_ACCESS_TOKEN must be set");
    let font = fs::read("assets/Teko-wght.ttf")
        .or_else(|_| fs::read("assets/DejaVuSans-Bold.ttf"))
        .expect("Teko font or fallback font must be available");
    let cover_font = fs::read("assets/Inter-Variable.ttf")
        .or_else(|_| fs::read("assets/Teko-wght.ttf"))
        .expect("Inter font or fallback font must be available");
    let ocr = initialize_ocr().expect("Could not initialize English OCR engine");
    let state = AppState {
        http: reqwest::Client::new(),
        tmdb_access_token: tmdb_access_token.into(),
        font: font.into(),
        cover_font: cover_font.into(),
        ocr: Arc::new(Mutex::new(ocr)),
        cache: Arc::new(DiskCache::new()),
    };

    Router::new()
        .route("/bust/{asset_type}", get(cache::bust_cache))
        .route("/posters/{media_type}/{id}", get(posters::get_poster))
        .route("/backdrops/{media_type}/{id}", get(media::get_backdrop))
        .route("/logos/{media_type}/{id}", get(media::get_logo))
        .route("/covers/{service}/{media_type}", get(covers::get_cover))
        .with_state(state)
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
        "paramount-plus.png",
        "peacock.png",
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
        let bytes = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
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
