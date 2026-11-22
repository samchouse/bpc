use crate::posters::{error_response, log_image_request, poster_text_suitability};
use crate::tmdb::{TmdbDetails, TmdbPoster, fetch_tmdb_images, preferred_language_images};
use crate::{AppState, HTTP_CACHE_CONTROL, cached_response};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::Response,
};
use futures_util::StreamExt;
use rusto::{DetectTextBuilderExt, DetectTextResult, DetectionRunOptions};
use std::{collections::HashMap, sync::Arc, time::Instant};

pub(super) async fn get_logo(
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

pub(super) async fn get_backdrop(
    State(state): State<AppState>,
    Path((media_type, id)): Path<(String, u64)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let request_started = Instant::now();
    let text = query.get("text").is_some_and(|value| value == "true");
    let cache_key = format!("backdrop:v2:{media_type}:{id}:text:{text}");
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
    let mut backdrops =
        preferred_language_images(images.backdrops.clone(), &details.original_language);
    if !text {
        backdrops.extend(
            images
                .backdrops
                .into_iter()
                .filter(|image| image.iso_639_1.is_none()),
        );
    }
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
    let mut ranked: Vec<_> = backdrops.into_iter().collect();
    ranked.sort_by(|a, b| {
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
    });
    ranked.reverse();
    let Some((backdrop_path, bytes, content_type)) =
        select_backdrop_with_ocr(&state, &ranked, text).await
    else {
        return error_response(StatusCode::NOT_FOUND, "Backdrop not found");
    };
    if let Some((bytes, content_type)) = state.cache.renew_if_same(&cache_key, &backdrop_path).await
    {
        log_image_request("backdrop", &media_type, id, true, request_started.elapsed());
        return cached_response(bytes, &content_type);
    }
    log_image_request(
        "backdrop",
        &media_type,
        id,
        false,
        request_started.elapsed(),
    );
    state
        .cache
        .write(&cache_key, &backdrop_path, &content_type, &bytes)
        .await;
    drop(request_guard);
    cached_response(bytes, &content_type)
}

async fn select_backdrop_with_ocr(
    state: &AppState,
    candidates: &[TmdbPoster],
    want_text: bool,
) -> Option<(String, Vec<u8>, String)> {
    for candidate in candidates {
        let response = state
            .http
            .get(format!(
                "https://image.tmdb.org/t/p/w1280{}",
                candidate.file_path
            ))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            continue;
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let bytes = response.bytes().await.ok()?.to_vec();
        let image = match image::load_from_memory(&bytes) {
            Ok(image) => image,
            Err(_) => continue,
        };
        let rgb = image.to_rgb8();
        let (width, height) = (rgb.width(), rgb.height());
        let ocr = Arc::clone(&state.ocr);
        let recognized = tokio::task::spawn_blocking(move || {
            let mut engine = ocr.lock().ok()?;
            let result = engine
                .detect_text_rgb_builder(rgb.as_raw(), width, height, width as usize * 3)
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
                DetectTextResult::Structured(lines) => Some(lines),
                _ => Some(Vec::new()),
            }
        })
        .await
        .ok()
        .flatten()?;
        if poster_text_suitability(&recognized, width, height) == want_text {
            return Some((candidate.file_path.clone(), bytes, content_type));
        }
    }
    None
}
