use super::*;
use crate::tmdb::{TmdbDetails, TmdbPoster, fetch_tmdb_images, preferred_language_images};
use ab_glyph::{FontRef, PxScale, VariableFont};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use image::{DynamicImage, ImageFormat, Rgba};
use imageproc::drawing::{draw_text_mut, text_size};
use rusto::{DetectTextBuilderExt, DetectTextResult, DetectionRunOptions};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) async fn get_poster(
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
                DetectTextResult::Structured(lines) => Some(lines),
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

pub(crate) fn log_image_request(
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

pub(crate) fn error_response(status: StatusCode, message: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(message))
        .unwrap()
}

pub(crate) fn poster_text_suitability(
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
            (
                (x / width.max(1) as f32 * 3.0).floor() as u8,
                (y / height.max(1) as f32 * 3.0).floor() as u8,
            )
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
