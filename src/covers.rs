use crate::posters::error_response;
use crate::{AppState, cached_response};
use ab_glyph::{FontRef, PxScale, VariableFont};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use image::{DynamicImage, ImageFormat, Rgba};
use imageproc::drawing::{draw_text_mut, text_size};
use std::{fs, io::Cursor, path::PathBuf};

pub(super) async fn get_cover(
    State(state): State<AppState>,
    Path((service, media_type)): Path<(String, String)>,
) -> Response {
    if !matches!(media_type.as_str(), "movie" | "series") {
        return error_response(StatusCode::NOT_FOUND, "Unknown cover type");
    }
    let service = service.to_ascii_lowercase();
    let cache_key = format!("cover:v1:{service}:{media_type}");
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
                .write(&cache_key, "generated:v1", "image/png", &bytes)
                .await;
            cover_response(bytes, "image/png")
        }
        Err(status) => error_response(status, "Could not render cover"),
    }
}

fn cover_response(bytes: impl Into<Body>, content_type: &str) -> Response {
    let mut response = cached_response(bytes, content_type);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
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
        let projection =
            ((to_x * sweep_dx + to_y * sweep_dy) / sweep_length_squared).clamp(0.0, 1.0);
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
                + grain)
                .clamp(0.0, 255.0) as u8,
            (10.0
                + accent[1] as f32 * (logo_glow + title_glow)
                + secondary[1] as f32 * color_bloom
                + secondary[1] as f32 * edge_glow
                + accent[1] as f32 * light_sweep
                + grain)
                .clamp(0.0, 255.0) as u8,
            (18.0
                + accent[2] as f32 * (logo_glow + title_glow)
                + secondary[2] as f32 * color_bloom
                + secondary[2] as f32 * edge_glow
                + accent[2] as f32 * light_sweep
                + grain)
                .clamp(0.0, 255.0) as u8,
            255,
        ]);
    }
    let logo = fs::read(PathBuf::from("assets/logos").join(logo))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let service_logo = image::load_from_memory(&logo)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .to_rgba8();
    let logo_scale =
        (320.0 / service_logo.width() as f32).min(220.0 / service_logo.height() as f32);
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
    draw_text_mut(
        &mut image,
        Rgba([255, 255, 255, 255]),
        title_x,
        420,
        title_scale,
        &font,
        "TOP 10",
    );
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
