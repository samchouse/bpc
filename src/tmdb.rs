use crate::AppState;
use axum::http::StatusCode;
use serde::Deserialize;

#[derive(Default, Deserialize)]
pub(crate) struct TmdbImages {
    #[serde(default)]
    pub(crate) posters: Vec<TmdbPoster>,
    #[serde(default)]
    pub(crate) backdrops: Vec<TmdbPoster>,
    #[serde(default)]
    pub(crate) logos: Vec<TmdbPoster>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct TmdbPoster {
    pub(crate) file_path: String,
    pub(crate) iso_639_1: Option<String>,
    #[serde(default)]
    pub(crate) width: u32,
    #[serde(default)]
    pub(crate) vote_count: u64,
    #[serde(default)]
    pub(crate) vote_average: f32,
}

#[derive(Deserialize)]
pub(crate) struct TmdbDetails {
    #[serde(default)]
    pub(crate) original_language: String,
}

pub(crate) fn preferred_language_images(
    images: Vec<TmdbPoster>,
    original_language: &str,
) -> Vec<TmdbPoster> {
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

pub(crate) async fn fetch_tmdb_images(
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
