use super::GCSBucket;
use crate::{ConcurrencyLimiter, DownloadError, ListingMode, RemotePath, RemoteStorage};
use async_trait::async_trait;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use futures::StreamExt;
use gcp_auth::{Token, TokenProvider};
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const MOCK_BUCKET_NAME: &str = "test-bucket";

#[derive(Clone, Copy)]
enum TokenStyle {
    Plain,
    NeedsEncoding,
}

impl TokenStyle {
    fn make_token(self, offset: usize) -> String {
        match self {
            Self::Plain => format!("page{offset}"),
            Self::NeedsEncoding => format!("tok+{offset}/=="),
        }
    }

    fn parse_token(self, token: &str) -> Option<usize> {
        let offset = match self {
            Self::Plain => token.strip_prefix("page")?,
            Self::NeedsEncoding => token.strip_prefix("tok+")?.strip_suffix("/==")?,
        };
        offset.parse().ok()
    }
}

struct FakeTokenProvider;

#[async_trait]
impl TokenProvider for FakeTokenProvider {
    async fn token(&self, _scopes: &[&str]) -> Result<Arc<Token>, gcp_auth::Error> {
        Ok(Arc::new(
            serde_json::from_str::<Token>(r#"{"access_token":"test-token","expires_in":3600}"#)
                .unwrap(),
        ))
    }

    async fn project_id(&self) -> Result<Arc<str>, gcp_auth::Error> {
        Ok(Arc::from("test-project"))
    }
}

struct MockGcsState {
    bucket_name: String,
    objects: Vec<String>,
    request_log: Mutex<Vec<Vec<(String, String)>>>,
    request_count: AtomicUsize,
    status_overrides: Mutex<VecDeque<StatusCode>>,
    token_style: TokenStyle,
}

fn error_response(status: StatusCode, message: &str, reason: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "error": {
                "code": status.as_u16(),
                "message": message,
                "errors": [{
                    "message": message,
                    "domain": "global",
                    "reason": reason,
                }]
            }
        })),
    )
}

async fn list_objects(
    Path(bucket): Path<String>,
    RawQuery(raw_query): RawQuery,
    State(state): State<Arc<MockGcsState>>,
) -> (StatusCode, Json<Value>) {
    let query: Vec<(String, String)> = raw_query
        .as_deref()
        .map(|raw_query| {
            url::form_urlencoded::parse(raw_query.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default();
    state.request_log.lock().unwrap().push(query.clone());

    if state.request_count.fetch_add(1, Ordering::Relaxed) >= 20 {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Mock request cap exceeded.",
            "backendError",
        );
    }

    if let Some(status) = state.status_overrides.lock().unwrap().pop_front() {
        return error_response(status, "Mock status override.", "mockError");
    }

    if bucket != state.bucket_name {
        return error_response(
            StatusCode::NOT_FOUND,
            "The specified bucket does not exist.",
            "notFound",
        );
    }

    let prefix = query
        .iter()
        .find(|(name, _)| name == "prefix")
        .map(|(_, value)| value.as_str())
        .unwrap_or_default();
    let max_results = query
        .iter()
        .find(|(name, _)| name == "maxResults")
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(1000);

    let offset = match query
        .iter()
        .find(|(name, _)| name == "pageToken")
        .map(|(_, value)| value.as_str())
    {
        Some(token) => match state.token_style.parse_token(token) {
            Some(offset) => offset,
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "The page token is invalid.",
                    "invalid",
                );
            }
        },
        None => 0,
    };

    let matching_objects: Vec<_> = state
        .objects
        .iter()
        .filter(|name| name.starts_with(prefix))
        .collect();
    let items: Vec<_> = matching_objects
        .iter()
        .skip(offset)
        .take(max_results)
        .map(|name| {
            json!({
                "name": name,
                "bucket": state.bucket_name.as_str(),
                "generation": "1",
                "metageneration": "1",
                "storageClass": "STANDARD",
                "size": "1",
                "crc32c": "AAAAAA==",
                "timeCreated": "2024-01-01T00:00:00.000Z",
                "updated": "2024-01-01T00:00:00.000Z",
                "timeStorageClassUpdated": "2024-01-01T00:00:00.000Z",
                "timeFinalized": "2024-01-01T00:00:00.000Z",
            })
        })
        .collect();
    let next_offset = offset + items.len();
    let mut body = json!({
        "kind": "storage#objects",
        "items": items,
    });
    if next_offset < matching_objects.len() {
        body["nextPageToken"] = json!(state.token_style.make_token(next_offset));
    }

    (StatusCode::OK, Json(body))
}

async fn start_mock_gcs(
    bucket_name: &str,
    status_overrides: Vec<StatusCode>,
    token_style: TokenStyle,
) -> (GCSBucket, Arc<MockGcsState>, JoinHandle<()>) {
    let state = Arc::new(MockGcsState {
        bucket_name: MOCK_BUCKET_NAME.to_string(),
        objects: (0..2500)
            .map(|index| format!("tenant/obj-{index:05}"))
            .collect(),
        request_log: Mutex::new(Vec::new()),
        request_count: AtomicUsize::new(0),
        status_overrides: Mutex::new(status_overrides.into()),
        token_style,
    });
    let app = Router::new()
        .route("/storage/v1/b/{bucket}/o", get(list_objects))
        .with_state(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let bucket = GCSBucket {
        token_provider: Arc::new(FakeTokenProvider),
        bucket_name: bucket_name.to_string(),
        prefix_in_bucket: None,
        max_keys_per_list_response: None,
        concurrency_limiter: ConcurrencyLimiter::new(10),
        timeout: Duration::from_secs(10),
        base_url: format!("http://{addr}"),
    };

    (bucket, state, server)
}

fn expect_error<T>(result: Result<T, DownloadError>) -> DownloadError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("expected an error"),
    }
}

fn listing_keys(listing: &crate::Listing) -> HashSet<String> {
    listing
        .keys
        .iter()
        .map(|key| key.key.get_path().as_str().to_string())
        .collect()
}

fn expected_keys() -> HashSet<String> {
    (0..2500)
        .map(|index| format!("tenant/obj-{index:05}"))
        .collect()
}

fn assert_page_tokens(requests: &[Vec<(String, String)>], first_token: &str, second_token: &str) {
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| {
        request
            .iter()
            .filter(|(name, _)| name == "pageToken")
            .count()
            <= 1
    }));
    assert_eq!(
        requests[1]
            .iter()
            .find(|(name, _)| name == "pageToken")
            .map(|(_, value)| value.as_str()),
        Some(first_token)
    );
    assert_eq!(
        requests[2]
            .iter()
            .find(|(name, _)| name == "pageToken")
            .map(|(_, value)| value.as_str()),
        Some(second_token)
    );
}

#[tokio::test]
async fn gcs_list_paginates_across_pages() {
    let (bucket, state, server) = start_mock_gcs(MOCK_BUCKET_NAME, vec![], TokenStyle::Plain).await;
    let prefix = RemotePath::from_string("tenant").unwrap();
    let cancel = CancellationToken::new();
    let listing = tokio::time::timeout(
        Duration::from_secs(30),
        bucket.list(Some(&prefix), ListingMode::NoDelimiter, None, &cancel),
    )
    .await
    .unwrap()
    .unwrap();

    let actual = listing_keys(&listing);
    assert_eq!(
        actual.len(),
        listing.keys.len(),
        "listing contains duplicate keys"
    );
    assert_eq!(listing.keys.len(), 2500);
    assert_eq!(actual, expected_keys());

    let requests = state.request_log.lock().unwrap();
    assert_page_tokens(&requests, "page1000", "page2000");

    server.abort();
}

#[tokio::test]
async fn gcs_list_encodes_page_token() {
    let (bucket, state, server) =
        start_mock_gcs(MOCK_BUCKET_NAME, vec![], TokenStyle::NeedsEncoding).await;
    let prefix = RemotePath::from_string("tenant").unwrap();
    let cancel = CancellationToken::new();
    let listing = tokio::time::timeout(
        Duration::from_secs(30),
        bucket.list(Some(&prefix), ListingMode::NoDelimiter, None, &cancel),
    )
    .await
    .unwrap()
    .unwrap();

    let actual = listing_keys(&listing);
    assert_eq!(
        actual.len(),
        listing.keys.len(),
        "listing contains duplicate keys"
    );
    assert_eq!(listing.keys.len(), 2500);
    assert_eq!(actual, expected_keys());

    let requests = state.request_log.lock().unwrap();
    assert_page_tokens(&requests, "tok+1000/==", "tok+2000/==");

    server.abort();
}

#[tokio::test]
async fn gcs_list_missing_bucket_is_error() {
    let (bucket, _, server) = start_mock_gcs("no-such-bucket", vec![], TokenStyle::Plain).await;
    let prefix = RemotePath::from_string("tenant").unwrap();
    let cancel = CancellationToken::new();
    let error = expect_error(
        tokio::time::timeout(
            Duration::from_secs(30),
            bucket.list(Some(&prefix), ListingMode::NoDelimiter, None, &cancel),
        )
        .await
        .unwrap(),
    );
    assert!(error.is_permanent());
    assert!(error.to_string().contains("404"));

    let stream = bucket.list_streaming(Some(&prefix), ListingMode::NoDelimiter, None, &cancel);
    let mut stream = Box::pin(stream);
    let error = expect_error(
        tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(error.is_permanent());
    assert!(error.to_string().contains("404"));
    assert!(
        tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .unwrap()
            .is_none()
    );

    server.abort();
}

#[tokio::test]
async fn gcs_list_forbidden_is_error() {
    let (bucket, state, server) = start_mock_gcs(
        MOCK_BUCKET_NAME,
        vec![StatusCode::FORBIDDEN],
        TokenStyle::Plain,
    )
    .await;
    let prefix = RemotePath::from_string("tenant").unwrap();
    let cancel = CancellationToken::new();
    let error = expect_error(
        tokio::time::timeout(
            Duration::from_secs(30),
            bucket.list(Some(&prefix), ListingMode::NoDelimiter, None, &cancel),
        )
        .await
        .unwrap(),
    );
    assert!(error.is_permanent());
    assert!(error.to_string().contains("403"));

    state
        .status_overrides
        .lock()
        .unwrap()
        .push_back(StatusCode::FORBIDDEN);
    let stream = bucket.list_streaming(Some(&prefix), ListingMode::NoDelimiter, None, &cancel);
    let mut stream = Box::pin(stream);
    let error = expect_error(
        tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(error.is_permanent());
    assert!(error.to_string().contains("403"));
    assert!(
        tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .unwrap()
            .is_none()
    );

    server.abort();
}

#[tokio::test]
async fn gcs_list_server_error_is_retryable() {
    let (bucket, _, server) = start_mock_gcs(
        MOCK_BUCKET_NAME,
        vec![StatusCode::SERVICE_UNAVAILABLE],
        TokenStyle::Plain,
    )
    .await;
    let prefix = RemotePath::from_string("tenant").unwrap();
    let cancel = CancellationToken::new();
    let stream = bucket.list_streaming(Some(&prefix), ListingMode::NoDelimiter, None, &cancel);
    let mut stream = Box::pin(stream);
    let error = expect_error(
        tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(!error.is_permanent());

    let listing = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(listing.keys.len(), 1000);

    server.abort();
}
