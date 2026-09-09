use std::collections::HashSet;
use std::io::Read;
use std::time::Duration;

use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::header::LINK;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};

use crate::cli::{RepoSpec, RepoType};
use crate::error::{AppError, AppResult};
use crate::text::escape_control;

const HUB_ENDPOINT: &str = "https://huggingface.co/";
const ERROR_BODY_LIMIT: u64 = 8 * 1024;
pub const TOKEN_SETTINGS_URL: &str = "https://huggingface.co/settings/tokens";

#[derive(Clone, Debug)]
pub struct HubClient {
    http: Client,
    endpoint: Url,
    token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UserRecord {
    #[serde(rename = "username")]
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrganizationRecord {
    #[serde(rename = "organization")]
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RepositoryRecord {
    #[serde(rename = "type")]
    pub repo_type: RepoType,
    pub id: String,
    pub private: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TreeEntry {
    #[serde(rename = "type")]
    pub entry_type: String,
    pub oid: String,
    pub size: u64,
    pub path: String,
}

impl TreeEntry {
    pub fn is_file(&self) -> bool {
        self.entry_type == "file"
    }

    pub fn is_directory(&self) -> bool {
        self.entry_type == "directory"
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Identity {
    #[serde(alias = "username", alias = "user")]
    pub name: String,
    #[serde(default)]
    pub orgs: Vec<IdentityOrganization>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IdentityOrganization {
    pub name: String,
}

#[derive(Debug, Default, Deserialize)]
struct QuickSearchResponse {
    #[serde(default)]
    users: Vec<QuickUser>,
    #[serde(default)]
    orgs: Vec<QuickOrganization>,
    #[serde(default)]
    models: Vec<QuickRepository>,
    #[serde(default)]
    datasets: Vec<QuickRepository>,
    #[serde(default)]
    spaces: Vec<QuickRepository>,
}

#[derive(Debug, Deserialize)]
struct QuickUser {
    user: String,
    fullname: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QuickOrganization {
    name: String,
    fullname: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QuickRepository {
    id: String,
    #[serde(default)]
    private: bool,
}

#[derive(Deserialize)]
struct RepositoryRevision {
    sha: String,
}

#[derive(Serialize)]
struct QuickSearchQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    q: Option<&'a str>,
    limit: usize,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    result_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
}

impl HubClient {
    pub fn new(token: Option<String>) -> AppResult<Self> {
        Self::at_endpoint(HUB_ENDPOINT, token)
    }

    pub(crate) fn at_endpoint(endpoint: &str, token: Option<String>) -> AppResult<Self> {
        let endpoint = Url::parse(endpoint).map_err(|error| {
            AppError::message(format!("invalid Hub endpoint {endpoint:?}: {error}"))
        })?;
        let http = Client::builder()
            .user_agent(concat!("xhf/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| AppError::http("could not construct the HTTP client", error))?;
        Ok(Self {
            http,
            endpoint,
            token,
        })
    }

    pub fn search_users(&self, query: &str, limit: usize) -> AppResult<Vec<UserRecord>> {
        let response = self.quick_search(Some(query), None, Some("user"), limit)?;
        Ok(response
            .users
            .into_iter()
            .map(|user| UserRecord {
                id: user.user,
                name: user.fullname,
            })
            .collect())
    }

    pub fn search_organizations(
        &self,
        query: &str,
        limit: usize,
    ) -> AppResult<Vec<OrganizationRecord>> {
        let response = self.quick_search(Some(query), None, Some("org"), limit)?;
        Ok(response
            .orgs
            .into_iter()
            .map(|organization| OrganizationRecord {
                id: organization.name,
                name: organization.fullname,
            })
            .collect())
    }

    pub fn search_repositories(
        &self,
        query: Option<&str>,
        owner: Option<&str>,
        repo_type: Option<RepoType>,
        limit: usize,
    ) -> AppResult<Vec<RepositoryRecord>> {
        let result_type = repo_type.map(RepoType::singular);
        let response = self.quick_search(query, owner, result_type, limit)?;
        Ok(match repo_type {
            Some(RepoType::Model) => repositories(response.models, RepoType::Model, limit),
            Some(RepoType::Dataset) => repositories(response.datasets, RepoType::Dataset, limit),
            Some(RepoType::Space) => repositories(response.spaces, RepoType::Space, limit),
            None => interleave_repositories(response, limit),
        })
    }

    pub fn resolve_revision(
        &self,
        repo: &RepoSpec,
        repo_type: RepoType,
        revision: &str,
    ) -> AppResult<String> {
        validate_revision(revision)?;
        let url = self.endpoint_url(&[
            "api",
            repo_type.plural(),
            repo.owner(),
            repo.name(),
            "revision",
            revision,
        ])?;
        let request = self.authenticated(self.http.get(url).query(&[("expand", "sha")]));
        let response = self.send_checked(request, "could not resolve repository revision")?;
        let resolved: RepositoryRevision = serde_json::from_reader(response)
            .map_err(|error| AppError::json("could not decode repository revision", error))?;
        if resolved.sha.len() != 40 || !resolved.sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(AppError::message(
                "Hub returned an invalid repository commit hash",
            ));
        }
        Ok(resolved.sha)
    }

    pub fn list_tree(
        &self,
        repo: &RepoSpec,
        repo_type: RepoType,
        revision: &str,
    ) -> AppResult<Vec<TreeEntry>> {
        validate_revision(revision)?;
        let mut next = Some(self.tree_url(repo, repo_type, revision)?);
        let mut visited = HashSet::new();
        let mut entries = Vec::new();
        while let Some(url) = next {
            if !visited.insert(url.as_str().to_owned()) {
                return Err(AppError::message(
                    "Hub tree pagination returned a repeated cursor",
                ));
            }
            self.validate_same_origin(&url)?;
            let request = self.authenticated(self.http.get(url));
            let response = self.send_checked(request, "could not list repository contents")?;
            next = next_link(&response, &self.endpoint)?;
            let page: Vec<TreeEntry> = serde_json::from_reader(response)
                .map_err(|error| AppError::json("could not decode repository contents", error))?;
            for entry in &page {
                validate_remote_path(&entry.path, false)?;
                if !entry.is_file() && !entry.is_directory() {
                    return Err(AppError::message(format!(
                        "Hub returned an unsupported tree entry type {:?} for {:?}",
                        entry.entry_type, entry.path
                    )));
                }
            }
            entries.extend(page);
        }
        Ok(entries)
    }

    pub fn file_response(
        &self,
        repo: &RepoSpec,
        repo_type: RepoType,
        revision: &str,
        path: &str,
    ) -> AppResult<Response> {
        let url = self.file_url(repo, repo_type, revision, path)?;
        self.file_response_for_url(url, path)
    }

    pub(crate) fn file_response_for_url(&self, url: Url, path: &str) -> AppResult<Response> {
        self.validate_same_origin(&url)?;
        let request = self.authenticated(self.http.get(url));
        self.send_checked(request, &format!("could not download {path:?}"))
    }

    pub fn whoami(&self) -> AppResult<Identity> {
        if self.token.is_none() {
            return Err(AppError::message("no Hugging Face token is configured"));
        }
        let url = self.endpoint_url(&["api", "whoami-v2"])?;
        let request = self.authenticated(self.http.get(url));
        let response = self.send_checked(request, "could not authenticate with Hugging Face")?;
        serde_json::from_reader(response)
            .map_err(|error| AppError::json("could not decode authenticated identity", error))
    }

    fn quick_search(
        &self,
        query: Option<&str>,
        owner: Option<&str>,
        result_type: Option<&str>,
        limit: usize,
    ) -> AppResult<QuickSearchResponse> {
        let url = self.endpoint_url(&["api", "quicksearch"])?;
        let parameters = QuickSearchQuery {
            q: query,
            limit,
            result_type,
            namespace: owner,
        };
        let request = self.authenticated(self.http.get(url).query(&parameters));
        let response = self.send_checked(request, "Hub search failed")?;
        serde_json::from_reader(response)
            .map_err(|error| AppError::json("could not decode Hub search results", error))
    }

    fn tree_url(&self, repo: &RepoSpec, repo_type: RepoType, revision: &str) -> AppResult<Url> {
        let mut url = self.endpoint_url(&[
            "api",
            repo_type.plural(),
            repo.owner(),
            repo.name(),
            "tree",
            revision,
        ])?;
        url.query_pairs_mut()
            .append_pair("recursive", "true")
            .append_pair("limit", "1000");
        Ok(url)
    }

    pub(crate) fn file_url(
        &self,
        repo: &RepoSpec,
        repo_type: RepoType,
        revision: &str,
        path: &str,
    ) -> AppResult<Url> {
        validate_revision(revision)?;
        validate_remote_path(path, true)?;
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| AppError::message("Hub endpoint cannot contain repository paths"))?;
            segments.clear();
            if let Some(prefix) = repo_type.url_prefix() {
                segments.push(prefix);
            }
            segments
                .push(repo.owner())
                .push(repo.name())
                .push("resolve")
                .push(revision);
            for component in path.split('/') {
                segments.push(component);
            }
        }
        Ok(url)
    }

    fn endpoint_url(&self, segments: &[&str]) -> AppResult<Url> {
        let mut url = self.endpoint.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|()| AppError::message("Hub endpoint cannot contain API paths"))?;
        path.clear();
        path.extend(segments.iter().copied());
        drop(path);
        Ok(url)
    }

    fn authenticated(&self, request: RequestBuilder) -> RequestBuilder {
        match self.token.as_deref() {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn send_checked(&self, request: RequestBuilder, context: &str) -> AppResult<Response> {
        let response = request
            .send()
            .map_err(|error| AppError::http(context.to_owned(), error))?;
        if response.status().is_success() {
            return Ok(response);
        }
        http_error(response, context)
    }

    fn validate_same_origin(&self, url: &Url) -> AppResult<()> {
        let same_origin = url.scheme() == self.endpoint.scheme()
            && url.host_str() == self.endpoint.host_str()
            && url.port_or_known_default() == self.endpoint.port_or_known_default();
        if same_origin {
            Ok(())
        } else {
            Err(AppError::message(format!(
                "Hub request attempted to leave the configured origin: {url}"
            )))
        }
    }
}

fn repositories(
    values: Vec<QuickRepository>,
    repo_type: RepoType,
    limit: usize,
) -> Vec<RepositoryRecord> {
    values
        .into_iter()
        .take(limit)
        .map(|repository| RepositoryRecord {
            repo_type,
            id: repository.id,
            private: repository.private,
        })
        .collect()
}

fn interleave_repositories(response: QuickSearchResponse, limit: usize) -> Vec<RepositoryRecord> {
    let mut models = response.models.into_iter();
    let mut datasets = response.datasets.into_iter();
    let mut spaces = response.spaces.into_iter();
    let mut output = Vec::with_capacity(limit);
    while output.len() < limit {
        let previous_length = output.len();
        if let Some(repository) = models.next() {
            output.push(repository_record(repository, RepoType::Model));
        }
        if output.len() < limit
            && let Some(repository) = datasets.next()
        {
            output.push(repository_record(repository, RepoType::Dataset));
        }
        if output.len() < limit
            && let Some(repository) = spaces.next()
        {
            output.push(repository_record(repository, RepoType::Space));
        }
        if output.len() == previous_length {
            break;
        }
    }
    output
}

fn repository_record(repository: QuickRepository, repo_type: RepoType) -> RepositoryRecord {
    RepositoryRecord {
        repo_type,
        id: repository.id,
        private: repository.private,
    }
}

fn next_link(response: &Response, endpoint: &Url) -> AppResult<Option<Url>> {
    let Some(value) = response.headers().get(LINK) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|error| {
        AppError::message(format!(
            "Hub returned a malformed pagination header: {error}"
        ))
    })?;
    parse_next_link(value, endpoint)
}

fn parse_next_link(value: &str, endpoint: &Url) -> AppResult<Option<Url>> {
    for part in value.split(',') {
        if !part.contains("rel=\"next\"") {
            continue;
        }
        let Some(start) = part.find('<') else {
            return Err(AppError::message("Hub returned a malformed next-page link"));
        };
        let Some(relative_end) = part[start + 1..].find('>') else {
            return Err(AppError::message("Hub returned a malformed next-page link"));
        };
        let value = &part[start + 1..start + 1 + relative_end];
        let url = endpoint.join(value).map_err(|error| {
            AppError::message(format!("Hub returned an invalid next-page URL: {error}"))
        })?;
        return Ok(Some(url));
    }
    Ok(None)
}

fn http_error(response: Response, context: &str) -> AppResult<Response> {
    let status = response.status();
    let url = response.url().clone();
    let mut body = Vec::new();
    response
        .take(ERROR_BODY_LIMIT)
        .read_to_end(&mut body)
        .map_err(|error| {
            AppError::io(
                format!("{context}; could not read error response from {url}"),
                error,
            )
        })?;
    let body = String::from_utf8_lossy(&body);
    let hint = response_hint(status, body.trim(), &url);
    let body = escape_control(body.trim());
    let detail = if body.is_empty() {
        status_description(status).to_owned()
    } else {
        body.into_owned()
    };
    let message = format!("{context}: {status} for {url}: {detail}");
    match hint {
        Some(hint) => Err(AppError::message(format!("{message}\nhint: {hint}"))),
        None => Err(AppError::message(message)),
    }
}

fn response_hint(status: StatusCode, body: &str, url: &Url) -> Option<String> {
    if status == StatusCode::UNAUTHORIZED {
        return Some(format!(
            "authenticate with 'xhf auth login'; create a token at {TOKEN_SETTINGS_URL}"
        ));
    }
    if status != StatusCode::FORBIDDEN || !is_gated_access_error(body) {
        return None;
    }
    let repository_url = repository_page_url(url)?;
    Some(format!(
        "the token's Hugging Face user has not been granted access to this gated repository; visit {repository_url} in a browser, request access, and retry after approval"
    ))
}

fn is_gated_access_error(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    body.contains("not in the authorized list")
        || body.contains("ask for access")
        || body.contains("gated repo")
        || body.contains("gated model")
}

fn repository_page_url(url: &Url) -> Option<String> {
    let segments = url.path_segments()?.collect::<Vec<_>>();
    let (prefix, owner, name) = match segments.as_slice() {
        [owner, name, "resolve", ..] => (None, *owner, *name),
        [kind, owner, name, "resolve", ..] if matches!(*kind, "datasets" | "spaces") => {
            (Some(*kind), *owner, *name)
        }
        ["api", "models", owner, name, "tree" | "revision", ..] => (None, *owner, *name),
        ["api", kind, owner, name, "tree" | "revision", ..]
            if matches!(*kind, "datasets" | "spaces") =>
        {
            (Some(*kind), *owner, *name)
        }
        _ => return None,
    };
    if owner.is_empty() || name.is_empty() {
        return None;
    }
    let origin = url.origin().ascii_serialization();
    match prefix {
        Some(prefix) => Some(format!("{origin}/{prefix}/{owner}/{name}")),
        None => Some(format!("{origin}/{owner}/{name}")),
    }
}

fn status_description(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "authentication required or token rejected",
        StatusCode::FORBIDDEN => "access forbidden",
        StatusCode::NOT_FOUND => "resource not found",
        StatusCode::TOO_MANY_REQUESTS => "rate limit exceeded",
        _ => "Hub request failed",
    }
}

fn validate_revision(revision: &str) -> AppResult<()> {
    if revision.is_empty() {
        Err(AppError::message("revision cannot be empty"))
    } else {
        Ok(())
    }
}

pub fn validate_remote_path(path: &str, require_file: bool) -> AppResult<()> {
    if path.is_empty() {
        return if require_file {
            Err(AppError::message("file path cannot be empty"))
        } else {
            Ok(())
        };
    }
    if path.starts_with('/') || path.ends_with('/') || path.contains('\\') {
        return Err(AppError::message(format!(
            "unsafe repository path returned by Hub: {path:?}"
        )));
    }
    if path
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(AppError::message(format!(
            "unsafe repository path returned by Hub: {path:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};

    use reqwest::Url;

    use crate::cli::RepoType;

    use super::{
        HubClient, QuickRepository, QuickSearchResponse, interleave_repositories, parse_next_link,
        repository_page_url, response_hint, validate_remote_path,
    };
    use reqwest::StatusCode;

    fn revision_server(path: &str, status: &str, body: &str) -> (HubClient, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let expected_request = format!("GET {path}?expand=sha HTTP/1.1\r\n");
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request, expected_request);
            let mut authenticated = false;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" || header.is_empty() {
                    break;
                }
                if header.eq_ignore_ascii_case("authorization: Bearer test-token\r\n") {
                    authenticated = true;
                }
            }
            assert!(authenticated);
            stream.write_all(response.as_bytes()).unwrap();
        });
        (
            HubClient::at_endpoint(&endpoint, Some("test-token".to_owned())).unwrap(),
            server,
        )
    }

    #[test]
    fn resolves_branches_tags_and_hashes_for_all_repository_types() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let body = format!(r#"{{"sha":"{sha}"}}"#);
        for (repo_type, revision, path) in [
            (
                RepoType::Model,
                "main",
                "/api/models/owner/repo/revision/main",
            ),
            (
                RepoType::Dataset,
                "v1.0",
                "/api/datasets/owner/repo/revision/v1.0",
            ),
            (
                RepoType::Space,
                "refs/pr/1",
                "/api/spaces/owner/repo/revision/refs%2Fpr%2F1",
            ),
            (
                RepoType::Model,
                sha,
                "/api/models/owner/repo/revision/0123456789abcdef0123456789abcdef01234567",
            ),
        ] {
            let (client, server) = revision_server(path, "200 OK", &body);
            let result =
                client.resolve_revision(&"owner/repo".parse().unwrap(), repo_type, revision);
            server.join().unwrap();
            assert_eq!(result.unwrap(), sha);
        }
    }

    #[test]
    fn rejects_unresolved_or_malformed_commit_hashes() {
        for body in [
            r#"{"sha":"main"}"#,
            r#"{"sha":""}"#,
            r#"{"sha":"0123456789abcdef0123456789abcdef0123456z"}"#,
            r#"{"sha":null}"#,
            "{}",
        ] {
            let (client, server) =
                revision_server("/api/models/owner/repo/revision/main", "200 OK", body);
            let result =
                client.resolve_revision(&"owner/repo".parse().unwrap(), RepoType::Model, "main");
            server.join().unwrap();
            assert!(result.is_err(), "accepted {body}");
        }
    }

    #[test]
    fn propagates_revision_resolution_http_errors() {
        let (client, server) = revision_server(
            "/api/models/owner/repo/revision/missing",
            "404 Not Found",
            "Revision not found",
        );
        let result =
            client.resolve_revision(&"owner/repo".parse().unwrap(), RepoType::Model, "missing");
        server.join().unwrap();
        let error = result.unwrap_err().to_string();
        assert!(error.contains("could not resolve repository revision"));
        assert!(error.contains("404 Not Found"));
    }

    fn repository(id: &str) -> QuickRepository {
        QuickRepository {
            id: id.to_owned(),
            private: false,
        }
    }

    #[test]
    fn interleaves_repository_types_up_to_limit() {
        let response = QuickSearchResponse {
            models: vec![repository("m/1"), repository("m/2")],
            datasets: vec![repository("d/1"), repository("d/2")],
            spaces: vec![repository("s/1"), repository("s/2")],
            ..QuickSearchResponse::default()
        };
        let records = interleave_repositories(response, 4);
        assert_eq!(
            records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            ["m/1", "d/1", "s/1", "m/2"]
        );
    }

    #[test]
    fn rejects_unsafe_remote_paths() {
        assert!(validate_remote_path("../secret", true).is_err());
        assert!(validate_remote_path("folder\\secret", true).is_err());
        assert!(validate_remote_path("/rooted", true).is_err());
    }

    #[test]
    fn same_origin_comparison_includes_scheme_and_port() {
        let client = HubClient::at_endpoint("https://huggingface.co/", None).unwrap();
        let same = Url::parse("https://huggingface.co/api/models").unwrap();
        let different = Url::parse("http://huggingface.co/api/models").unwrap();
        assert!(client.validate_same_origin(&same).is_ok());
        assert!(client.validate_same_origin(&different).is_err());
    }

    #[test]
    fn parses_next_page_link() {
        let endpoint = Url::parse("https://huggingface.co/").unwrap();
        let header = "<https://huggingface.co/api/models/o/r/tree/main?cursor=next>; rel=\"next\"";
        assert_eq!(
            parse_next_link(header, &endpoint)
                .unwrap()
                .unwrap()
                .as_str(),
            "https://huggingface.co/api/models/o/r/tree/main?cursor=next"
        );
        assert!(parse_next_link("", &endpoint).unwrap().is_none());
    }

    #[test]
    fn decodes_current_quick_search_shape() {
        let json = r#"{
            "users":[{"user":"alice","fullname":"Alice"}],
            "orgs":[{"name":"acme","fullname":"Acme"}],
            "models":[{"id":"acme/model","private":false}],
            "datasets":[],
            "spaces":[]
        }"#;
        let response: QuickSearchResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.users[0].user, "alice");
        assert_eq!(response.orgs[0].name, "acme");
        assert_eq!(response.models[0].id, "acme/model");
    }

    #[test]
    fn explains_how_to_authenticate_after_unauthorized_response() {
        let url = Url::parse("https://huggingface.co/owner/repo/resolve/main/file").unwrap();
        let hint =
            response_hint(StatusCode::UNAUTHORIZED, "authentication required", &url).unwrap();
        assert!(hint.contains("xhf auth login"));
        assert!(hint.contains("https://huggingface.co/settings/tokens"));
    }

    #[test]
    fn explains_how_to_request_gated_repository_access() {
        let url = Url::parse("https://huggingface.co/orcarouter/model/resolve/main/.gitattributes")
            .unwrap();
        let hint = response_hint(
            StatusCode::FORBIDDEN,
            "Access is restricted and you are not in the authorized list. Ask for access.",
            &url,
        )
        .unwrap();
        assert!(hint.contains("token's Hugging Face user"));
        assert!(hint.contains("https://huggingface.co/orcarouter/model"));
        assert!(hint.contains("retry after approval"));
    }

    #[test]
    fn leaves_unrelated_forbidden_responses_without_a_gated_hint() {
        let url = Url::parse("https://huggingface.co/api/resource").unwrap();
        assert!(response_hint(StatusCode::FORBIDDEN, "forbidden", &url).is_none());
        assert!(repository_page_url(&url).is_none());
    }

    #[test]
    fn derives_repository_pages_from_tree_api_urls() {
        let model =
            Url::parse("https://huggingface.co/api/models/owner/model/tree/main?recursive=true")
                .unwrap();
        let dataset =
            Url::parse("https://huggingface.co/api/datasets/owner/data/tree/main?recursive=true")
                .unwrap();
        assert_eq!(
            repository_page_url(&model).as_deref(),
            Some("https://huggingface.co/owner/model")
        );
        assert_eq!(
            repository_page_url(&dataset).as_deref(),
            Some("https://huggingface.co/datasets/owner/data")
        );
    }

    #[test]
    fn preserves_gated_access_hints_during_revision_resolution() {
        for (kind, prefix) in [
            ("models", ""),
            ("datasets", "datasets/"),
            ("spaces", "spaces/"),
        ] {
            let url = Url::parse(&format!(
                "https://huggingface.co/api/{kind}/owner/repo/revision/main?expand=sha"
            ))
            .unwrap();
            let hint = response_hint(
                StatusCode::FORBIDDEN,
                "Access to this gated repo is restricted",
                &url,
            )
            .unwrap();
            assert!(hint.contains(&format!("https://huggingface.co/{prefix}owner/repo")));
            assert!(hint.contains("retry after approval"));
        }
    }
}
