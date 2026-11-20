use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// File error codes reuse Pi's public vocabulary deliberately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileErrorCode {
    Aborted,
    NotFound,
    PermissionDenied,
    NotDirectory,
    IsDirectory,
    Invalid,
    NotSupported,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileError {
    pub code: FileErrorCode,
    pub path: Option<PathBuf>,
    pub message: String,
}

impl FileError {
    pub fn new(code: FileErrorCode, path: Option<PathBuf>, message: impl Into<String>) -> Self {
        Self {
            code,
            path,
            message: message.into(),
        }
    }

    pub fn from_io(path: &Path, error: &std::io::Error) -> Self {
        use std::io::ErrorKind;
        let code = match error.kind() {
            ErrorKind::NotFound => FileErrorCode::NotFound,
            ErrorKind::PermissionDenied => FileErrorCode::PermissionDenied,
            ErrorKind::NotADirectory => FileErrorCode::NotDirectory,
            ErrorKind::IsADirectory => FileErrorCode::IsDirectory,
            ErrorKind::InvalidData | ErrorKind::InvalidInput => FileErrorCode::Invalid,
            ErrorKind::Unsupported => FileErrorCode::NotSupported,
            _ => FileErrorCode::Unknown,
        };
        Self::new(code, Some(path.to_path_buf()), error.to_string())
    }
}

impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file {:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for FileError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub path: PathBuf,
    pub kind: FileKind,
    pub size: u64,
    pub mtime_ms: i64,
}

pub type FileByteStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, FileError>> + Send>>;

/// Provider-bound search requests; no execution environment is required.
#[derive(Clone, Debug)]
pub enum SearchRequest {
    Grep(GrepQuery),
    Find(FindQuery),
    List(ListQuery),
}

#[derive(Clone, Debug)]
pub struct GrepQuery {
    pub root: PathBuf,
    pub pattern: String,
    pub glob: Option<String>,
    pub literal: bool,
    pub ignore_case: bool,
    pub context: usize,
    pub limit: usize,
    pub byte_cap: usize,
    pub cursor: Option<String>,
}
#[derive(Clone, Debug)]
pub struct FindQuery {
    pub root: PathBuf,
    pub pattern: String,
    pub limit: usize,
    pub byte_cap: usize,
    pub cursor: Option<String>,
}
#[derive(Clone, Debug)]
pub struct ListQuery {
    pub root: PathBuf,
    pub include_hidden: bool,
    pub limit: usize,
    pub byte_cap: usize,
    pub cursor: Option<String>,
}
#[derive(Clone, Debug)]
pub struct SearchPage {
    /// Opaque continuation for the same operation and query, if more output exists.
    pub next_cursor: Option<String>,
    pub lines: Vec<String>,
    pub matched: u64,
    pub has_more: bool,
    pub truncated_bytes: bool,
    pub truncated_lines: bool,
    /// Totals for rendered lines before the byte cap, if known exactly.
    pub output_totals: Option<(u64, u64)>,
}

/// The workspace file boundary. `id` namespaces files: the same id means the
/// same files at the same paths on this host.
#[async_trait::async_trait]
pub trait FileSystem: Send + Sync {
    fn id(&self) -> &str;
    fn cwd(&self) -> &Path;

    async fn open_read(
        &self,
        path: &Path,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<FileByteStream, FileError> {
        Err(FileError::new(
            FileErrorCode::NotSupported,
            Some(path.to_path_buf()),
            "streaming reads are not supported",
        ))
    }
    async fn search(
        &self,
        _request: SearchRequest,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<SearchPage, FileError> {
        Err(FileError::new(
            FileErrorCode::NotSupported,
            None,
            "search is not supported",
        ))
    }

    async fn read_text_file(&self, path: &Path) -> Result<String, FileError>;
    async fn read_bytes(&self, path: &Path) -> Result<Vec<u8>, FileError>;
    async fn write_file(&self, path: &Path, bytes: &[u8]) -> Result<(), FileError>;
    async fn append_file(&self, path: &Path, bytes: &[u8]) -> Result<(), FileError>;
    async fn truncate_file(&self, path: &Path, len: u64) -> Result<(), FileError>;
    async fn rename_file(&self, from: &Path, to: &Path) -> Result<(), FileError>;
    async fn list_dir(&self, path: &Path) -> Result<Vec<DirEntry>, FileError>;
    async fn canonical_path(&self, path: &Path) -> Result<PathBuf, FileError>;
    async fn exists(&self, path: &Path) -> Result<bool, FileError>;
    async fn create_dir(&self, path: &Path, recursive: bool) -> Result<(), FileError>;
    async fn remove_file(&self, path: &Path) -> Result<(), FileError>;
}
