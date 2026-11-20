use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusty_bot_core::{
    DirEntry, FileByteStream, FileError, FileErrorCode, FileKind, FileSystem, SearchPage,
    SearchRequest,
};
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

/// Filesystem rooted at a workspace directory. Relative paths resolve against
/// `cwd` and may not escape the root; absolute paths are allowed in trusted-host
/// mode and logged when outside the root.
#[derive(Debug)]
pub struct LocalFileSystem {
    id: String,
    root: PathBuf,
    cwd: PathBuf,
}

impl LocalFileSystem {
    pub fn new(root: PathBuf, cwd: PathBuf) -> Self {
        let id = format!("local:{}", root.display());
        Self { id, root, cwd }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.cwd.join(path)
        }
    }

    /// Validate that a relative path cannot escape the workspace root, including
    /// through a symlinked existing ancestor. Absolute paths are logged but
    /// permitted in v1 trusted-host mode.
    async fn validate(&self, path: &Path) -> Result<PathBuf, FileError> {
        let resolved = self.resolve(path);
        if path.is_absolute() {
            if !resolved.starts_with(&self.root) {
                tracing::warn!(
                    path = %resolved.display(),
                    root = %self.root.display(),
                    "absolute path outside workspace root (trusted-host mode)"
                );
            }
            return Ok(resolved);
        }

        let invalid = || {
            FileError::new(
                FileErrorCode::Invalid,
                Some(resolved.clone()),
                "relative path escapes the workspace root",
            )
        };
        let mut current = tokio::fs::canonicalize(&self.cwd)
            .await
            .map_err(|error| FileError::from_io(&self.cwd, &error))?;
        if !current.starts_with(&self.root) {
            return Err(invalid());
        }
        for component in path.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if current == self.root || !current.pop() {
                        return Err(invalid());
                    }
                }
                std::path::Component::Normal(name) => {
                    current.push(name);
                    match tokio::fs::symlink_metadata(&current).await {
                        Ok(_) => {
                            current = tokio::fs::canonicalize(&current)
                                .await
                                .map_err(|error| FileError::from_io(&current, &error))?;
                            if !current.starts_with(&self.root) {
                                return Err(invalid());
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(FileError::from_io(&current, &error)),
                    }
                }
                _ => return Err(invalid()),
            }
        }
        Ok(current)
    }
}

#[async_trait::async_trait]
impl FileSystem for LocalFileSystem {
    fn id(&self) -> &str {
        &self.id
    }

    fn cwd(&self) -> &Path {
        &self.cwd
    }

    async fn search(
        &self,
        request: SearchRequest,
        cancel: CancellationToken,
    ) -> Result<SearchPage, FileError> {
        if cancel.is_cancelled() {
            return Err(FileError::new(
                FileErrorCode::Aborted,
                None,
                "search aborted",
            ));
        }
        let path = match &request {
            SearchRequest::Grep(q) => &q.root,
            SearchRequest::Find(q) => &q.root,
            SearchRequest::List(q) => &q.root,
        };
        let root = self.validate(path).await?;
        tokio::task::spawn_blocking(move || crate::search::search(root, request, cancel))
            .await
            .map_err(|e| FileError::new(FileErrorCode::Unknown, None, e.to_string()))?
    }

    async fn open_read(
        &self,
        path: &Path,
        cancel: CancellationToken,
    ) -> Result<FileByteStream, FileError> {
        let aborted = || {
            FileError::new(
                FileErrorCode::Aborted,
                Some(path.to_path_buf()),
                "read aborted",
            )
        };
        if cancel.is_cancelled() {
            return Err(aborted());
        }
        let resolved = self.validate(path).await?;
        let file = tokio::fs::File::open(&resolved)
            .await
            .map_err(|e| FileError::from_io(&resolved, &e))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| FileError::from_io(&resolved, &e))?;
        if metadata.is_dir() {
            return Err(FileError::new(
                FileErrorCode::IsDirectory,
                Some(resolved),
                "cannot read a directory",
            ));
        }
        let stream = futures::stream::try_unfold(
            (file, resolved, cancel, metadata.len()),
            |(mut file, path, cancel, remaining)| async move {
                if remaining == 0 {
                    return Ok(None);
                }
                let mut buffer = vec![0; remaining.min(8192) as usize];
                let read = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(FileError::new(FileErrorCode::Aborted, Some(path), "read aborted")),
                    result = file.read(&mut buffer) => result.map_err(|e| FileError::from_io(&path, &e))?,
                };
                if read == 0 {
                    return Ok(None);
                }
                buffer.truncate(read);
                Ok(Some((
                    bytes::Bytes::from(buffer),
                    (file, path, cancel, remaining - read as u64),
                )))
            },
        );
        Ok(Box::pin(stream))
    }

    async fn read_text_file(&self, path: &Path) -> Result<String, FileError> {
        let bytes = self.read_bytes(path).await?;
        String::from_utf8(bytes).map_err(|error| {
            FileError::new(
                FileErrorCode::Invalid,
                Some(self.resolve(path)),
                format!("file is not valid UTF-8: {error}"),
            )
        })
    }

    async fn read_bytes(&self, path: &Path) -> Result<Vec<u8>, FileError> {
        let resolved = self.validate(path).await?;
        tokio::fs::read(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))
    }

    async fn write_file(&self, path: &Path, bytes: &[u8]) -> Result<(), FileError> {
        let resolved = self.validate(path).await?;
        tokio::fs::write(&resolved, bytes)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))
    }

    async fn append_file(&self, path: &Path, bytes: &[u8]) -> Result<(), FileError> {
        let resolved = self.validate(path).await?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))?;
        file.write_all(bytes)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))?;
        file.flush()
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))
    }

    async fn truncate_file(&self, path: &Path, len: u64) -> Result<(), FileError> {
        let resolved = self.validate(path).await?;
        let file = OpenOptions::new()
            .write(true)
            .open(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))?;
        file.set_len(len)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))
    }

    async fn rename_file(&self, from: &Path, to: &Path) -> Result<(), FileError> {
        let from = self.validate(from).await?;
        let to = self.validate(to).await?;
        tokio::fs::rename(&from, &to)
            .await
            .map_err(|error| FileError::from_io(&from, &error))
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<DirEntry>, FileError> {
        let resolved = self.validate(path).await?;
        let mut dir = tokio::fs::read_dir(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))?;
        let mut entries = Vec::new();
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))?
        {
            let entry_path = entry.path();
            let metadata = tokio::fs::symlink_metadata(&entry_path)
                .await
                .map_err(|error| FileError::from_io(&entry_path, &error))?;
            let file_type = metadata.file_type();
            let kind = if file_type.is_symlink() {
                FileKind::Symlink
            } else if file_type.is_dir() {
                FileKind::Directory
            } else if file_type.is_file() {
                FileKind::File
            } else {
                FileKind::Other
            };
            let mtime_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or(0);
            entries.push(DirEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                path: entry_path,
                kind,
                size: metadata.len(),
                mtime_ms,
            });
        }
        Ok(entries)
    }

    async fn canonical_path(&self, path: &Path) -> Result<PathBuf, FileError> {
        let resolved = self.validate(path).await?;
        let canonical = tokio::fs::canonicalize(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))?;
        if !path.is_absolute() && !canonical.starts_with(&self.root) {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                Some(resolved),
                "canonical path escapes the workspace root",
            ));
        }
        Ok(canonical)
    }

    async fn exists(&self, path: &Path) -> Result<bool, FileError> {
        let resolved = self.validate(path).await?;
        tokio::fs::try_exists(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))
    }

    async fn create_dir(&self, path: &Path, recursive: bool) -> Result<(), FileError> {
        let resolved = self.validate(path).await?;
        let result = if recursive {
            tokio::fs::create_dir_all(&resolved).await
        } else {
            tokio::fs::create_dir(&resolved).await
        };
        result.map_err(|error| FileError::from_io(&resolved, &error))
    }

    async fn remove_file(&self, path: &Path) -> Result<(), FileError> {
        let resolved = self.validate(path).await?;
        tokio::fs::remove_file(&resolved)
            .await
            .map_err(|error| FileError::from_io(&resolved, &error))
    }
}

/// Map an arbitrary I/O error encountered outside these helpers.
pub fn io_error(path: &Path, error: &std::io::Error) -> FileError {
    FileError::from_io(path, error)
}
