use rusty_bot_core::{FileErrorCode, FileSystem, SessionId};
use rusty_bot_local::LocalFileSystem;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

struct Fixture {
    base: PathBuf,
    fs: LocalFileSystem,
}
impl Fixture {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("rusty-fs-{}", SessionId::new()));
        std::fs::create_dir_all(base.join("workspace")).unwrap();
        let base = std::fs::canonicalize(base).unwrap();
        let root = base.join("workspace");
        Self {
            base,
            fs: LocalFileSystem::new(root.clone(), root),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

#[tokio::test]
async fn file_mutations_round_trip_without_touching_existing_files() {
    let fixture = Fixture::new();
    let path = Path::new("sample.txt");
    fixture.fs.write_file(path, b"hello").await.unwrap();
    fixture.fs.append_file(path, b" world").await.unwrap();
    assert_eq!(
        fixture.fs.read_text_file(path).await.unwrap(),
        "hello world"
    );
    fixture.fs.truncate_file(path, 5).await.unwrap();
    fixture
        .fs
        .rename_file(path, Path::new("renamed.txt"))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .fs
            .read_bytes(Path::new("renamed.txt"))
            .await
            .unwrap(),
        b"hello"
    );
    assert_eq!(
        fixture.fs.read_bytes(path).await.unwrap_err().code,
        FileErrorCode::NotFound
    );
}

#[tokio::test]
async fn relative_traversal_is_rejected_without_modifying_outside_files() {
    let fixture = Fixture::new();
    let outside = fixture.base.join("outside.txt");
    std::fs::write(&outside, b"unchanged").unwrap();
    let error = fixture
        .fs
        .write_file(Path::new("../outside.txt"), b"bad")
        .await
        .unwrap_err();
    assert_eq!(error.code, FileErrorCode::Invalid);
    assert_eq!(std::fs::read(&outside).unwrap(), b"unchanged");
}

#[tokio::test]
async fn relative_symlink_escape_is_rejected_for_existing_and_new_files() {
    let fixture = Fixture::new();
    std::fs::create_dir(fixture.base.join("outside")).unwrap();
    std::fs::write(fixture.base.join("outside/existing"), b"secret").unwrap();
    std::os::unix::fs::symlink(fixture.base.join("outside"), fixture.fs.root().join("link"))
        .unwrap();
    assert_eq!(
        fixture
            .fs
            .read_bytes(Path::new("link/existing"))
            .await
            .unwrap_err()
            .code,
        FileErrorCode::Invalid
    );
    assert_eq!(
        fixture
            .fs
            .write_file(Path::new("link/new"), b"bad")
            .await
            .unwrap_err()
            .code,
        FileErrorCode::Invalid
    );
    assert!(!fixture.base.join("outside/new").exists());
}

#[tokio::test]
async fn absolute_paths_remain_allowed_in_trusted_host_mode() {
    let fixture = Fixture::new();
    let outside = fixture.base.join("absolute.txt");
    fixture.fs.write_file(&outside, b"allowed").await.unwrap();
    assert_eq!(fixture.fs.read_bytes(&outside).await.unwrap(), b"allowed");
}

#[tokio::test]
async fn invalid_utf8_and_cancelled_reads_return_typed_errors() {
    let fixture = Fixture::new();
    fixture
        .fs
        .write_file(Path::new("binary"), &[0xff, 0xfe])
        .await
        .unwrap();
    assert_eq!(
        fixture
            .fs
            .read_text_file(Path::new("binary"))
            .await
            .unwrap_err()
            .code,
        FileErrorCode::Invalid
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = fixture.fs.open_read(Path::new("binary"), cancel).await;
    assert!(matches!(result, Err(error) if error.code == FileErrorCode::Aborted));
}
