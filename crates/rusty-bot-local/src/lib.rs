//! Local filesystem and trusted-host subprocess execution.
//!
//! This adapter is **not a sandbox**. Shell commands run with the embedding process's
//! privileges against the same directory the file tools see. It is intended for
//! local development and single-host deployments.

#[cfg(not(unix))]
compile_error!("rusty-bot-local requires a Unix host");

mod find_fd;
pub mod fs;
mod grep_rg;
#[allow(dead_code)]
mod search;
mod search_cursor;
mod search_key_page;
pub mod shell;
#[allow(dead_code)]
mod stream_match;
pub mod workspace;

pub use fs::LocalFileSystem;
pub use shell::LocalShell;
pub use workspace::LocalWorkspaceProvider;
