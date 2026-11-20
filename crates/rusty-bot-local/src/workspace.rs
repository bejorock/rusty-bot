use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusty_bot_core::{
    ExecutionSelection, FileSystem, Shell, Workspace, WorkspaceError, WorkspaceKey,
    WorkspaceProvider, WorkspaceSpec,
};

use crate::fs::LocalFileSystem;
use crate::shell::LocalShell;

#[derive(Debug)]
struct CachedEnv {
    shell: Arc<LocalShell>,
}

#[derive(Debug)]
struct CachedWorkspace {
    root: PathBuf,
    files: Arc<LocalFileSystem>,
    envs: HashMap<String, CachedEnv>,
    refs: usize,
}

#[derive(Debug)]
struct ProviderInner {
    host_id: String,
    default_root: PathBuf,
    entries: Mutex<HashMap<WorkspaceKey, CachedWorkspace>>,
}

impl Drop for ProviderInner {
    fn drop(&mut self) {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for workspace in entries.values() {
            for env in workspace.envs.values() {
                env.shell.kill_all_blocking();
            }
        }
    }
}

/// Opens and caches local workspaces by `WorkspaceKey`, sharing one execution
/// environment per `(root, env_id)` and refcounting workspace references.
#[derive(Debug)]
pub struct LocalWorkspaceProvider {
    inner: Arc<ProviderInner>,
}

impl LocalWorkspaceProvider {
    pub fn new(default_root: PathBuf) -> Result<Self, WorkspaceError> {
        Self::with_host_id(default_root, default_host_id())
    }

    pub fn with_host_id(
        default_root: PathBuf,
        host_id: impl Into<String>,
    ) -> Result<Self, WorkspaceError> {
        std::fs::create_dir_all(&default_root)
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        let default_root = std::fs::canonicalize(&default_root)
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(ProviderInner {
                host_id: host_id.into(),
                default_root,
                entries: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn host_id(&self) -> &str {
        &self.inner.host_id
    }

    pub fn default_root(&self) -> &Path {
        &self.inner.default_root
    }

    fn key_lock(&self) -> std::sync::MutexGuard<'_, HashMap<WorkspaceKey, CachedWorkspace>> {
        self.inner
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Test/diagnostics helper: number of cached execution environments.
    pub fn env_count(&self) -> usize {
        self.key_lock()
            .values()
            .map(|workspace| workspace.envs.len())
            .sum()
    }
}

#[derive(Debug)]
struct LocalWorkspace {
    key: WorkspaceKey,
    root: PathBuf,
    files: Arc<LocalFileSystem>,
    shell: Option<Arc<LocalShell>>,
    env_id: Option<String>,
    host_id: String,
    spec: WorkspaceSpec,
}

impl Workspace for LocalWorkspace {
    fn key(&self) -> &WorkspaceKey {
        &self.key
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn files(&self) -> Arc<dyn FileSystem> {
        self.files.clone()
    }

    fn execution(&self) -> Option<Arc<dyn Shell>> {
        self.shell.clone().map(|shell| shell as Arc<dyn Shell>)
    }

    fn execution_env_id(&self) -> Option<&str> {
        self.env_id.as_deref()
    }

    fn host_id(&self) -> &str {
        &self.host_id
    }

    fn workspace_spec(&self) -> Option<WorkspaceSpec> {
        Some(self.spec.clone())
    }
}

#[async_trait::async_trait]
impl WorkspaceProvider for LocalWorkspaceProvider {
    async fn open(&self, spec: WorkspaceSpec) -> Result<Arc<dyn Workspace>, WorkspaceError> {
        let root = match &spec.root {
            Some(root) => root.clone(),
            None => self.inner.default_root.join(&spec.key.0),
        };
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        let root = tokio::fs::canonicalize(&root)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        if !root.starts_with(&self.inner.default_root) {
            tracing::warn!(
                root = %root.display(),
                "workspace root is outside the provider default root (trusted-host mode)"
            );
        }

        let mut guard = self.key_lock();
        if let Some(existing) = guard.get(&spec.key) {
            if existing.root != root {
                return Err(WorkspaceError::Invalid(format!(
                    "workspace {} is already open at {} (requested {})",
                    spec.key,
                    existing.root.display(),
                    root.display()
                )));
            }
        }

        let entry = guard
            .entry(spec.key.clone())
            .or_insert_with(|| CachedWorkspace {
                root: root.clone(),
                files: Arc::new(LocalFileSystem::new(root.clone(), root.clone())),
                envs: HashMap::new(),
                refs: 0,
            });
        entry.refs += 1;
        let files = entry.files.clone();

        let (shell, env_id) = match &spec.execution {
            ExecutionSelection::None => (None, None),
            ExecutionSelection::Local { env_id, cwd } => {
                let env_id = env_id
                    .clone()
                    .unwrap_or_else(|| format!("local:{}:{}", self.inner.host_id, root.display()));
                let cwd = cwd.clone().unwrap_or_else(|| root.clone());
                let env = entry
                    .envs
                    .entry(env_id.clone())
                    .or_insert_with(|| CachedEnv {
                        shell: Arc::new(LocalShell::new(env_id.clone(), cwd)),
                    });
                (Some(env.shell.clone()), Some(env_id))
            }
            ExecutionSelection::Existing { env_id } => {
                let env = entry
                    .envs
                    .get(env_id)
                    .ok_or_else(|| WorkspaceError::UnknownEnv(env_id.clone()))?;
                (Some(env.shell.clone()), Some(env_id.clone()))
            }
        };

        let persisted_spec = WorkspaceSpec {
            key: spec.key.clone(),
            root: Some(root.clone()),
            execution: spec.execution,
        };
        Ok(Arc::new(LocalWorkspace {
            key: spec.key,
            root,
            files,
            shell,
            env_id,
            host_id: self.inner.host_id.clone(),
            spec: persisted_spec,
        }))
    }

    async fn release(&self, key: &WorkspaceKey) -> Result<(), WorkspaceError> {
        let envs = {
            let mut guard = self.key_lock();
            let Some(entry) = guard.get_mut(key) else {
                return Ok(());
            };
            entry.refs = entry.refs.saturating_sub(1);
            if entry.refs > 0 {
                return Ok(());
            }
            let workspace = guard.remove(key);
            workspace
                .map(|workspace| workspace.envs)
                .unwrap_or_default()
        };
        for (_, env) in envs {
            env.shell
                .cleanup()
                .await
                .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        }
        Ok(())
    }
}

fn default_host_id() -> String {
    "localhost".to_string()
}

/// Convenience constructor for a workspace with files only (no shell).
pub fn files_only_spec(key: impl Into<String>, root: PathBuf) -> WorkspaceSpec {
    WorkspaceSpec {
        key: WorkspaceKey::new(key),
        root: Some(root),
        execution: ExecutionSelection::None,
    }
}

/// Convenience constructor for a local workspace with execution.
pub fn local_spec(key: impl Into<String>, root: PathBuf, env_id: Option<String>) -> WorkspaceSpec {
    WorkspaceSpec {
        key: WorkspaceKey::new(key),
        root: Some(root.clone()),
        execution: ExecutionSelection::Local {
            env_id,
            cwd: Some(root),
        },
    }
}
