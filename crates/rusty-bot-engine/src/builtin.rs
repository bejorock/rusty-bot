//! Rust-native extension registry and built-in workspace tools.

use std::sync::Arc;

use rusty_bot_core::{Extension, PromptSection, RegistryError, Tool};

/// The built-in extension: workspace file tools, shell, and the base prompt.
pub fn builtin_extension() -> Arc<dyn Extension> {
    Arc::new(BuiltinExtension)
}

struct BuiltinExtension;

impl Extension for BuiltinExtension {
    fn name(&self) -> &str {
        "rusty-bot.builtin"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![
            Arc::new(crate::tools::ReadTool),
            Arc::new(crate::tools::WriteTool),
            Arc::new(crate::tools::EditTool),
            Arc::new(crate::tools::LsTool),
            Arc::new(crate::tools::GrepTool),
            Arc::new(crate::tools::FindTool),
            Arc::new(crate::tools::BashTool),
            Arc::new(crate::tools::PowerShellTool),
        ]
    }

    fn sections(&self) -> Vec<PromptSection> {
        vec![PromptSection {
            key: "rusty_bot_base".into(),
            content: BASE_PROMPT.into(),
        }]
    }
}

const BASE_PROMPT: &str = "\
You are Rusty Bot, a local-first coding agent. You work inside a workspace \
directory chosen by the caller and must use the provided tools to inspect and \
modify it. Prefer reading files and searching before editing. File paths are \
relative to the workspace root. Shell commands are trusted-host processes, not \
a sandbox. Report failures and uncertainty honestly; never claim work you did \
not perform.";

/// A registry containing only the built-in extension.
pub fn builtin_registry() -> Result<crate::registry::Registry, RegistryError> {
    Ok(crate::registry::RegistryBuilder::new()
        .add_builtin_tools()
        .build())
}
