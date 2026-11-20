//! Process-local Rust extension registry and Pi-compatible selection/
//! ordering semantics. The registry is never persisted; sessions persist the
//! selected extension names/versions so running code drift is visible.

use std::collections::BTreeMap;
use std::sync::Arc;

use rusty_bot_core::{
    Extension, Hook, HookDescriptor, PromptSection, RegistryError, Tool, ToolSpec,
};

use crate::session::{ExtensionSelection, ToolFilter};

/// Immutable snapshot of the registered extensions for one worker process.
#[derive(Clone)]
pub struct Registry {
    extensions: Vec<Arc<dyn Extension>>,
    version: String,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("version", &self.version)
            .field("extensions", &self.extension_names())
            .finish()
    }
}

impl Registry {
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::new()
    }

    /// Fingerprint of extension names and versions. It changes whenever the
    /// set or versions change, which is what registry-drift detection needs.
    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn extension_names(&self) -> Vec<String> {
        self.extensions
            .iter()
            .map(|extension| extension.name().to_string())
            .collect()
    }

    pub fn resolve(
        &self,
        selection: &ExtensionSelection,
    ) -> Result<ResolvedExtensions, RegistryError> {
        let names = match selection {
            ExtensionSelection::All => self.extension_names(),
            ExtensionSelection::Only(requested) => {
                let mut names = Vec::new();
                for name in requested {
                    if self.find(name).is_none() {
                        tracing::warn!(extension = %name, "selected extension is not registered; skipping");
                        continue;
                    }
                    if !names.contains(name) {
                        names.push(name.clone());
                    }
                }
                names
            }
            ExtensionSelection::AddRemove { add, remove } => {
                let mut names = self.extension_names();
                for name in add {
                    if self.find(name).is_none() {
                        tracing::warn!(extension = %name, "added extension is not registered; skipping");
                        continue;
                    }
                    if !names.contains(name) {
                        names.push(name.clone());
                    }
                }
                names.retain(|name| !remove.contains(name));
                names
            }
        };

        let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
        let mut sections: Vec<PromptSection> = Vec::new();
        let mut versions = BTreeMap::new();
        let mut hooks = Vec::new();
        for name in &names {
            let extension = self
                .find(name)
                .ok_or_else(|| RegistryError::UnknownExtension(name.clone()))?;
            versions.insert(name.clone(), extension.version().to_string());
            for hook in extension.hooks() {
                let descriptor = HookDescriptor {
                    extension: name.clone(),
                    extension_version: extension.version().to_owned(),
                    hook: hook.id().to_owned(),
                    hook_version: hook.version().to_owned(),
                    points: hook.points(),
                    config: extension.hook_config(hook.id()),
                };
                hooks.push(ResolvedHook { descriptor, hook });
            }

            for tool in extension.tools() {
                let tool_name = tool.spec().name;
                // A later same-name tool replaces the earlier one in place.
                if let Some(slot) = tools
                    .iter_mut()
                    .find(|existing| existing.spec().name == tool_name)
                {
                    *slot = tool;
                } else {
                    tools.push(tool);
                }
            }
            for section in extension.sections() {
                if let Some(slot) = sections
                    .iter_mut()
                    .find(|existing| existing.key == section.key)
                {
                    *slot = section;
                } else {
                    sections.push(section);
                }
            }
        }

        Ok(ResolvedExtensions {
            names,
            tools,
            sections,
            versions,
            hooks,
        })
    }

    /// Executable tools after the `tools` filter. `model_tools` is applied by
    /// [`Registry::resolve_tool_set`] and only affects what the model sees.
    pub fn resolve_tools(
        &self,
        resolved: &ResolvedExtensions,
        tools: &ToolFilter,
        model_tools: &ToolFilter,
    ) -> Vec<Arc<dyn Tool>> {
        self.resolve_tool_set(resolved, tools, model_tools).tools
    }

    pub fn resolve_tool_set(
        &self,
        resolved: &ResolvedExtensions,
        tools: &ToolFilter,
        model_tools: &ToolFilter,
    ) -> ResolvedToolSet {
        let universe: Vec<Arc<dyn Tool>> = self
            .extensions
            .iter()
            .flat_map(|extension| extension.tools())
            .collect();
        let executable = apply_tool_filter(&resolved.tools, tools, &universe);
        let model_visible = apply_tool_filter(&executable, model_tools, &executable);
        ResolvedToolSet {
            model_visible: model_visible.iter().map(|tool| tool.spec().name).collect(),
            tools: executable,
        }
    }

    /// Extension prompt sections in resolution order (no instructions).
    pub fn sections(&self, resolved: &ResolvedExtensions) -> Vec<PromptSection> {
        resolved.sections.clone()
    }

    /// Sections plus `instructions` appended last (Pi ordering).
    pub fn resolve_sections(
        &self,
        resolved: &ResolvedExtensions,
        instructions: Option<&str>,
    ) -> Vec<PromptSection> {
        let mut sections = self.sections(resolved);
        if let Some(instructions) = instructions {
            let section = PromptSection {
                key: "instructions".into(),
                content: instructions.to_string(),
            };
            if let Some(slot) = sections.iter_mut().find(|s| s.key == section.key) {
                *slot = section;
            } else {
                sections.push(section);
            }
        }
        sections
    }

    pub fn tool(&self, resolved: &ResolvedExtensions, name: &str) -> Option<Arc<dyn Tool>> {
        resolved
            .tools
            .iter()
            .find(|tool| tool.spec().name == name)
            .cloned()
    }

    fn find(&self, name: &str) -> Option<&Arc<dyn Extension>> {
        self.extensions
            .iter()
            .find(|extension| extension.name() == name)
    }
}

/// Effective tool set for one session configuration.
#[derive(Clone, Default)]
pub struct ResolvedToolSet {
    pub tools: Vec<Arc<dyn Tool>>,
    /// Tool names visible to the model, in tool order.
    pub model_visible: Vec<String>,
}

impl ResolvedToolSet {
    pub fn model_specs(&self) -> Vec<ToolSpec> {
        self.tools
            .iter()
            .filter(|tool| self.model_visible.contains(&tool.spec().name))
            .map(|tool| tool.spec())
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools
            .iter()
            .find(|tool| tool.spec().name == name)
            .cloned()
    }
}

#[derive(Clone)]
pub struct ResolvedHook {
    pub descriptor: HookDescriptor,
    pub hook: Arc<dyn Hook>,
}

#[derive(Clone, Default)]
pub struct ResolvedExtensions {
    pub names: Vec<String>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub sections: Vec<PromptSection>,
    pub versions: BTreeMap<String, String>,
    pub hooks: Vec<ResolvedHook>,
}

impl std::fmt::Debug for ResolvedExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedExtensions")
            .field("names", &self.names)
            .field(
                "tools",
                &self
                    .tools
                    .iter()
                    .map(|tool| tool.spec().name)
                    .collect::<Vec<_>>(),
            )
            .field("sections", &self.sections)
            .field("versions", &self.versions)
            .finish()
    }
}

impl PartialEq for ResolvedExtensions {
    fn eq(&self, other: &Self) -> bool {
        self.names == other.names
            && self
                .tools
                .iter()
                .map(|tool| tool.spec().name)
                .eq(other.tools.iter().map(|tool| tool.spec().name))
            && self.sections == other.sections
            && self.versions == other.versions
            && self
                .hooks
                .iter()
                .map(|h| &h.descriptor)
                .eq(other.hooks.iter().map(|h| &h.descriptor))
    }
}

fn apply_tool_filter(
    base: &[Arc<dyn Tool>],
    filter: &ToolFilter,
    universe: &[Arc<dyn Tool>],
) -> Vec<Arc<dyn Tool>> {
    match filter {
        ToolFilter::All => base.to_vec(),
        ToolFilter::Only(names) => base
            .iter()
            .filter(|tool| names.contains(&tool.spec().name))
            .cloned()
            .collect(),
        ToolFilter::AddRemove { add, remove } => {
            let mut tools: Vec<Arc<dyn Tool>> = base
                .iter()
                .filter(|tool| !remove.contains(&tool.spec().name))
                .cloned()
                .collect();
            for name in add {
                if tools.iter().any(|tool| &tool.spec().name == name) {
                    continue;
                }
                match universe.iter().find(|tool| &tool.spec().name == name) {
                    Some(tool) => tools.push(tool.clone()),
                    None => tracing::warn!(tool = %name, "added tool is not registered; skipping"),
                }
            }
            tools
        }
    }
}

pub struct RegistryBuilder {
    extensions: Vec<Arc<dyn Extension>>,
}

impl Default for RegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistryBuilder {
    pub fn new() -> Self {
        Self {
            extensions: Vec::new(),
        }
    }

    pub fn add_extension(mut self, extension: Arc<dyn Extension>) -> Result<Self, RegistryError> {
        let name = extension.name();
        if !is_valid_extension_name(name) {
            return Err(RegistryError::InvalidSectionKey(name.to_string()));
        }
        if self
            .extensions
            .iter()
            .any(|existing| existing.name() == name)
        {
            return Err(RegistryError::DuplicateExtension(name.to_string()));
        }
        if extension.version().trim().is_empty() {
            return Err(RegistryError::InvalidIdentity(name.to_owned()));
        }
        let mut hook_ids = std::collections::HashSet::new();
        for hook in extension.hooks() {
            if hook.id().trim().is_empty() || hook.version().trim().is_empty() {
                return Err(RegistryError::InvalidIdentity(name.to_owned()));
            }
            if !hook_ids.insert(hook.id().to_owned()) {
                return Err(RegistryError::DuplicateHook(format!(
                    "{}:{}",
                    name,
                    hook.id()
                )));
            }
        }
        for section in extension.sections() {
            if !is_valid_section_key(&section.key) {
                return Err(RegistryError::InvalidSectionKey(section.key));
            }
        }
        self.extensions.push(extension);
        Ok(self)
    }

    pub fn add_builtin_tools(self) -> Self {
        // The built-in extension is registered once by construction.
        self.add_extension(crate::builtin::builtin_extension())
            .expect("builtin extension is valid")
    }

    pub fn build(self) -> Registry {
        let version = format!(
            "v1:{}",
            self.extensions
                .iter()
                .map(|extension| format!("{}@{}", extension.name(), extension.version()))
                .collect::<Vec<_>>()
                .join(",")
        );
        Registry {
            extensions: self.extensions,
            version,
        }
    }
}

fn is_valid_extension_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
}

fn is_valid_section_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}
