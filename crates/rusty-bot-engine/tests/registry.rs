use rusty_bot_core::{Extension, PromptSection, RegistryError, Tool};
use rusty_bot_engine::{builtin_registry, ExtensionSelection, Registry, SessionConfig, ToolFilter};
use std::sync::Arc;

struct TestExtension {
    version: &'static str,
}
impl Extension for TestExtension {
    fn name(&self) -> &str {
        "test"
    }
    fn version(&self) -> &str {
        self.version
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![]
    }
    fn sections(&self) -> Vec<PromptSection> {
        vec![PromptSection {
            key: "test_context".into(),
            content: "context".into(),
        }]
    }
}

#[test]
fn duplicate_extension_registration_is_rejected() {
    let builder = Registry::builder()
        .add_extension(Arc::new(TestExtension { version: "1" }))
        .unwrap();
    assert!(
        matches!(builder.add_extension(Arc::new(TestExtension { version:"1" })), Err(RegistryError::DuplicateExtension(name)) if name == "test")
    );
}

#[test]
fn registry_fingerprint_changes_with_extension_version() {
    let build = |version| {
        Registry::builder()
            .add_extension(Arc::new(TestExtension { version }))
            .unwrap()
            .build()
    };
    assert_ne!(build("1").version(), build("2").version());
    assert_eq!(build("1").version(), build("1").version());
}

#[test]
fn model_visibility_cannot_grant_execution_permissions() {
    let registry = builtin_registry().unwrap();
    let resolved = registry.resolve(&ExtensionSelection::All).unwrap();
    let tools = registry.resolve_tool_set(
        &resolved,
        &ToolFilter::Only(vec!["read".into(), "ls".into()]),
        &ToolFilter::Only(vec!["read".into(), "bash".into()]),
    );
    assert!(tools.get("read").is_some());
    assert!(tools.get("ls").is_some());
    assert!(tools.get("bash").is_none());
    assert_eq!(
        tools
            .model_specs()
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        ["read"]
    );
}

#[test]
fn config_rejects_provider_mismatch_unknown_extensions_and_versions() {
    let registry = builtin_registry().unwrap();
    let mut config = SessionConfig::default();
    config.model.provider = "test".into();
    config.model.model_id = "model".into();
    config.validate_for_provider(&registry, "test").unwrap();
    assert!(config.validate_for_provider(&registry, "other").is_err());
    config.extensions = ExtensionSelection::Only(vec!["missing".into()]);
    assert!(config.validate_for_provider(&registry, "test").is_err());
    config.extensions = ExtensionSelection::All;
    config.config_version = 99;
    assert!(config.validate_for_provider(&registry, "test").is_err());
}

#[test]
fn host_instructions_follow_extension_prompt_sections() {
    let registry = Registry::builder()
        .add_extension(Arc::new(TestExtension { version: "1" }))
        .unwrap()
        .build();
    let resolved = registry.resolve(&ExtensionSelection::All).unwrap();
    let sections = registry.resolve_sections(&resolved, Some("host instructions"));
    assert_eq!(sections.len(), 2);
    assert_eq!(sections[0].content, "context");
    assert_eq!(sections[1].key, "instructions");
    assert_eq!(sections[1].content, "host instructions");
}
