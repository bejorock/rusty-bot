//! Only server-reported capabilities. `None` means unknown, never unsupported.
use crate::DiscoveredModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilities {
    pub image_input: Option<bool>,
    pub structured_outputs: Option<bool>,
    pub thinking: Option<bool>,
    pub thinking_disabled: Option<bool>,
    pub thinking_enabled: Option<bool>,
    pub thinking_adaptive: Option<bool>,
    pub effort_levels: Vec<String>,
}
impl DiscoveredModel {
    /// Reads the provider's explicit capability fields, not the model name.
    /// These describe the model, not every setting this SDK can translate.
    pub fn capabilities(&self) -> ModelCapabilities {
        let c = &self.raw["capabilities"];
        let supported =
            |v: &serde_json::Value| v.get("supported").and_then(serde_json::Value::as_bool);
        ModelCapabilities {
            image_input: supported(&c["image_input"]),
            structured_outputs: supported(&c["structured_outputs"]),
            thinking: supported(&c["thinking"]),
            thinking_disabled: supported(&c["thinking"]["types"]["disabled"]),
            thinking_enabled: supported(&c["thinking"]["types"]["enabled"]),
            thinking_adaptive: supported(&c["thinking"]["types"]["adaptive"]),
            effort_levels: ["minimal", "low", "medium", "high", "xhigh", "max"]
                .into_iter()
                .filter(|name| supported(&c["effort"][*name]) == Some(true))
                .map(str::to_owned)
                .collect(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn unknown_is_not_false_and_names_are_not_inferred() {
        let mut m = DiscoveredModel {
            id: "gpt-image-thinking".into(),
            display_name: None,
            max_input_tokens: None,
            max_output_tokens: None,
            raw: json!({}),
        };
        assert_eq!(m.capabilities(), ModelCapabilities::default());
        m.raw = json!({"capabilities":{"image_input":{"supported":true},"structured_outputs":{"supported":false},"thinking":{"supported":true,"types":{"disabled":{"supported":false},"adaptive":{"supported":true}}},"effort":{"low":{"supported":true},"max":{"supported":false}}}});
        let c = m.capabilities();
        assert_eq!(c.image_input, Some(true));
        assert_eq!(c.structured_outputs, Some(false));
        assert_eq!(c.thinking_disabled, Some(false));
        assert_eq!(c.thinking_enabled, None);
        assert_eq!(c.effort_levels, ["low"]);
        assert_eq!(
            serde_json::from_value::<ModelCapabilities>(serde_json::to_value(c.clone()).unwrap())
                .unwrap(),
            c
        );
    }
}
