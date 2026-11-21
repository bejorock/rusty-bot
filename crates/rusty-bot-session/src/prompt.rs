//! Small deterministic templates: no evaluation, I/O, recursive substitution or escaping of values.
use crate::{ContentBlock, ConversationMessage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type PromptVariables = BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptTemplate {
    source: String,
    #[serde(default)]
    bindings: PromptVariables,
}
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid prompt template: {0}")]
pub struct PromptError(pub String);

#[derive(Debug)]
enum Segment {
    Literal(String),
    Variable(String),
}
fn parse(source: &str) -> Result<Vec<Segment>, PromptError> {
    let mut chars = source.chars().peekable();
    let mut segments = vec![];
    let mut text = String::new();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                text.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                text.push('}');
            }
            '{' => {
                if !text.is_empty() {
                    segments.push(Segment::Literal(std::mem::take(&mut text)));
                }
                let mut name = String::new();
                let mut closed = false;
                for c in chars.by_ref() {
                    if c == '}' {
                        closed = true;
                        break;
                    }
                    name.push(c);
                }
                if !closed || !identifier(&name) {
                    return Err(PromptError(
                        "expected {identifier}; unmatched brace or unsupported expression".into(),
                    ));
                }
                segments.push(Segment::Variable(name));
            }
            '}' => return Err(PromptError("unmatched closing brace".into())),
            _ => text.push(c),
        }
    }
    if !text.is_empty() {
        segments.push(Segment::Literal(text));
    }
    Ok(segments)
}
fn identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}
impl PromptTemplate {
    pub fn new(source: impl Into<String>) -> Result<Self, PromptError> {
        let result = Self {
            source: source.into(),
            bindings: BTreeMap::new(),
        };
        result.variables()?;
        Ok(result)
    }
    pub fn source(&self) -> &str {
        &self.source
    }
    pub fn variables(&self) -> Result<BTreeSet<String>, PromptError> {
        Ok(parse(&self.source)?
            .into_iter()
            .filter_map(|s| match s {
                Segment::Variable(v) => Some(v),
                _ => None,
            })
            .collect())
    }
    pub fn required_variables(&self) -> Result<BTreeSet<String>, PromptError> {
        let vars = self.variables()?;
        if self.bindings.keys().any(|k| !vars.contains(k)) {
            return Err(PromptError("unknown bound variable".into()));
        }
        Ok(vars
            .into_iter()
            .filter(|v| !self.bindings.contains_key(v))
            .collect())
    }
    /// Returns a variation without mutating the base. Rebinding overrides a prior binding.
    pub fn partial(&self, values: PromptVariables) -> Result<Self, PromptError> {
        self.required_variables()?;
        let vars = self.variables()?;
        if values.keys().any(|k| !vars.contains(k)) {
            return Err(PromptError("unknown partial variable".into()));
        }
        let mut result = self.clone();
        result.bindings.extend(values);
        Ok(result)
    }
    /// Explicit render values override partial bindings. Unknown or missing keys are errors.
    pub fn render(&self, values: &PromptVariables) -> Result<String, PromptError> {
        self.required_variables()?;
        let vars = self.variables()?;
        if values.keys().any(|k| !vars.contains(k)) {
            return Err(PromptError("unknown render variable".into()));
        }
        let mut bindings = self.bindings.clone();
        bindings.extend(values.clone());
        let mut output = String::new();
        for segment in parse(&self.source)? {
            match segment {
                Segment::Literal(s) => output.push_str(&s),
                Segment::Variable(name) => output.push_str(
                    bindings
                        .get(&name)
                        .ok_or_else(|| PromptError(format!("missing variable {name}")))?,
                ),
            }
        }
        Ok(output)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptRole {
    System,
    Developer,
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PromptMessage {
    Text {
        role: PromptRole,
        template: PromptTemplate,
    },
    /// Preserves multimodal content verbatim, including braces; never template-scanned.
    Literal { message: ConversationMessage },
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatPromptTemplate {
    pub messages: Vec<PromptMessage>,
}
impl ChatPromptTemplate {
    pub fn render(
        &self,
        values: &PromptVariables,
    ) -> Result<Vec<ConversationMessage>, PromptError> {
        let mut all = BTreeSet::new();
        for message in &self.messages {
            if let PromptMessage::Text { template, .. } = message {
                all.extend(template.variables()?);
            }
        }
        if values.keys().any(|k| !all.contains(k)) {
            return Err(PromptError("unknown chat prompt variable".into()));
        }
        self.messages
            .iter()
            .map(|message| {
                let rendered = match message {
                    PromptMessage::Literal { message } => message.clone(),
                    PromptMessage::Text { role, template } => {
                        let vars = template.variables()?;
                        let selected = values
                            .iter()
                            .filter(|(k, _)| vars.contains(*k))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        let content = vec![ContentBlock::text(template.render(&selected)?)];
                        match role {
                            PromptRole::System => ConversationMessage::System { content },
                            PromptRole::Developer => ConversationMessage::Developer { content },
                            PromptRole::User => ConversationMessage::User { content },
                            PromptRole::Assistant => ConversationMessage::Assistant { content },
                        }
                    }
                };
                rendered
                    .validate()
                    .map_err(|e| PromptError(e.to_string()))?;
                Ok(rendered)
            })
            .collect()
    }
}
