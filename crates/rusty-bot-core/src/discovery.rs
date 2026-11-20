//! Optional, caller-driven discovery types; no registry or automatic selection.

use crate::ImageLimits;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    Unknown,
    Supported,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelCapabilities {
    pub images_in: Support,
    pub images_out: Support,
    pub tools: Support,
    pub reasoning: Support,
    pub image_limits: Option<ImageLimits>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelPageRequest {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelInfo {
    pub id: String,
    pub display_name: Option<String>,
    pub capabilities: ModelCapabilities,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelPage {
    pub items: Vec<ModelInfo>,
    pub next_cursor: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelDiscovery {
    Unsupported,
    Page(ModelPage),
}
