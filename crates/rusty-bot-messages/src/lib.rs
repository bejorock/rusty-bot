#![doc = include_str!("../README.md")]
use serde::{Deserialize, Serialize};

mod generation;
pub use generation::*;
mod history;
pub use history::*;
mod message_stream;
pub use message_stream::*;
mod prompt;
pub use prompt::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageStatus {
    Generating,
    Completed,
    Failed,
}

#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod message_contract_tests;
#[cfg(test)]
mod prompt_tests;
