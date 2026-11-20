//! Boolean Thompson-NFA simulation over decoded characters, without retaining a line.
//! Look assertions see the complete preceding and following UTF-8 character, so
//! Unicode word boundaries and line anchors remain correct across input chunks.
use regex_automata::{
    nfa::thompson::{self, State, NFA},
    util::{primitives::StateID, syntax},
};

#[cfg(test)]
#[path = "stream_match_tests.rs"]
mod tests;

pub struct Matcher {
    nfa: NFA,
    active: Vec<StateID>,
    next: Vec<StateID>,
    stack: Vec<StateID>,
    seen: Vec<u32>,
    generation: u32,
    previous: [u8; 4],
    previous_len: usize,
    matched: bool,
}

impl Matcher {
    pub fn new(pattern: &str, ignore_case: bool) -> Result<Self, String> {
        let nfa = thompson::Compiler::new()
            .configure(
                NFA::config()
                    .nfa_size_limit(Some(10 * 1024 * 1024))
                    .which_captures(thompson::WhichCaptures::None),
            )
            .syntax(syntax::Config::new().case_insensitive(ignore_case))
            .build(pattern)
            .map_err(|e| e.to_string())?;
        let count = nfa.states().len();
        Ok(Self {
            nfa,
            active: Vec::new(),
            next: Vec::new(),
            stack: Vec::new(),
            seen: vec![0; count],
            generation: 0,
            previous: [0; 4],
            previous_len: 0,
            matched: false,
        })
    }

    pub fn reset(&mut self) {
        self.active.clear();
        self.next.clear();
        self.previous_len = 0;
        self.matched = false;
    }

    // Resolve epsilon transitions at the current byte position. Unanchored
    // start is injected at each character boundary, never inside UTF-8.
    fn step(&mut self, context: &[u8], at: usize, boundary: bool, byte: Option<u8>) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen.fill(0);
            self.generation = 1;
        }
        self.stack.clear();
        self.stack.append(&mut self.active);
        if boundary {
            self.stack.push(self.nfa.start_anchored());
        }
        self.next.clear();
        while let Some(id) = self.stack.pop() {
            let i = id.as_usize();
            if self.seen[i] == self.generation {
                continue;
            }
            self.seen[i] = self.generation;
            match self.nfa.state(id) {
                State::ByteRange { trans } => {
                    if let Some(b) = byte {
                        if trans.matches_byte(b) {
                            self.next.push(trans.next);
                        }
                    }
                }
                State::Sparse(trans) => {
                    if let Some(next) = byte.and_then(|b| trans.matches_byte(b)) {
                        self.next.push(next);
                    }
                }
                State::Dense(trans) => {
                    if let Some(next) = byte.and_then(|b| trans.matches_byte(b)) {
                        self.next.push(next);
                    }
                }
                State::Look { look, next } => {
                    if self.nfa.look_matcher().matches(*look, context, at) {
                        self.stack.push(*next);
                    }
                }
                State::Union { alternates } => self.stack.extend(alternates.iter().copied()),
                State::BinaryUnion { alt1, alt2 } => {
                    self.stack.push(*alt1);
                    self.stack.push(*alt2);
                }
                State::Capture { next, .. } => self.stack.push(*next),
                State::Match { .. } => self.matched = true,
                State::Fail => {}
            }
        }
        std::mem::swap(&mut self.active, &mut self.next);
    }

    pub fn push(&mut self, ch: char) {
        if self.matched {
            return;
        }
        let mut buf = [0; 4];
        let current = ch.encode_utf8(&mut buf).as_bytes();
        let mut context = [0; 8];
        context[..self.previous_len].copy_from_slice(&self.previous[..self.previous_len]);
        context[self.previous_len..self.previous_len + current.len()].copy_from_slice(current);
        for (i, b) in current.iter().copied().enumerate() {
            self.step(
                &context[..self.previous_len + current.len()],
                self.previous_len + i,
                i == 0,
                Some(b),
            );
            if self.matched {
                return;
            }
        }
        self.previous[..current.len()].copy_from_slice(current);
        self.previous_len = current.len();
    }

    pub fn finish(&mut self) -> bool {
        if !self.matched {
            let previous = self.previous;
            self.step(
                &previous[..self.previous_len],
                self.previous_len,
                true,
                None,
            );
        }
        self.matched
    }
}
