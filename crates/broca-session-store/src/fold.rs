//! Folds: rebuilding a session's state by reading its records in order. Each stream has its own
//! fold, and a fold reads only its own stream's records, so the conversation can be rebuilt from
//! the transcript alone and the operational state from the ledger alone.
//!
//! A session lives for years, so no fold state may grow with its history. Every field of every
//! fold state is declared with the `state!` macro below, which makes the field's owner, its reset
//! rule and its bound part of the declaration: a field can't be added without saying what keeps it
//! small. The kinds of bound are the variants of [`Bound`], and none of them is "unbounded".

/// What keeps a fold field from growing with the session's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
    /// A value of fixed size.
    Fixed,
    /// At most as large as a limit in the run's frozen configuration, named here.
    Config(&'static str),
    /// Holds only work that is still open, named here: one step's tool calls, or messages
    /// waiting for a step boundary. It empties when that work ends.
    OpenWork(&'static str),
    /// Grows within one run, at most by the named amount per step, and resets when the next run
    /// starts.
    PerRun(&'static str),
    /// Holds the active window: what lies above the compaction cut. The compaction contract
    /// keeps the window within the model's context.
    Window,
    /// Capped at this many items; the oldest go first.
    Capped(usize),
    /// A struct whose own fields are declared with `state!`, each with its own bound.
    Declared,
}

/// One declared field of a fold state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSpec {
    pub name: &'static str,
    /// The component whose records change this field.
    pub owner: &'static str,
    /// The record that resets it, or "never".
    pub reset: &'static str,
    pub bound: Bound,
}

/// Declares a fold state struct together with its schema, `SCHEMA`: one [`FieldSpec`] per field,
/// in declaration order. Each field is written as
/// `name: Type => { owner: "...", reset: "...", bound: Bound::... },`.
macro_rules! state {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field_vis:vis $field:ident: $ty:ty => {
                    owner: $owner:literal,
                    reset: $reset:literal,
                    bound: $bound:expr $(,)?
                },
            )*
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $(
                $(#[$field_meta])*
                $field_vis $field: $ty,
            )*
        }

        impl $name {
            pub const SCHEMA: &'static [$crate::fold::FieldSpec] = &[
                $(
                    $crate::fold::FieldSpec {
                        name: stringify!($field),
                        owner: $owner,
                        reset: $reset,
                        bound: $bound,
                    },
                )*
            ];
        }
    };
}

pub mod conversation;
pub mod operations;

#[cfg(test)]
mod tests;
