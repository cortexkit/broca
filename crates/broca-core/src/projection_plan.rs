//! Semantic planning for materialized WAL projections. Stores load a consistent
//! snapshot and atomically apply the batch only if its expected state still matches.

use crate::ReplayError;

#[derive(Debug)]
pub enum ProjectionPlanError {
    Json(serde_json::Error),
    Replay(ReplayError),
    Int(std::num::TryFromIntError),
    InconsistentSnapshot(&'static str),
}

impl std::fmt::Display for ProjectionPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "json: {e}"),
            Self::Replay(e) => write!(f, "replay: {e}"),
            Self::Int(e) => write!(f, "integer conversion: {e}"),
            Self::InconsistentSnapshot(message) => write!(f, "inconsistent snapshot: {message}"),
        }
    }
}
impl std::error::Error for ProjectionPlanError {}
impl From<serde_json::Error> for ProjectionPlanError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
impl From<ReplayError> for ProjectionPlanError {
    fn from(e: ReplayError) -> Self {
        Self::Replay(e)
    }
}
impl From<std::num::TryFromIntError> for ProjectionPlanError {
    fn from(e: std::num::TryFromIntError) -> Self {
        Self::Int(e)
    }
}
