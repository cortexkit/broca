use super::*;
use std::future::Future;

pub(super) enum FloorEvidence {
    Damage,
    Transient(String),
}
impl FloorEvidence {
    pub(super) fn part(error: PartReadError) -> Self {
        match error {
            PartReadError::Damaged(_) => Self::Damage,
            PartReadError::Transient(detail) => Self::Transient(detail),
        }
    }
    pub(super) fn session(error: SessionError) -> Self {
        use broca_session_store::{
            backend::BackendError, directory::DirectoryError, head::HeadError, idindex::IndexError,
            keys::UnwrapError, log::LogError,
        };
        match error {
            SessionError::Backend(BackendError::Io(_) | BackendError::Fenced)
            | SessionError::Index(IndexError::Backend(BackendError::Io(_)))
            | SessionError::Directory(DirectoryError::Backend(BackendError::Io(_)))
            | SessionError::Log(LogError::Io(_))
            | SessionError::Index(IndexError::Backend(BackendError::Fenced))
            | SessionError::Directory(DirectoryError::Backend(BackendError::Fenced))
            | SessionError::Head(
                HeadError::NotLoaded | HeadError::NoKey | HeadError::Unwrap(UnwrapError::NotLoaded),
            )
            | SessionError::Moved
            | SessionError::SessionMoving { .. }
            | SessionError::SessionMoved { .. }
            | SessionError::OwnerGone
            | SessionError::Poisoned
            | SessionError::ChangeOpen
            | SessionError::NeedsRecovery => Self::Transient(format!("{error:?}")),
            SessionError::Missing
            | SessionError::Damaged(_)
            | SessionError::Corrupt { .. }
            | SessionError::CheckpointLost
            | SessionError::Checkpoint(_)
            | SessionError::Fold(_)
            | SessionError::ImportedIdsDamaged(_)
            | SessionError::EnrollmentDamaged(_)
            | SessionError::Backend(BackendError::NoSuchLog | BackendError::Corrupt(_))
            | SessionError::Index(
                IndexError::Missing(_)
                | IndexError::Corrupt(_)
                | IndexError::Backend(BackendError::NoSuchLog | BackendError::Corrupt(_)),
            )
            | SessionError::Directory(
                DirectoryError::Missing(_)
                | DirectoryError::Corrupt(_)
                | DirectoryError::OutOfOrder
                | DirectoryError::Backend(BackendError::NoSuchLog | BackendError::Corrupt(_)),
            )
            | SessionError::Segment(_)
            | SessionError::Head(
                HeadError::Malformed
                | HeadError::Decode(_)
                | HeadError::Authentication
                | HeadError::Unwrap(UnwrapError::Malformed | UnwrapError::Authentication),
            )
            | SessionError::Log(
                LogError::Frame(_)
                | LogError::CopiedSeqs
                | LogError::Undecodable { .. }
                | LogError::CheckpointNotFirst
                | LogError::Checkpoint(_),
            ) => Self::Damage,
            SessionError::Head(HeadError::Format(_))
            | SessionError::Log(LogError::BatchTooLarge(_))
            | SessionError::TripleMismatch
            | SessionError::BadCut(_)
            | SessionError::ViewNotCarried
            | SessionError::EnrollmentNotCarried
            | SessionError::RevertRefused(_)
            | SessionError::Media(_)
            | SessionError::NotInMemory
            | SessionError::NotHeld(_) => Self::Damage,
        }
    }
}

pub(super) async fn retry<T, F: Future<Output = Result<T, FloorEvidence>>>(
    mut read: impl FnMut() -> F,
) -> Result<Option<T>, LoopError> {
    let result = match read().await {
        Err(FloorEvidence::Transient(_)) => read().await,
        result => result,
    };
    match result {
        Ok(value) => Ok(Some(value)),
        Err(FloorEvidence::Damage) => Ok(None),
        Err(FloorEvidence::Transient(detail)) => Err(LoopError::FloorRead(detail)),
    }
}
