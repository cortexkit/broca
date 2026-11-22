//! The run loop's entry point, and how its errors convert.

use broca_core::provider::Provider;
use broca_core::tool_plane::ToolPlane;
use broca_session_store::session::SessionError;

use super::{Admission, Finished, LoopError, Seams};
use crate::parts::Parts;
use crate::writer::Writer;

impl From<SessionError> for LoopError {
    fn from(error: SessionError) -> Self {
        LoopError::Session(error)
    }
}

/// Runs `admission` to its end on the session behind `writer`.
///
/// The loop is one large future, and it is boxed here, so a caller that awaits it in place keeps
/// only a pointer on its stack. A test awaiting it directly in its own task otherwise holds the
/// whole state machine there, which overflowed Windows' smaller test thread stack.
pub async fn run<W, P, T>(
    writer: &mut W,
    parts: &mut dyn Parts,
    admission: Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    Box::pin(super::run_loop(writer, parts, admission, seams)).await
}
