//! What a `session.read` page with every opt-in costs a session in the new storage, counted at
//! its backend ([`crate::counting`]): a log converted and stored as the run loop stores a
//! session, sealed at every run's input as runs end, then read page by page. A page should read
//! the objects its own messages lie in, whatever the session's length.

use broca_module::serve::SessionReadRequest;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Placement, Record};
use broca_session_store::session::{Session, SessionError};
use broca_wal::FramedRecord;
use futures::executor::block_on;

use crate::counting::{Counting, Reads};

/// A converted log stored in a counting backend.
pub struct Stored {
    pub backend: Counting<MemoryBackend>,
    pub addr: SessionAddr,
    pub triple: SessionTriple,
    pub deployment: DeploymentKey,
    pub parts: MemoryParts,
    /// How many times the session was sealed, each sealing the run before the cut.
    pub seals: usize,
    /// The messages the session holds.
    pub messages: u64,
}

/// The log `old`, whose lineage is `lineage`, converted and stored as session `name`: each run
/// appended in one batch, from its input to the next run's, and the session sealed at each run's
/// input once the next run is in, as a session's runs seal it.
pub fn store(old: &[FramedRecord], lineage: Option<&str>, name: &str) -> Result<Stored, String> {
    stored(old, lineage, name, true)
}

/// [`store`], with every run appended before the first seal, as a converted session is stored:
/// its records then take the seqs the converter numbered them by, from 1 in order, which a
/// seal between runs would move past (its own records take seqs).
pub fn store_then_seal(
    old: &[FramedRecord],
    lineage: Option<&str>,
    name: &str,
) -> Result<Stored, String> {
    stored(old, lineage, name, false)
}

fn stored(
    old: &[FramedRecord],
    lineage: Option<&str>,
    name: &str,
    seal_as_runs_end: bool,
) -> Result<Stored, String> {
    let converted = crate::convert(old, lineage).map_err(|error| format!("convert: {error:?}"))?;
    let backend = Counting::new(MemoryBackend::default());
    let addr = SessionAddr(name.to_owned());
    let triple = SessionTriple {
        project_root: "/read-cost".into(),
        harness: "read-cost".into(),
        session: name.to_owned(),
    };
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let inputs: Vec<usize> = converted
        .records
        .iter()
        .enumerate()
        .filter(|(_, record)| {
            matches!(record, Record::Input(input) if input.placement == Placement::RunInput)
        })
        .map(|(index, _)| index)
        .collect();
    let mut bounds = vec![0];
    bounds.extend(inputs.iter().copied().filter(|index| *index > 0));
    bounds.push(converted.records.len());
    bounds.dedup();
    let (seals, messages) = block_on(async {
        let mut session =
            Session::create(&backend, addr.clone(), triple.clone(), &deployment, false)
                .await
                .map_err(|error| format!("create: {error:?}"))?;
        let mut seals = 0;
        let mut cuts = Vec::new();
        for (index, batch) in bounds.windows(2).enumerate() {
            let first = session
                .append(&converted.records[batch[0]..batch[1]], false)
                .await
                .map_err(|error| format!("append: {error:?}"))?;
            // Converted records that name others by seq count on taking the seqs they were
            // numbered by; a seal between runs moves the later ones past them.
            if !seal_as_runs_end && first != batch[0] as u64 + crate::FIRST_SEQ {
                return Err(format!("converted record {} took seq {first}", batch[0]));
            }
            // The batch starts at a run's input (but the first): everything before it is sealed.
            if index > 0 {
                cuts.push(first);
            }
            if seal_as_runs_end {
                for cut in cuts.drain(..) {
                    match session.seal(Some(cut), 0).await {
                        Ok(_) => seals += 1,
                        Err(SessionError::BadCut(_)) => {}
                        Err(error) => return Err(format!("seal: {error:?}")),
                    }
                }
            }
        }
        for cut in cuts {
            match session.seal(Some(cut), 0).await {
                Ok(_) => seals += 1,
                Err(SessionError::BadCut(_)) => {}
                Err(error) => return Err(format!("seal: {error:?}")),
            }
        }
        let messages = session.conversation.placed;
        session
            .close()
            .await
            .map_err(|error| format!("close: {error:?}"))?;
        Ok::<_, String>((seals, messages))
    })?;
    Ok(Stored {
        backend,
        addr,
        triple,
        deployment,
        parts: MemoryParts {
            parts: converted.parts,
        },
        seals,
        messages,
    })
}

/// What each page of `stored`, `limit` messages long from each ordinal in `from`, read from
/// storage beyond loading the session's view: with every opt-in on when `opted`, else the
/// default read.
pub fn page_reads(
    stored: &Stored,
    from: &[u64],
    limit: u64,
    opted: bool,
) -> Result<Vec<Reads>, String> {
    block_on(async {
        let view = Session::view(
            &stored.backend,
            stored.addr.clone(),
            &stored.triple,
            &stored.deployment,
        )
        .await
        .map_err(|error| format!("view: {error:?}"))?;
        let mut reads = Vec::with_capacity(from.len());
        for from in from {
            let key = broca_protocol::BindIdentity::new(
                "/read-cost",
                "read-cost",
                stored.triple.session.as_str(),
            );
            let mut request = SessionReadRequest::new(key, Some(*from), Some(limit));
            request.include_display_facts = opted;
            request.include_tools = opted;
            request.include_rich_ui = opted;
            stored.backend.take();
            let reply = broca_module::store_read::serve_view(&*view, &stored.parts, &request)
                .await
                .map_err(|error| format!("read from {from}: {error:?}"))?;
            if reply.messages.first().map(|message| message.ordinal) != Some(*from) {
                return Err(format!("the page from {from} starts elsewhere"));
            }
            reads.push(stored.backend.take());
        }
        Ok(reads)
    })
}

/// Every object the session stores, with its bytes.
pub fn stored_objects(stored: &Stored) -> (usize, u64) {
    let names = stored.backend.inner.object_names(&stored.addr);
    let bytes = block_on(async {
        let mut bytes = 0;
        for name in &names {
            use broca_session_store::backend::SessionBackend;
            bytes += stored
                .backend
                .inner
                .get_object(&stored.addr, name)
                .await
                .ok()
                .flatten()
                .map_or(0, |object| object.len() as u64);
        }
        bytes
    });
    (names.len(), bytes)
}
