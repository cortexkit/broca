use super::*;

impl Engine {
    pub(super) async fn superseded_alias(
        &self,
        source: &Source,
        records: &[FramedRecord],
        lineage: Option<WalLineageId>,
        digest: &str,
        deployment: &DeploymentKey,
    ) -> Result<Option<String>, Failure> {
        let mut identities: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RunStarted { session, .. } => Some(session.clone()),
                _ => None,
            })
            .collect();
        if let Some(index) = &self.source_index {
            let names: std::collections::BTreeSet<_> = identities
                .iter()
                .map(|identity| identity.session.clone())
                .collect();
            for name in names {
                let found = index.session_refs_named(&name, 101).map_err(io)?;
                if !found.complete || found.sessions.len() >= 101 {
                    return Ok(None);
                }
                identities.extend(found.sessions.into_values().filter_map(
                    |(address, identity)| {
                        (broca_core::session_addr(&identity) == address).then_some(identity)
                    },
                ));
            }
        }
        identities.sort_by(|a, b| {
            (&a.project_root, &a.harness, &a.session).cmp(&(
                &b.project_root,
                &b.harness,
                &b.session,
            ))
        });
        identities.dedup();
        let mut canonical = Vec::new();
        for identity in identities {
            let address = broca_core::session_addr(&identity);
            if Some(address.as_str()) != source.member.as_deref()
                && self
                    .ledger
                    .converted_prefix(&address, digest, &source.class)
                    .map_err(io)?
            {
                canonical.push((address, identity));
            }
        }
        let [(address, identity)] = canonical.as_slice() else {
            return Ok(None);
        };
        let lineage_text = lineage.map(|id| id.to_string());
        let media = crate::store_move::media_for_class(&self.root, class(source)?, records)
            .map_err(|error| io(error.message))?;
        let prepared = broca_session_convert::stage::prepare(
            records,
            lineage_text.as_deref(),
            media.as_ref().map(|media| &media.store as &dyn MediaBytes),
        )
        .map_err(stage_failure)?;
        let destination = class(source)?.store_root(&self.root);
        broca_session_convert::verify::verify_canonical_reads(Verify {
            records,
            lineage,
            session: identity,
            deployment,
            store_root: &destination,
            staging: &destination,
            converted: &prepared.converted,
            mode: Mode::Move,
            media: None,
            media_root: None,
            todays_updated_at: 0,
        })
        .await
        .map_err(|error| {
            Failure::refused(
                "identity_unresolved",
                format!("canonical {address} cannot serve the alias: {error}"),
            )
        })?;
        Ok(Some(address.clone()))
    }
}
