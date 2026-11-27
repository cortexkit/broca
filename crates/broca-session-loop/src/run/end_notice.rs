use super::*;

fn note_notice_history<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    floor: Option<u64>,
) -> Option<Arc<RenderHistory>> {
    writer
        .operations()
        .plan
        .as_ref()
        .filter(|plan| plan.has_tool_additions())?;
    let mut history = RenderHistory::from(render_episodes(writer, parts).ok()?);
    history.signed_floor = floor;
    Some(Arc::new(history))
}

pub(super) async fn tell_run_end<W: Writer, P, T>(
    writer: &W,
    parts: &dyn Parts,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    compaction: Option<&crate::compaction::Ctx>,
) {
    let Some(changes) = crate::compaction::Changes::of(admission, seams, None) else {
        return;
    };
    let Ok(floor) = signed_floor(writer, parts, &mut ToolSetCache::default()).await else {
        return;
    };
    changes
        .sharing(compaction)
        .with_floor(floor)
        .tell_with_history(
            writer,
            seams,
            note_notice_history(writer, parts, floor).as_ref(),
        )
        .await;
}
