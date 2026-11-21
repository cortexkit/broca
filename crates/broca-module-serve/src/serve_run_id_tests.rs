//! The run ids broca mints for a send, a keyed send and an import.

use super::*;

#[test]
fn minted_run_ids_are_unique_and_carry_the_launch_nonce() {
    let a = mint_run_id("s1");
    let b = mint_run_id("s1");
    // Distinct sends never collide (the process-global counter).
    assert_ne!(
        a.0, b.0,
        "two sends in the same session mint distinct run_ids"
    );
    // The id carries the per-launch nonce, so a restarted process (a new nonce) can
    // never re-mint an id an earlier process used (the cross-restart collision that
    // let the run index clobber an old terminal / cancel target the wrong run).
    let nonce = launch_nonce();
    assert!(
        a.0.contains(nonce) && b.0.contains(nonce),
        "minted ids embed the launch nonce: {} / {}",
        a.0,
        b.0
    );
    assert!(a.0.starts_with("run-s1-"), "id is session-scoped: {}", a.0);
}

#[test]
fn send_id_derived_run_ids_are_deterministic_and_session_scoped() {
    let session_a = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
    let session_b = BrocaBindIdentity::new("/tmp/project-b", "runner", "s1");

    let a1 = run_id_for_send(&session_a, Some("logical-send"));
    let a2 = run_id_for_send(&session_a, Some("logical-send"));
    let b1 = run_id_for_send(&session_b, Some("logical-send"));

    assert_eq!(
        a1.0, a2.0,
        "the same session + send_id reuses the same run_id"
    );
    assert_ne!(
        a1.0, b1.0,
        "the same send_id on another session hashes differently"
    );
    assert!(
        a1.0.starts_with("run-sid-"),
        "derived ids keep the send-id prefix: {}",
        a1.0
    );
    let digest = a1.0.strip_prefix("run-sid-").unwrap();
    assert!(
        digest.len() > 16 && digest.chars().all(|ch| ch.is_ascii_alphanumeric()),
        "send ids use the wider FNV-1a-128 base62 digest: {digest}"
    );
}

#[test]
fn import_ids_are_deterministic_session_scoped_and_separate_from_send_ids() {
    let session_a = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
    let session_b = BrocaBindIdentity::new("/tmp/project-b", "runner", "s1");
    let a1 = run_id_for_import(&session_a, "logical-import");
    let a2 = run_id_for_import(&session_a, "logical-import");
    let b = run_id_for_import(&session_b, "logical-import");
    assert_eq!(a1, a2);
    assert_ne!(a1, b);
    assert_ne!(a1, run_id_for_send(&session_a, Some("logical-import")));
    assert!(a1.0.starts_with("run-iid-"));
}
