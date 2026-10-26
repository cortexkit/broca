//! Shared checks for render goldens and their renderer-version pins.

use std::fs;

fn stored_bytes(golden: &[u8]) -> &[u8] {
    let mut trimmed = golden;
    while trimmed.last() == Some(&b'\n') {
        trimmed = &trimmed[..trimmed.len() - 1];
    }
    trimmed
}

fn parse_version(pin: &str, name: &str) -> u32 {
    pin.trim().parse().unwrap_or_else(|error| {
        panic!("{name} golden has an invalid renderer version pin: {error}")
    })
}

/// Check the version rule independently from the byte comparison.
///
/// A changed render is only an accepted transition when the code's renderer version is newer
/// than the version recorded beside the stored bytes. Keeping this check separate lets the
/// regression test prove both sides of the rule without mutating a committed fixture.
pub fn validate_version_for_change(
    rendered: &[u8],
    stored_golden: &[u8],
    stored_version: &str,
    current_version: u32,
    name: &str,
) -> Result<(), String> {
    if rendered == stored_bytes(stored_golden) {
        return Ok(());
    }

    let recorded_version = parse_version(stored_version, name);
    if current_version > recorded_version {
        Ok(())
    } else {
        Err(format!(
            concat!(
                "{} render bytes changed, but renderer version {} is not greater ",
                "than the golden pin {}; a render change requires a renderer version bump",
            ),
            name, current_version, recorded_version,
        ))
    }
}

/// Assert the golden bytes and require a version bump before reporting a changed render.
pub fn assert_matches_or_requires_bump(
    rendered: &[u8],
    stored_golden: &[u8],
    stored_version: &str,
    current_version: u32,
    name: &str,
) {
    if let Err(message) = validate_version_for_change(
        rendered,
        stored_golden,
        stored_version,
        current_version,
        name,
    ) {
        panic!("{message}");
    }

    assert_eq!(
        rendered,
        stored_bytes(stored_golden),
        "{name} render bytes changed; regenerate the golden and its renderer-version pin"
    );
}

/// Regenerate a golden and its sibling version pin when explicitly requested.
pub fn update_if_requested(
    golden_path: &str,
    version_path: &str,
    rendered: &[u8],
    stored_golden: &[u8],
    stored_version: &str,
    current_version: u32,
    name: &str,
) -> bool {
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_none() {
        return false;
    }

    if let Err(message) = validate_version_for_change(
        rendered,
        stored_golden,
        stored_version,
        current_version,
        name,
    ) {
        panic!("{message}");
    }

    fs::write(golden_path, rendered).expect("write render golden");
    fs::write(version_path, format!("{current_version}\n")).expect("write render golden version");
    true
}
/// Exercise the explicit no-bundle path against the same bytes the golden test pins.
pub(crate) fn render_without_bundle(
    family: &dyn broca_provider::wire::WireFamily,
    frozen: &broca_provider::wire::FrozenRenderConfig,
    opts: &broca_protocol::CallOptions,
) -> Vec<u8> {
    let original = family.render(frozen, opts);
    #[cfg(feature = "live")]
    {
        let mut frozen = frozen.clone();
        for with_profile in [false, true] {
            if with_profile {
                frozen.media = Some(broca_provider::media_profile::select_media_profile(
                    frozen.family_id,
                    Some(broca_provider::media_profile::MediaQuirk::Anthropic),
                    &frozen.wire_model_id,
                    broca_provider::spec::InputModalities::default(),
                    None,
                ));
            }
            let bytes = broca_provider::media_request::render_prepared(
                family,
                &frozen,
                opts,
                None,
                &|_| panic!("no bundle must not project the prompt"),
            )
            .unwrap();
            assert_eq!(bytes, original, "no-bundle {:?} drifted", frozen.family_id);
        }
    }
    original
}
