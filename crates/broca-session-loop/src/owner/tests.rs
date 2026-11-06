use broca_protocol::BindPrincipal;
use broca_session_store::fold::operations::Owner;

use super::ownership;

fn reserved(module_id: &str) -> serde_json::Value {
    serde_json::to_value(BindPrincipal::Reserved {
        module_id: module_id.into(),
    })
    .unwrap()
}

/// The fold keeps only the first two distinct principals, and scope adoption, the only rule that
/// reads them, answers the same from those as from all of them: whichever scope owner is asked
/// about, the first recorded principal other than it is among the first two.
#[test]
fn the_first_two_principals_answer_scope_adoption_as_all_do() {
    let all: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|module_id| BindPrincipal::Reserved {
            module_id: module_id.into(),
        })
        .collect();
    let owner = Owner {
        episodes: 4,
        first_principal: Some(reserved("a")),
        any_unrecorded_principal: false,
        principals: vec![reserved("a"), reserved("b")],
        scope: None,
    };
    let kept = ownership(&owner).unwrap().recorded_principals;
    for asked in all.iter().chain(std::iter::once(&BindPrincipal::Reserved {
        module_id: "e".into(),
    })) {
        assert_eq!(
            kept.iter().find(|recorded| *recorded != asked),
            all.iter().find(|recorded| *recorded != asked),
            "asked about {asked:?}"
        );
    }
}
