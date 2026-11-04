#[test]
fn served_reply_cleaners_agree() {
    let replies = [
        "§32§ I have read the file",
        "§3§ hi as §1§ says",
        "hello §3§",
        "§3§hello",
        "§3",
        "é漢§3§🙂 hi",
        "",
        "§3§",
        "§3§ §4§ §5§ hello",
        "hi §bad§ there",
        "hi §3 there",
        "hi §3§§ there",
        "  hi  ",
        "§5.1 hi",
        "§12.3 hi",
        "§3\"> hi",
        "hi §3) there",
        "hi §3] there",
        "hi §3- there",
        "hi §3_ there",
        "hi §3: there",
        "hi §3a there",
        "hi § there",
        "hi §§ there",
        "§18446744073709551616§ hi",
        "\u{a0}§3§ hi\u{a0}",
        "2024",
        "99 files",
        // Test inputs from cortexkit-compaction-view 0.1.1's five clean_reply vectors.
        "§12§ Done.",
        "§12§ §13§ Dropped §4§ and §5\">§6§ as asked.",
        "See §5.1 of the spec.",
        "  §12§ Done.\n\n",
        "§18446744073709551616§ §7§ Done.",
        // Additional edge cases pinned by the view library's clean module.
        "§103012$ Fixed it",
        "§11865ҩ done",
        "§42 files changed",
        "§42important",
        "mid §11865ҩ text",
        "§12.3 cites the specification; see §123.45 and §5.1.",
        "§123.45 begins this citation",
        "§12.",
        "end §12",
        "§12 before",
        "§42§ hi",
        "before §12§ after",
        "§2030§ §2030§ Run",
        "Looking at §40827§ the result is X",
        "2030  §42§ Hello",
        "Hello §40827\">Result confirmed",
        "§15298\">§15298§ ",
        "2030  2030  2030° Run clippy",
        "99  Actually executing now. Running fmt:",
        "§58§",
        "§42§ ",
        "   ",
        "\u{feff}§3§\u{feff}hi\u{feff}",
        "hi\u{85}",
    ];
    for reply in replies {
        let old = cortexkit_role_llm_runner_v1::tags::exposed_model_text(
            cortexkit_role_llm_runner_v1::tags::TextWriter::TagRenderingRun,
            reply,
        );
        let view = cortexkit_compaction_view::clean::clean_reply(reply).text;
        println!("{reply:?}\t{old:?}\t{view:?}");
        assert_eq!(old, view, "input: {reply:?}");
    }
}
