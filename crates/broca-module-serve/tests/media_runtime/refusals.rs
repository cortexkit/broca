use super::*;

struct NoCompaction;
#[async_trait::async_trait]
impl broca_core::compaction::CompactionPlane for NoCompaction {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::compaction::CompactionUnavailable,
    > {
        panic!("these sessions have no compaction provider")
    }
}

pub(super) fn configure(rig: &Rig, case: &str) {
    if case == "head-mime" {
        *rig.script.omit_media_profile.lock().unwrap() = true;
        let mut config = frozen(WireFamilyId::OpenAiChat);
        config.media = Some(select_media_profile(
            config.family_id,
            Some(broca_provider::media_profile::MediaQuirk::Xai),
            &config.wire_model_id,
            InputModalities::from_value(&json!(["text", "image"])),
            Some(128000),
        ));
        *rig.script.media_frozen.lock().unwrap() = Some(config);
    }
}

async fn begin(rig: &mut Rig, bytes: usize, sha: String) -> String {
    result(
        &call(
            rig,
            "session.upload_begin",
            json!({"mime":"image/png","bytes":bytes,"sha256":sha}),
        )
        .await,
    )["upload_id"]
        .as_str()
        .unwrap()
        .into()
}
async fn append(rig: &mut Rig, id: &str, offset: usize, bytes: &[u8]) -> Frame {
    call(
        rig,
        "session.upload_append",
        json!({"upload_id":id,"offset":offset,"data":STANDARD.encode(bytes)}),
    )
    .await
}

pub(super) async fn run(rig: &mut Rig, enabled: bool, case: &str) -> bool {
    match case {
        "upload-refusals" => upload_refusals(rig).await,
        "strict-append" => {
            let id = begin(rig, PNG.len(), digest(PNG)).await;
            assert_eq!(result(&append(rig, &id, 0, &PNG[..3]).await)["received"], 3);
            let before = rig.wal_bytes();
            let replay = append(rig, &id, 0, &PNG[..3]).await;
            assert_eq!(refusal(&replay)["code"], "upload_offset_mismatch");
            assert_eq!(refusal(&replay)["detail"]["received"], 3);
            assert_eq!(
                std::fs::read(rig.dir.join("media-uploads/objects").join(id)).unwrap(),
                &PNG[..3]
            );
            assert_eq!(rig.wal_bytes(), before);
        }
        "head-mime" => {
            let before = rig.wal_bytes();
            let head = result(&call(rig, "session.head", json!({})).await);
            assert_eq!(
                head["input"]["mime_types"],
                json!(["image/png", "image/jpeg"])
            );
            assert_eq!(
                rig.wal_bytes(),
                before,
                "a head quote wrote or refroze the config"
            );
            assert!(rig
                .records()
                .iter()
                .filter_map(|record| match record {
                    broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
                    _ => None,
                })
                .all(|config| config.frozen_render_config.get("media").is_none()));
        }
        "model-media" | "model-free" | "blocks-identity" => {
            if case != "model-free" {
                let id = upload(rig, "image/png", PERSON_PNG).await;
                rig.script.push(&[Step::Call("photo"), Step::Text("seen")]);
                result(
                    &rig.send_with(
                        "media",
                        "look",
                        None,
                        json!({"content":[{"type":"upload","upload_id":id}]}),
                    )
                    .await,
                );
                rig.wait_idle().await;
            }
            if case == "blocks-identity" {
                let blocks = result(&call(rig, "session.read", json!({"media":"blocks"})).await);
                let mut found = 0;
                for message in blocks["messages"].as_array().unwrap() {
                    if message["message"].to_string().contains(&digest(PERSON_PNG))
                        || message["message"].to_string().contains(&digest(PNG))
                    {
                        found += 1;
                        assert_eq!(
                            message["digest"],
                            cortexkit_role_llm_runner::content_digest::content_digest(
                                &message["message"]
                            )
                        );
                    }
                }
                assert_eq!(found, 2);
                return true;
            }
            rig.set_compaction_plane(Arc::new(NoCompaction));
            let inline = call(
                rig,
                "session.read",
                json!({"view":"model","from_ordinal":0}),
            )
            .await;
            let value = result(&inline);
            if case == "model-media" {
                for (kind, mime, bytes) in [
                    ("image", "image/png", PERSON_PNG),
                    ("image", "image/png", PNG),
                    ("document", "application/pdf", TOOL_PDF),
                ] {
                    let marker = cortexkit_role_llm_runner::attachments::marker(
                        kind,
                        mime,
                        bytes.len() as u64,
                        &digest(bytes),
                        "not_requested",
                    );
                    assert!(value.to_string().contains(&marker), "{value}");
                    assert!(!value.to_string().contains(&digest(bytes)), "{value}");
                }
                let blocks = result(
                    &call(
                        rig,
                        "session.read",
                        json!({"view":"model","media":"blocks","from_ordinal":0}),
                    )
                    .await,
                );
                assert!(blocks.to_string().contains(&digest(PERSON_PNG)));
                assert!(blocks.to_string().contains(&digest(PNG)));
            } else {
                let lineage = value["lineage_id"].as_str().unwrap();
                let bytes = String::from_utf8(inline.body.to_vec())
                    .unwrap()
                    .replace(lineage, "@LINEAGE@");
                assert_eq!(
                    bytes,
                    include_str!("golden/model_read.media_free.json").trim_end()
                );
            }
        }
        "off" => {
            assert!(!enabled);
            let before = rig.wal_bytes();
            let reply = rig
                .send_with(
                    "off-upload",
                    "look",
                    None,
                    json!({"content":[{"type":"upload","upload_id":"missing-upload"}]}),
                )
                .await;
            assert_eq!(
                String::from_utf8(reply.body.to_vec()).unwrap(),
                r#"{"code":"invalid_params","message":"upload missing-upload cannot be sent: attachment admission is unavailable","detail":{"field":"content"}}"#
            );
            assert_eq!(rig.wal_bytes(), before);
            let id = begin(rig, PNG.len(), digest(PNG)).await;
            let first = append(rig, &id, 0, &PNG[..3]).await;
            let replay = append(rig, &id, 0, &PNG[..3]).await;
            assert_eq!(first.body, replay.body);
            assert_eq!(
                String::from_utf8(first.body.to_vec()).unwrap(),
                r#"{"result":{"received":3}}"#
            );
        }
        _ => return false,
    }
    true
}

async fn upload_refusals(rig: &mut Rig) {
    let open = begin(rig, PNG.len(), digest(PNG)).await;
    let gone = begin(rig, PNG.len(), digest(PNG)).await;
    std::fs::remove_file(rig.dir.join("media-uploads/objects").join(&gone)).unwrap();
    let other = rig.bind_session(19, "foreign").await;
    let foreign = broca_core::media_uploads::Uploads::at(&rig.dir)
        .begin(
            &other,
            &cortexkit_role_llm_runner::attachments::UploadBeginRequest::new(
                "image/png",
                PNG.len() as u64,
                digest(PNG),
            ),
            broca_module::person_media::now_seconds(),
        )
        .unwrap()
        .upload_id;
    let mut cases = vec![
        (open, "upload_not_committed"),
        ("missing-upload".into(), "unknown_upload"),
        (foreign, "unknown_upload"),
        (gone, "upload_gone"),
    ];
    for size_bad in [false, true] {
        let id = begin(
            rig,
            PNG.len() + usize::from(size_bad),
            if size_bad {
                digest(PNG)
            } else {
                "0".repeat(64)
            },
        )
        .await;
        result(&append(rig, &id, 0, PNG).await);
        let failed = call(rig, "session.upload_commit", json!({"upload_id":id})).await;
        assert_eq!(
            refusal(&failed)["code"],
            if size_bad {
                "upload_size_mismatch"
            } else {
                "upload_sha256_mismatch"
            }
        );
        cases.push((id, "upload_not_committed"));
    }
    for (index, (id, code)) in cases.into_iter().enumerate() {
        let before = rig.wal_bytes();
        let calls = rig.script.rendered.lock().unwrap().len();
        let reply = rig
            .send_with(
                &format!("refused-{index}"),
                "inspect",
                None,
                json!({"content":[{"type":"upload","upload_id":id}]}),
            )
            .await;
        let error = refusal(&reply);
        assert_eq!(error["code"], code, "{error}");
        assert_eq!(error["detail"]["class"], "permanent", "{error}");
        assert_eq!(rig.wal_bytes(), before);
        assert_eq!(rig.script.rendered.lock().unwrap().len(), calls);
    }
    rig.bind_session(23, "never-written").await;
    let before = std::fs::read_dir(rig.dir.join("wal")).unwrap().count();
    let reply = rig.request(23, json!({"method":"session.send","params":{
        "prompt":"inspect","send_id":"fresh-invalid-upload","model":{"provider":"fake","model":"fake"},
        "content":[{"type":"upload","upload_id":"missing-upload"}]
    }})).await;
    let error = refusal(&reply);
    assert_eq!(error["code"], "unknown_upload");
    assert_eq!(error["detail"]["class"], "permanent");
    assert_eq!(
        std::fs::read_dir(rig.dir.join("wal")).unwrap().count(),
        before
    );
}
