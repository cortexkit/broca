//! A private broker and credential service that outlive a killed runner.
use super::*;
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use broca_module_serve::wire::SendParamsExt;
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::{ConnectConfig, NatsConnection};
use cortexkit_bus_testkit::{
    trust::{box_root_id, signer_root_id, unix_now},
    BusOptions, BusServer,
};
use nkeys::KeyPair;
use sha2::{Digest, Sha256};

pub(super) struct BusRig {
    server: BusServer,
    pub names: AccountNames,
    user: Arc<KeyPair>,
    jwt: String,
}

fn jwt(signer: &KeyPair, subject: &str, nats: Value) -> String {
    let mut claims =
        json!({"jti":"","iat":unix_now()-120,"iss":signer.public_key(),"sub":subject,"nats":nats});
    claims["jti"] = format!("{:X}", Sha256::digest(claims.to_string())).into();
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"ed25519-nkey","typ":"JWT"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes()).unwrap())
    )
}
impl BusRig {
    pub async fn new(root: &Path) -> Option<Self> {
        let server = match BusServer::start(BusOptions {
            directory: Some(root.join("broker")),
            ..Default::default()
        })
        .await
        {
            Ok(server) => server,
            Err(error) => {
                cortexkit_bus_testkit::skip_or_fail(error);
                return None;
            }
        };
        println!("private {}", server.version);
        let account = KeyPair::new_account();
        std::fs::write(server.dir.join("jwt").join(format!("{}.jwt", account.public_key())), jwt(
            &server.trust.signer.root(&signer_root_id()).pair, &account.public_key(),
            json!({"type":"account","version":2,"signing_keys":[server.trust.box_root_public()],"limits":{"subs":-1,"data":-1,"payload":-1,"imports":-1,"exports":-1,"wildcards":true,"conn":-1,"leaf":-1,"mem_storage":-1,"disk_storage":-1,"streams":-1,"consumer":-1},"default_permissions":{"pub":{},"sub":{}}}),
        )).unwrap();
        let user = Arc::new(KeyPair::new_user());
        let jwt = server.trust.user_jwt(
            &box_root_id(),
            &account.public_key(),
            &user,
            unix_now() - 60,
        );
        let key = user.clone();
        let options = async_nats::ConnectOptions::with_jwt(jwt.clone(), move |nonce| {
            let key = key.clone();
            async move { key.sign(&nonce).map_err(async_nats::AuthError::new) }
        });
        let connection = NatsConnection::connect(
            &server.url,
            options,
            ConnectConfig::new(user.public_key()).unwrap(),
        )
        .await
        .unwrap();
        let names = AccountNames::derive("box_compaction_test").unwrap();
        let stream = async_nats::jetstream::new(connection.client())
            .create_stream(async_nats::jetstream::stream::Config {
                name: names.streams().event.clone(),
                subjects: vec![names.event_binding()],
                ..Default::default()
            })
            .await
            .unwrap();
        let spec = cortexkit_bus_naming::event_consumer_specs(&names)
            .unwrap()
            .into_iter()
            .find(|s| s.consumer.durable == "m_broca")
            .unwrap();
        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(spec.consumer.durable),
                filter_subjects: spec.consumer.filter_subjects,
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: spec.ack_wait,
                max_ack_pending: spec.max_ack_pending,
                ..Default::default()
            })
            .await
            .unwrap();
        Some(Self {
            server,
            names,
            user,
            jwt,
        })
    }
    pub async fn register(self: &Arc<Self>, connection_file: &Path) -> tokio::task::JoinHandle<()> {
        let manifest = subc_protocol::manifest::ModuleManifest::builder("ckbus", "test")
            .provides(vec![
                subc_protocol::manifest::ProviderRole::ManagementSurface {
                    operations: [
                        "ckbus.credential",
                        "ckbus.credential_renew",
                        "ckbus.nonce_sign",
                    ]
                    .iter()
                    .map(|name| subc_protocol::manifest::ManagementOperation {
                        name: name.to_string(),
                        kind: subc_protocol::manifest::ManagementOperationKind::Mutate,
                        description: None,
                    })
                    .collect(),
                    config_schema: json!({}),
                    concurrency: subc_protocol::manifest::Concurrency::ModuleManaged,
                    observability: vec![],
                    identity_scope: vec![],
                },
            ])
            .build();
        let (_, serve) =
            subc_client_rs::serve_with_handle(connection_file, manifest, Credentials(self.clone()))
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = serve.await;
        })
    }
}
struct Credentials(Arc<BusRig>);

fn with_prompt(mut fields: Map<String, Value>, send_id: &str, prompt: &str) -> Map<String, Value> {
    fields.insert("send_id".into(), json!(send_id));
    fields.insert("prompt".into(), json!(prompt));
    fields
}

#[test]
fn bus_fixture_sends_decode_the_public_prompt_field() {
    let fields = with_prompt(
        json!({"model":{"provider":"mock","model":"m"}})
            .as_object()
            .unwrap()
            .clone(),
        "send",
        "original",
    );
    let params: broca_module_serve::wire::SendParams =
        serde_json::from_value(Value::Object(fields)).unwrap();
    let request = params
        .into_send_request(
            broca_protocol::BindIdentity::new("/test", "h", "s"),
            broca_protocol::RunId("run".into()),
        )
        .unwrap();
    assert_eq!(
        request.request.input,
        vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text {
                text: "original".into()
            }]
        )]
    );
}
#[async_trait]
impl subc_client_rs::ModuleHandler for Credentials {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let public = self.0.user.public_key();
        let result = match request["method"].as_str().unwrap_or_default() {
            "ckbus.credential" => {
                json!({"jwt":self.0.jwt,"exp":unix_now()+3600,"acct":self.0.names.account(),"inbox_prefix":ConnectConfig::new(&public).unwrap().inbox_prefix(),"server_url":self.0.server.url,"credential_public":public})
            }
            "ckbus.credential_renew" => {
                json!({"jwt":self.0.jwt,"exp":unix_now()+3600,"credential_public":public})
            }
            "ckbus.nonce_sign" => {
                json!({"signature_b64":STANDARD.encode(self.0.user.sign(&STANDARD.decode(request["params"]["nonce_b64"].as_str().unwrap()).unwrap()).unwrap())})
            }
            method => {
                return subc_client_rs::HandlerOutcome::Error {
                    code: "unknown_method".into(),
                    message: method.into(),
                }
            }
        };
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"result":result})).unwrap(),
        )
    }
}




