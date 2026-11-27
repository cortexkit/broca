use super::*;
use crate::account_routing::ListingCache;
use crate::config::SessionStorageConfig;
use broca_module::registry::LineageOpener;
use broca_module::store_open::{is_key_unavailable, triple};
use serde_json::{json, Value};

fn grant(listing: &ListingCache, ids: &[u16]) {
    let credentials: Vec<_> = ids
        .iter()
        .map(|id| {
            json!({
                "id": credential_id(*id), "categories": [], "type": "opaque", "serves": [],
                "state": "active", "record_version": 1, "operations": ["read"]
            })
        })
        .collect();
    listing.store(
        serde_json::from_value(json!({
            "credentials": credentials, "grants": ids.len(), "grant_tuples": [], "view": "test"
        }))
        .unwrap(),
    );
}

struct ListedKeys(Arc<ListingCache>);

#[async_trait]
impl DeploymentKeySource for ListedKeys {
    async fn read(&self, id: &str) -> broca_subc::VaultGet {
        let (listing, _) = self.0.get().unwrap();
        let result = [1_u8, 2, 3, 5]
            .into_iter()
            .find(|&key| {
                credential_id(key.into()) == id
                    && listing.credentials.iter().any(|row| row.id == id)
            })
            .map(|key| broca_subc::VaultToken {
                token: format!("{key:02x}").repeat(32),
                expires_at_ms: None,
                record_version: 1,
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
                hosts: None,
                gateway: None,
            })
            .ok_or(broca_subc::VaultError::NotFound);
        broca_subc::VaultGet::replied(result)
    }
}

struct Selection {
    rig: Rig,
    listing: Arc<ListingCache>,
    loading: tokio::task::JoinHandle<()>,
}

impl Selection {
    async fn start(name: &str, config: &str, grants: &[u16]) -> Self {
        let config = SessionStorageConfig::parse(config).unwrap();
        let rig = Rig::with_slot(name, |root| {
            StoreSlot::awaiting_key(root, config.deployment_key_id).creating(config.new_sessions())
        })
        .await;
        let listing = Arc::new(ListingCache::default());
        grant(&listing, grants);
        let loader = rig.loader(Some(Arc::new(ListedKeys(listing.clone()))));
        let loading =
            tokio::spawn(loader.run_following(rig.registry.clone(), listing.clone(), || async {}));
        Self {
            rig,
            listing,
            loading,
        }
    }

    async fn wait_for_keys(&self, ids: &[u16]) {
        let runtime = self.rig.slot.runtime().unwrap();
        broca_test_support::wait_until("the listed deployment keys to load", || async {
            ids.iter()
                .all(|&id| runtime.deployment().has_key(id))
                .then_some(())
                .ok_or_else(|| format!("not all of {ids:?} have loaded"))
        })
        .await;
    }

    async fn create(&self, name: &str, expected: u16) -> BindIdentity {
        let health = health_of(&self.rig.slot);
        assert_eq!(health["key_id"], expected);
        assert_eq!(health["loaded"], true);
        let runtime = self.rig.opener.store_creating().unwrap();
        let bind = BindIdentity::new(self.rig.root.join("project"), "runner", name);
        let root = StorageClass::Ordinary.store_root(&self.rig.root);
        let addr = SessionAddr(broca_core::session_addr(&bind));
        let triple = triple(&bind);
        drop(
            Session::create(
                LocalBackend::new(&root),
                addr.clone(),
                triple.clone(),
                runtime.deployment(),
                false,
            )
            .await
            .unwrap(),
        );
        let pinned = DeploymentKey::new(expected, &[u8::try_from(expected).unwrap(); 32]);
        Session::view(LocalBackend::new(&root), addr, &triple, &pinned)
            .await
            .unwrap();
        bind
    }

    async fn stop(self) {
        self.rig.registry.begin_stop();
        tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, self.loading)
            .await
            .unwrap()
            .unwrap();
        assert!(self.rig.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn absent_id_uses_key_one_when_it_is_the_only_grant() {
    let selection = Selection::start("only-one", "{}", &[1]).await;
    selection.wait_for_keys(&[1]).await;
    selection.create("new", 1).await;
    selection.stop().await;
}

#[tokio::test]
async fn absent_id_uses_the_lowest_readable_key_not_key_one() {
    let selection = Selection::start("lowest-not-one", "{}", &[5, 2]).await;
    selection.wait_for_keys(&[2, 5]).await;
    selection.create("new", 2).await;
    selection.stop().await;
}

#[tokio::test]
async fn absent_id_follows_the_listing_and_existing_sessions_keep_their_key() {
    let logs = broca_test_support::LogCapture::observe(|event| {
        event.target == "session_storage"
            && event.level == tracing::Level::INFO
            && event
                .message()
                .starts_with("new encrypted storage uses deployment key")
    });
    let selection = Selection::start("switch-lowest", "{}", &[2]).await;
    selection.wait_for_keys(&[2]).await;
    let before = selection.create("before", 2).await;
    grant(&selection.listing, &[1, 2]);
    selection.wait_for_keys(&[1]).await;
    let after = selection.create("after", 1).await;
    selection.rig.registry.actor_for(&before).await.unwrap();
    grant(&selection.listing, &[2]);
    broca_test_support::wait_until("the removed grant to stop being selected", || async {
        let key_id = health_of(&selection.rig.slot)["key_id"].clone();
        if key_id == 2 {
            Ok(())
        } else {
            Err(format!("still selecting {key_id}"))
        }
    })
    .await;
    selection.create("after-removal", 2).await;
    selection.rig.registry.actor_for(&after).await.unwrap();
    selection.stop().await;
    let choices: Vec<_> = logs
        .events()
        .into_iter()
        .map(|event| event.fields["key_id"].clone())
        .collect();
    assert_eq!(choices, ["2", "1", "2"]);
}

#[tokio::test]
async fn explicit_id_wins_over_a_lower_readable_key() {
    let config = r#"{"session_storage":{"deployment_key_id":3}}"#;
    let selection = Selection::start("pinned-three", config, &[1]).await;
    selection.wait_for_keys(&[1]).await;
    let Err(error) = selection.rig.opener.store_creating() else {
        panic!("a lower key cannot replace the pinned key while it is missing");
    };
    assert!(is_key_unavailable(&error), "{error:?}");
    let health = health_of(&selection.rig.slot);
    assert_eq!(health["key_id"], 3);
    assert_eq!(health["loaded"], false);
    grant(&selection.listing, &[1, 3]);
    selection.wait_for_keys(&[3]).await;
    selection.create("new", 3).await;
    selection.stop().await;
}

#[tokio::test]
async fn absent_id_without_grants_refuses_creation_until_a_key_becomes_readable() {
    let selection = Selection::start("no-grants", "{}", &[]).await;
    let Err(error) = selection.rig.opener.store_creating() else {
        panic!("creation should require a readable deployment key");
    };
    assert!(is_key_unavailable(&error), "{error:?}");
    let health = health_of(&selection.rig.slot);
    assert_eq!(health["key_id"], Value::Null);
    assert_eq!(health["loaded"], false);
    grant(&selection.listing, &[5]);
    selection.wait_for_keys(&[5]).await;
    selection.create("after", 5).await;
    selection.stop().await;
}
