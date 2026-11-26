//! Print broca's `serves` value from a catalog copy and a scoped metadata listing.

use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;

use broca_catalog::CatalogCache;
use broca_module_serve::serves::snapshot_from_loaded;
use broca_subc::vault::ScopedList;
use clap::Parser;

#[derive(Parser)]
#[command(
    about = "Print the serves value from catalog and vault metadata files, without opening a route"
)]
struct Args {
    /// Owner-only copy of the bootable Fusiform catalog (catalog/fusiform-lkg.json).
    #[arg(long)]
    catalog: PathBuf,
    /// The raw credential.list_scoped result under broca's grants, without its reply envelope.
    #[arg(long)]
    listing: PathBuf,
}

fn print_serves(args: Args, output: impl Write) -> Result<(), String> {
    let catalog = CatalogCache::open(args.catalog);
    if let Some(error) = catalog.lkg_load_error() {
        return Err(error.to_owned());
    }
    let catalog = catalog.snapshot().map_err(|error| error.to_string())?;

    let file = File::open(&args.listing)
        .map_err(|error| format!("read listing {}: {error}", args.listing.display()))?;
    let listing: ScopedList = serde_json::from_reader(BufReader::new(file)).map_err(|error| {
        // A malformed input should not echo a row or an unexpected field value.
        format!(
            "decode ScopedList {}: {} at line {} column {}",
            args.listing.display(),
            match error.classify() {
                serde_json::error::Category::Io => "read error",
                serde_json::error::Category::Syntax => "invalid JSON",
                serde_json::error::Category::Data => "wrong listing shape",
                serde_json::error::Category::Eof => "incomplete JSON",
            },
            error.line(),
            error.column(),
        )
    })?;
    serde_json::to_writer(output, &snapshot_from_loaded(&catalog, &listing))
        .map_err(|error| format!("write serves value: {error}"))
}

fn main() -> std::process::ExitCode {
    match print_serves(Args::parse(), io::stdout().lock()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("serves-dry-read: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_lkg_and_scoped_listing_to_emit_only_the_wire_value() {
        let dir = broca_test_support::scratch_root().join("serves-dry-read");
        broca_owner_only::create_dir_all(&dir).unwrap();
        let catalog = dir.join("fusiform-lkg.json");
        let listing = dir.join("listing.json");
        let row = |npm, api| {
            json!({
                "existence": "present", "limit.context": 32000,
                "capability.attachment": false, "capability.reasoning": false,
                "capability.tool_call": true,
                "wire.npm": {"value": npm, "from": "fixture"},
                "wire.api": {"value": api, "from": "fixture"},
            })
        };
        let fixture = json!({
            "source": "fixture", "catalog_version": 1, "resolved_at_ms": 42,
            "models": {
                "anthropic/claude-fixture": row("@ai-sdk/anthropic", "https://anthropic.example/v1"),
                "openai/gpt-fixture": row("@ai-sdk/openai", "https://openai.example/v1"),
            },
        });
        CatalogCache::open(&catalog)
            .refresh(serde_json::from_value(fixture).unwrap())
            .unwrap();
        let gateway = |provider, operations| {
            json!({
                "id": "fixture-record", "categories": ["llm-gateway"], "type": "apikey",
                "serves": ["anthropic"], "state": "active", "record_version": 1,
                "auth_method": "gateway", "provider_ids": [provider], "operations": operations,
            })
        };
        let fixture = json!({
            "credentials": [
                gateway("anthropic-gw-fixture", vec!["read"]),
                gateway("anthropic-gw-no-read", vec!["list"]),
            ],
            "grants": 1, "grant_tuples": [], "view": "fixture",
        });
        std::fs::write(&listing, serde_json::to_vec(&fixture).unwrap()).unwrap();

        let args = Args::try_parse_from([
            std::ffi::OsStr::new("serves-dry-read"),
            std::ffi::OsStr::new("--catalog"),
            catalog.as_os_str(),
            std::ffi::OsStr::new("--listing"),
            listing.as_os_str(),
        ])
        .unwrap();
        let mut stdout = Vec::new();
        print_serves(args, &mut stdout).unwrap();
        assert_eq!(
            std::str::from_utf8(&stdout).unwrap(),
            concat!(
                r#"{"generation":"b3f646d18c33a941e7271a54d4ad0d6e92255b3d596841457ec54f24bd40b3d0","harnesses":{"broca":["#,
                r#"{"provider":"anthropic","models":"all"},{"provider":"anthropic-gw-fixture","models":"all"},"#,
                r#"{"provider":"anthropic-platform","models":"all"},{"provider":"openai-platform","models":"all"}],"#,
                r#""broca-uncapped":[{"provider":"openai","models":"all"}]}}"#,
            )
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
