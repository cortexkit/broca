use std::io;

#[cfg(windows)]
fn create_scratch(lane: &str) -> io::Result<std::path::PathBuf> {
    use std::time::{SystemTime, UNIX_EPOCH};

    if lane.is_empty()
        || lane.len() > 64
        || !lane
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid lane name",
        ));
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    // Use a fresh native-temp leaf, not a possibly foreign ACL left in a shared
    // user profile. The crate installs an owner/SYSTEM access-control list at creation.
    let directory = std::env::temp_dir().join(format!(
        "broca-prepush-{lane}-{}-{nonce}",
        std::process::id()
    ));
    broca_owner_only::create_dir(&directory)?;
    broca_owner_only::validate_inheritable_directory(&directory)?;
    Ok(directory)
}

#[cfg(windows)]
fn main() -> io::Result<()> {
    let lane = std::env::args().nth(1).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "usage: prepush_scratch LANE")
    })?;
    println!("{}", create_scratch(&lane)?.display());
    Ok(())
}

#[cfg(not(windows))]
fn main() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "prepush_scratch requires Windows",
    ))
}

#[cfg(all(test, windows))]
mod tests {
    #[test]
    fn scratch_has_native_owner_only_inheritable_acl() {
        let directory = super::create_scratch("acl-test").unwrap();
        broca_owner_only::validate_inheritable_directory(&directory).unwrap();
        let file = directory.join("inherited.txt");
        std::fs::write(&file, b"fixture").unwrap();
        broca_owner_only::validate(&file).unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn lane_cannot_escape_native_temp() {
        let error = super::create_scratch("../escape").unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}
