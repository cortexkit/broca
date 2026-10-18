use sha2::{Digest, Sha256};
use std::ffi::OsString;
#[cfg(any(not(unix), all(test, target_os = "linux")))]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
static PRUNE_ONCE: OnceLock<()> = OnceLock::new();
const UNUSED_COPY_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// Run a build through a reusable, non-production executable name.
///
/// Process sentinels and operating-system privacy settings treat a production `ck-*` name as
/// the installed principal. Fresh hard links can also be killed at startup on macOS under
/// suite concurrency. Instead, copy each build once to `<exec root>/ckdev-exec/<sha256-prefix>/`
/// and publish it by atomic rename, so concurrent rigs never see an incomplete executable.
/// See [`exec_root`] for where that is on each platform. `scratch_dir` is retained for caller
/// compatibility.
///
/// Copies unused for 24 hours may be pruned. A path handed out more than 24 hours before its
/// launch, without an intervening `ckdev_binary` call, can vanish. A stale scan can also race
/// reuse between its mtime read and rename; call `ckdev_binary` again to recreate a missing copy.
pub fn ckdev_binary(binary: impl AsRef<Path>, _scratch_dir: impl AsRef<Path>) -> PathBuf {
    publish_binaries(&[(binary.as_ref(), dev_file_name(binary.as_ref()))]).remove(0)
}

/// Publish two related binaries in one content-addressed directory with dev-prefixed names.
///
/// The vault rig uses this for `ck-claustrum` and `ck-auth`, so both executable identities are
/// development-only while their paths still identify the same build pair. Both contents enter
/// the directory digest, so rebuilding either member cannot overwrite an older pair.
pub fn ckdev_binary_pair(
    first: impl AsRef<Path>,
    second: impl AsRef<Path>,
    _scratch_dir: impl AsRef<Path>,
) -> (PathBuf, PathBuf) {
    let first = first.as_ref();
    let second = second.as_ref();
    let mut paths = publish_binaries(&[
        (first, dev_file_name(first)),
        (second, dev_file_name(second)),
    ]);
    let second = paths.pop().unwrap();
    (paths.pop().unwrap(), second)
}

/// Publish a dev-named daemon beside an unchanged, non-`ck-` companion executable.
///
/// Some subc rigs find the `ck` client next to the daemon executable. Keeping both files in one
/// directory preserves that lookup while ensuring the daemon itself runs as `ckdev-subc`.
/// Hashing both files prevents a new companion from changing an already published build.
pub fn ckdev_binary_with_companion(
    binary: impl AsRef<Path>,
    companion: impl AsRef<Path>,
    _scratch_dir: impl AsRef<Path>,
) -> (PathBuf, PathBuf) {
    let binary = binary.as_ref();
    let companion = companion.as_ref();
    let companion_name = companion
        .file_name()
        .expect("companion binary path must have a file name")
        .to_os_string();
    let mut paths =
        publish_binaries(&[(binary, dev_file_name(binary)), (companion, companion_name)]);
    let companion = paths.pop().unwrap();
    (paths.pop().unwrap(), companion)
}

/// Build a process command that executes the atomically published dev-named copy of `binary`.
pub fn ckdev_command(binary: impl AsRef<Path>, scratch_dir: impl AsRef<Path>) -> Command {
    Command::new(ckdev_binary(binary, scratch_dir))
}

/// A forked child holds the inherited descriptor only until it calls exec, but on a loaded
/// machine that can take far longer than a few milliseconds, so allow about two seconds.
const EXECUTABLE_FILE_BUSY_RETRIES: usize = 40;
const EXECUTABLE_FILE_BUSY_BACKOFF: Duration = Duration::from_millis(50);

/// Retry a process launch when another process still has the published executable open for writing.
///
/// A fork can inherit a writable descriptor before the publisher closes its own copy. Linux then
/// refuses to execute that inode until every inherited descriptor is closed.
pub fn retry_executable_file_busy<T>(mut launch: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut retries = 0;
    loop {
        match launch() {
            Err(error)
                if error.kind() == io::ErrorKind::ExecutableFileBusy
                    && retries < EXECUTABLE_FILE_BUSY_RETRIES =>
            {
                retries += 1;
                std::thread::sleep(EXECUTABLE_FILE_BUSY_BACKOFF);
            }
            result => return result,
        }
    }
}

struct Source {
    file: File,
    size: u64,
    digest: [u8; 32],
}

impl Source {
    fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut hash = Sha256::new();
        let mut size = 0;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            size += read as u64;
        }
        // Keep the same open file for hashing and copying: Cargo may replace the source path
        // with a new build while another rig is still publishing the old build.
        file.rewind()?;
        Ok(Self {
            file,
            size,
            digest: hash.finalize().into(),
        })
    }
}

/// Where published copies live.
///
/// On macOS copies go under the per-task temporary directory when `TMPDIR` is set and is not
/// the system's per-user `/var/folders` directory, or under `/tmp` otherwise. `codesign` can
/// hang in `/var/folders`, and macOS may hold a fresh executable there for 30 s or more, so an
/// ordinary shell's TMPDIR is not used. Both locations are on the internal disk, unlike build folders that may
/// live on removable volumes. A rig launches these copies as their own responsible process (the
/// daemon spawns the module), so a copy executed from a removable volume asks the user for access
/// once per build, and while that prompt is open every process's listing of the volume hangs.
/// Elsewhere copies stay beside the build, in its profile directory.
fn exec_root(_binary: &Path) -> PathBuf {
    #[cfg(test)]
    if let Some(root) = TEST_EXEC_ROOT.with(|root| root.borrow().clone()) {
        return root;
    }
    #[cfg(target_os = "macos")]
    {
        macos_exec_root(std::env::var_os("TMPDIR"))
    }
    #[cfg(not(target_os = "macos"))]
    {
        profile_directory(_binary)
    }
}

#[cfg(target_os = "macos")]
fn macos_exec_root(tmpdir: Option<OsString>) -> PathBuf {
    tmpdir
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .filter(|path| {
            !path.starts_with("/var/folders") && !path.starts_with("/private/var/folders")
        })
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("broca-exec")
}

#[cfg(any(not(target_os = "macos"), test))]
fn profile_directory(binary: &Path) -> PathBuf {
    let source = fs::canonicalize(binary).unwrap_or_else(|error| {
        panic!("resolving test binary {} failed: {error}", binary.display())
    });
    let parent = source
        .parent()
        .expect("binary must have a parent directory");
    // Cargo's test executables live in <profile>/deps, including cross-target builds and
    // custom CARGO_TARGET_DIRs. A staged fixture simply uses its own parent directory.
    if parent.file_name().is_some_and(|name| name == "deps") {
        parent.parent().expect("deps must have a parent").to_owned()
    } else {
        parent.to_owned()
    }
}

fn publish_binaries(binaries: &[(&Path, OsString)]) -> Vec<PathBuf> {
    publish_binaries_with_pruning(binaries, &PRUNE_ONCE)
}

fn publish_binaries_with_pruning(
    binaries: &[(&Path, OsString)],
    prune_once: &OnceLock<()>,
) -> Vec<PathBuf> {
    let mut sources: Vec<_> = binaries
        .iter()
        .map(|(path, _)| {
            Source::open(path).unwrap_or_else(|error| {
                panic!("hashing test binary {} failed: {error}", path.display())
            })
        })
        .collect();
    let digest = if sources.len() == 1 {
        sources[0].digest
    } else {
        let mut hash = Sha256::new();
        for source in &sources {
            hash.update(source.digest);
        }
        hash.finalize().into()
    };
    #[cfg(target_os = "macos")]
    let digest: [u8; 32] = {
        // Give the stripped-copy format a new identity so old unstripped files cannot be reused.
        let mut hash = Sha256::new();
        hash.update(digest);
        hash.update(b"broca-test-support-macos-strip-s-v1");
        hash.finalize().into()
    };
    let prefix: String = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let cache = exec_root(binaries[0].0).join("ckdev-exec");
    let directory = cache.join(prefix);
    prune_once.get_or_init(|| {
        prune_copies(&cache, &directory, SystemTime::now());
    });
    // A pruner in another process may have inspected the old mtime before our reuse.
    // Recreate the entire group if its directory disappears, including during a copy.
    for attempt in 0..4 {
        let result = (|| -> io::Result<Vec<PathBuf>> {
            fs::create_dir_all(&directory)?;
            let paths = binaries
                .iter()
                .zip(&mut sources)
                .map(|((_, name), source)| {
                    source.file.rewind()?;
                    publish_copy(source, &directory, name)
                })
                .collect::<io::Result<Vec<_>>>()?;
            touch_directory(&directory, SystemTime::now())?;
            if paths.iter().any(|path| !path.is_file()) {
                return Err(io::ErrorKind::NotFound.into());
            }
            Ok(paths)
        })();
        match result {
            Ok(paths) => return paths,
            Err(error) if error.kind() == io::ErrorKind::NotFound && attempt < 3 => continue,
            Err(error) => panic!(
                "publishing test executables in {} failed: {error}",
                directory.display()
            ),
        }
    }
    unreachable!()
}

fn touch_directory(directory: &Path, now: SystemTime) -> io::Result<()> {
    let mut options = File::options();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // Opening a directory for SetFileTime requires backup semantics and attribute access.
        options.custom_flags(0x02000000).access_mode(0x100);
    }
    options
        .open(directory)?
        .set_times(fs::FileTimes::new().set_modified(now))
}

fn prune_copies(cache: &Path, current: &Path, now: SystemTime) {
    let entries = match fs::read_dir(cache) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            eprintln!(
                "pruning test executable cache {} failed: {error}",
                cache.display()
            );
            return;
        }
    };
    for entry in entries {
        let result = (|| -> io::Result<()> {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            // Only direct copy directories and tombstones from interrupted pruning belong to
            // this cache. Do not follow directory symlinks or remove unrelated files.
            if path == current
                || !name.to_str().is_some_and(is_copy_directory)
                || !entry.file_type()?.is_dir()
            {
                return Ok(());
            }
            let modified = entry.metadata()?.modified()?;
            if now.duration_since(modified).unwrap_or_default() < UNUSED_COPY_AGE {
                return Ok(());
            }
            #[cfg(test)]
            BEFORE_PRUNE.with(|hook| {
                if let Some(hook) = hook.borrow().as_ref() {
                    hook(&path);
                }
            });
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let tombstone = cache.join(format!(".prune-{}-{sequence}", std::process::id()));
            fs::rename(&path, &tombstone)?;
            fs::remove_dir_all(tombstone)
        })();
        if let Err(error) = result {
            eprintln!(
                "pruning test executable cache {} failed: {error}",
                cache.display()
            );
        }
    }
}

fn is_copy_directory(name: &str) -> bool {
    (name.len() == 16 && name.bytes().all(|byte| byte.is_ascii_hexdigit()))
        || name.strip_prefix(".prune-").is_some_and(|suffix| {
            suffix.split_once('-').is_some_and(|(pid, sequence)| {
                !pid.is_empty()
                    && !sequence.is_empty()
                    && pid.bytes().all(|byte| byte.is_ascii_digit())
                    && sequence.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
}

fn dev_file_name(binary: &Path) -> OsString {
    let file_name = binary
        .file_name()
        .expect("binary path must have a file name")
        .to_string_lossy();
    let (stem, extension) = match file_name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => (stem, Some(extension)),
        _ => (file_name.as_ref(), None),
    };
    let stem = stem
        .strip_prefix("ckdev-")
        .or_else(|| stem.strip_prefix("ck-"))
        .unwrap_or(stem);
    let mut name = OsString::from(format!("ckdev-{stem}"));
    if let Some(extension) = extension {
        name.push(".");
        name.push(extension);
    }
    name
}

struct TemporaryCopy {
    path: PathBuf,
    #[cfg(not(unix))]
    file: Option<File>,
    #[cfg(unix)]
    directory: PathBuf,
}

impl Drop for TemporaryCopy {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        #[cfg(unix)]
        let _ = fs::remove_dir(&self.directory);
    }
}

fn unique_temp(directory: &Path) -> io::Result<TemporaryCopy> {
    loop {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(".ckdev-copy-{}-{sequence}.tmp", std::process::id()));
        // Reserve a per-copy directory without opening an executable for writing. A child
        // forked by another test thread could inherit a writer and block the later exec.
        #[cfg(unix)]
        match fs::create_dir(&path) {
            Ok(()) => {
                return Ok(TemporaryCopy {
                    path: path.join("executable"),
                    directory: path,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
        #[cfg(not(unix))]
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => {
                return Ok(TemporaryCopy {
                    path,
                    file: Some(file),
                })
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

fn has_right_size(path: &Path, size: u64) -> bool {
    fs::metadata(path).is_ok_and(|metadata| metadata.is_file() && metadata.len() == size)
}

fn fill_copy(source: &mut Source, temporary: &TemporaryCopy, destination: &Path) -> io::Result<()> {
    #[cfg(test)]
    BEFORE_COPY.with(|hook| {
        if let Some(hook) = hook.borrow().as_ref() {
            hook(destination);
        }
    });
    #[cfg(not(test))]
    let _ = destination;
    #[cfg(unix)]
    {
        // Copying opens the executable for writing only in cp, which exits before the final rename.
        // A child forked by another test thread therefore cannot inherit a writer that
        // would block exec. Read the already-hashed inode even if Cargo replaces its path.
        let output = Command::new("/bin/cp")
            .args(["-p", "/dev/fd/0"])
            .arg(&temporary.path)
            .stdin(std::process::Stdio::from(source.file.try_clone()?))
            .output()?;
        if !output.status.success() {
            if !temporary.directory.is_dir() {
                return Err(io::ErrorKind::NotFound.into());
            }
            return Err(io::Error::other(format!(
                "copying test executable to {} failed ({}): {}",
                temporary.path.display(),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        // macOS reports read-only permissions for /dev/fd/0, not the source's real mode.
        // Keep owner write access so fixtures can set timestamps or simulate truncation.
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = source.file.metadata()?.permissions();
        permissions.set_mode((permissions.mode() & 0o777) | 0o711);
        fs::set_permissions(&temporary.path, permissions)?;
        #[cfg(test)]
        AFTER_COPY.with(|hook| {
            if let Some(hook) = hook.borrow().as_ref() {
                hook(&temporary.path);
            }
        });
    }
    #[cfg(not(unix))]
    {
        let mut file = temporary.file.as_ref().expect("temporary file is open");
        io::copy(&mut source.file, &mut file)?;
        file.sync_all()?;
    }
    #[cfg(all(test, not(unix)))]
    AFTER_COPY.with(|hook| {
        if let Some(hook) = hook.borrow().as_ref() {
            hook(&temporary.path);
        }
    });
    // Signing changes Mach-O bytes, so validate the copied build before signing it.
    let copied = Source::open(&temporary.path)?;
    if copied.size != source.size || copied.digest != source.digest {
        return Err(io::Error::other(
            "test binary copy differs from hashed source",
        ));
    }
    Ok(())
}

/// Sign the copy where it will run, before it is published, so the executable a rig launches
/// carries a signature made on the internal disk. Ad-hoc; skipped for files that are not Mach-O
/// executables, as unit-test fixtures are.
#[cfg(target_os = "macos")]
fn is_mach_o(path: &Path) -> io::Result<bool> {
    let mut magic = [0u8; 4];
    let mut file = File::open(path)?;
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(matches!(
            u32::from_le_bytes(magic),
            0xfeed_facf | 0xfeed_face | 0xcafe_babe | 0xbeba_feca
        )),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

/// Remove Mach-O debug maps before signing so symbolication cannot open build-tree object files.
#[cfg(target_os = "macos")]
fn strip_debug_map(path: &Path) -> io::Result<()> {
    if !is_mach_o(path)? {
        return Ok(());
    }
    let output = Command::new("/usr/bin/strip")
        .arg("-S")
        .arg(path)
        .output()
        .map_err(|error| {
            io::Error::other(format!(
                "/usr/bin/strip -S {} failed to run: {error}",
                path.display()
            ))
        })?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "/usr/bin/strip -S {} failed ({}): {}",
            path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(target_os = "macos")]
fn sign_copy(path: &Path) -> io::Result<()> {
    if !is_mach_o(path)? {
        return Ok(());
    }
    let output = Command::new("/usr/bin/codesign")
        .args(["--force", "--sign", "-"])
        .arg(path)
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "codesign {} failed: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

/// The size the published copy should have. Signing changes a copy's size, so a publisher
/// records the published length beside it; without that record the copy must match the source.
fn published_size(directory: &Path, name: &OsString, source_size: u64) -> u64 {
    fs::read_to_string(size_record(directory, name))
        .ok()
        .and_then(|recorded| recorded.trim().parse().ok())
        .unwrap_or(source_size)
}

fn size_record(directory: &Path, name: &OsString) -> PathBuf {
    let mut record = OsString::from(".");
    record.push(name);
    record.push(".size");
    directory.join(record)
}

fn publish_copy(source: &mut Source, directory: &Path, name: &OsString) -> io::Result<PathBuf> {
    let destination = directory.join(name);
    if has_right_size(&destination, published_size(directory, name, source.size)) {
        return Ok(destination);
    }
    let temporary = unique_temp(directory)?;
    fill_copy(source, &temporary, &destination)?;
    #[cfg(target_os = "macos")]
    strip_debug_map(&temporary.path)?;
    #[cfg(target_os = "macos")]
    sign_copy(&temporary.path)?;
    let size = fs::metadata(&temporary.path)?.len();
    #[cfg(not(unix))]
    let temporary = {
        let mut temporary = temporary;
        drop(temporary.file.take());
        temporary
    };
    if let Err(error) = fs::rename(&temporary.path, &destination) {
        // Windows may refuse to replace an executable opened by a racing rig. A complete
        // copy already published by another writer is just as usable as our own copy.
        if !has_right_size(&destination, size) {
            return Err(error);
        }
    }
    // Every publisher of this content writes the same length: the directory is named by the
    // content, and ad-hoc signing the same bytes gives the same size.
    fs::write(size_record(directory, name), size.to_string())?;
    Ok(destination)
}

// Hooks run before copying and after the child copy exits, before the final rename makes the
// executable visible to rigs. Thread-local hooks coordinate publishers without affecting other tests.
#[cfg(test)]
type BeforeCopy = Option<Box<dyn Fn(&Path)>>;
#[cfg(test)]
thread_local! {
    static BEFORE_COPY: std::cell::RefCell<BeforeCopy> = const { std::cell::RefCell::new(None) };
    static AFTER_COPY: std::cell::RefCell<BeforeCopy> = const { std::cell::RefCell::new(None) };
    static BEFORE_PRUNE: std::cell::RefCell<BeforeCopy> = const { std::cell::RefCell::new(None) };
    /// A unit test's own publish root, so its copies start fresh in its scratch directory
    /// rather than meeting another run's copies in the shared `/tmp` root.
    static TEST_EXEC_ROOT: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    /// A fresh scratch directory, which is also this thread's publish root.
    fn fixture(label: &str) -> PathBuf {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let root = crate::scratch_root().join(format!("ckdev-{label}-{sequence}"));
        fs::create_dir_all(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        publish_into(&root);
        root
    }

    fn publish_into(root: &Path) {
        TEST_EXEC_ROOT.with(|slot| *slot.borrow_mut() = Some(root.join("exec")));
    }

    fn source(root: &Path, relative: &str, bytes: &[u8]) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        path
    }

    fn publish_pruning_once(built: &Path, once: &OnceLock<()>) -> PathBuf {
        publish_binaries_with_pruning(&[(built, dev_file_name(built))], once).remove(0)
    }

    fn age_directory(path: &Path) {
        touch_directory(
            path,
            SystemTime::now() - UNUSED_COPY_AGE - Duration::from_secs(60),
        )
        .unwrap();
    }

    #[test]
    fn pruning_removes_old_unused_copies() {
        let scratch = fixture("prune-old");
        let built = source(&scratch, "target/debug/ck-probe", b"current");
        let cache = exec_root(&built).join("ckdev-exec");
        let old = cache.join("0000000000000000");
        let recent = cache.join("1111111111111111");
        let unrelated = cache.join("not-a-copy");
        let outside = cache.parent().unwrap().join("2222222222222222");
        for path in [&old, &recent, &unrelated, &outside] {
            fs::create_dir_all(path).unwrap();
            fs::write(path.join("executable"), b"keep contents outside old copy").unwrap();
        }
        for path in [&old, &unrelated, &outside] {
            age_directory(path);
        }
        let published = publish_pruning_once(&built, &OnceLock::new());
        assert!(!old.exists(), "unused copy older than 24 hours survived");
        for path in [&recent, &unrelated, &outside] {
            assert!(
                path.join("executable").is_file(),
                "removed {}",
                path.display()
            );
        }
        assert!(published.is_file());
        assert!(fs::read_dir(&cache).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".prune-")
        }));
    }

    #[test]
    fn pruning_keeps_recently_reused_copies() {
        let scratch = fixture("prune-reuse");
        let built = source(&scratch, "target/debug/ck-probe", b"reused");
        let once = OnceLock::new();
        let published = publish_pruning_once(&built, &once);
        let directory = published.parent().unwrap();
        age_directory(directory);
        let old_mtime = fs::metadata(directory).unwrap().modified().unwrap();
        assert_eq!(publish_pruning_once(&built, &once), published);
        let refreshed = fs::metadata(directory).unwrap().modified().unwrap();
        assert!(refreshed > old_mtime, "reuse did not refresh the directory");
        let other = source(&scratch, "target/debug/ck-other", b"other build");
        publish_pruning_once(&other, &OnceLock::new());
        assert!(published.is_file(), "recently reused copy was pruned");
    }

    #[test]
    fn pruning_retries_old_tombstones() {
        let scratch = fixture("prune-tombstone");
        let built = source(&scratch, "target/debug/ck-probe", b"current");
        let old = exec_root(&built).join("ckdev-exec/.prune-123-0");
        fs::create_dir_all(&old).unwrap();
        fs::write(old.join("executable"), b"left by a failed deletion").unwrap();
        age_directory(&old);
        publish_pruning_once(&built, &OnceLock::new());
        assert!(
            !old.exists(),
            "unused copy older than 24 hours survived: failed deletion was never retried"
        );
    }

    #[test]
    fn pruning_keeps_the_copy_being_published() {
        let scratch = fixture("prune-current");
        let built = source(&scratch, "target/debug/ck-probe", b"current");
        let published = publish_pruning_once(&built, &OnceLock::new());
        age_directory(published.parent().unwrap());
        let copied = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = copied.clone();
        BEFORE_COPY.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| {
                observed.store(true, Ordering::Relaxed);
            }));
        });
        let reused = publish_pruning_once(&built, &OnceLock::new());
        BEFORE_COPY.with(|hook| *hook.borrow_mut() = None);
        assert_eq!(reused, published);
        assert!(
            !copied.load(Ordering::Relaxed),
            "current copy was pruned and recopied"
        );
        assert_eq!(fs::read(reused).unwrap(), b"current");
    }

    #[test]
    fn pruning_runs_at_most_once() {
        let scratch = fixture("prune-once");
        let built = source(&scratch, "target/debug/ck-probe", b"current");
        let once = OnceLock::new();
        let published = publish_pruning_once(&built, &once);
        let old = published
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("0000000000000000");
        fs::create_dir(&old).unwrap();
        age_directory(&old);
        publish_pruning_once(&built, &once);
        assert!(old.is_dir(), "a second publish pruned again");
        publish_pruning_once(&built, &OnceLock::new());
        assert!(
            !old.exists(),
            "unused copy older than 24 hours survived: fresh prune control did not remove the old copy"
        );
    }

    #[cfg(unix)]
    #[test]
    fn pruning_does_not_follow_directory_symlinks() {
        let scratch = fixture("prune-symlink");
        let built = source(&scratch, "target/debug/ck-probe", b"current");
        let cache = exec_root(&built).join("ckdev-exec");
        fs::create_dir_all(&cache).unwrap();
        let outside = scratch.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("keep"), b"outside cache").unwrap();
        age_directory(&outside);
        let link = cache.join("0000000000000000");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        publish_pruning_once(&built, &OnceLock::new());
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
        assert_eq!(fs::read(outside.join("keep")).unwrap(), b"outside cache");
    }

    #[test]
    fn pruning_failures_do_not_prevent_publication() {
        let scratch = fixture("prune-failure");
        let built = source(&scratch, "target/debug/ck-probe", b"current");
        let cache = exec_root(&built).join("ckdev-exec");
        let old = cache.join("0000000000000000");
        fs::create_dir_all(&old).unwrap();
        age_directory(&old);
        BEFORE_PRUNE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(|path| {
                // Another pruner wins the rename, so our rename must fail harmlessly.
                fs::remove_dir(path).unwrap();
            }));
        });
        let published = publish_pruning_once(&built, &OnceLock::new());
        BEFORE_PRUNE.with(|hook| *hook.borrow_mut() = None);
        assert_eq!(fs::read(published).unwrap(), b"current");
    }

    #[test]
    fn publication_survives_pruning_during_copy() {
        let scratch = fixture("prune-copy-race");
        let built = source(&scratch, "target/debug/ck-probe", b"complete executable");
        let pruned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = pruned.clone();
        AFTER_COPY.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |temporary| {
                if !observed.swap(true, Ordering::Relaxed) {
                    let directory = temporary.parent().unwrap();
                    #[cfg(unix)]
                    let directory = directory.parent().unwrap();
                    age_directory(directory);
                    std::thread::scope(|scope| {
                        scope
                            .spawn(|| {
                                prune_copies(
                                    directory.parent().unwrap(),
                                    Path::new(""),
                                    SystemTime::now(),
                                );
                                // Windows retains the directory while the temporary copy is open;
                                // pruning must not interrupt publication even when removal fails.
                                if cfg!(windows) {
                                    assert!(directory.is_dir());
                                } else {
                                    assert!(!directory.exists());
                                }
                            })
                            .join()
                            .unwrap();
                    });
                }
            }));
        });
        let published = publish_pruning_once(&built, &OnceLock::new());
        AFTER_COPY.with(|hook| *hook.borrow_mut() = None);
        assert!(pruned.load(Ordering::Relaxed), "copy race was not reached");
        assert_eq!(fs::read(published).unwrap(), b"complete executable");
    }

    #[test]
    fn concurrent_launch_recovers_after_pruning() {
        let scratch = fixture("prune-launch-race");
        let built = scratch.join(format!("ck-probe{}", std::env::consts::EXE_SUFFIX));
        fs::copy(std::env::current_exe().unwrap(), &built).unwrap();
        let published = ckdev_binary(&built, &scratch);
        let directory = published.parent().unwrap();
        age_directory(directory);
        let inspected = Arc::new(Barrier::new(2));
        let reused = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let (pruner_inspected, pruner_reused) = (inspected.clone(), reused.clone());
            let pruner = scope.spawn(|| {
                BEFORE_PRUNE.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move |_| {
                        pruner_inspected.wait();
                        pruner_reused.wait();
                    }));
                });
                prune_copies(
                    directory.parent().unwrap(),
                    Path::new(""),
                    SystemTime::now(),
                );
                BEFORE_PRUNE.with(|hook| *hook.borrow_mut() = None);
            });
            inspected.wait();
            let mut command = ckdev_command(&built, &scratch);
            reused.wait();
            pruner.join().unwrap();
            assert_eq!(
                command.arg("--list").output().unwrap_err().kind(),
                io::ErrorKind::NotFound
            );
            let recreated = ckdev_binary(&built, &scratch);
            assert_eq!(recreated, published);
            let output =
                retry_executable_file_busy(|| Command::new(&recreated).arg("--list").output())
                    .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8(output.stdout)
                .unwrap()
                .contains("ckdev::tests::concurrent_launch_recovers_after_pruning: test"));
        });
    }

    #[test]
    fn dev_binary_reuses_content_and_keeps_the_executable_suffix() {
        let scratch = fixture("reuse");
        let built = source(
            &scratch,
            "target/debug/ck-probe.exe",
            b"test executable bytes",
        );
        let first = ckdev_binary(&built, &scratch);
        let expected_prefix = if cfg!(target_os = "macos") {
            "ee2bbe0ad803bd41"
        } else {
            "e5c8d6e5563f01ff"
        };
        assert_eq!(
            first,
            exec_root(&built).join(format!("ckdev-exec/{expected_prefix}/ckdev-probe.exe"))
        );
        assert_eq!(fs::read(&first).unwrap(), b"test executable bytes");
        // A second copy would change this planted timestamp even if it reproduced the bytes.
        let marker = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        File::options()
            .write(true)
            .open(&first)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(marker))
            .unwrap();
        let first_modified = fs::metadata(&first).unwrap().modified().unwrap();
        let second = ckdev_binary(&built, scratch.join("a-different-rig"));
        assert_eq!(first, second);
        assert_eq!(
            fs::metadata(&second).unwrap().modified().unwrap(),
            first_modified
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_ne!(
                fs::metadata(&built).unwrap().ino(),
                fs::metadata(&first).unwrap().ino()
            );
        }

        fs::write(&built, b"another build").unwrap();
        let rebuilt = ckdev_binary(&built, &scratch);
        assert_ne!(rebuilt.parent(), first.parent());
        assert_eq!(fs::read(&first).unwrap(), b"test executable bytes");
        assert_eq!(fs::read(&rebuilt).unwrap(), b"another build");
    }

    #[test]
    fn source_build_directory_includes_custom_targets_and_deps() {
        let scratch = fixture("profile");
        let built = source(
            &scratch,
            "custom-target/x86_64-pc-windows-gnu/release/deps/ck-probe.exe",
            b"probe",
        );
        assert_eq!(
            profile_directory(&built),
            scratch.join("custom-target/x86_64-pc-windows-gnu/release")
        );
        let staged = source(&scratch, "rigs/ck-probe", b"staged");
        assert_eq!(profile_directory(&staged), scratch.join("rigs"));
    }

    /// Published executables stay outside Cargo's build tree and removable volumes.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_publishes_outside_the_build_on_the_internal_disk() {
        let scratch = fixture("internal");
        TEST_EXEC_ROOT.with(|slot| *slot.borrow_mut() = None);
        let built = source(&scratch, "target/debug/ck-probe", b"internal disk");
        // A worker's per-task TMPDIR; the path is only compared, never created. It must not sit
        // under /var/folders, which `scratch_root()` does in an ordinary shell.
        let worker_tmpdir =
            OsString::from("/Users/agent/.local/share/cortexkit/runner/bash-tasks/t 1/tmp");
        assert_eq!(
            macos_exec_root(Some(worker_tmpdir.clone())),
            PathBuf::from(&worker_tmpdir).join("broca-exec")
        );
        assert_eq!(macos_exec_root(None), PathBuf::from("/tmp/broca-exec"));
        assert_eq!(
            macos_exec_root(Some(OsString::new())),
            PathBuf::from("/tmp/broca-exec")
        );
        for system_tmp in ["/var/folders/18/abc/T/", "/private/var/folders/18/abc/T/"] {
            assert_eq!(
                macos_exec_root(Some(system_tmp.into())),
                PathBuf::from("/tmp/broca-exec"),
                "{system_tmp} must not hold published executables"
            );
        }

        let expected_root = macos_exec_root(std::env::var_os("TMPDIR"));
        let selected_root = exec_root(&built);
        let published = ckdev_binary(&built, &scratch);
        assert!(published.starts_with(selected_root.join("ckdev-exec")));

        let resolved_published = fs::canonicalize(&published).unwrap();
        let executable = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let target_dir = fs::canonicalize(
            executable
                .ancestors()
                .nth(3)
                .expect("test executable lives below Cargo's target directory"),
        )
        .unwrap();
        assert!(
            !resolved_published.starts_with(&target_dir),
            "published executable resolves inside Cargo's target directory: {}",
            resolved_published.display()
        );
        assert!(
            !resolved_published.starts_with("/Volumes"),
            "published executable resolves on a removable volume: {}",
            resolved_published.display()
        );
        assert_eq!(selected_root, expected_root);
        assert!(published.starts_with(expected_root.join("ckdev-exec")));
    }

    #[cfg(target_os = "macos")]
    fn nm_output(path: &Path) -> String {
        let output = Command::new("/usr/bin/nm")
            .args(["-ap"])
            .arg(path)
            .output()
            .unwrap_or_else(|error| panic!("running nm -ap on {} failed: {error}", path.display()));
        assert!(
            output.status.success(),
            "nm -ap {} failed ({}): {}",
            path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_published_copy_has_no_debug_map() {
        let source = std::env::current_exe().unwrap();
        let scratch = fixture("stripped");
        let source_nm = nm_output(&source);
        assert!(
            source_nm.contains("OSO"),
            "test binary source has no OSO entries"
        );

        let published = ckdev_binary(&source, &scratch);
        assert!(
            !nm_output(&published).contains("OSO"),
            "published copy still contains OSO entries"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_republishes_a_copy_from_the_unstripped_cache_format() {
        let source = std::env::current_exe().unwrap();
        let scratch = fixture("legacy-format");
        let source_info = Source::open(&source).unwrap();
        let legacy_prefix: String = source_info.digest[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let legacy_directory = exec_root(&source).join("ckdev-exec").join(legacy_prefix);
        fs::create_dir_all(&legacy_directory).unwrap();
        let name = dev_file_name(&source);
        let legacy_copy = legacy_directory.join(&name);
        fs::copy(&source, &legacy_copy).unwrap();
        assert!(nm_output(&legacy_copy).contains("OSO"));

        let published = ckdev_binary(&source, &scratch);
        assert_ne!(published, legacy_copy, "pre-strip cache entry was reused");
        assert!(published.is_file());
    }

    #[cfg(target_os = "macos")]
    #[inline(never)]
    fn named_backtrace_function() {
        panic!("backtrace symbol check");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn panic_backtrace_child() {
        if std::env::var_os("BROCA_TEST_SUPPORT_BACKTRACE_CHILD").is_some() {
            named_backtrace_function();
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_published_copy_backtrace_keeps_function_names() {
        let source = std::env::current_exe().unwrap();
        let published = ckdev_binary(&source, fixture("backtrace"));
        let output = Command::new(&published)
            .args([
                "--exact",
                "ckdev::tests::panic_backtrace_child",
                "--nocapture",
            ])
            .env("BROCA_TEST_SUPPORT_BACKTRACE_CHILD", "1")
            .env("RUST_BACKTRACE", "1")
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "child panic test unexpectedly passed"
        );
        let output = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.contains("named_backtrace_function"),
            "published copy's panic backtrace omitted the function name:\n{output}"
        );
    }

    #[test]
    fn dev_prefix_is_not_duplicated_and_companion_stays_beside_the_daemon() {
        let scratch = fixture("companion");
        let daemon = source(&scratch, "target/debug/ckdev-subc", b"daemon");
        let companion = source(&scratch, "target/debug/ck", b"client");
        let (dev_daemon, copied_companion) =
            ckdev_binary_with_companion(&daemon, &companion, &scratch);
        assert_eq!(dev_daemon.file_name().unwrap(), "ckdev-subc");
        assert_eq!(copied_companion.file_name().unwrap(), "ck");
        assert_eq!(dev_daemon.parent(), copied_companion.parent());
        assert_eq!(fs::read(&copied_companion).unwrap(), b"client");
        fs::write(&companion, b"new client").unwrap();
        let (new_daemon, new_companion) =
            ckdev_binary_with_companion(&daemon, &companion, &scratch);
        assert_ne!(new_daemon.parent(), dev_daemon.parent());
        assert_eq!(new_daemon.parent(), new_companion.parent());
        assert_eq!(fs::read(&copied_companion).unwrap(), b"client");
        assert_eq!(fs::read(&new_companion).unwrap(), b"new client");
    }

    #[test]
    fn binary_pair_keeps_both_dev_names_and_tracks_both_contents() {
        let scratch = fixture("pair");
        let vault = source(&scratch, "target/debug/ck-claustrum.exe", b"vault");
        let auth = source(&scratch, "other-target/release/ck-auth.exe", b"auth");
        let (dev_vault, dev_auth) = ckdev_binary_pair(&vault, &auth, &scratch);
        assert_eq!(dev_vault.file_name().unwrap(), "ckdev-claustrum.exe");
        assert_eq!(dev_auth.file_name().unwrap(), "ckdev-auth.exe");
        assert_eq!(dev_vault.parent(), dev_auth.parent());
        assert_eq!(fs::read(&dev_auth).unwrap(), b"auth");
        fs::write(&auth, b"new auth").unwrap();
        let (new_vault, new_auth) = ckdev_binary_pair(&vault, &auth, &scratch);
        assert_ne!(new_vault.parent(), dev_vault.parent());
        assert_eq!(new_vault.parent(), new_auth.parent());
        assert_eq!(fs::read(&dev_auth).unwrap(), b"auth");
        assert_eq!(fs::read(&new_auth).unwrap(), b"new auth");
    }

    #[test]
    fn ckdev_command_uses_the_published_copy() {
        let scratch = fixture("command");
        let built = source(&scratch, "target/debug/ck-probe", b"test executable bytes");
        let command = ckdev_command(&built, &scratch);
        let program = Path::new(command.get_program());
        assert_eq!(program.file_name().unwrap(), "ckdev-probe");
        assert!(program.starts_with(exec_root(&built).join("ckdev-exec")));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn publication_never_holds_a_writable_executable_descriptor() {
        use std::os::unix::fs::MetadataExt;

        let scratch = fixture("no-writer");
        let built = scratch.join("ck-probe");
        fs::copy("/bin/true", &built).unwrap();
        let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = observed.clone();
        AFTER_COPY.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |temporary| {
                let inode = fs::metadata(temporary).unwrap();
                // Inspect descriptors from another test thread before the final rename.
                // A writable descriptor here could survive in a child forked by that thread
                // and block a daemon's exec of the renamed executable.
                std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            for entry in fs::read_dir("/proc/self/fd").unwrap() {
                                let entry = entry.unwrap();
                                let Ok(metadata) = fs::metadata(entry.path()) else {
                                    continue;
                                };
                                if metadata.dev() != inode.dev() || metadata.ino() != inode.ino() {
                                    continue;
                                }
                                let info = fs::read_to_string(
                                    Path::new("/proc/self/fdinfo").join(entry.file_name()),
                                )
                                .unwrap();
                                let flags = info
                                    .lines()
                                    .find_map(|line| line.strip_prefix("flags:\t"))
                                    .expect("fdinfo must report access flags");
                                let flags = u32::from_str_radix(flags.trim(), 8).unwrap();
                                assert_eq!(
                                    flags & 3,
                                    0,
                                    "test process holds a writable descriptor to publishable inode: {}",
                                    entry.path().display()
                                );
                            }
                        })
                        .join()
                        .unwrap();
                });
                seen.store(true, Ordering::Relaxed);
            }));
        });
        let published = ckdev_binary(&built, &scratch);
        AFTER_COPY.with(|hook| *hook.borrow_mut() = None);
        assert!(observed.load(Ordering::Relaxed), "copy hook never executed");
        // Launch the published executable without retry_executable_file_busy, just as an
        // external daemon does: retrying would hide a writer accidentally left open.
        assert!(Command::new(&published).status().unwrap().success());
    }

    #[cfg(unix)]
    #[test]
    fn copy_reads_the_hashed_inode_when_the_source_path_is_replaced() {
        let scratch = fixture("replaced-source");
        let built = source(&scratch, "ck-probe", b"original build");
        let replacement = source(&scratch, "replacement", b"different build");
        let original = built.clone();
        BEFORE_COPY.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| {
                fs::rename(&replacement, &original).unwrap();
            }));
        });
        let published = ckdev_binary(&built, &scratch);
        BEFORE_COPY.with(|hook| *hook.borrow_mut() = None);
        assert_eq!(fs::read(published).unwrap(), b"original build");
    }

    #[cfg(unix)]
    #[test]
    fn copy_digest_rejects_same_size_corruption_before_publication() {
        let scratch = fixture("copy-digest");
        let built = source(&scratch, "ck-probe", b"original build");
        let directory = scratch.join("destination");
        fs::create_dir(&directory).unwrap();
        AFTER_COPY.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |temporary| {
                fs::write(temporary, b"tampered build").unwrap();
            }));
        });
        let result = publish_copy(
            &mut Source::open(&built).unwrap(),
            &directory,
            &OsString::from("ckdev-probe"),
        );
        AFTER_COPY.with(|hook| *hook.borrow_mut() = None);
        assert_eq!(
            result.unwrap_err().to_string(),
            "test binary copy differs from hashed source"
        );
        assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn published_executables_retry_etxtbsy_after_an_inherited_writer_closes() {
        use std::process::{Child, Stdio};
        use std::time::Instant;

        const READY: &str = "BROCA_CKDEV_ETXTBSY_READY";
        const RELEASE: &str = "BROCA_CKDEV_ETXTBSY_RELEASE";

        struct Holder {
            child: Option<Child>,
            release: PathBuf,
        }

        impl Drop for Holder {
            fn drop(&mut self) {
                let _ = fs::write(&self.release, b"release");
                if let Some(child) = self.child.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }

        let scratch = fixture("etxtbsy");
        let test_executable = std::env::current_exe().unwrap();
        let published = ckdev_binary(&test_executable, &scratch);
        let ready = scratch.join("writer-ready");
        let release = scratch.join("writer-release");
        let writer = OpenOptions::new().write(true).open(&published).unwrap();
        let mut holder = Holder {
            child: Some(
                Command::new(&test_executable)
                    .args([
                        "--ignored",
                        "--exact",
                        "ckdev::tests::etxtbsy_writer_process",
                    ])
                    .stdin(Stdio::from(writer))
                    .env(READY, &ready)
                    .env(RELEASE, &release)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            ),
            release: release.clone(),
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready.exists() {
            if let Some(status) = holder.child.as_mut().unwrap().try_wait().unwrap() {
                panic!("writer process exited before holding the descriptor: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "writer process did not become ready"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let raw_error = Command::new(&published)
            .arg("--list")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect_err("a writer held in another process must block exec");
        assert_eq!(raw_error.kind(), io::ErrorKind::ExecutableFileBusy);

        let mut command = Command::new(&published);
        command
            .arg("--list")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut attempts = 0;
        let mut child = retry_executable_file_busy(|| {
            attempts += 1;
            let result = command.spawn();
            if matches!(&result, Err(error) if error.kind() == io::ErrorKind::ExecutableFileBusy) {
                fs::write(&release, b"release").unwrap();
            }
            result
        })
        .expect("the launch helper should retry until the inherited writer closes");
        assert!(attempts > 1, "launch succeeded without retrying");
        assert!(child.wait().unwrap().success());

        let status = holder.child.take().unwrap().wait().unwrap();
        assert!(status.success(), "writer process failed: {status}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "started as a child by the ETXTBSY regression test"]
    fn etxtbsy_writer_process() {
        use std::time::Instant;

        let ready = PathBuf::from(
            std::env::var_os("BROCA_CKDEV_ETXTBSY_READY").expect("parent supplies the ready path"),
        );
        let release = PathBuf::from(
            std::env::var_os("BROCA_CKDEV_ETXTBSY_RELEASE")
                .expect("parent supplies the release path"),
        );
        fs::write(&ready, b"ready").unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !release.exists() {
            assert!(
                Instant::now() < deadline,
                "parent never released the writer"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn truncated_published_executable_is_replaced() {
        let scratch = fixture("truncated");
        let built = source(&scratch, "target/debug/ck-probe", b"complete executable");
        let published = ckdev_binary(&built, &scratch);
        fs::write(&published, b"bad").unwrap();
        assert_eq!(ckdev_binary(&built, &scratch), published);
        assert_eq!(fs::read(&published).unwrap(), b"complete executable");
    }

    #[test]
    fn concurrent_publishers_only_expose_complete_executables() {
        let scratch = fixture("concurrent");
        let built = scratch.join(format!(
            "target/debug/ck-probe{}",
            std::env::consts::EXE_SUFFIX
        ));
        fs::create_dir_all(built.parent().unwrap()).unwrap();
        fs::copy(std::env::current_exe().unwrap(), &built).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&built, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let expected_size = fs::metadata(&built).unwrap().len();
        let start = Arc::new(Barrier::new(8));
        let copying = Arc::new(Barrier::new(8));
        let premature = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let paths = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let (start, copying, premature) =
                        (start.clone(), copying.clone(), premature.clone());
                    let (built, scratch) = (&built, &scratch);
                    scope.spawn(move || {
                        publish_into(scratch);
                        BEFORE_COPY.with(|hook| {
                            *hook.borrow_mut() = Some(Box::new(move |destination| {
                                // Hold every writer before its first byte. Atomic publishers
                                // leave the final name absent; direct writers expose an empty file.
                                if fs::metadata(destination).is_ok_and(|m| m.len() != expected_size)
                                {
                                    premature.store(true, Ordering::Relaxed);
                                }
                                copying.wait();
                            }));
                        });
                        start.wait();
                        let path = ckdev_binary(built, scratch);
                        BEFORE_COPY.with(|hook| *hook.borrow_mut() = None);
                        path
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(
            !premature.load(Ordering::Relaxed),
            "final executable became visible before publication completed"
        );
        for path in &paths {
            assert_eq!(path, &paths[0]);
        }
        // macOS signs the copy before publishing it, which changes its bytes; elsewhere the copy
        // is the build itself.
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(fs::metadata(&paths[0]).unwrap().len(), expected_size);
            assert_eq!(fs::read(&paths[0]).unwrap(), fs::read(&built).unwrap());
        }
        #[cfg(target_os = "macos")]
        {
            let _ = expected_size;
            let verify = Command::new("/usr/bin/codesign")
                .args(["--verify", "--strict"])
                .arg(&paths[0])
                .output()
                .unwrap();
            assert!(
                verify.status.success(),
                "published copy is not validly signed: {}",
                String::from_utf8_lossy(&verify.stderr)
            );
        }
        // Only the published executable and its size record; no temporary copy is left behind.
        let names: Vec<_> = fs::read_dir(paths[0].parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(
            names
                .iter()
                .all(|name| !name.to_string_lossy().ends_with(".tmp")),
            "{names:?}"
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| !name.to_string_lossy().starts_with('.'))
                .count(),
            1,
            "{names:?}"
        );
        let output =
            retry_executable_file_busy(|| Command::new(&paths[0]).arg("--list").output()).unwrap();
        assert!(
            output.status.success(),
            "published executable failed: {}",
            crate::process::describe_exit_status(output.status)
        );
        assert!(String::from_utf8(output.stdout).unwrap().contains(
            "ckdev::tests::concurrent_publishers_only_expose_complete_executables: test"
        ));
    }
}
