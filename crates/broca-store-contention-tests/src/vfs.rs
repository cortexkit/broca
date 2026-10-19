use super::busy::sleep_between_busy_attempts;
use rusqlite::ffi;
use std::ffi::{c_char, c_double, c_int, c_void};
use std::sync::Once;
use std::time::Duration;

thread_local! {
    static SLEEP_PANICKED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn sleep_panicked() -> bool {
    SLEEP_PANICKED.with(|panicked| panicked.get())
}

// Pass the original VFS pointer to every platform method. A shallow method-table
// copy alone would change pointer identity, the name and registry links seen by
// platform implementations, even if its application-data pointer stayed intact.
macro_rules! forward_methods {
    ($($field:ident => $name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {
        $(unsafe extern "C" fn $name(vfs: *mut ffi::sqlite3_vfs, $($arg: $ty),*) -> $ret {
            // SAFETY: Only the registered shim uses these callbacks. Its application
            // data is the process-lifetime platform VFS, and a forwarder is installed
            // only when that platform method exists. SQLite supplies its original args.
            let platform = unsafe { (*vfs).pAppData.cast::<ffi::sqlite3_vfs>() };
            let method = unsafe { (*platform).$field }.expect("forwarded VFS method exists");
            unsafe { method(platform, $($arg),*) }
        })*

        fn install_forwarders(vfs: &mut ffi::sqlite3_vfs) {
            $(vfs.$field = vfs.$field.map(|_| $name as _);)*
        }
    };
}

forward_methods! {
    xOpen => open(name: *const c_char, file: *mut ffi::sqlite3_file, flags: c_int, out: *mut c_int) -> c_int;
    xDelete => delete(name: *const c_char, sync_dir: c_int) -> c_int;
    xAccess => access(name: *const c_char, flags: c_int, out: *mut c_int) -> c_int;
    xFullPathname => full_pathname(name: *const c_char, size: c_int, out: *mut c_char) -> c_int;
    xDlOpen => dl_open(name: *const c_char) -> *mut c_void;
    xDlError => dl_error(size: c_int, out: *mut c_char) -> ();
    xDlSym => dl_sym(handle: *mut c_void, symbol: *const c_char) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)>;
    xDlClose => dl_close(handle: *mut c_void) -> ();
    xRandomness => randomness(size: c_int, out: *mut c_char) -> c_int;
    xCurrentTime => current_time(out: *mut c_double) -> c_int;
    xGetLastError => last_error(size: c_int, out: *mut c_char) -> c_int;
    xCurrentTimeInt64 => current_time_int64(out: *mut ffi::sqlite3_int64) -> c_int;
    xSetSystemCall => set_system_call(name: *const c_char, call: ffi::sqlite3_syscall_ptr) -> c_int;
    xGetSystemCall => get_system_call(name: *const c_char) -> ffi::sqlite3_syscall_ptr;
    xNextSystemCall => next_system_call(name: *const c_char) -> *const c_char;
}

// Only xSleep changes behavior. SQLite's real default busy handler and the
// migration handler then share one deterministic foreground-writer schedule.
pub(super) fn register() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        // SAFETY: SQLite owns its default VFS for the process lifetime. Initialization
        // precedes lookup, and platform method pointers do not change in this test crate.
        let mut vfs = unsafe {
            assert_eq!(ffi::sqlite3_initialize(), ffi::SQLITE_OK);
            let platform = ffi::sqlite3_vfs_find(std::ptr::null());
            assert!(!platform.is_null());
            let mut copy = Box::new(std::ptr::read(platform));
            copy.pAppData = platform.cast();
            copy
        };
        install_forwarders(&mut vfs);
        vfs.zName = c"migration-scheduled-sleep".as_ptr();
        // SQLite owns the new registry link after registration, not the copied link.
        vfs.pNext = std::ptr::null_mut();
        vfs.xSleep = Some(scheduled_sleep);
        // SAFETY: The allocation and literal name live until process exit. Retaining
        // this single test-only registration avoids dangling references on panic paths.
        // It is not the default VFS; only explicitly named test connections use it.
        assert_eq!(
            unsafe { ffi::sqlite3_vfs_register(Box::into_raw(vfs), 0) },
            ffi::SQLITE_OK
        );
    });
}

unsafe extern "C" fn scheduled_sleep(_: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    // Never unwind through SQLite's C frames; report scheduler failures under the
    // Rust test's name after the writer thread and connections can be cleaned up.
    if std::panic::catch_unwind(|| {
        let micros = u64::try_from(micros).expect("SQLite sleep duration is nonnegative");
        sleep_between_busy_attempts(Duration::from_micros(micros));
    })
    .is_err()
    {
        SLEEP_PANICKED.with(|panicked| panicked.set(true));
    }
    micros
}
