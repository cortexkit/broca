use super::OpenOptions;
use std::{
    fs::File,
    io,
    mem::{size_of, zeroed},
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle},
    },
    path::Path,
    ptr::{null, null_mut},
};
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, LocalFree, ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS, GENERIC_READ,
        GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE,
    },
    Security::{
        AddAccessAllowedAceEx,
        Authorization::{GetSecurityInfo, SE_FILE_OBJECT},
        CreateWellKnownSid, EqualSid, GetAce, GetLengthSid, GetTokenInformation, InitializeAcl,
        InitializeSecurityDescriptor, IsValidAcl, IsValidSid, SetKernelObjectSecurity,
        SetSecurityDescriptorControl, SetSecurityDescriptorDacl, SetSecurityDescriptorOwner,
        TokenUser, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, CONTAINER_INHERIT_ACE,
        DACL_SECURITY_INFORMATION, OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSID, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR,
        SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
    },
    Storage::FileSystem::{
        CreateDirectoryW, CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        CREATE_NEW, FILE_ALL_ACCESS, FILE_APPEND_DATA, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_WRITE, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

// Win32's simple access-allowed ACE header type (winnt.h).
const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

struct Handle(HANDLE);
impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: Handle owns a live handle and closes it exactly once.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

// Process inspection exists only for the test-support survivor dump, whose result type lives in
// that module, so it is compiled under the same condition; a plain build has neither.
#[cfg(any(test, feature = "test-support"))]
pub(super) use processes::{process_is_in_job, process_snapshot, process_wait_for_exit};

#[cfg(any(test, feature = "test-support"))]
mod processes {
    use super::Handle;
    use crate::test_support::ProcessWaitError;
    use std::{
        io,
        mem::{size_of, zeroed},
        ptr::null_mut,
    };
    use windows_sys::Win32::{
        Foundation::{
            GetLastError, ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE, WAIT_FAILED, WAIT_OBJECT_0,
            WAIT_TIMEOUT,
        },
        System::{
            Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
                TH32CS_SNAPPROCESS,
            },
            JobObjects::IsProcessInJob,
            Threading::{OpenProcess, WaitForSingleObject, PROCESS_QUERY_LIMITED_INFORMATION},
        },
    };

    pub(crate) fn process_snapshot() -> io::Result<Vec<crate::test_support::ProcessDetails>> {
        // SAFETY: Toolhelp accepts a process-snapshot flag and no caller-owned pointers.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            // SAFETY: GetLastError reads the calling thread's last Win32 error.
            return Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ));
        }
        let _snapshot = Handle(snapshot);

        // SAFETY: PROCESSENTRY32W is a plain Win32 output structure; dwSize is set below.
        let mut entry: PROCESSENTRY32W = unsafe { zeroed() };
        entry.dwSize = size_of::<PROCESSENTRY32W>() as u32;
        // SAFETY: snapshot is a live Toolhelp process snapshot and entry is writable.
        if unsafe { Process32FirstW(snapshot, &mut entry) } == 0 {
            // SAFETY: GetLastError reads the calling thread's last Win32 error.
            let error = unsafe { GetLastError() };
            if error == ERROR_NO_MORE_FILES {
                return Ok(Vec::new());
            }
            return Err(io::Error::from_raw_os_error(error as i32));
        }

        let mut processes = Vec::new();
        loop {
            let image_end = entry
                .szExeFile
                .iter()
                .position(|unit| *unit == 0)
                .unwrap_or(entry.szExeFile.len());
            processes.push(crate::test_support::ProcessDetails {
                image: String::from_utf16_lossy(&entry.szExeFile[..image_end]),
                pid: entry.th32ProcessID,
                parent_pid: entry.th32ParentProcessID,
            });
            // SAFETY: snapshot is a live Toolhelp process snapshot and entry is writable.
            if unsafe { Process32NextW(snapshot, &mut entry) } != 0 {
                continue;
            }
            // SAFETY: GetLastError reads the calling thread's last Win32 error.
            let error = unsafe { GetLastError() };
            if error == ERROR_NO_MORE_FILES {
                break;
            }
            return Err(io::Error::from_raw_os_error(error as i32));
        }
        Ok(processes)
    }

    pub(crate) fn process_is_in_job(pid: u32) -> io::Result<bool> {
        // SAFETY: OpenProcess receives a PID and requests read-only process query access.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if process.is_null() {
            // SAFETY: GetLastError reads the calling thread's last Win32 error.
            return Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ));
        }
        let process = Handle(process);
        let mut in_job = 0;
        // SAFETY: process is a live handle and in_job is a writable BOOL output.
        if unsafe { IsProcessInJob(process.0, null_mut(), &mut in_job) } == 0 {
            // SAFETY: GetLastError reads the calling thread's last Win32 error.
            return Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ));
        }
        Ok(in_job != 0)
    }

    pub(crate) fn process_wait_for_exit(
        pid: u32,
        timeout_ms: u32,
    ) -> Result<bool, ProcessWaitError> {
        // windows-sys does not expose the generic Win32 SYNCHRONIZE access right here.
        const SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
        // SAFETY: OpenProcess receives a PID and requests only wait and read-only query access.
        let process = unsafe {
            OpenProcess(
                SYNCHRONIZE_ACCESS | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                pid,
            )
        };
        if process.is_null() {
            // SAFETY: GetLastError reads the calling thread's last Win32 error.
            return Err(ProcessWaitError::Open(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32,
            )));
        }
        let process = Handle(process);
        // SAFETY: process is a live handle with SYNCHRONIZE access.
        match unsafe { WaitForSingleObject(process.0, timeout_ms) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            WAIT_FAILED => {
                // SAFETY: GetLastError reads the calling thread's last Win32 error.
                Err(ProcessWaitError::Wait(io::Error::from_raw_os_error(
                    unsafe { GetLastError() } as i32,
                )))
            }
            result => Err(ProcessWaitError::Wait(io::Error::other(format!(
                "WaitForSingleObject returned unexpected result {result}"
            )))),
        }
    }
}

struct Descriptor(*mut std::ffi::c_void);
impl Drop for Descriptor {
    fn drop(&mut self) {
        // SAFETY: GetSecurityInfo allocated this descriptor with LocalAlloc.
        unsafe {
            LocalFree(self.0);
        }
    }
}
fn wide(path: &Path) -> io::Result<Vec<u16>> {
    let mut name: Vec<u16> = path.as_os_str().encode_wide().collect();
    if name.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains NUL",
        ));
    }
    name.push(0);
    Ok(name)
}
#[derive(Debug)]
struct AclRefusal(&'static str);
impl std::fmt::Display for AclRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "owner-only ACL refused: {}", self.0)
    }
}
impl std::error::Error for AclRefusal {}
fn refused(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, AclRefusal(message))
}

pub(super) fn is_private(path: &Path) -> io::Result<bool> {
    match validate(path) {
        Ok(()) => Ok(true),
        Err(error)
            if error
                .get_ref()
                .and_then(|e| e.downcast_ref::<AclRefusal>())
                .is_some() =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

struct User(Vec<usize>);
impl User {
    fn current() -> io::Result<Self> {
        let mut token = null_mut();
        // SAFETY: The process pseudo-handle is valid; token is a writable output.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = Handle(token);
        let mut bytes = 0;
        // SAFETY: A zero-length query asks Windows for the required token buffer size.
        unsafe {
            GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut bytes);
        }
        if (bytes as usize) < size_of::<TOKEN_USER>() {
            return Err(io::Error::last_os_error());
        }
        let mut user = Self(vec![0; (bytes as usize).div_ceil(size_of::<usize>())]);
        // SAFETY: The aligned buffer has the requested size and owns the embedded SID.
        if unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                user.0.as_mut_ptr().cast(),
                bytes,
                &mut bytes,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(user)
    }
    fn sid(&self) -> PSID {
        // SAFETY: current initialized a TOKEN_USER in this aligned, live allocation.
        unsafe { (*(self.0.as_ptr().cast::<TOKEN_USER>())).User.Sid }
    }
}
fn system_sid() -> io::Result<Vec<u32>> {
    let mut sid = vec![0u32; 17];
    let mut bytes = (sid.len() * size_of::<u32>()) as u32;
    // SAFETY: The aligned buffer is SECURITY_MAX_SID_SIZE bytes; Windows writes a SID.
    if unsafe {
        CreateWellKnownSid(
            WinLocalSystemSid,
            null_mut(),
            sid.as_mut_ptr().cast(),
            &mut bytes,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(sid)
}
struct Security {
    acl: Vec<u32>,
    descriptor: SECURITY_DESCRIPTOR,
}
impl Security {
    fn new(owner: PSID, directory: bool) -> io::Result<Self> {
        let system = system_sid()?;
        // SAFETY: The owner is a live token or security-descriptor SID; system is initialized.
        let bytes = unsafe {
            size_of::<ACL>()
                + 2 * (size_of::<ACCESS_ALLOWED_ACE>() - size_of::<u32>())
                + GetLengthSid(owner) as usize
                + GetLengthSid(system.as_ptr() as PSID) as usize
        };
        // SAFETY: SECURITY_DESCRIPTOR is a plain Win32 output structure initialized below.
        let mut security = Self {
            acl: vec![0; bytes.div_ceil(size_of::<u32>())],
            descriptor: unsafe { zeroed() },
        };
        let flags = if directory {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            0
        };
        // SAFETY: All buffers are aligned and sized for the two ACEs. The ACE calls copy
        // SIDs; the descriptor references acl, whose allocation remains live with Security.
        let ok = unsafe {
            InitializeAcl(
                security.acl.as_mut_ptr().cast(),
                (security.acl.len() * size_of::<u32>()) as u32,
                ACL_REVISION,
            ) != 0
                && AddAccessAllowedAceEx(
                    security.acl.as_mut_ptr().cast(),
                    ACL_REVISION,
                    flags,
                    FILE_ALL_ACCESS,
                    owner,
                ) != 0
                && AddAccessAllowedAceEx(
                    security.acl.as_mut_ptr().cast(),
                    ACL_REVISION,
                    flags,
                    FILE_ALL_ACCESS,
                    system.as_ptr() as PSID,
                ) != 0
                && InitializeSecurityDescriptor(
                    (&mut security.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                    1,
                ) != 0
                && SetSecurityDescriptorOwner(
                    (&mut security.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                    owner,
                    0,
                ) != 0
                && SetSecurityDescriptorDacl(
                    (&mut security.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                    1,
                    security.acl.as_mut_ptr().cast(),
                    0,
                ) != 0
                && SetSecurityDescriptorControl(
                    (&mut security.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                    SE_DACL_PROTECTED,
                    SE_DACL_PROTECTED,
                ) != 0
        };
        if !ok {
            return Err(io::Error::last_os_error());
        }
        Ok(security)
    }
    fn attributes(&mut self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: (&mut self.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            bInheritHandle: 0,
        }
    }
}
fn open_inspection(path: &Path, repair: bool) -> io::Result<Handle> {
    let name = wide(path)?;
    // SAFETY: name is terminated; OPEN_REPARSE_POINT opens a final junction/link itself.
    let handle = unsafe {
        CreateFileW(
            name.as_ptr(),
            READ_CONTROL | if repair { WRITE_DAC } else { 0 },
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    Ok(Handle(handle))
}
fn attributes(handle: HANDLE) -> io::Result<u32> {
    // SAFETY: This Win32 output structure is zero-initializable.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { zeroed() };
    // SAFETY: The handle is live and info is a writable output.
    if unsafe { GetFileInformationByHandle(handle, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(refused("reparse points are not private state objects"));
    }
    Ok(info.dwFileAttributes)
}
fn descriptor(handle: HANDLE) -> io::Result<(Descriptor, PSID, *mut ACL)> {
    let mut owner = null_mut();
    let mut dacl = null_mut();
    let mut sd = null_mut();
    // SAFETY: All output pointers are writable. owner/dacl borrow the returned descriptor.
    let error = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut sd,
        )
    };
    if error != 0 {
        return Err(io::Error::from_raw_os_error(error as i32));
    }
    let sd = Descriptor(sd);
    // SAFETY: GetSecurityInfo returned these pointers within sd; nulls are checked first.
    if owner.is_null()
        || dacl.is_null()
        || unsafe { IsValidSid(owner) == 0 || IsValidAcl(dacl) == 0 }
    {
        return Err(refused("missing or invalid owner/DACL"));
    }
    Ok((sd, owner, dacl))
}
fn validate_handle(handle: HANDLE) -> io::Result<()> {
    attributes(handle)?;
    let (_sd, _owner, dacl) = descriptor(handle)?;
    let user = User::current()?;
    let system = system_sid()?;
    let mut account_grant = false;
    // SAFETY: descriptor validated dacl and its header. GetAce bounds-checks each index.
    for i in 0..unsafe { (*dacl).AceCount } as u32 {
        let mut ace = null_mut();
        // SAFETY: dacl is valid and ace is a writable output for this in-range index.
        if unsafe { GetAce(dacl, i, &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: The returned ACE lives in sd. Check its type before treating it as allowed.
        let header = unsafe { &*(ace.cast::<windows_sys::Win32::Security::ACE_HEADER>()) };
        if header.AceType != ACCESS_ALLOWED_ACE_TYPE {
            return Err(refused("unexpected or inherited ACE"));
        }
        // SAFETY: ACCESS_ALLOWED_ACE has this layout and its SID starts at SidStart.
        let allowed = unsafe { &*(ace.cast::<ACCESS_ALLOWED_ACE>()) };
        let sid = (&allowed.SidStart as *const u32) as PSID;
        // SAFETY: These SIDs are in valid ACEs or validated descriptor/token buffers.
        let (is_account, is_system) = unsafe {
            (
                EqualSid(sid, user.sid()) != 0,
                EqualSid(sid, system.as_ptr() as PSID) != 0,
            )
        };
        if !is_account && !is_system {
            return Err(refused(
                "access granted to a principal other than the account or SYSTEM",
            ));
        }
        if is_account && allowed.Mask & FILE_ALL_ACCESS == FILE_ALL_ACCESS {
            account_grant = true;
        }
    }
    if !account_grant {
        return Err(refused("account lacks full access"));
    }
    Ok(())
}

pub(super) fn validate(path: &Path) -> io::Result<()> {
    let handle = open_inspection(path, false)?;
    validate_handle(handle.0)
}

pub(super) fn validate_inheritable_directory(path: &Path) -> io::Result<()> {
    let handle = open_inspection(path, false)?;
    if attributes(handle.0)? & FILE_ATTRIBUTE_DIRECTORY == 0 {
        return Err(refused("SQLite parent is not a directory"));
    }
    validate_handle(handle.0)?;
    // Restrictive inherited ACEs are safe too: an ordinary nested directory below a
    // protected private root passes the same owner+SYSTEM policy to its children.
    let (_sd, _, dacl) = descriptor(handle.0)?;
    // SAFETY: dacl is valid and belongs to the live sd allocation.
    for i in 0..unsafe { (*dacl).AceCount } as u32 {
        let mut ace = null_mut();
        // SAFETY: The index is in range and ace is a writable output.
        if unsafe { GetAce(dacl, i, &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: validate_handle already checked that every ACE has the allowed layout.
        let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
        let required = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
        if allowed.Header.AceFlags & required != required
            || allowed.Mask & FILE_ALL_ACCESS != FILE_ALL_ACCESS
        {
            return Err(refused(
                "SQLite parent does not pass full private access to children",
            ));
        }
    }
    Ok(())
}

pub(super) fn open_file(path: &Path, options: &OpenOptions) -> io::Result<File> {
    let name = wide(path)?;
    let user = User::current()?;
    let mut security = Security::new(user.sid(), false)?;
    let sa = security.attributes();
    let access = READ_CONTROL
        | if options.read { GENERIC_READ } else { 0 }
        | if options.append {
            FILE_GENERIC_WRITE & !windows_sys::Win32::Storage::FileSystem::FILE_WRITE_DATA
                | FILE_APPEND_DATA
        } else if options.write {
            GENERIC_WRITE
        } else {
            0
        };
    if !options.read && !options.write && !options.append {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file open requires read or write access",
        ));
    }
    let create = options.create || options.create_new;
    // SAFETY: name and sa remain live; the security descriptor is supplied at creation.
    let mut raw = unsafe {
        CreateFileW(
            name.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &sa,
            if create { CREATE_NEW } else { OPEN_EXISTING },
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            null_mut(),
        )
    };
    if raw == INVALID_HANDLE_VALUE {
        let error = io::Error::last_os_error();
        if !options.create_new
            && options.create
            && matches!(error.raw_os_error(), Some(code) if code == ERROR_ALREADY_EXISTS as i32 || code == ERROR_FILE_EXISTS as i32)
        {
            // SAFETY: Existing files are opened without truncation or following a final reparse point.
            raw = unsafe {
                CreateFileW(
                    name.as_ptr(),
                    access,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    null(),
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                    null_mut(),
                )
            };
        } else {
            return Err(error);
        }
    }
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW returned an owned disk handle; File now owns its lifetime.
    let file = unsafe { File::from_raw_handle(raw) };
    validate_handle(file.as_raw_handle())?;
    if options.truncate {
        file.set_len(0)?;
    }
    Ok(file)
}

pub(super) fn create_directory(path: &Path, recursive: bool) -> io::Result<()> {
    if recursive {
        if path.as_os_str().is_empty() {
            return Ok(());
        }
        match std::fs::symlink_metadata(path) {
            Ok(meta) if meta.is_dir() && !super::is_reparse(&meta) => return Ok(()),
            Ok(_) => return Err(refused("directory is a reparse point or non-directory")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(parent) = path.parent() {
            create_directory(parent, true)?;
        }
    }
    let name = wide(path)?;
    let user = User::current()?;
    let mut security = Security::new(user.sid(), true)?;
    let sa = security.attributes();
    // SAFETY: The name and security descriptor remain alive across CreateDirectoryW.
    if unsafe { CreateDirectoryW(name.as_ptr(), &sa) } == 0 {
        let error = io::Error::last_os_error();
        if recursive && error.raw_os_error() == Some(ERROR_ALREADY_EXISTS as i32) && {
            let metadata = std::fs::symlink_metadata(path)?;
            metadata.is_dir() && !super::is_reparse(&metadata)
        } {
            return Ok(());
        }
        return Err(error);
    }
    Ok(())
}

pub(super) fn repair(path: &Path) -> io::Result<()> {
    let handle = open_inspection(path, true)?;
    let directory = attributes(handle.0)? & FILE_ATTRIBUTE_DIRECTORY != 0;
    let (_sd, _owner, _) = descriptor(handle.0)?;
    let user = User::current()?;
    let mut security = Security::new(user.sid(), directory)?;
    // SAFETY: The handle and ACL are live. Only the DACL is changed, never the owner.
    // Unlike SetSecurityInfo, SetKernelObjectSecurity does not propagate inheritable ACEs
    // to descendants. The maintenance walker must inspect each child without crossing links.
    if unsafe {
        SetKernelObjectSecurity(
            handle.0,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            (&mut security.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    validate_handle(handle.0)
}
