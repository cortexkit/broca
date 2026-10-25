use std::io;

/// A rig-owned process identity, recorded immediately after spawn rather than rediscovered
/// by command name at teardown. Start times prevent stale records from targeting reused PIDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordedProcess {
    pub pid: u32,
    pub start_time: u64,
}

impl RecordedProcess {
    pub fn capture(pid: u32) -> io::Result<Self> {
        start_time(pid).map(|start_time| Self { pid, start_time })
    }

    pub fn is_running(self) -> bool {
        Self::capture(self.pid).is_ok_and(|current| current == self)
    }

    /// Signal only the recorded incarnation of the process. Missing or unverifiable
    /// identities are never signalled; cleanup must not adopt a new owner of the PID.
    pub fn signal(self, signal: nix::sys::signal::Signal) -> io::Result<()> {
        if self.is_running() {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(self.pid as i32), signal)
                .map_err(io::Error::from)?;
        }
        Ok(())
    }

    pub fn stop(self) -> io::Result<()> {
        self.signal(nix::sys::signal::Signal::SIGSTOP)
    }

    pub fn kill(self) -> io::Result<()> {
        self.signal(nix::sys::signal::Signal::SIGKILL)
    }
}

#[cfg(target_os = "macos")]
fn start_time(pid: u32) -> io::Result<u64> {
    use libproc::libproc::bsd_info::BSDInfo;
    use libproc::libproc::proc_pid::pidinfo;
    let info = pidinfo::<BSDInfo>(pid as i32, 0).map_err(io::Error::other)?;
    // Zombies cannot execute or write into the rig, even before init reaps them.
    if info.pbi_status == 5 {
        return Err(io::ErrorKind::NotFound.into());
    }
    Ok(info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec)
}

#[cfg(target_os = "linux")]
fn start_time(pid: u32) -> io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    // The process command-name field may contain spaces and ')'; the last ')'
    // ends it. Field 22 is kernel start time in ticks, not rounded wall-clock time.
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or_else(|| io::Error::other("process stat has no command terminator"))?
        .1
        .split_whitespace()
        .collect();
    if matches!(fields.first(), Some(&"Z" | &"X")) {
        return Err(io::ErrorKind::NotFound.into());
    }
    fields
        .get(19)
        .and_then(|time| time.parse().ok())
        .ok_or_else(|| io::Error::other("process stat has no start time"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stale_start_time_never_signals_a_live_process() {
        let current = RecordedProcess::capture(std::process::id()).unwrap();
        let stale = RecordedProcess {
            start_time: current.start_time + 1,
            ..current
        };
        assert!(current.is_running());
        assert!(!stale.is_running());
        stale.signal(nix::sys::signal::Signal::SIGKILL).unwrap();
        assert!(current.is_running());
    }
}
