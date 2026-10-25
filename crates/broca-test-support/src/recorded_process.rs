use std::io;
use std::time::{Duration, Instant};

const CLEANUP_GRACE: Duration = Duration::from_secs(5);

/// A rig-owned process identity, recorded immediately after spawn rather than rediscovered
/// by command name at teardown. Start times prevent stale records from targeting reused PIDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordedProcess {
    pub pid: u32,
    pub start_time: u64,
}

impl RecordedProcess {
    pub fn capture(pid: u32) -> io::Result<Self> {
        positive_pid(pid)?;
        process_info(pid).map(|(start_time, _, _)| Self { pid, start_time })
    }

    pub fn parent_pid(self) -> io::Result<u32> {
        let (start_time, parent, _) = process_info(self.pid)?;
        if start_time != self.start_time {
            return Err(io::ErrorKind::NotFound.into());
        }
        Ok(parent)
    }

    pub fn is_running(self) -> bool {
        Self::capture(self.pid).is_ok_and(|current| current == self)
    }

    /// Signal only the recorded incarnation of the process. Missing or unverifiable
    /// identities are never signalled; cleanup must not adopt a new owner of the PID.
    fn signal(self, signal: nix::sys::signal::Signal) -> io::Result<()> {
        let pid = positive_pid(self.pid)?;
        if self.is_running() {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), signal)
                .map_err(io::Error::from)?;
        }
        Ok(())
    }

    pub fn stop(self) -> io::Result<()> {
        self.signal(nix::sys::signal::Signal::SIGSTOP)?;
        let deadline = Instant::now() + CLEANUP_GRACE;
        while self.is_running() {
            if process_info(self.pid).is_ok_and(|(_, _, stopped)| stopped) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("process did not stop: {self:?}"),
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    pub fn kill(self) -> io::Result<()> {
        self.signal(nix::sys::signal::Signal::SIGKILL)
    }

    /// Drop cannot await Tokio children on a current-thread runtime. Use one
    /// shared synchronous grace for recorded descendants, including orphans.
    pub fn await_exit(processes: &[Self]) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_GRACE;
        while processes.iter().any(|process| process.is_running()) {
            if Instant::now() >= deadline {
                let survivors: Vec<_> = processes.iter().filter(|p| p.is_running()).collect();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("recorded processes survived cleanup: {survivors:?}"),
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}

fn positive_pid(pid: u32) -> io::Result<i32> {
    i32::try_from(pid)
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a positive process PID",
            )
        })
}

#[cfg(target_os = "macos")]
fn process_info(pid: u32) -> io::Result<(u64, u32, bool)> {
    use libproc::libproc::bsd_info::BSDInfo;
    use libproc::libproc::proc_pid::pidinfo;
    let info = pidinfo::<BSDInfo>(pid as i32, 0).map_err(io::Error::other)?;
    // Zombies cannot execute or write into the rig, even before init reaps them.
    if info.pbi_status == 5 {
        return Err(io::ErrorKind::NotFound.into());
    }
    Ok((
        info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
        info.pbi_ppid,
        info.pbi_status == 4,
    ))
}

#[cfg(target_os = "linux")]
fn process_info(pid: u32) -> io::Result<(u64, u32, bool)> {
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
    let start = fields
        .get(19)
        .and_then(|time| time.parse().ok())
        .ok_or_else(|| io::Error::other("process stat has no start time"))?;
    let parent = fields
        .get(1)
        .and_then(|parent| parent.parse().ok())
        .ok_or_else(|| io::Error::other("process stat has no parent pid"))?;
    Ok((start, parent, matches!(fields.first(), Some(&"T" | &"t"))))
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

    #[test]
    fn zero_and_negative_process_group_selectors_are_rejected() {
        for pid in [0, u32::MAX, i32::MAX as u32 + 1] {
            assert_eq!(
                RecordedProcess { pid, start_time: 0 }
                    .kill()
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
