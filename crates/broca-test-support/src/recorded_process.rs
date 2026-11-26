use std::io;
use std::time::{Duration, Instant};

const CLEANUP_GRACE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A rig-owned process identity, recorded immediately after spawn rather than rediscovered
/// by command name at teardown. Start times prevent stale records from targeting reused PIDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordedProcess {
    pub pid: u32,
    pub start_time: u64,
}

impl RecordedProcess {
    pub fn capture(pid: u32) -> io::Result<Self> {
        process_info(positive_pid(pid)?).map(|(start_time, _, _)| Self { pid, start_time })
    }

    pub fn parent_pid(self) -> io::Result<u32> {
        self.matching_process_info()?
            .map(|(parent, _)| parent)
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    fn matching_process_info(self) -> io::Result<Option<(u32, bool)>> {
        match process_info(positive_pid(self.pid)?) {
            Ok((start_time, parent, stopped)) => {
                Ok((start_time == self.start_time).then_some((parent, stopped)))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn is_running(self) -> io::Result<bool> {
        Ok(self.matching_process_info()?.is_some())
    }

    /// Signal only the recorded incarnation of the process. Missing or unverifiable
    /// identities are never signalled; cleanup must not adopt a new owner of the PID.
    fn signal(self, signal: nix::sys::signal::Signal) -> io::Result<bool> {
        let pid = positive_pid(self.pid)?;
        if !self.is_running()? {
            return Ok(false);
        }
        match nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), signal) {
            Ok(()) => Ok(true),
            Err(nix::errno::Errno::ESRCH) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub fn stop(self) -> io::Result<()> {
        if !self.signal(nix::sys::signal::Signal::SIGSTOP)? {
            return Ok(());
        }
        let deadline = Instant::now() + CLEANUP_GRACE;
        while let Some((_, false)) = self.matching_process_info()? {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("process did not stop: {self:?}"),
                ));
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        Ok(())
    }

    pub fn kill(self) -> io::Result<()> {
        self.signal(nix::sys::signal::Signal::SIGKILL).map(|_| ())
    }

    /// Drop cannot await Tokio children on a current-thread runtime. Use one
    /// shared synchronous grace for recorded descendants, including orphans.
    pub fn kill_and_await_exit(processes: &[Self]) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_GRACE;
        let mut remaining: Vec<_> = processes.iter().map(|&process| (process, false)).collect();
        loop {
            let mut errors = Vec::new();
            remaining.retain_mut(|(process, signalled)| {
                let running = if *signalled {
                    process.is_running()
                } else {
                    // If the identity was unreadable, keep it pending instead of declaring
                    // it dead or signalling a PID whose start time we could not verify.
                    process
                        .signal(nix::sys::signal::Signal::SIGKILL)
                        .inspect(|sent| *signalled = *sent)
                };
                match running {
                    Ok(running) => running,
                    Err(error) => {
                        errors.push(format!("{process:?}: {error}"));
                        true
                    }
                }
            });
            if remaining.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let pending: Vec<_> = remaining.iter().map(|(process, _)| process).collect();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "recorded processes not confirmed exited: {pending:?}; errors: {errors:?}"
                    ),
                ));
            }
            std::thread::sleep(POLL_INTERVAL);
        }
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

fn lookup_failed(pid: i32, error: io::Error) -> io::Error {
    // An identity lookup can fail while the PID still exists. Signal 0 only probes
    // existence; it cannot authorize any later signal without the recorded start time.
    if matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
        Err(nix::errno::Errno::ESRCH)
    ) {
        io::ErrorKind::NotFound.into()
    } else {
        io::Error::other(error)
    }
}

#[cfg(target_os = "macos")]
fn process_info(pid: i32) -> io::Result<(u64, u32, bool)> {
    use libproc::libproc::bsd_info::BSDInfo;
    use libproc::libproc::proc_pid::pidinfo;
    // PROC_PIDTBSDINFO needs a nonzero argument to include zombies.
    let info =
        pidinfo::<BSDInfo>(pid, 1).map_err(|error| lookup_failed(pid, io::Error::other(error)))?;
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
fn process_info(pid: i32) -> io::Result<(u64, u32, bool)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|error| lookup_failed(pid, error))?;
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
        assert!(current.is_running().unwrap());
        assert!(!stale.is_running().unwrap());
        stale.signal(nix::sys::signal::Signal::SIGKILL).unwrap();
        assert!(current.is_running().unwrap());
    }

    #[test]
    fn a_failed_identity_lookup_does_not_confirm_a_live_pid_exited() {
        let pid = positive_pid(std::process::id()).unwrap();
        let lookup = io::Error::new(io::ErrorKind::NotFound, "injected identity lookup failure");
        let error = lookup_failed(pid, lookup);
        assert_ne!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(error.to_string(), "injected identity lookup failure");
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
