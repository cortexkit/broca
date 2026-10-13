//! Single-write diagnostics for processes sharing a supervisor log.

/// Write one diagnostic line to stderr in a single `write` call.
///
/// The supervisor gives each module its stderr by inheritance, so several processes share ONE
/// open file description on the same log. `eprintln!` formats incrementally and issues a
/// syscall per fragment, and no lock spans processes -- so a line emitted that way can be
/// interleaved with another process's line and arrive cut in half.
///
/// Formatting first and writing once keeps our own lines whole. It cannot keep them at the
/// start of a line: another process mid-write still lands its fragment ahead of ours, so a
/// reader has to match our lines unanchored. Whole-but-unanchored is the property worth
/// having, because a cut line silently fails any pattern longer than the surviving fragment,
/// and a scan that misses reports "this never happened" rather than failing.
pub fn diag(args: std::fmt::Arguments<'_>) {
    diag_to(&mut std::io::stderr(), args);
}

/// The write half, taking its sink so the single-write property can be asserted.
fn diag_to(sink: &mut impl std::io::Write, args: std::fmt::Arguments<'_>) {
    let mut line = format!("{args}");
    line.push('\n');
    let _ = sink.write_all(line.as_bytes());
}

/// `eprintln!` for the shared daemon log: same call shape, one syscall per line.
#[macro_export]
macro_rules! diagln {
    ($($arg:tt)*) => { $crate::diag(format_args!($($arg)*)) };
}

// START EVERY MESSAGE WITH `broca:` OR A `broca-<crate>:` PREFIX.
//
// The log is shared: the supervisor hands several modules the same file, and AFT,
// magic-context and others write to it too. A reader counting occurrences of a phrase is
// answering an attribution question whether or not they realise it, and generic wording
// belongs to whoever else also chose it. A grep for "store projection" here returns 4,710
// hits, none of them ours -- AFT emits "callgraph store projection unavailable", and our
// own projector warning has never fired. That reads as a live incident.
//
// Both directions are wrong and only one feels like a discovery: an over-count promises an
// incident that is not yours, while an under-count says a branch never fires and invites
// deleting it. The prefix is what makes the count mean something, and it has to be at the
// front, because a reader who cannot anchor to line start (see `diag`) matches on the prefix
// instead.

#[cfg(test)]
mod tests {
    use std::io::Write;

    /// Counts write calls so the single-syscall property is observable. The property cannot be
    /// asserted from the log's contents: interleaving needs a second process writing to the
    /// same open file description concurrently, and a passing in-process test would only prove
    /// that no one raced, not that a race is survivable.
    #[derive(Default)]
    struct CountingSink {
        writes: usize,
        bytes: Vec<u8>,
    }

    impl Write for CountingSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_diagnostic_line_is_one_write_however_many_fragments_it_has() {
        let mut sink = CountingSink::default();
        let module = "claustrum";
        let count = 12;
        super::diag_to(
            &mut sink,
            format_args!("broca: {module} is not catalog-live; {count} handles"),
        );

        assert_eq!(
            sink.writes, 1,
            "a line with interpolated fragments must still reach the log in ONE write"
        );
        assert_eq!(
            String::from_utf8(sink.bytes).unwrap(),
            "broca: claustrum is not catalog-live; 12 handles\n",
            "the newline rides in the same write rather than following as a second one"
        );
    }
}
