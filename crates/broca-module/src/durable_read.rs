use broca_wal::{FramedRecord, WalRangeRead, WalRangeRecords};

/// Absence and an unreadable history have different security meanings. Only a proven
/// contiguous range may establish that an owner, sender, key or tombstone was not recorded.
pub(crate) enum DurableRead<'a> {
    Proven(&'a [FramedRecord]),
    Absent,
    Unproven,
}

impl<'a> DurableRead<'a> {
    pub(crate) fn classify(range: &'a WalRangeRead, after: u64, head: Option<u64>) -> Self {
        let records = match range.records() {
            WalRangeRecords::Complete(records) => records,
            WalRangeRecords::Absent if head.is_none_or(|head| head == 0) && after == 0 => {
                return Self::Absent;
            }
            _ => return Self::Unproven,
        };
        let observed_head = records.last().map_or(after, |frame| frame.seq);
        let expected_head = head.unwrap_or(observed_head);
        let contiguous = expected_head >= after
            && records.iter().enumerate().all(|(index, frame)| {
                after
                    .checked_add(index as u64)
                    .and_then(|seq| seq.checked_add(1))
                    == Some(frame.seq)
            })
            && observed_head == expected_head;
        if contiguous {
            Self::Proven(records)
        } else {
            Self::Unproven
        }
    }

    pub(crate) fn records(self) -> Result<&'a [FramedRecord], String> {
        match self {
            Self::Proven(records) => Ok(records),
            Self::Absent => Ok(&[]),
            Self::Unproven => Err("WAL history coverage is unproven".into()),
        }
    }
}
