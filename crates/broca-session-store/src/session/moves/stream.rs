use super::*;
use std::io::{Read as _, Seek as _, Write as _};

fn digest_frame(file: &mut std::fs::File, len: u64) -> Result<[u8; 32], Error> {
    let mut hash = Sha256::new();
    let mut left = len;
    let mut buffer = [0; 65536];
    while left > 0 {
        let take = left.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..take])?;
        hash.update(&buffer[..take]);
        left -= take as u64;
    }
    Ok(hash.finalize().into())
}

/// Find a covered frame boundary without allocating old snapshots, checking each retained
/// frame. The tail of a partially admitted frame must still be present and authenticated.
pub fn stream_prefix(path: &Path, covered: u64) -> Result<u64, Error> {
    if covered == 0 {
        return Ok(0);
    }
    let mut file = std::fs::File::open(path)?;
    if file.metadata()?.len() < covered {
        return Err(invalid("cached stream is short"));
    }
    let mut magic = vec![0; STREAM.len()];
    file.read_exact(&mut magic)?;
    if magic != STREAM {
        return Err(invalid("cached stream format"));
    }
    let mut end = STREAM.len() as u64;
    while end < covered {
        let mut header = [0; 40];
        file.read_exact(&mut header)?;
        let len = u64::from_le_bytes(header[..8].try_into().unwrap());
        if digest_frame(&mut file, len)? != header[8..] {
            return Err(invalid("cached stream digest"));
        }
        end = file.stream_position()?;
    }
    Ok(end)
}

/// Read only the last snapshot into memory; hash the complete sealed prefix incrementally.
pub fn stream_file(path: &Path, budget: u64) -> Result<(Vec<u8>, u64, String), Error> {
    let end = std::fs::metadata(path)?.len();
    stream_prefix(path, end)?;
    let mut file = std::fs::File::open(path)?;
    file.seek(std::io::SeekFrom::Start(STREAM.len() as u64))?;
    let mut latest = None;
    while file.stream_position()? < end {
        let at = file.stream_position()?;
        let mut header = [0; 40];
        file.read_exact(&mut header)?;
        let len = u64::from_le_bytes(header[..8].try_into().unwrap());
        let next = file
            .stream_position()?
            .checked_add(len)
            .ok_or_else(|| invalid("stream length overflow"))?;
        if next > end {
            return Err(invalid("stream tail missing"));
        }
        latest = Some((at, len));
        file.seek(std::io::SeekFrom::Start(next))?;
    }
    let (at, len) = latest.ok_or_else(|| invalid("empty stream"))?;
    if len > budget {
        return Err(invalid("newest export exceeds move-import budget"));
    }
    file.seek(std::io::SeekFrom::Start(at))?;
    let mut latest = STREAM.to_vec();
    let frame_len = usize::try_from(
        len.checked_add(40)
            .ok_or_else(|| invalid("frame length overflow"))?,
    )
    .map_err(invalid)?;
    latest.resize(
        STREAM
            .len()
            .checked_add(frame_len)
            .ok_or_else(|| invalid("frame allocation overflow"))?,
        0,
    );
    file.read_exact(&mut latest[STREAM.len()..])?;
    file.seek(std::io::SeekFrom::Start(0))?;
    let digest = hex(&digest_frame(&mut file, end)?);
    Ok((latest, end, digest))
}

/// Publish the exact prefix before admission. Its old snapshots stay on disk, never in a
/// plaintext scratch file or an allocation proportional to the number of previous moves.
pub fn append_stream_file(path: &Path, prefix: u64, latest: &[u8]) -> Result<(u64, String), Error> {
    let parent = path.parent().ok_or_else(|| invalid("stream parent"))?;
    super::files::create_dirs(parent)?;
    let mut file = broca_owner_only::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    if file.metadata()?.len() < prefix {
        return Err(invalid("cached stream prefix missing"));
    }
    file.set_len(prefix)?;
    file.seek(std::io::SeekFrom::Start(prefix))?;
    file.write_all(if prefix == 0 {
        latest
    } else {
        latest
            .get(STREAM.len()..)
            .ok_or_else(|| invalid("latest frame header"))?
    })?;
    crate::durable::sync(&file)?;
    super::files::sync(parent)?;
    let end = file.stream_position()?;
    file.seek(std::io::SeekFrom::Start(0))?;
    Ok((end, hex(&digest_frame(&mut file, end)?)))
}

pub fn planned_stream(path: &Path, prefix: u64, latest: &[u8]) -> Result<(u64, String), Error> {
    let mut hash = Sha256::new();
    if prefix != 0 {
        let mut file = std::fs::File::open(path)?;
        let mut left = prefix;
        let mut buffer = [0; 65536];
        while left > 0 {
            let take = left.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..take])?;
            hash.update(&buffer[..take]);
            left -= take as u64;
        }
    }
    let suffix = if prefix == 0 {
        latest
    } else {
        &latest[STREAM.len()..]
    };
    hash.update(suffix);
    Ok((
        prefix
            .checked_add(suffix.len() as u64)
            .ok_or_else(|| invalid("stream offset overflow"))?,
        hex(&hash.finalize()),
    ))
}
