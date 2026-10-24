//! Byte-preserving validation shared by upload admission and tool capture.
//!
//! This layer has no profile or store: callers validate before publishing, then
//! publish the same bytes. Provider-specific limits belong at admission/rendering.

use broca_protocol::{MediaKind, MediaMarkerReason, MediaRef};
use sha2::{Digest, Sha256};

/// Fixed decoded-byte maxima from `docs/media-families.md`, independent of profiles.
pub const fn store_max_bytes(kind: MediaKind) -> u64 {
    match kind {
        MediaKind::Image => 33_554_432,
        MediaKind::Document | MediaKind::Audio => 52_428_800,
        MediaKind::Video => 67_108_864,
    }
}

/// Inspect received bytes without decoding pixels or transforming the payload.
///
/// The declared MIME is advisory only: a recognized, allowed magic signature wins
/// even when the declaration names another type. The returned digest and size are
/// over exactly `bytes`; callers must not publish anything on an error.
pub fn validate_media(bytes: &[u8], _declared_mime: &str) -> Result<MediaRef, MediaMarkerReason> {
    let (kind, mime) = detect(bytes).ok_or(MediaMarkerReason::Mime)?;
    if bytes.len() as u64 > store_max_bytes(kind) {
        return Err(MediaMarkerReason::TooLarge);
    }
    let dimensions = if kind == MediaKind::Image {
        Some(
            match mime {
                "image/png" => png(bytes),
                "image/jpeg" => jpeg(bytes),
                "image/gif" => gif(bytes),
                "image/webp" => webp(bytes),
                _ => None,
            }
            .ok_or(MediaMarkerReason::Undecodable)?,
        )
    } else {
        None
    };
    Ok(MediaRef {
        kind,
        mime: mime.into(),
        byte_size: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        width: dimensions.map(|(w, _)| w),
        height: dimensions.map(|(_, h)| h),
    })
}

fn detect(bytes: &[u8]) -> Option<(MediaKind, &'static str)> {
    use MediaKind::{Audio, Document, Image, Video};
    let detected = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        (Image, "image/png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        (Image, "image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        (Image, "image/gif")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        (Image, "image/webp")
    } else if bytes.starts_with(b"%PDF-") {
        (Document, "application/pdf")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE") {
        (Audio, "audio/wav")
    } else if bytes.starts_with(b"fLaC") {
        (Audio, "audio/flac")
    } else if bytes.starts_with(b"OggS") {
        (Audio, "audio/ogg")
    } else if bytes.starts_with(b"FORM") && matches!(bytes.get(8..12), Some(b"AIFF" | b"AIFC")) {
        (Audio, "audio/aiff")
    } else if bytes.starts_with(b"ID3") {
        (Audio, "audio/mpeg")
    } else if bytes.len() >= 4 && bytes[0] == 0xff && bytes[1] & 0xf6 == 0xf0 {
        (Audio, "audio/aac")
    } else if bytes.len() >= 4
        && bytes[0] == 0xff
        && bytes[1] & 0xe0 == 0xe0
        && bytes[1] & 0x06 != 0
        && bytes[1] & 0x18 != 0x08
        && bytes[2] & 0xf0 != 0xf0
        && bytes[2] & 0xf0 != 0
        && bytes[2] & 0x0c != 0x0c
    {
        (Audio, "audio/mpeg")
    } else if bytes.get(4..8) == Some(b"ftyp") {
        match bytes.get(8..12)? {
            b"M4A " | b"M4B " | b"M4P " => (Audio, "audio/mp4"),
            b"qt  " => (Video, "video/quicktime"),
            brand if brand.starts_with(b"3gp") => (Video, "video/3gpp"),
            b"isom" | b"iso2" | b"mp41" | b"mp42" | b"avc1" | b"dash" | b"M4V " => {
                (Video, "video/mp4")
            }
            _ => return None,
        }
    } else if bytes.starts_with(b"\x00\x00\x01\xb3") || bytes.starts_with(b"\x00\x00\x01\xba") {
        (Video, "video/mpeg")
    } else if bytes.starts_with(b"FLV\x01") {
        (Video, "video/x-flv")
    } else if bytes.starts_with(b"\x30\x26\xb2\x75\x8e\x66\xcf\x11\xa6\xd9\x00\xaa\x00\x62\xce\x6c")
    {
        (Video, "video/x-ms-wmv")
    } else if bytes.starts_with(b"\x1a\x45\xdf\xa3") {
        (Video, ebml_mime(bytes)?)
    } else {
        return None;
    };
    Some(detected)
}

// EBML's DocType distinguishes WebM from Matroska; a filename or declaration
// cannot distinguish the two containers. Only inspect elements in the header.
fn ebml_mime(bytes: &[u8]) -> Option<&'static str> {
    fn vint(bytes: &[u8], at: &mut usize, keep_marker: bool) -> Option<u64> {
        let first = *bytes.get(*at)?;
        let len = first.leading_zeros() as usize + 1;
        if len > 8 {
            return None;
        }
        let mut value = u64::from(if keep_marker {
            first
        } else {
            first & (0xff_u16 >> len) as u8
        });
        *at += 1;
        for _ in 1..len {
            value = (value << 8) | u64::from(*bytes.get(*at)?);
            *at += 1;
        }
        Some(value)
    }
    let mut at = 4;
    let len = usize::try_from(vint(bytes, &mut at, false)?).ok()?;
    let header = bytes.get(at..at.checked_add(len)?)?;
    at = 0;
    while at < header.len() {
        let id = vint(header, &mut at, true)?;
        let len = usize::try_from(vint(header, &mut at, false)?).ok()?;
        let value = take(header, &mut at, len)?;
        if id == 0x4282 {
            return match value {
                b"webm" => Some("video/webm"),
                b"matroska" => Some("video/x-matroska"),
                _ => None,
            };
        }
    }
    None
}

fn take<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = at.checked_add(len)?;
    let value = bytes.get(*at..end)?;
    *at = end;
    Some(value)
}

fn be32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes[..4].try_into().expect("four-byte field"))
}

fn le32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().expect("four-byte field"))
}

fn dimensions(width: u32, height: u32) -> Option<(u32, u32)> {
    (width > 0 && height > 0).then_some((width, height))
}

fn png(bytes: &[u8]) -> Option<(u32, u32)> {
    let mut at = 8;
    let mut size = None;
    let mut image_data = false;
    loop {
        let len = be32(take(bytes, &mut at, 4)?) as usize;
        let chunk = take(bytes, &mut at, len.checked_add(4)?)?;
        let crc = be32(take(bytes, &mut at, 4)?);
        if png_crc(chunk) != crc {
            return None;
        }
        let (kind, data) = chunk.split_at(4);
        if size.is_none() && kind != b"IHDR" {
            return None;
        }
        match kind {
            b"IHDR" => {
                if size.is_some() || data.len() != 13 {
                    return None;
                }
                size = dimensions(be32(data), be32(&data[4..]));
                size?;
                let valid_depth = match data[9] {
                    0 => matches!(data[8], 1 | 2 | 4 | 8 | 16),
                    2 | 4 | 6 => matches!(data[8], 8 | 16),
                    3 => matches!(data[8], 1 | 2 | 4 | 8),
                    _ => false,
                };
                if !valid_depth || data[10] != 0 || data[11] != 0 || data[12] > 1 {
                    return None;
                }
            }
            b"IDAT" => image_data = true,
            b"IEND" => return (len == 0 && image_data && at == bytes.len()).then_some(size?),
            _ => {}
        }
    }
}

const PNG_CRC_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
};

fn png_crc(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc = PNG_CRC_TABLE[((crc ^ u32::from(byte)) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

fn jpeg(bytes: &[u8]) -> Option<(u32, u32)> {
    let mut at = 2;
    let mut size = None;
    let mut scanned = false;
    let mut entropy = false;
    loop {
        if entropy {
            while *bytes.get(at)? != 0xff {
                at += 1;
            }
        } else if *bytes.get(at)? != 0xff {
            return None;
        }
        while *bytes.get(at)? == 0xff {
            at += 1;
        }
        let marker = *bytes.get(at)?;
        at += 1;
        if entropy && (marker == 0 || (0xd0..=0xd7).contains(&marker)) {
            continue;
        }
        entropy = false;
        match marker {
            0xd9 => return (scanned && at == bytes.len()).then_some(size?),
            0 | 0xd8 | 0xd0..=0xd7 => return None,
            0x01 => continue,
            _ => {}
        }
        let len_bytes = take(bytes, &mut at, 2)?;
        let len = usize::from(u16::from_be_bytes(len_bytes.try_into().ok()?));
        let data = take(bytes, &mut at, len.checked_sub(2)?)?;
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            if data.len() < 6 || data[5] == 0 || data.len() != 6 + 3 * usize::from(data[5]) {
                return None;
            }
            let height = u32::from(u16::from_be_bytes([data[1], data[2]]));
            let width = u32::from(u16::from_be_bytes([data[3], data[4]]));
            size = dimensions(width, height);
            size?;
        } else if marker == 0xda {
            if size.is_none()
                || data.is_empty()
                || data[0] == 0
                || data.len() != 4 + 2 * usize::from(data[0])
            {
                return None;
            }
            scanned = true;
            entropy = true;
        }
    }
}

fn gif(bytes: &[u8]) -> Option<(u32, u32)> {
    fn subblocks(bytes: &[u8], at: &mut usize) -> Option<()> {
        loop {
            let len = usize::from(take(bytes, at, 1)?[0]);
            if len == 0 {
                return Some(());
            }
            take(bytes, at, len)?;
        }
    }
    fn color_table(bytes: &[u8], at: &mut usize, flags: u8) -> Option<()> {
        if flags & 0x80 != 0 {
            take(bytes, at, 3 * (1 << ((flags & 7) + 1)))?;
        }
        Some(())
    }
    let mut at = 6;
    let screen = take(bytes, &mut at, 7)?;
    let size = dimensions(
        u32::from(u16::from_le_bytes([screen[0], screen[1]])),
        u32::from(u16::from_le_bytes([screen[2], screen[3]])),
    )?;
    color_table(bytes, &mut at, screen[4])?;
    let mut image = false;
    loop {
        match take(bytes, &mut at, 1)?[0] {
            0x3b => return (image && at == bytes.len()).then_some(size),
            0x21 => {
                take(bytes, &mut at, 1)?;
                subblocks(bytes, &mut at)?;
            }
            0x2c => {
                let descriptor = take(bytes, &mut at, 9)?;
                if descriptor[4..6] == [0, 0] || descriptor[6..8] == [0, 0] {
                    return None;
                }
                color_table(bytes, &mut at, descriptor[8])?;
                if !(2..=8).contains(&take(bytes, &mut at, 1)?[0]) {
                    return None;
                }
                subblocks(bytes, &mut at)?;
                image = true;
            }
            _ => return None,
        }
    }
}

fn webp(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.get(8..12) != Some(b"WEBP")
        || u64::from(le32(bytes.get(4..8)?)) + 8 != bytes.len() as u64
    {
        return None;
    }
    let mut at = 12;
    let mut size = None;
    let mut image = false;
    while at < bytes.len() {
        let kind = take(bytes, &mut at, 4)?;
        let len = le32(take(bytes, &mut at, 4)?) as usize;
        let data = take(bytes, &mut at, len)?;
        if !len.is_multiple_of(2) {
            take(bytes, &mut at, 1)?;
        }
        let header_size = match kind {
            b"VP8 " | b"VP8L" => {
                image = true;
                webp_image_size(kind, data)?
            }
            b"VP8X" => {
                if data.len() != 10 {
                    return None;
                }
                let read24 =
                    |b: &[u8]| u32::from(b[0]) | (u32::from(b[1]) << 8) | (u32::from(b[2]) << 16);
                (read24(&data[4..]) + 1, read24(&data[7..]) + 1)
            }
            b"ANMF" => {
                // A frame has padded image chunks of its own. A complete outer
                // RIFF does not prove that these nested chunks fit the frame.
                if data.len() < 16 || !webp_frame(&data[16..]) {
                    return None;
                }
                image = true;
                continue;
            }
            _ => continue,
        };
        if size.is_none() {
            size = Some(header_size);
        }
    }
    if image {
        size
    } else {
        None
    }
}

fn webp_image_size(kind: &[u8], data: &[u8]) -> Option<(u32, u32)> {
    match kind {
        b"VP8 " => {
            if data.len() < 10 || data[0] & 1 != 0 || data[3..6] != [0x9d, 1, 0x2a] {
                return None;
            }
            dimensions(
                u32::from(u16::from_le_bytes([data[6], data[7]]) & 0x3fff),
                u32::from(u16::from_le_bytes([data[8], data[9]]) & 0x3fff),
            )
        }
        b"VP8L" => {
            if data.len() < 5 || data[0] != 0x2f || data[4] & 0xe0 != 0 {
                return None;
            }
            let bits = le32(&data[1..]);
            Some(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1))
        }
        _ => None,
    }
}

fn webp_frame(bytes: &[u8]) -> bool {
    let mut at = 0;
    let mut image = false;
    while at < bytes.len() {
        let Some(header) = take(bytes, &mut at, 8) else {
            return false;
        };
        let len = le32(&header[4..]) as usize;
        let Some(data) = take(bytes, &mut at, len) else {
            return false;
        };
        if !len.is_multiple_of(2) && take(bytes, &mut at, 1).is_none() {
            return false;
        }
        match &header[..4] {
            b"VP8 " | b"VP8L" => {
                if webp_image_size(&header[..4], data).is_none() {
                    return false;
                }
                image = true;
            }
            b"ALPH" => {}
            _ => return false,
        }
    }
    image
}

#[cfg(test)]
mod tests;
