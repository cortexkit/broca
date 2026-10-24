use super::*;

// PNG chunks and CRCs were generated with Python's zlib, not the validator's CRC.
const PNG: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
const WIDE_PNG: &str = "89504e470d0a1a0a0000000d49484452000000030000000208020000001216f14d0000001049444154789c63f8cfc000410c70160041d205fb87f0b9480000000049454e44ae426082";
const GIF: &str =
    "47494638396101000100800000000000ffffff21f90401000000002c00000000010001000002024401003b";
const WEBP: &str =
    "52494646220000005745425056503820160000003001009d012a010001000ec0fe25a400037000000000";
// A one-pixel JPEG encoded by sips, including EXIF, tables and a restart interval.
const JPEG: &str = concat!(
    "ffd8ffe000104a46494600010100004800480000ffe1004c4578696600004d4d002a00000008000187690004000000010000001a000000000003a00100030000000100010000a00200040000000100000001a0030004000000010000000100000000ffed003850686f746f73686f7020332e30003842494d04040000000000003842494d0425000000000010d41d8cd98f00b204e9800998ecf8427effc00011080001000103012200021101031101",
    "ffc4001f0000010501010101010100000000000000000102030405060708090a0bffc400b5100002010303020403050504040000017d01020300041105122131410613516107227114328191a1082342b1c11552d1f02433627282090a161718191a25262728292a3435363738393a434445464748494a535455565758595a636465666768696a737475767778797a838485868788898a92939495969798999aa2a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7d8d9dae1e2e3e4e5e6e7e8e9eaf1f2f3f4f5f6f7f8f9fa",
    "ffc4001f0100030101010101010101010000000000000102030405060708090a0bffc400b51100020102040403040705040400010277000102031104052131061241510761711322328108144291a1b1c109233352f0156272d10a162434e125f11718191a262728292a35363738393a434445464748494a535455565758595a636465666768696a737475767778797a82838485868788898a92939495969798999aa2a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7d8d9dae2e3e4e5e6e7e8e9eaf2f3f4f5f6f7f8f9fa",
    "ffdb004300020202020202030202030503030305060505050506080606060606080a0808080808080a0a0a0a0a0a0a0a0c0c0c0c0c0c0e0e0e0e0e0f0f0f0f0f0f0f0f0f0fffdb00430102020204040407040407100b090b1010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010ffdd00040001ffda000c03010002110311003f00f8be8a28afe533fdfc3fffd9"
);

fn unhex(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn png_magic_overrides_declared_jpeg_without_changing_bytes() {
    let bytes = unhex(PNG);
    let original = bytes.clone();
    let media = validate_media(&bytes, "image/jpeg").unwrap();
    assert_eq!(media.kind, MediaKind::Image);
    assert_eq!(media.mime, "image/png");
    assert_eq!(media.byte_size, 69);
    assert_eq!(
        media.sha256,
        "b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640"
    );
    assert_eq!(bytes, original);
    assert_eq!(
        validate_media(&bytes, "application/octet-stream").unwrap(),
        media
    );
}

#[test]
fn image_dimensions_come_from_headers() {
    let media = validate_media(&unhex(WIDE_PNG), "image/png").unwrap();
    assert_eq!((media.width, media.height), (Some(3), Some(2)));
    for (fixture, mime) in [(JPEG, "image/jpeg"), (GIF, "image/gif")] {
        let media = validate_media(&unhex(fixture), mime).unwrap();
        assert_eq!(media.mime, mime);
        assert_eq!((media.width, media.height), (Some(1), Some(1)));
    }
}

#[test]
fn valid_webp_is_accepted_with_original_digest() {
    let media = validate_media(&unhex(WEBP), "image/webp").unwrap();
    assert_eq!(media.mime, "image/webp");
    assert_eq!(media.byte_size, 42);
    assert_eq!((media.width, media.height), (Some(1), Some(1)));
    assert_eq!(
        media.sha256,
        "c90cff659645a312a28804965f3dbc34061338f7234ff5d6ddb2c57e9eadec15"
    );
}

#[test]
fn png_intact_header_does_not_hide_a_truncated_container() {
    let bytes = unhex(PNG);
    for end in 8..bytes.len() {
        assert_eq!(
            validate_media(&bytes[..end], "image/png"),
            Err(MediaMarkerReason::Undecodable),
            "prefix {end}"
        );
    }
    let mut too_long = bytes.clone();
    too_long[33..37].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        validate_media(&too_long, "image/png"),
        Err(MediaMarkerReason::Undecodable)
    );
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        validate_media(&trailing, "image/png"),
        Err(MediaMarkerReason::Undecodable)
    );
}

#[test]
fn png_crc_covers_body_and_end_chunks() {
    for at in [29, 45, 56, 68] {
        let mut bytes = unhex(PNG);
        bytes[at] ^= 1;
        assert_eq!(
            validate_media(&bytes, "image/png"),
            Err(MediaMarkerReason::Undecodable),
            "corruption at {at}"
        );
    }
}

#[test]
fn jpeg_walks_segments_and_entropy_to_eoi() {
    let bytes = unhex(JPEG);
    for end in 3..bytes.len() {
        assert_eq!(
            validate_media(&bytes[..end], "image/jpeg"),
            Err(MediaMarkerReason::Undecodable),
            "prefix {end}"
        );
    }
    let mut malformed = bytes.clone();
    malformed[4..6].copy_from_slice(&[0xff, 0xff]);
    assert_eq!(
        validate_media(&malformed, "image/jpeg"),
        Err(MediaMarkerReason::Undecodable)
    );
    // Stuffed FF and restart markers stay in scan data, not length-bearing segments.
    let mut escaped = bytes[..bytes.len() - 2].to_vec();
    escaped.extend_from_slice(b"\xff\x00\x42\xff\xd0\x21\xff\xd9");
    assert!(validate_media(&escaped, "image/jpeg").is_ok());
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        validate_media(&trailing, "image/jpeg"),
        Err(MediaMarkerReason::Undecodable)
    );
}

#[test]
fn gif_walks_tables_extensions_and_subblocks_to_trailer() {
    let bytes = unhex(GIF);
    for end in 6..bytes.len() {
        assert_eq!(
            validate_media(&bytes[..end], "image/gif"),
            Err(MediaMarkerReason::Undecodable),
            "prefix {end}"
        );
    }
    let mut malformed = bytes.clone();
    malformed[37] = 255;
    assert_eq!(
        validate_media(&malformed, "image/gif"),
        Err(MediaMarkerReason::Undecodable)
    );
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        validate_media(&trailing, "image/gif"),
        Err(MediaMarkerReason::Undecodable)
    );
}

#[test]
fn webp_checks_riff_size_and_chunk_boundaries() {
    let bytes = unhex(WEBP);
    for end in 12..bytes.len() {
        assert_eq!(
            validate_media(&bytes[..end], "image/webp"),
            Err(MediaMarkerReason::Undecodable),
            "prefix {end}"
        );
    }
    let mut wrong_size = bytes.clone();
    wrong_size[4] += 1;
    assert_eq!(
        validate_media(&wrong_size, "image/webp"),
        Err(MediaMarkerReason::Undecodable)
    );
    let mut wrong_chunk = bytes.clone();
    wrong_chunk[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        validate_media(&wrong_chunk, "image/webp"),
        Err(MediaMarkerReason::Undecodable)
    );
    let mut wrong_form = bytes;
    wrong_form[8..12].copy_from_slice(b"XXXX");
    assert_eq!(
        validate_media(&wrong_form, "image/webp"),
        Err(MediaMarkerReason::Mime)
    );
}

fn riff_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut bytes = kind.to_vec();
    bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
    bytes.extend_from_slice(data);
    if !data.len().is_multiple_of(2) {
        bytes.push(0);
    }
    bytes
}

fn webp_container(chunks: &[u8]) -> Vec<u8> {
    let mut bytes = b"RIFF".to_vec();
    bytes.extend_from_slice(&(4 + chunks.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"WEBP");
    bytes.extend_from_slice(chunks);
    bytes
}

#[test]
fn webp_lossless_extended_and_odd_chunk_padding() {
    let lossless = riff_chunk(b"VP8L", &[0x2f, 1, 0x40, 0, 0]);
    let media = validate_media(&webp_container(&lossless), "image/webp").unwrap();
    assert_eq!((media.width, media.height), (Some(2), Some(2)));
    let mut extended = riff_chunk(b"VP8X", &[0, 0, 0, 0, 2, 0, 0, 1, 0, 0]);
    extended.extend_from_slice(&unhex(WEBP)[12..]);
    extended.extend_from_slice(&riff_chunk(b"EXIF", &[42]));
    let media = validate_media(&webp_container(&extended), "image/webp").unwrap();
    assert_eq!((media.width, media.height), (Some(3), Some(2)));
    extended.pop();
    assert_eq!(
        validate_media(&webp_container(&extended), "image/webp"),
        Err(MediaMarkerReason::Undecodable)
    );
}

#[test]
fn animated_webp_walks_nested_frame_chunks() {
    let mut chunks = riff_chunk(b"VP8X", &[2, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    chunks.extend_from_slice(&riff_chunk(b"ANIM", &[0; 6]));
    let mut frame = vec![0; 16];
    frame.extend_from_slice(&unhex(WEBP)[12..]);
    let mut complete = chunks.clone();
    complete.extend_from_slice(&riff_chunk(b"ANMF", &frame));
    let media = validate_media(&webp_container(&complete), "image/webp").unwrap();
    assert_eq!((media.width, media.height), (Some(1), Some(1)));
    frame.pop();
    chunks.extend_from_slice(&riff_chunk(b"ANMF", &frame));
    assert_eq!(
        validate_media(&webp_container(&chunks), "image/webp"),
        Err(MediaMarkerReason::Undecodable)
    );
}

#[test]
fn outside_table_types_and_unknown_magic_are_mime() {
    for bytes in [
        b"".as_slice(),
        b"not an image",
        b"BM\0\0\0\0",
        b"II\x2a\0",
        b"<svg/>",
        b"RIFF\0\0\0\0AVI ",
    ] {
        assert_eq!(
            validate_media(bytes, "image/png"),
            Err(MediaMarkerReason::Mime)
        );
    }
    let avif = b"\0\0\0\x18ftypavif\0\0\0\0";
    assert_eq!(
        validate_media(avif, "image/avif"),
        Err(MediaMarkerReason::Mime)
    );
    assert_eq!(
        validate_media(b"\0\0\0\x18ftyp3g2a", "video/3gpp2"),
        Err(MediaMarkerReason::Mime)
    );
}

#[test]
fn all_document_audio_video_table_types_are_detected() {
    use MediaKind::{Audio, Document, Video};
    for (bytes, kind, mime) in [
        (b"%PDF-1.7\n".as_slice(), Document, "application/pdf"),
        (b"RIFF\0\0\0\0WAVE", Audio, "audio/wav"),
        (b"ID3\x04\0\0", Audio, "audio/mpeg"),
        (b"\xff\xfb\x90\0", Audio, "audio/mpeg"),
        (b"\xff\xf1\x50\x80", Audio, "audio/aac"),
        (b"OggS\0", Audio, "audio/ogg"),
        (b"fLaC\0", Audio, "audio/flac"),
        (b"FORM\0\0\0\0AIFF", Audio, "audio/aiff"),
        (b"\0\0\0\x18ftypM4A ", Audio, "audio/mp4"),
        (b"\0\0\0\x18ftypmp42", Video, "video/mp4"),
        (b"\0\0\x01\xba", Video, "video/mpeg"),
        (b"\0\0\0\x18ftypqt  ", Video, "video/quicktime"),
        (b"\x1a\x45\xdf\xa3\x87\x42\x82\x84webm", Video, "video/webm"),
        (b"FLV\x01", Video, "video/x-flv"),
        (
            b"\x30\x26\xb2\x75\x8e\x66\xcf\x11\xa6\xd9\x00\xaa\x00\x62\xce\x6c",
            Video,
            "video/x-ms-wmv",
        ),
        (b"\0\0\0\x18ftyp3gp5", Video, "video/3gpp"),
        (
            b"\x1a\x45\xdf\xa3\x8b\x42\x82\x88matroska",
            Video,
            "video/x-matroska",
        ),
    ] {
        let media = validate_media(bytes, "application/octet-stream").unwrap();
        assert_eq!((media.kind, media.mime.as_str()), (kind, mime));
        assert_eq!((media.width, media.height), (None, None));
    }
}

#[test]
fn fixed_store_maxima_reject_oversized_payloads_before_publication() {
    use MediaKind::{Audio, Document, Image, Video};
    for (kind, maximum, prefix) in [
        (Image, 33_554_432, unhex(PNG)),
        (Document, 52_428_800, b"%PDF-1.7\n".to_vec()),
        (Audio, 52_428_800, b"fLaC".to_vec()),
        (Video, 67_108_864, b"\0\0\0\x18ftypmp42".to_vec()),
    ] {
        assert_eq!(store_max_bytes(kind), maximum);
        let mut bytes = prefix;
        bytes.resize(maximum as usize + 1, 0);
        assert_eq!(
            validate_media(&bytes, "ignored"),
            Err(MediaMarkerReason::TooLarge)
        );
    }
}

#[test]
fn store_maximum_is_inclusive_and_not_a_profile_limit() {
    let mut bytes = b"%PDF-1.7\n".to_vec();
    bytes.resize(52_428_800, b' ');
    let media = validate_media(&bytes, "image/png").unwrap();
    assert_eq!(media.kind, MediaKind::Document);
    assert_eq!(media.byte_size, 52_428_800);
}
