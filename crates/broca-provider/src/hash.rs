//! Small, platform-stable hash primitives for values that must reproduce identically
//! across builds, hosts, and a resume in a different process — e.g. the Antigravity
//! `requestId` digest and the cross-family tool-call-id alias.
//!
//! These deliberately do NOT use the standard library's `DefaultHasher`: its output is
//! process-seeded (randomized per run) AND explicitly not stable across releases, so it
//! cannot back a value whose bytes must be reproducible for cache/resume stability.

/// FNV-1a 64-bit over `seed`, with `salt` mixed into the initial basis so the same seed
/// can produce several independent 64-bit lanes (call with salt 0, 1, … for a wider
/// digest). A small, explicit, platform-stable hash.
pub fn fnv1a64(seed: &str, salt: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET ^ salt.wrapping_mul(FNV_PRIME);
    for byte in seed.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// A 128-bit platform-stable digest of `seed` as two salted FNV-1a lanes. Used where a
/// 64-bit space is too collision-prone for a fail-loud-but-rare guarantee (the tool-call
/// id alias).
pub fn fnv1a128(seed: &str) -> u128 {
    ((fnv1a64(seed, 0) as u128) << 64) | (fnv1a64(seed, 1) as u128)
}

/// Keep provider prompt-cache keys within the documented 64-character limit. Long opaque
/// caller scopes use a stable 128-bit FNV-1a digest so every render and affinity header names
/// the same cache scope after a restart.
pub fn clamp_prompt_cache_key(key: &str) -> String {
    const MAX_PROMPT_CACHE_KEY_LEN: usize = 64;
    if key.len() <= MAX_PROMPT_CACHE_KEY_LEN {
        return key.to_string();
    }
    format!("{:032x}", fnv1a128(key))
}

/// Lowercase+uppercase alphanumeric, the densest URL/identifier-safe alphabet that fits
/// the strictest tool-call-id grammar this codebase targets (Anthropic's
/// `^[a-zA-Z0-9_-]+$`).
const BASE62: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Base62-encode `n` (least-significant digit first — order is irrelevant, only
/// determinism + injectivity matter). A 128-bit value yields ~22 characters.
pub fn base62(mut n: u128) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut out = String::new();
    while n > 0 {
        out.push(BASE62[(n % 62) as usize] as char);
        n /= 62;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_is_deterministic_and_salt_separates_lanes() {
        assert_eq!(fnv1a64("abc", 0), fnv1a64("abc", 0));
        assert_ne!(fnv1a64("abc", 0), fnv1a64("abc", 1));
        assert_ne!(fnv1a64("abc", 0), fnv1a64("abd", 0));
    }

    #[test]
    fn prompt_cache_key_clamps_long_values_to_a_stable_digest() {
        let long = "caller-cache-scope-".repeat(5);
        let clamped = clamp_prompt_cache_key(&long);
        assert_eq!(clamped.len(), 32);
        assert_eq!(clamped, clamp_prompt_cache_key(&long));
        assert_eq!(clamp_prompt_cache_key("short"), "short");
    }

    #[test]
    fn base62_is_injective_and_alphabet_safe() {
        let a = base62(fnv1a128("one"));
        let b = base62(fnv1a128("two"));
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
        // ~22 chars for a 128-bit value — fits Anthropic's 64-char budget with margin.
        assert!(
            a.len() <= 22,
            "128-bit base62 is at most 22 chars, got {}",
            a.len()
        );
    }
}
