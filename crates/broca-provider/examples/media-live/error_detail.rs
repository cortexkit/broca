use std::{
    fs::{File, Metadata},
    io::{self, Write},
    path::Path,
};

use serde_json::{json, Value};

const MAX_RECORD_BYTES: usize = 1024;

pub struct ErrorDetail(File);

impl ErrorDetail {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = match broca_owner_only::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let before = std::fs::symlink_metadata(path)?;
                private_file(&before)?;
                broca_owner_only::validate(path)?;
                let file = broca_owner_only::OpenOptions::new()
                    .append(true)
                    .open(path)?;
                let opened = file.metadata()?;
                private_file(&opened)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if before.dev() != opened.dev() || before.ino() != opened.ino() {
                        return Err(io::Error::other("error-detail file changed while opening"));
                    }
                }
                file
            }
            Err(error) => return Err(error),
        };
        private_file(&file.metadata()?)?;
        Ok(Self(file))
    }

    /// Copy only error fields, never the response object or headers. Known credential values
    /// are removed even from this private opt-in file because providers may echo request data.
    pub fn record(
        &mut self,
        body: &[u8],
        fallback_type: &str,
        fallback_code: Option<&str>,
        fallback_message: &str,
        secret: &str,
    ) -> io::Result<()> {
        self.append_fields(
            wire_errors(body).into_iter().next(),
            fallback_type,
            fallback_code,
            fallback_message,
            secret,
        )
    }

    pub fn record_wire_errors(&mut self, body: &[u8], secret: &str) -> io::Result<bool> {
        let errors = wire_errors(body);
        let found = !errors.is_empty();
        for fields in errors {
            self.append_fields(
                Some(fields),
                "provider_error",
                None,
                "provider error fields unavailable",
                secret,
            )?;
        }
        Ok(found)
    }

    fn append_fields(
        &mut self,
        fields: Option<Value>,
        fallback_type: &str,
        fallback_code: Option<&str>,
        fallback_message: &str,
        secret: &str,
    ) -> io::Result<()> {
        let value = |name: &str| {
            fields
                .as_ref()
                .and_then(|fields| fields.get(name))
                .and_then(|value| match value {
                    Value::String(text) => Some(text.clone()),
                    Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
        };
        let clean = |text: &str, limit: usize| {
            let mut text = if secret.is_empty() {
                text.to_owned()
            } else {
                text.replace(secret, "[redacted]")
            };
            truncate(&mut text, limit);
            text
        };
        let kind = value("type").or_else(|| value("status"));
        let code = value("code");
        let message = value("message");
        let mut record = json!({
            "type": clean(kind.as_deref().unwrap_or(fallback_type), 64),
            "code": code.as_deref().or(fallback_code).map(|code| clean(code, 64)),
            "message": clean(message.as_deref().unwrap_or(fallback_message), MAX_RECORD_BYTES),
        });
        let mut bytes = serde_json::to_vec(&record)?;
        // JSON escaping can expand control characters. Measure the encoded record, including
        // its newline, rather than promising a byte cap over the unescaped message alone.
        while bytes.len() + 1 > MAX_RECORD_BYTES {
            let message = record["message"].as_str().unwrap_or_default();
            if message.is_empty() {
                return Err(io::Error::other("error-detail fields exceed limit"));
            }
            let mut shorter = message.to_owned();
            shorter.pop();
            record["message"] = Value::String(shorter);
            bytes = serde_json::to_vec(&record)?;
        }
        bytes.push(b'\n');
        self.0.write_all(&bytes)?;
        self.0.sync_data()
    }
}

fn private_file(metadata: &Metadata) -> io::Result<()> {
    if !metadata.is_file() || broca_owner_only::is_reparse(metadata) {
        return Err(io::Error::other(
            "error-detail path must be a regular private file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o777 != 0o600 || metadata.nlink() != 1 {
            return Err(io::Error::other(
                "error-detail file must be mode 0600 without hard links",
            ));
        }
    }
    Ok(())
}

fn truncate(text: &mut String, limit: usize) {
    if text.len() <= limit {
        return;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

fn wire_errors(bytes: &[u8]) -> Vec<Value> {
    fn fields(value: Value) -> Option<Value> {
        value
            .get("error")
            .filter(|error| error.is_object())
            .or_else(|| {
                value
                    .pointer("/response/error")
                    .filter(|error| error.is_object())
            })
            .cloned()
    }
    if let Ok(value) = serde_json::from_slice(bytes) {
        if let Some(error) = fields(value) {
            return vec![error];
        }
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return vec![];
    };
    text.lines()
        .filter_map(|line| {
            let payload = line.strip_prefix("data:")?.trim();
            fields(serde_json::from_str(payload).ok()?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> std::path::PathBuf {
        broca_test_support::scratch_root().join(format!("media-error-detail-{name}"))
    }

    #[test]
    fn private_error_records_append_are_capped_and_exclude_headers_and_secrets() {
        let path = path("append.jsonl");
        let secret = "synthetic-secret-not-a-credential";
        let body = json!({"headers":{"authorization":secret}, "error":{"type":"invalid_request_error", "code":"unsupported", "message":format!("{secret} {}", "界\n".repeat(2000))}}).to_string();
        let mut detail = ErrorDetail::open(&path).unwrap();
        detail
            .record(body.as_bytes(), "fallback", None, "fallback", secret)
            .unwrap();
        drop(detail);
        let mut detail = ErrorDetail::open(&path).unwrap();
        detail.record(b"data: {\"error\":{\"type\":\"billing_error\",\"code\":\"no_credit\",\"message\":\"unfunded\"}}\n", "fallback", None, "fallback", secret).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            assert!(line.len() <= 1024);
        }
        let text = String::from_utf8(bytes).unwrap();
        let records: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["type"], "invalid_request_error");
        assert_eq!(records[0]["code"], "unsupported");
        assert_eq!(records[1]["message"], "unfunded");
        assert!(!text.contains("authorization"));
        assert!(!text.contains("headers"));
        assert!(!text.contains(secret));
        assert!(text.contains("[redacted]"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn every_structured_stream_error_is_recorded_without_the_response_body() {
        let path = path("stream.jsonl");
        let mut detail = ErrorDetail::open(&path).unwrap();
        let body = b"data: {\"response\":{\"error\":{\"type\":\"billing_error\",\"code\":429,\"message\":\"no credit\"}},\"other\":\"not retained\"}\n\ndata: {\"error\":{\"type\":\"invalid_request_error\",\"code\":\"input\",\"message\":\"not available\"}}\n\n";
        assert!(detail.record_wire_errors(body, "").unwrap());
        let text = std::fs::read_to_string(&path).unwrap();
        let records: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["code"], "429");
        assert_eq!(records[1]["message"], "not available");
        assert!(!text.contains("not retained"));
    }

    #[cfg(unix)]
    #[test]
    fn error_detail_rejects_insecure_files_and_links_without_writing() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let path = path("insecure.jsonl");
        std::fs::write(&path, "unchanged").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(ErrorDetail::open(&path).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "unchanged");
        let link = path.with_extension("link");
        symlink(&path, &link).unwrap();
        assert!(ErrorDetail::open(&link).is_err());
    }
}
