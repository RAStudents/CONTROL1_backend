use crate::domain::error::{Error, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
pub(crate) fn sha(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(Sha256::digest(bytes))
}
pub(crate) fn canonical(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json_canonicalizer::to_vec(value).map_err(|_| Error::internal())
}

pub(crate) fn sign(key: &[u8], value: &impl Serialize) -> Result<String> {
    let encoded = URL_SAFE_NO_PAD.encode(canonical(value)?);
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| Error::internal())?;
    mac.update(encoded.as_bytes());
    Ok(format!(
        "{encoded}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    ))
}
pub(crate) fn verify<T: DeserializeOwned>(key: &[u8], token: &str) -> Option<T> {
    let (data, tag) = token.split_once('.')?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
    mac.update(data.as_bytes());
    mac.verify_slice(&URL_SAFE_NO_PAD.decode(tag).ok()?).ok()?;
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(data).ok()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn canonical_json_digest_and_cursor_signature() {
        let first: Value = serde_json::from_str(r#"{"b":"str","a":1}"#).unwrap();
        let second: Value = serde_json::from_str(r#"{"a":1,"b":"str"}"#).unwrap();
        assert_eq!(canonical(&first).unwrap(), br#"{"a":1,"b":"str"}"#);
        assert_eq!(
            sha(canonical(&first).unwrap()),
            sha(canonical(&second).unwrap())
        );
        let token = sign(b"cursor A", &first).unwrap();
        assert_eq!(verify::<Value>(b"cursor A", &token), Some(second));
        assert!(verify::<Value>(b"cursor B", &token).is_none());
    }
}
