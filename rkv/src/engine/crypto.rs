use std::fs;
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use argon2::Argon2;

use super::error::{Error, Result};

/// Nonce size for AES-256-GCM (96 bits / 12 bytes).
const NONCE_LEN: usize = 12;

/// Salt size for Argon2id key derivation (128 bits / 16 bytes).
const SALT_LEN: usize = 16;

/// Derive a 256-bit encryption key from a password and salt using Argon2id.
pub(crate) fn derive_key(password: &str, salt: &[u8; SALT_LEN]) -> [u8; 32] {
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .expect("Argon2 key derivation should not fail with valid params");
    key
}

/// Encrypt plaintext using AES-256-GCM.
///
/// Returns `[nonce (12 bytes) || ciphertext || tag (16 bytes)]`.
pub(crate) fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let cipher = Aes256Gcm::new(key.into());
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .expect("AES-256-GCM encryption should not fail");
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    out
}

/// Decrypt ciphertext produced by [`encrypt`].
///
/// Expects input format: `[nonce (12 bytes) || ciphertext || tag (16 bytes)]`.
/// Returns the original plaintext, or `Error::Corruption` if the data is
/// tampered with or the key is wrong.
pub(crate) fn decrypt(key: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < NONCE_LEN {
        return Err(Error::Corruption(
            "encrypted data too short (missing nonce)".into(),
        ));
    }
    let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);
    let cipher = Aes256Gcm::new(key.into());
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| Error::Corruption("decryption failed (wrong key or corrupted data)".into()))
}

/// Generate a random 16-byte salt.
pub(crate) fn generate_salt() -> [u8; SALT_LEN] {
    let mut salt = [0u8; SALT_LEN];
    for byte in &mut salt {
        *byte = fastrand::u8(..);
    }
    salt
}

/// Load or create a per-namespace salt file at `<db_path>/crypto/<ns_name>.salt`.
///
/// If the file exists, reads and returns the salt. Otherwise generates a new
/// random salt, writes it to disk, and returns it.
pub(crate) fn load_or_create_salt(db_path: &Path, ns_name: &str) -> Result<[u8; SALT_LEN]> {
    let crypto_dir = db_path.join("crypto");
    let salt_path = crypto_dir.join(format!("{ns_name}.salt"));

    if salt_path.exists() {
        let data = fs::read(&salt_path).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!("failed to read salt file '{}': {e}", salt_path.display()),
            ))
        })?;
        if data.len() != SALT_LEN {
            return Err(Error::Corruption(format!(
                "salt file '{}' has invalid length {} (expected {SALT_LEN})",
                salt_path.display(),
                data.len()
            )));
        }
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&data);
        Ok(salt)
    } else {
        fs::create_dir_all(&crypto_dir).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "failed to create crypto dir '{}': {e}",
                    crypto_dir.display()
                ),
            ))
        })?;
        let salt = generate_salt();
        fs::write(&salt_path, salt).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!("failed to write salt file '{}': {e}", salt_path.display()),
            ))
        })?;
        Ok(salt)
    }
}

/// Known plaintext used for encryption verification tokens.
const VERIFY_PLAINTEXT: &[u8] = b"rkv-encryption-verify";

/// Create a verification token: `salt (16 bytes) || encrypt(VERIFY_PLAINTEXT, key)`.
///
/// The token is written to `ns.meta` so that on reopen we can verify the
/// password is correct before returning garbled data.
pub(crate) fn create_verification_token(password: &str) -> Vec<u8> {
    let salt = generate_salt();
    let key = derive_key(password, &salt);
    let encrypted = encrypt(&key, VERIFY_PLAINTEXT);
    let mut token = Vec::with_capacity(SALT_LEN + encrypted.len());
    token.extend_from_slice(&salt);
    token.extend_from_slice(&encrypted);
    token
}

/// Verify a password against a stored verification token.
///
/// Returns `Ok(())` if the password matches, or `Err(Corruption)` if not.
pub(crate) fn verify_token(password: &str, token: &[u8]) -> Result<()> {
    if token.len() < SALT_LEN + NONCE_LEN {
        return Err(Error::Corruption(
            "encryption verification token too short".into(),
        ));
    }
    let (salt_bytes, encrypted) = token.split_at(SALT_LEN);
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(salt_bytes);
    let key = derive_key(password, &salt);
    let decrypted = decrypt(&key, encrypted)?;
    if decrypted != VERIFY_PLAINTEXT {
        return Err(Error::Corruption(
            "encryption verification failed (wrong password)".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = derive_key("test-password", &[1u8; SALT_LEN]);
        let plaintext = b"hello, world!";
        let encrypted = encrypt(&key, plaintext);
        let decrypted = decrypt(&key, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_empty_data() {
        let key = derive_key("pw", &[2u8; SALT_LEN]);
        let encrypted = encrypt(&key, b"");
        let decrypted = decrypt(&key, &encrypted).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn wrong_key_returns_corruption() {
        let key1 = derive_key("password-a", &[3u8; SALT_LEN]);
        let key2 = derive_key("password-b", &[3u8; SALT_LEN]);
        let encrypted = encrypt(&key1, b"secret");
        let err = decrypt(&key2, &encrypted).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn corrupted_ciphertext_returns_corruption() {
        let key = derive_key("pw", &[4u8; SALT_LEN]);
        let mut encrypted = encrypt(&key, b"data");
        // Flip a byte in the ciphertext
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0xFF;
        let err = decrypt(&key, &encrypted).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn data_too_short_returns_corruption() {
        let key = derive_key("pw", &[5u8; SALT_LEN]);
        let err = decrypt(&key, &[0u8; 5]).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn derive_key_deterministic() {
        let salt = [6u8; SALT_LEN];
        let k1 = derive_key("same", &salt);
        let k2 = derive_key("same", &salt);
        assert_eq!(k1, k2);
    }

    #[test]
    fn derive_key_different_passwords() {
        let salt = [7u8; SALT_LEN];
        let k1 = derive_key("password-a", &salt);
        let k2 = derive_key("password-b", &salt);
        assert_ne!(k1, k2);
    }

    #[test]
    fn derive_key_different_salts() {
        let k1 = derive_key("same", &[8u8; SALT_LEN]);
        let k2 = derive_key("same", &[9u8; SALT_LEN]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn each_encryption_produces_different_ciphertext() {
        let key = derive_key("pw", &[10u8; SALT_LEN]);
        let e1 = encrypt(&key, b"data");
        let e2 = encrypt(&key, b"data");
        assert_ne!(e1, e2); // different nonces
    }

    #[test]
    fn salt_roundtrip_via_filesystem() {
        let tmp = tempfile::tempdir().unwrap();
        let salt1 = load_or_create_salt(tmp.path(), "test_ns").unwrap();
        let salt2 = load_or_create_salt(tmp.path(), "test_ns").unwrap();
        assert_eq!(salt1, salt2); // same salt on second call

        // Different namespace gets different salt
        let salt3 = load_or_create_salt(tmp.path(), "other_ns").unwrap();
        // Very unlikely to collide (random 16 bytes)
        assert_ne!(salt1, salt3);
    }

    #[test]
    fn verification_token_correct_password() {
        let token = create_verification_token("secret");
        assert!(verify_token("secret", &token).is_ok());
    }

    #[test]
    fn verification_token_wrong_password() {
        let token = create_verification_token("correct");
        let err = verify_token("wrong", &token).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn verification_token_truncated() {
        let err = verify_token("pw", &[0u8; 10]).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn verification_token_corrupted() {
        let mut token = create_verification_token("pw");
        let last = token.len() - 1;
        token[last] ^= 0xFF;
        let err = verify_token("pw", &token).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn salt_file_wrong_length_returns_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let crypto_dir = tmp.path().join("crypto");
        fs::create_dir_all(&crypto_dir).unwrap();
        fs::write(crypto_dir.join("bad.salt"), [0u8; 5]).unwrap();
        let err = load_or_create_salt(tmp.path(), "bad").unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }
}
