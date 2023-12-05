mod thold_crypto;

use serde::{Deserialize, Serialize};
use crate::error::*;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PublicKeyPart {
    key: thold_crypto::PublicKeyPart,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PrivateKeyPart {
    key: thold_crypto::PrivateKeyPart,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PublicKey {
    key: thold_crypto::PublicKey,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PrivateKey {
    key: thold_crypto::PrivateKey,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PartialSignature {
    sig: thold_crypto::PartialSignature,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Signature {
    sig: thold_crypto::Signature,
}

impl PublicKey {



}

impl PublicKeyPart {
    pub fn verify(&self, msg: &[u8], sig: &PartialSignature) -> Result<()> {
        self.key.verify(msg, &sig.sig)
    }
}

impl PrivateKeyPart {
    pub fn public_key_part(&self) -> PublicKeyPart {
        PublicKeyPart { key: self.key.public_key_part() }
    }

    pub fn partially_sign(&self, msg: &[u8]) -> Result<PartialSignature> {
        Ok(PartialSignature { sig: self.key.partially_sign(msg)? })
    }
}

