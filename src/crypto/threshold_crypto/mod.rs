mod thold_crypto;

use serde::{Deserialize, Serialize};
use crate::error::*;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PublicKeyPart {
    key: thold_crypto::PublicKeyPart
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PrivateKeyPart {
    key: thold_crypto::PrivateKeyPart
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PartialSignature {
    sig: thold_crypto::PartialSignature
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Signature {
    sig: thold_crypto::Signature
}

