use std::collections::BTreeMap;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use threshold_crypto::group::CurveAffine;
use threshold_crypto::{Fr, G1, IntoFr, SecretKeyShare};
use threshold_crypto::ff::Field;
use threshold_crypto::poly::{BivarCommitment, BivarPoly, Commitment, Poly};
use crate::Err;

use crate::error::*;


#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PublicKey {
    key: threshold_crypto::PublicKey,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PrivateKey {
    key: threshold_crypto::SecretKey,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PublicKeyPart {
    key: threshold_crypto::PublicKeyShare,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PrivateKeyPart {
    key: threshold_crypto::SecretKeyShare,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PartialSignature {
    sig: threshold_crypto::SignatureShare,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Signature {
    sig: threshold_crypto::Signature,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct SecretKeySet {
    sk_set: threshold_crypto::SecretKeySet,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct PublicKeySet {
    pk_set: threshold_crypto::PublicKeySet,
}

impl PublicKeyPart {
    pub fn verify(&self, msg: &[u8], sig: &PartialSignature) -> Result<()> {
        if self.key.verify(&sig.sig, msg) {
            Ok(())
        } else {
            Err(anyhow!("Signature verification failed"))
        }
    }
}

impl SecretKeySet {
    pub fn generate_random(n: usize) -> Result<SecretKeySet> {
        let mut rand = ring::rand::SystemRandom::new();

        let sk_set = threshold_crypto::SecretKeySet::try_random(n, &mut rand)?;

        Ok(SecretKeySet {
            sk_set,
        })
    }

    pub fn get_key_share(&self, i: usize) -> Result<PrivateKeyPart> {
        let key = self.sk_set.secret_key_share(i)?;

        Ok(PrivateKeyPart {
            key,
        })
    }

    pub fn public_key_set(&self) -> PublicKeySet {
        PublicKeySet {
            pk_set: self.sk_set.public_keys()
        }
    }
}

impl PrivateKeyPart {
    pub fn public_key_part(&self) -> PublicKeyPart {
        PublicKeyPart {
            key: self.key.public_key_share()
        }
    }

    pub fn partially_sign(&self, msg: &[u8]) -> Result<PartialSignature> {
        let sig = self.key.sign(msg);

        Ok(PartialSignature {
            sig,
        })
    }
}

impl PublicKey {
    #[inline]
    pub fn verify_combined_signatures(&self, sig: &Signature, msg: &[u8]) -> bool {
        self.key.verify(&sig.sig, msg)
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; threshold_crypto::PK_SIZE] {
        self.key.to_bytes()
    }

    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Result<PublicKey> {
        let key = threshold_crypto::PublicKey::from_bytes(bytes)?;

        Ok(PublicKey {
            key,
        })
    }
}

impl PublicKeySet {
    pub fn public_key(&self) -> PublicKey {
        let key = self.pk_set.public_key();

        PublicKey {
            key,
        }
    }

    pub fn get_public_key_part(&self, i: usize) -> Result<PublicKeyPart> {
        let key = self.pk_set.public_key_share(i)?;

        Ok(PublicKeyPart {
            key,
        })
    }

    pub fn verify_partial_signature(&self, index: usize, msg: &[u8], sig: &PartialSignature) -> Result<()> {
        if self.get_public_key_part(index)?.verify(msg, sig) {
            Ok(())
        } else {
            Err(anyhow!("Signature verification failed"))
        }
    }

    pub fn combine_signatures(&self, sigs: &[(usize, PartialSignature)]) -> Result<Signature> {
        let mut sigs = sigs.iter().map(|(index, sign)| (index, &sign.sig)).collect::<Vec<_>>();

        let sig = self.pk_set.combine_signatures(&mut sigs)?;

        Ok(Signature {
            sig,
        })
    }

    pub fn verify_combined_signature(&self, msg: &[u8], sig: &Signature) -> Result<()> {
        if self.public_key().verify_combined_signatures(&sig, msg) {
            Ok(())
        } else {
            Err(anyhow!("Signature verification failed"))
        }
    }
}

/// Parameters for the distributed generation
pub struct DistributedGenerationParams {
    dealers: usize,
    faulty: usize,
}

/// The generator for the distributed key generation
/// This represents the information contained in a given node
///
pub struct DistributedKeyGenerator {
    my_index: usize,

    params: DistributedGenerationParams,

    poly: BivarPoly,
    commitment: BivarCommitment,

    received_rows: BTreeMap<usize, (Poly, Commitment)>,

    received_votes: BTreeMap<usize, Vec<usize>>,

    final_sec_key: Fr,
}

impl DistributedGenerationParams {
    pub fn dealers(&self) -> usize {
        self.dealers
    }
    pub fn faulty(&self) -> usize {
        self.faulty
    }
}

impl DistributedKeyGenerator {
    pub fn initialize_key_generation(my_index: usize, nodes: usize, faulty: usize) -> Result<Self> {
        let mut rng = ring::rand::SystemRandom::new();

        let bivar_poly = BivarPoly::try_random(faulty, &mut rng)?;

        let pub_bivar_commits = bivar_poly.commitment();

        Ok(Self {
            params: DistributedGenerationParams {
                dealers: nodes,
                faulty,
            },
            my_index,
            poly: bivar_poly,
            commitment: pub_bivar_commits,
            received_rows: Default::default(),
            received_votes: Default::default(),
            final_sec_key: Fr::zero(),
        })
    }

    /// Each dealer sends row `m` to node `m`, where the index starts at `1`. Don't send row `0`
    pub fn get_keys_to_send(&mut self) -> Result<BTreeMap<usize, (Poly, Commitment)>> {
        let mut resulting_map = BTreeMap::new();

        for index in 1..=self.params.nodes() {
            resulting_map.insert(index, (self.poly.row(index), self.commitment.row(index)));
        }

        Ok(resulting_map)
    }

    /// Node `m` receives a row from dealer `index`
    /// This will return a map of the values that node `m` (us) should send to the other nodes for verification
    pub fn receive_value(&mut self, index: usize, row_poly: Poly) -> Result<(usize, BTreeMap<usize, G1>)> {
        // FIXME: Why is this here? Is this necessary? I see no reason for its existence
        // It exists in the distributed key generation example from threshold_crypto but doesn't
        // Seem to serve any actual purpose
        // Verification maybe? But are we supposed to send our row poly to all other nodes so they can
        // Verify it? That seems like a bad idea

        let mut verification_map = BTreeMap::new();

        for s in 1..=self.params.nodes() {
            let val = row_poly.evaluate(s);
            let val_g1 = threshold_crypto::G1Affine::one().mul(val);

            verification_map.insert(s, val_g1);
        }

        self.received_rows.insert(index, (row_poly, row_poly.commitment()));
        self.received_votes.insert(index, vec![]);

        Ok((index, verification_map))
    }

    pub fn verify_value(&self, m: usize, val: G1) -> Result<()> {
        if self.commitment.evaluate(m, self.my_index) != val {
            return Err!(DistributedKeyGenError::FailedVerifyingReceivedValue(m, self.my_index));
        }

        Ok(())
    }

    pub fn received_verification_of_value(&mut self, index: usize, s: usize) -> bool {
        let received_votes_for_row = self.received_votes.entry(index).or_insert_with(|| vec![]);

        received_votes_for_row.push(s);

        received_votes_for_row.len() >= self.params.faulty() * 2 + 1
    }

    pub fn finalize_value_verification_for_dealer(&mut self, index: usize) {
        if let Some((poly, p_commit) )= self.received_rows.remove(&index) {
            self.final_sec_key.add_assign(&poly.evaluate(Fr::zero()));
        }
    }

    pub fn finalize(mut self) -> (PrivateKeyPart, PublicKeyPart) {

        let priv_key = PrivateKeyPart {
            key: SecretKeyShare::from_mut(&mut self.final_sec_key),
        };

        let pk = priv_key.public_key_part();

        (priv_key, pk)
    }
}

#[derive(Error, Debug)]
pub enum DistributedKeyGenError {
    #[error("Received commitment does not match calculated commitment")]
    ReceivedCommitmentDoesNotMatchCalculatedCommitment,
    #[error("Failed to verify the given value received from node {0} with my index {1}")]
    FailedVerifyingReceivedValue(usize, usize),
}
