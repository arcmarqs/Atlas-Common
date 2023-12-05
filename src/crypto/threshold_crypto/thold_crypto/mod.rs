use std::collections::BTreeMap;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
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
    pub fn threshold_verify(&self, sig: &Signature, msg: &[u8]) -> bool {
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
        if self.public_key().threshold_verify(&sig, msg) {
            Ok(())
        } else {
            Err(anyhow!("Signature verification failed"))
        }
    }
}

pub struct DistributedGenerationParams {
    dealers: usize,
    faulty: usize,
}

pub struct DistributedKeyGenerator {
    params: DistributedGenerationParams,
    my_index: usize,

    poly: BivarPoly,
    commitment: BivarCommitment,
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
        })
    }

    pub fn get_keys_to_send(&mut self) -> Result<BTreeMap<usize, (Poly, Commitment)>> {
        let mut resulting_map = BTreeMap::new();

        for index in 1..=self.params.nodes() {
            resulting_map.insert(index, (self.poly.row(index), self.commitment.row(index)));
        }

        Ok(resulting_map)
    }
}

/*
fn distributed_key_generation() {
    let mut rng = rand::thread_rng();
    let dealer_num = 3;
    let node_num = 5;
    let faulty_num = 2;

    // For distributed key generation, a number of dealers, only one of who needs to be honest,
    // generates random bivariate polynomials and publicly commits to them. In partice, the
    // dealers can e.g. be any `faulty_num + 1` nodes.
    let bi_polys: Vec<BivarPoly> = (0..dealer_num)
        .map(|_| {
            BivarPoly::random(faulty_num, &mut rng)
                .expect("Failed to create random `BivarPoly`")
        })
        .collect();
    let pub_bi_commits: Vec<_> = bi_polys.iter().map(BivarPoly::commitment).collect();

    let mut sec_keys = vec![Fr::zero(); node_num];

    // Each dealer sends row `m` to node `m`, where the index starts at `1`. Don't send row `0`
    // to anyone! The nodes verify their rows, and send _value_ `s` on to node `s`. They again
    // verify the values they received, and collect them.
    for (bi_poly, bi_commit) in bi_polys.iter().zip(&pub_bi_commits) {
        for m in 1..=node_num {
            // Node `m` receives its row and verifies it.
            let row_poly = bi_poly
                .row(m)
                .unwrap_or_else(|_| panic!("Failed to create row #{}", m));
            let row_commit = bi_commit.row(m);
            assert_eq!(row_poly.commitment(), row_commit);
            // Node `s` receives the `s`-th value and verifies it.
            for s in 1..=node_num {
                let val = row_poly.evaluate(s);
                let val_g1 = G1Affine::one().mul(val);
                assert_eq!(bi_commit.evaluate(m, s), val_g1);
                // The node can't verify this directly, but it should have the correct value:
                assert_eq!(bi_poly.evaluate(m, s), val);
            }

            // A cheating dealer who modified the polynomial would be detected.
            let x_pow_2 =
                Poly::monomial(2).expect("Failed to create monic polynomial of degree 2");
            let five = Poly::constant(5.into_fr())
                .expect("Failed to create polynomial with constant 5");
            let wrong_poly = row_poly.clone() + x_pow_2 * five;
            assert_ne!(wrong_poly.commitment(), row_commit);

            // If `2 * faulty_num + 1` nodes confirm that they received a valid row, then at
            // least `faulty_num + 1` honest ones did, and sent the correct values on to node
            // `s`. So every node received at least `faulty_num + 1` correct entries of their
            // column/row (remember that the bivariate polynomial is symmetric). They can
            // reconstruct the full row and in particular value `0` (which no other node knows,
            // only the dealer). E.g. let's say nodes `1`, `2` and `4` are honest. Then node
            // `m` received three correct entries from that row:
            let received: BTreeMap<_, _> = [1, 2, 4]
                .iter()
                .map(|&i| (i, bi_poly.evaluate(m, i)))
                .collect();
            let my_row =
                Poly::interpolate(received).expect("Failed to create `Poly` via interpolation");
            assert_eq!(bi_poly.evaluate(m, 0), my_row.evaluate(0));
            assert_eq!(row_poly, my_row);

            // The node sums up all values number `0` it received from the different dealer. No
            // dealer and no other node knows the sum in the end.
            sec_keys[m - 1].add_assign(&my_row.evaluate(Fr::zero()));
        }
    }

    // Each node now adds up all the first values of the rows it received from the different
    // dealers (excluding the dealers where fewer than `2 * faulty_num + 1` nodes confirmed).
    // The whole first column never gets added up in practice, because nobody has all the
    // information. We do it anyway here; entry `0` is the secret key that is not known to
    // anyone, neither a dealer, nor a node:
    let mut sec_key_set = Poly::zero().expect("Failed to create empty `Poly`");
    for bi_poly in &bi_polys {
        sec_key_set += bi_poly
            .row(0)
            .expect("Failed to create `Poly` from row #0 for `BivarPoly`");
    }
    for m in 1..=node_num {
        assert_eq!(sec_key_set.evaluate(m), sec_keys[m - 1]);
    }

    // The sum of the first rows of the public commitments is the commitment to the secret key
    // set.
    let mut sum_commit = Poly::zero()
        .expect("Failed to create empty `Poly`")
        .commitment();
    for bi_commit in &pub_bi_commits {
        sum_commit += bi_commit.row(0);
    }
    assert_eq!(sum_commit, sec_key_set.commitment());
}
 */