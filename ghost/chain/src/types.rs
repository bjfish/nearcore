use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;

use chrono::offset::LocalResult::Single;
use chrono::prelude::{DateTime, NaiveDateTime, Utc};
use chrono::serde::ts_nanoseconds;
use protobuf::{Message as ProtoMessage, RepeatedField, SingularPtrField};

use near_protos::chain as chain_proto;
use near_store::StoreUpdate;
use primitives::crypto::signature::{verify, PublicKey, Signature, DEFAULT_SIGNATURE};
use primitives::crypto::signer::EDSigner;
use primitives::hash::{hash, CryptoHash};
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockIndex, MerkleHash};
use primitives::utils::proto_to_type;

use crate::error::Error;

/// Number of nano seconds in one second.
const NS_IN_SECOND: u64 = 1_000_000_000;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockHeader {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Hash of the block previous to this in the chain.
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root hash of the transactions in the given block.
    pub tx_root: MerkleHash,
    /// Timestamp at which the block was built.
    #[serde(with = "ts_nanoseconds")]
    pub timestamp: DateTime<Utc>,
    /// Approval signatures.
    pub signatures: Vec<Signature>,
    /// Total weight.
    pub total_weight: Weight,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    hash: CryptoHash,
}

impl BlockHeader {
    fn header_body(
        height: BlockIndex,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        tx_root: MerkleHash,
        timestamp: DateTime<Utc>,
        signatures: Vec<Signature>,
        total_weight: Weight,
    ) -> chain_proto::BlockHeaderBody {
        chain_proto::BlockHeaderBody {
            height,
            prev_hash: prev_hash.into(),
            prev_state_root: prev_state_root.into(),
            tx_root: tx_root.into(),
            timestamp: timestamp.timestamp_nanos() as u64,
            signatures: RepeatedField::from_iter(signatures.iter().map(std::convert::Into::into)),
            total_weight: total_weight.to_num(),
            ..Default::default()
        }
    }

    pub fn new(
        height: BlockIndex,
        prev_hash: CryptoHash,
        prev_state_root: MerkleHash,
        tx_root: MerkleHash,
        timestamp: DateTime<Utc>,
        signatures: Vec<Signature>,
        total_weight: Weight,
        signer: Arc<EDSigner>,
    ) -> Self {
        let hb = Self::header_body(
            height,
            prev_hash,
            prev_state_root,
            tx_root,
            timestamp,
            signatures,
            total_weight,
        );
        let bytes = hb.write_to_bytes().expect("Failed to serialize");
        let hash = hash(&bytes);
        let h = chain_proto::BlockHeader {
            body: SingularPtrField::some(hb),
            signature: signer.sign(hash.as_ref()).into(),
            ..Default::default()
        };
        h.try_into().expect("Failed to parse just created header")
    }

    pub fn genesis(state_root: MerkleHash, timestamp: DateTime<Utc>) -> Self {
        chain_proto::BlockHeader {
            body: SingularPtrField::some(Self::header_body(
                0,
                CryptoHash::default(),
                state_root,
                MerkleHash::default(),
                timestamp,
                vec![],
                0.into(),
            )),
            signature: DEFAULT_SIGNATURE.into(),
            ..Default::default()
        }
        .try_into()
        .expect("Failed to parse just created header")
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Verifies that given public key produced the block.
    pub fn verify_block_producer(&self, public_key: &PublicKey) -> bool {
        verify(self.hash.as_ref(), &self.signature, public_key)
    }
}

impl TryFrom<chain_proto::BlockHeader> for BlockHeader {
    type Error = String;

    fn try_from(proto: chain_proto::BlockHeader) -> Result<Self, Self::Error> {
        let body = proto.body.into_option().ok_or("Missing Header body")?;
        let bytes = body.write_to_bytes().map_err(|err| err.to_string())?;
        let hash = hash(&bytes);
        let height = body.height;
        let prev_hash = body.prev_hash.try_into()?;
        let prev_state_root = body.prev_state_root.try_into()?;
        let tx_root = body.tx_root.try_into()?;
        let timestamp = DateTime::from_utc(
            NaiveDateTime::from_timestamp(
                (body.timestamp / NS_IN_SECOND) as i64,
                (body.timestamp % NS_IN_SECOND) as u32,
            ),
            Utc,
        );
        let signatures =
            body.signatures.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let total_weight = body.total_weight.into();
        let signature = proto.signature.try_into()?;
        Ok(BlockHeader {
            height,
            prev_hash,
            prev_state_root,
            tx_root,
            timestamp,
            signatures,
            total_weight,
            signature,
            hash,
        })
    }
}

impl From<BlockHeader> for chain_proto::BlockHeader {
    fn from(header: BlockHeader) -> Self {
        chain_proto::BlockHeader {
            body: SingularPtrField::some(chain_proto::BlockHeaderBody {
                height: header.height,
                prev_hash: header.prev_hash.into(),
                prev_state_root: header.prev_state_root.into(),
                tx_root: header.tx_root.into(),
                timestamp: header.timestamp.timestamp_nanos() as u64,
                signatures: RepeatedField::from_iter(
                    header.signatures.iter().map(std::convert::Into::into),
                ),
                total_weight: header.total_weight.to_num(),
                ..Default::default()
            }),
            signature: header.signature.into(),
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Bytes(Vec<u8>);

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<SignedTransaction>,
}

impl Block {
    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(state_root: MerkleHash, timestamp: DateTime<Utc>) -> Self {
        Block { header: BlockHeader::genesis(state_root, timestamp), transactions: vec![] }
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    pub fn produce(
        prev: &BlockHeader,
        state_root: MerkleHash,
        transactions: Vec<SignedTransaction>,
        signer: Arc<EDSigner>,
        // TODO: add collected signatures for previous block and total weight.
    ) -> Self {
        // TODO: merkelize transactions.
        let tx_root = CryptoHash::default();
        Block {
            header: BlockHeader::new(
                prev.height + 1,
                prev.hash(),
                state_root,
                tx_root,
                Utc::now(),
                vec![],
                (prev.total_weight.to_num() + 1).into(),
                signer,
            ),
            transactions,
        }
    }

    pub fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}

impl TryFrom<chain_proto::Block> for Block {
    type Error = String;

    fn try_from(proto: chain_proto::Block) -> Result<Self, Self::Error> {
        let transactions =
            proto.transactions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        Ok(Block { header: proto_to_type(proto.header)?, transactions })
    }
}

impl From<Block> for chain_proto::Block {
    fn from(block: Block) -> Self {
        chain_proto::Block {
            header: SingularPtrField::some(block.header.into()),
            transactions: block.transactions.into_iter().map(std::convert::Into::into).collect(),
            ..Default::default()
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum BlockStatus {
    /// Block is the "next" block, updating the chain head.
    Next,
    /// Block does not update the chain head and is a fork.
    Fork,
    /// Block updates the chain head via a (potentially disruptive) "reorg".
    /// Previous block was not our previous chain head.
    Reorg,
}

/// Options for block origin.
#[derive(Eq, PartialEq)]
pub enum Provenance {
    /// No provenance.
    NONE,
    /// Adds block while in syncing mode.
    SYNC,
    /// Block we produced ourselves.
    PRODUCED,
}

/// Information about valid transaction that was processed by chain + runtime.
pub struct ValidTransaction {
    pub transaction: SignedTransaction,
}

/// Bridge between the chain and the runtime.
/// Main function is to update state given transactions.
/// Additionally handles authorities and block weight computation.
pub trait RuntimeAdapter {
    /// Initialize state to genesis state and returns StoreUpdate and state root.
    /// StoreUpdate can be discarded if the chain past the genesis.
    fn genesis_state(&self) -> (StoreUpdate, MerkleHash);

    /// Verify block producer validity and return weight of given block for fork choice rule.
    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error>;

    /// Block proposer for given height. Return None if outside of known boundaries.
    fn get_block_proposer(&self, height: BlockIndex) -> Option<AccountId>;

    /// Apply transactions to given state root and return store update and new state root.
    fn apply_transactions(
        &self,
        merkle_hash: &MerkleHash,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<(StoreUpdate, MerkleHash), String>;
}

/// The weight is defined as the number of unique authorities approving this fork.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct Weight {
    num: u64,
}

impl Weight {
    pub fn to_num(&self) -> u64 {
        self.num
    }

    pub fn next(&self, num: u64) -> Self {
        Weight { num: self.num + num + 1 }
    }
}

impl From<u64> for Weight {
    fn from(num: u64) -> Self {
        Weight { num }
    }
}

impl fmt::Display for Weight {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.num)
    }
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience and the total weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tip {
    /// Height of the tip (max height of the fork)
    pub height: u64,
    /// Last block pushed to the fork
    pub last_block_hash: CryptoHash,
    /// Previous block
    pub prev_block_hash: CryptoHash,
    /// Total weight on that fork
    pub total_weight: Weight,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header(header: &BlockHeader) -> Tip {
        Tip {
            height: header.height,
            last_block_hash: header.hash(),
            prev_block_hash: header.prev_hash,
            total_weight: header.total_weight,
        }
    }

    /// The hash of the underlying block.
    fn hash(&self) -> CryptoHash {
        self.last_block_hash
    }
}