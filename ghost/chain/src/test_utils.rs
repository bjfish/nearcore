use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use near_primitives::crypto::signature::{PublicKey, Signature};
use near_primitives::crypto::signer::{EDSigner, InMemorySigner};
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::test_utils::get_public_key_from_seed;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId};
use near_store::test_utils::create_test_store;
use near_store::{Store, StoreUpdate};

use crate::error::{Error, ErrorKind};
use crate::types::{BlockHeader, RuntimeAdapter, Weight};
use crate::{Block, Chain};

impl Block {
    pub fn empty(prev: &BlockHeader, signer: Arc<EDSigner>) -> Self {
        Block::produce(
            prev,
            prev.height + 1,
            prev.prev_state_root,
            vec![],
            HashMap::default(),
            signer,
        )
    }
}

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    root: MerkleHash,
    authorities: Vec<(AccountId, PublicKey)>,
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self::new_with_authorities(store, vec!["test".to_string()])
    }

    pub fn new_with_authorities(store: Arc<Store>, authorities: Vec<AccountId>) -> Self {
        KeyValueRuntime {
            store,
            root: MerkleHash::default(),
            authorities: authorities
                .iter()
                .map(|account_id| (account_id.clone(), get_public_key_from_seed(account_id)))
                .collect(),
        }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.root
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self, shard_id: ShardId) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let (account_id, public_key) =
            &self.authorities[(header.height as usize) % self.authorities.len()];
        if !header.verify_block_producer(public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(&self, _height: BlockIndex) -> Vec<AccountId> {
        self.authorities.iter().map(|x| x.0.clone()).collect()
    }

    fn get_block_proposer(&self, height: BlockIndex) -> Result<AccountId, String> {
        Ok(self.authorities[(height as usize) % self.authorities.len()].0.clone())
    }

    fn validate_authority_signature(
        &self,
        _account_id: &AccountId,
        _signature: &Signature,
    ) -> bool {
        true
    }

    fn apply_transactions(
        &self,
        _shard_id: ShardId,
        state_root: &MerkleHash,
        _block_index: BlockIndex,
        _prev_block_hash: &CryptoHash,
        _transactions: &Vec<SignedTransaction>,
    ) -> Result<(StoreUpdate, MerkleHash), String> {
        Ok((self.store.store_update(), *state_root))
    }

    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        path: &str,
        data: &[u8],
    ) -> Result<ABCIQueryResponse, String> {
        Err("Not implemented".to_string())
    }
}

pub fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain = Chain::new(store, runtime.clone(), Utc::now()).unwrap();
    let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
    (chain, runtime, signer)
}
