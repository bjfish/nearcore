use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration as TimeDuration, Instant};

use chrono::prelude::{DateTime, Utc};
use chrono::Duration;
use log::{debug, info};

use near_store::Store;
use primitives::hash::CryptoHash;
use primitives::types::{BlockIndex, MerkleHash};

use crate::error::{Error, ErrorKind};
use crate::store::{ChainStore, ChainStoreUpdate};
use crate::types::{Block, BlockHeader, BlockStatus, Provenance, RuntimeAdapter, Tip};

/// Maximum number of orphans chain can store.
pub const MAX_ORPHAN_SIZE: usize = 1024;

/// Maximum age of orhpan to store in the chain.
const MAX_ORPHAN_AGE_SECS: u64 = 300;

/// Refuse blocks more than this many block intervals in the future (as in bitcoin).
const ACCEPTABLE_TIME_DIFFERENCE: i64 = 12 * 10;

pub struct Orphan {
    block: Block,
    provenance: Provenance,
    added: Instant,
}

pub struct OrphanBlockPool {
    orphans: HashMap<CryptoHash, Orphan>,
    height_idx: HashMap<u64, Vec<CryptoHash>>,
    evicted: usize,
}

impl OrphanBlockPool {
    fn new() -> OrphanBlockPool {
        OrphanBlockPool { orphans: HashMap::default(), height_idx: HashMap::default(), evicted: 0 }
    }

    fn len(&self) -> usize {
        self.orphans.len()
    }

    fn len_evicted(&self) -> usize {
        self.evicted
    }

    fn add(&mut self, orphan: Orphan) {
        let height_hashes = self.height_idx.entry(orphan.block.header.height).or_insert(vec![]);
        height_hashes.push(orphan.block.hash());
        self.orphans.insert(orphan.block.hash(), orphan);

        if self.orphans.len() > MAX_ORPHAN_SIZE {
            let old_len = self.orphans.len();

            self.orphans.retain(|_, ref mut x| {
                x.added.elapsed() < TimeDuration::from_secs(MAX_ORPHAN_AGE_SECS)
            });
            let mut heights = self.height_idx.keys().cloned().collect::<Vec<u64>>();
            heights.sort_unstable();
            let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
            for h in heights.iter().rev() {
                if let Some(hash) = self.height_idx.remove(h) {
                    for h in hash {
                        let _ = self.orphans.remove(&h);
                        removed_hashes.insert(h);
                    }
                }
                if self.orphans.len() < MAX_ORPHAN_SIZE {
                    break;
                }
            }
            self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(&x)));

            self.evicted += old_len - self.orphans.len();
        }
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains_key(hash)
    }

    pub fn remove_by_height(&mut self, height: BlockIndex) -> Option<Vec<Orphan>> {
        self.height_idx
            .remove(&height)
            .map(|hs| hs.iter().filter_map(|h| self.orphans.remove(h)).collect())
    }
}

/// Facade to the blockchain block processing and storage.
/// Provides current view on the state according to the chain state.
pub struct Chain {
    store: ChainStore,
    runtime_adapter: Arc<RuntimeAdapter>,
    orphans: OrphanBlockPool,
    genesis: BlockHeader,
}

impl Chain {
    pub fn new(
        store: Arc<Store>,
        runtime_adapter: Arc<RuntimeAdapter>,
        genesis_timestamp: DateTime<Utc>,
    ) -> Result<Chain, Error> {
        let mut store = ChainStore::new(store);

        // Get runtime initial state and create genesis block out of it.
        let (state_store_update, state_root) = runtime_adapter.genesis_state(0);
        let genesis = Block::genesis(state_root, genesis_timestamp);

        // Check if we have a head in the store, otherwise pick genesis block.
        let mut store_update = store.store_update();
        let head_res = store_update.head();
        let head: Tip;
        match head_res {
            Ok(h) => {
                head = h;
                // Check we have the header corresponding to the header_head.
                let header_head = store_update.header_head()?;
                if store_update.get_block_header(&header_head.last_block_hash).is_err() {
                    // Reset header head and "sync" head to be consistent with current block head.
                    store_update.save_header_head(&head);
                    store_update.save_sync_head(&head);
                } else {
                    // Reset sync head to be consistent with current header head.
                    store_update.save_sync_head(&header_head);
                }
                // TODO: check that genesis root / timestamp matches.
                // TODO: perform validation that latest state in runtime matches the stored chain.
            }
            Err(err) => match err.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    store_update
                        .save_post_state_root(&genesis.hash(), &genesis.header.prev_state_root);
                    store_update.save_block_header(genesis.header.clone());
                    store_update.save_block(genesis.clone());

                    head = Tip::from_header(&genesis.header);
                    store_update.save_head(&head);
                    store_update.save_sync_head(&head);

                    store_update.merge(state_store_update);

                    info!(target: "chain", "Init: saved genesis: {:?}", genesis.hash());
                }
                e => return Err(e.into()),
            },
        }
        store_update.finalize()?.commit()?;

        info!(target: "chain", "Init: head: {} @ {} [{}]", head.total_weight.to_num(), head.height, head.last_block_hash);

        Ok(Chain {
            store,
            runtime_adapter,
            orphans: OrphanBlockPool::new(),
            genesis: genesis.header,
        })
    }

    /// Reset "sync" head to current header head.
    /// Do this when first transition to header syncing.
    pub fn reset_sync_head(&mut self) -> Result<Tip, Error> {
        let mut chain_store_update = self.store.store_update();
        let header_head = chain_store_update.header_head()?;
        chain_store_update.save_sync_head(&header_head);
        chain_store_update.finalize()?.commit()?;
        Ok(header_head)
    }

    /// Process a block header received during "header first" propagation.
    pub fn process_block_header(&mut self, header: &BlockHeader) -> Result<(), Error> {
        // We create new chain update, but it's not going to be committed so it's read only.
        let mut chain_update =
            ChainUpdate::new(&mut self.store, self.runtime_adapter.clone(), &self.orphans);
        chain_update.process_block_header(header)?;
        Ok(())
    }

    /// Process a received or produced block, and unroll any orphans that may depend on it.
    /// Changes current state, and calls `block_accepted` callback in case block was successfully applied.
    pub fn process_block<F>(
        &mut self,
        block: Block,
        provenance: Provenance,
        block_accepted: F,
    ) -> Result<Option<Tip>, Error>
    where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
    {
        let height = block.header.height;
        let res = self.process_block_single(block, provenance, block_accepted);
        if res.is_ok() {
            if let Some(new_res) = self.check_orphans(height + 1, block_accepted) {
                return Ok(Some(new_res));
            }
        }
        res
    }

    pub fn sync_block_headers(&mut self, headers: Vec<BlockHeader>) -> Result<(), Error> {
        let mut chain_update = ChainUpdate::new(&mut self.store, self.runtime_adapter.clone(), &self.orphans);
        chain_update.sync_block_headers(headers)?;
        chain_update.commit()
    }

    fn determine_status(&self, head: Option<Tip>, prev_head: Tip) -> BlockStatus {
        let has_head = head.is_some();
        let mut is_next_block = false;

        if let Some(head) = head {
            if head.prev_block_hash == prev_head.last_block_hash {
                is_next_block = true;
            }
        }

        match (has_head, is_next_block) {
            (true, true) => BlockStatus::Next,
            (true, false) => BlockStatus::Reorg,
            (false, _) => BlockStatus::Fork,
        }
    }

    fn process_block_single<F>(
        &mut self,
        block: Block,
        provenance: Provenance,
        mut block_accepted: F,
    ) -> Result<Option<Tip>, Error>
    where
        F: FnMut(&Block, BlockStatus, Provenance) -> (),
    {
        let prev_head = self.store.head()?;
        let mut chain_update =
            ChainUpdate::new(&mut self.store, self.runtime_adapter.clone(), &self.orphans);
        let maybe_new_head = chain_update.process_block(&block, &provenance);

        if let Ok(_) = maybe_new_head {
            chain_update.commit()?;
        }

        match maybe_new_head {
            Ok(head) => {
                let status = self.determine_status(head.clone(), prev_head);

                // Notify other parts of the system of the update.
                block_accepted(&block, status, provenance);

                Ok(head)
            }
            Err(e) => match e.kind() {
                ErrorKind::Orphan => {
                    let block_hash = block.hash();
                    let orphan = Orphan { block, provenance, added: Instant::now() };

                    self.orphans.add(orphan);

                    debug!(
                        target: "chain",
                        "Process block: orphan: {:?}, # orphans {}{}",
                        block_hash,
                        self.orphans.len(),
                        if self.orphans.len_evicted() > 0 {
                            format!(", # evicted {}", self.orphans.len_evicted())
                        } else {
                            String::new()
                        },
                    );
                    Err(ErrorKind::Orphan.into())
                }
                ErrorKind::Unfit(ref msg) => {
                    debug!(
                        target: "chain",
                        "Block {} at {} is unfit at this time: {}",
                        block.hash(),
                        block.header.height,
                        msg
                    );
                    Err(ErrorKind::Unfit(msg.clone()).into())
                }
                _ => Err(ErrorKind::Other(format!("{:?}", e)).into()),
            },
        }
    }

    /// Check for orphans, once a block is successfully added.
    fn check_orphans<F>(&mut self, mut height: BlockIndex, block_accepted: F) -> Option<Tip>
    where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
    {
        let initial_height = height;

        let mut orphan_accepted = false;
        let mut maybe_new_head = None;

        // Check if there are orphans we can process.
        debug!(target: "chain", "Check orphans: at {}, # orphans {}", height, self.orphans.len());
        loop {
            if let Some(orphans) = self.orphans.remove_by_height(height) {
                debug!(target: "chain", "Check orphans: found {} orphans", orphans.len());
                for orphan in orphans.into_iter() {
                    let res =
                        self.process_block_single(orphan.block, orphan.provenance, block_accepted);
                    match res {
                        Ok(maybe_tip) => {
                            maybe_new_head = maybe_tip;
                            orphan_accepted = true;
                        }
                        Err(err) => {
                            debug!(target: "chain", "Orphan declined: {}", err);
                        }
                    }
                }

                if orphan_accepted {
                    // Accepted a block, so should check if there are now new orphans unlocked.
                    height += 1;
                    continue;
                }
            }
            break;
        }

        if initial_height != height {
            debug!(
                target: "chain",
                "Check orphans: {} blocks accepted since height {}, remaining # orphans {}",
                height - initial_height,
                initial_height,
                self.orphans.len(),
            );
        }

        maybe_new_head
    }
}

/// Various chain getters.
impl Chain {
    /// Gets chain head.
    #[inline]
    pub fn head(&self) -> Result<Tip, Error> {
        self.store.head()
    }

    /// Gets chain header head.
    #[inline]
    pub fn header_head(&self) -> Result<Tip, Error> {
        self.store.header_head()
    }

    /// Gets "sync" head. This may be significantly different to current header chain.
    #[inline]
    pub fn sync_head(&self) -> Result<Tip, Error> {
        self.store.sync_head()
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    #[inline]
    pub fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.store.head_header()
    }

    /// Gets a block by hash.
    #[inline]
    pub fn get_block(&mut self, hash: &CryptoHash) -> Result<&Block, Error> {
        self.store.get_block(hash)
    }

    /// Gets a block header by hash.
    #[inline]
    pub fn get_block_header(&mut self, hash: &CryptoHash) -> Result<&BlockHeader, Error> {
        self.store.get_block_header(hash)
    }

    /// Get previous block header.
    #[inline]
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.store.get_previous_header(header)
    }

    /// Check if block exists.
    #[inline]
    pub fn block_exists(&self, hash: &CryptoHash) -> Result<bool, Error> {
        self.store.block_exists(hash)
    }

    /// Get state root hash after applying header with given hash.
    #[inline]
    pub fn get_post_state_root(&mut self, hash: &CryptoHash) -> Result<&MerkleHash, Error> {
        self.store.get_post_state_root(hash)
    }

    /// Returns underlying ChainStore.
    #[inline]
    pub fn store(&self) -> &ChainStore {
        &self.store
    }

    /// Returns genesis block header.
    #[inline]
    pub fn genesis(&self) -> &BlockHeader {
        &self.genesis
    }

    /// Returns number of orphans currently in the orphan pool.
    #[inline]
    pub fn orphans_len(&self) -> usize {
        self.orphans.len()
    }

    /// Returns number of evicted orphans.
    #[inline]
    pub fn orphans_evicted_len(&self) -> usize {
        self.orphans.len_evicted()
    }

    /// Check if hash is for a known orphan.
    #[inline]
    pub fn is_orphan(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains(hash)
    }
}

/// Chain update helper, contains information that is needed to process block
/// and decide to accept it or reject it.
/// If rejected nothing will be updated in underlying storage.
/// Safe to stop process mid way (Ctrl+C or crash).
struct ChainUpdate<'a> {
    runtime_adapter: Arc<RuntimeAdapter>,
    chain_store_update: ChainStoreUpdate<'a>,
    orphans: &'a OrphanBlockPool,
}

impl<'a> ChainUpdate<'a> {
    pub fn new(
        store: &'a mut ChainStore,
        runtime_adapter: Arc<RuntimeAdapter>,
        orphans: &'a OrphanBlockPool,
    ) -> Self {
        let chain_store_update = store.store_update();
        ChainUpdate { runtime_adapter, chain_store_update, orphans }
    }

    /// Commit changes to the chain into the database.
    pub fn commit(self) -> Result<(), Error> {
        let store_update = self.chain_store_update.finalize()?;
        store_update.commit().map_err(|e| e.into())
    }

    /// Process block header as part of "header first" block propagation.
    /// We validate the header but we do not store it or update header head
    /// based on this. We will update these once we get the block back after
    /// requesting it.
    pub fn process_block_header(&mut self, header: &BlockHeader) -> Result<(), Error> {
        debug!(target: "chain", "Process block header: {} at {}", header.hash(), header.height);

        self.check_header_known(header)?;
        self.validate_header(header, &Provenance::NONE)?;
        Ok(())
    }

    /// Find previous header or return Orphan error if not found.
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.chain_store_update.get_previous_header(header).map_err(|e| match e.kind() {
            ErrorKind::DBNotFoundErr(_) => ErrorKind::Orphan.into(),
            other => other.into(),
        })
    }

    /// Runs the block processing, including validation and finding a place for the new block in the chain.
    /// Returns new head if chain head updated.
    fn process_block(
        &mut self,
        block: &Block,
        provenance: &Provenance,
    ) -> Result<Option<Tip>, Error> {
        debug!(target: "chain", "Process block {} at {}, approvals: {}, tx: {}", block.hash(), block.header.height, block.header.approval_sigs.len(), block.transactions.len());

        // Check if we have already processed this block previously.
        self.check_known(&block)?;

        // Delay hitting the db for current chain head until we know this block is not already known.
        let head = self.chain_store_update.head()?;
        let is_next = block.header.prev_hash == head.last_block_hash;

        // First real I/O expense.
        let prev = self.get_previous_header(&block.header)?;
        let prev_hash = prev.hash();

        // Block is an orphan if we do not know about the previous full block.
        // Skip this check if we have just processed the previous block
        // or the full txhashset state (fast sync) at the previous block height.
        if !is_next && self.chain_store_update.block_exists(&prev_hash)? {
            return Err(ErrorKind::Orphan.into());
        }

        // This is a fork in the context of both header and block processing
        // if this block does not immediately follow the chain head.
        let is_fork = !is_next;

        // Check the header is valid before we proceed with the full block.
        self.process_header_for_block(&block.header, provenance, is_fork)?;

        // Rewind current fork to the point of forking and re-apply ancestors of this block.
        if is_fork {
            self.rewind_and_apply_fork(block)?;
        }

        // Check that state root we computed from previous block matches recorded in this block.
        let state_root = self.chain_store_update.get_post_state_root(&prev_hash)?;
        if &block.header.prev_state_root != state_root {
            return Err(ErrorKind::InvalidStateRoot.into());
        }

        // Apply block to runtime.
        let (state_store_update, state_root) = self
            .runtime_adapter
            .apply_transactions(0, &block.header.prev_state_root, &block.transactions)
            .map_err(|e| ErrorKind::Other(e))?;
        self.chain_store_update.merge(state_store_update);
        self.chain_store_update.save_post_state_root(&block.hash(), &state_root);

        // Add validated block to the db, even if it's not the selected fork.
        self.chain_store_update.save_block(block.clone());

        // Update the chain head if total weight has increased.
        let res = self.update_head(block)?;
        Ok(res)
    }

    /// Process a block header as part of processing a full block.
    /// We want to be sure the header is valid before processing the full block.
    fn process_header_for_block(
        &mut self,
        header: &BlockHeader,
        provenance: &Provenance,
        is_fork: bool,
    ) -> Result<(), Error> {
        if is_fork {
            self.rewind_and_apply_header_fork(header)?;
        }

        self.validate_header(header, provenance)?;
        self.chain_store_update.save_block_header(header.clone());
        self.update_header_head(header)?;
        Ok(())
    }

    /// Process incoming block headers for syncing.
    fn sync_block_headers(&mut self, headers: Vec<BlockHeader>) -> Result<Option<Tip>, Error> {
        let first_header = if let Some(header) = headers.first() {
            debug!(target: "chain", "Sync block headers: {} headers from {} at {}", headers.len(), header.hash(), header.height);
            header
        } else {
            return Ok(None);
        };

        let all_known = if let Some(last_header) = headers.last() {
            self.chain_store_update.get_block_header(&last_header.hash()).is_ok()
        } else {
            false
        };


        if !all_known {
            // First add all headers to the chain.
            for header in headers.iter() {
                self.chain_store_update.save_block_header(header.clone());
            }
            // Then validate all headers (splitting into two, makes sure if they are out of order).
            for header in headers.iter() {
                self.validate_header(header, &Provenance::SYNC)?;
            }
        }

        if let Some(header) = headers.last() {
            // Update sync_head regardless of the total weight.
            self.update_sync_head(header)?;
            // Update header_head if total weight changed.
            let head = self.update_header_head(header)?;
            Ok(head)
        } else {
            Ok(None)
        }
    }

    fn validate_header(&mut self, header: &BlockHeader, provenance: &Provenance) -> Result<(), Error> {
        // Refuse blocks from the too distant future.
        if header.timestamp > Utc::now() + Duration::seconds(ACCEPTABLE_TIME_DIFFERENCE) {
            return Err(ErrorKind::InvalidBlockFutureTime(header.timestamp).into());
        }

        // Quick check if head is already known.
        if self.is_header_known(header)? {
            return Ok(());
        }

        // First I/O cost, delayed as late as possible.
        let prev_header = self.get_previous_header(header)?;
        let prev_hash = prev_header.hash();

        // Prevent time warp attacks and some timestamp manipulations by forcing strict
        // time progression.
        if header.timestamp <= prev_header.timestamp {
            return Err(
                ErrorKind::InvalidBlockPastTime(prev_header.timestamp, header.timestamp).into()
            );
        }

        // If this is not the block we produced (hence trust in it) - validates block
        // producer, confirmation signatures and returns new total weight.
        if *provenance != Provenance::PRODUCED {
            let prev_header = self.get_previous_header(header)?.clone();
            let weight = self.runtime_adapter.compute_block_weight(&prev_header, header)?;
            if weight != header.total_weight {
                return Err(ErrorKind::InvalidBlockWeight.into());
            }
        }

        Ok(())
    }

    /// Update the header head if this header has most work.
    fn update_header_head(&mut self, header: &BlockHeader) -> Result<Option<Tip>, Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.total_weight > header_head.total_weight {
            let tip = Tip::from_header(header);
            self.chain_store_update.save_header_head(&tip);
            debug!(target: "chain", "Header head updated to {} at {}", tip.last_block_hash, tip.height);

            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Directly updates the head if we've just appended a new block to it or handle
    /// the situation where we've just added enough weight to have a fork with more
    /// work than the head.
    fn update_head(&mut self, block: &Block) -> Result<Option<Tip>, Error> {
        // if we made a fork with more work than the head (which should also be true
        // when extending the head), update it
        let head = self.chain_store_update.head()?;
        if block.header.total_weight > head.total_weight {
            let tip = Tip::from_header(&block.header);

            self.chain_store_update.save_body_head(&tip);
            debug!(target: "chain", "Head updated to {} at {}", tip.last_block_hash, tip.height);
            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Updates "sync" head with given block header.
    fn update_sync_head(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let tip = Tip::from_header(header);
        self.chain_store_update.save_sync_head(&tip);
        debug!(target: "chain", "Sync head {} @ {}", tip.last_block_hash, tip.height);
        Ok(())
    }

    /// Rewind the header chain and reapply headers on a fork.
    pub fn rewind_and_apply_header_fork(&mut self, header: &BlockHeader) -> Result<(), Error> {
        unimplemented!();
        // Ok(())
    }

    /// Utility function to handle forks. From the forked block, jump backward
    /// to find to fork root. Rewind the runtime to the root and apply all the
    /// forked blocks prior to the one being processed to set the runtime in
    /// the expected state.
    fn rewind_and_apply_fork(&mut self, block: &Block) -> Result<(), Error> {
        unimplemented!();
        // Ok(())
    }

    /// Quick in-memory check to fast-reject any block header we've already handled
    /// recently. Keeps duplicates from the network in check.
    /// ctx here is specific to the header_head (tip of the header chain)
    fn check_header_known(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.hash() == header_head.last_block_hash
            || header.hash() == header_head.prev_block_hash
        {
            return Err(ErrorKind::Unfit("header already known".to_string()).into());
        }
        Ok(())
    }

    /// Quick in-memory check for fast-reject any block handled recently.
    fn check_known_head(&self, header: &BlockHeader) -> Result<(), Error> {
        let head = self.chain_store_update.head()?;
        let bh = header.hash();
        if bh == head.last_block_hash || bh == head.prev_block_hash {
            return Err(ErrorKind::Unfit("already known in head".to_string()).into());
        }
        Ok(())
    }

    /// Check if this block is in the set of known orphans.
    fn check_known_orphans(&self, header: &BlockHeader) -> Result<(), Error> {
        if self.orphans.contains(&header.hash()) {
            return Err(ErrorKind::Unfit("already known in orphans".to_string()).into());
        }
        Ok(())
    }

    /// Check if this block is ini the store already.
    fn check_known_store(&self, header: &BlockHeader) -> Result<(), Error> {
        match self.chain_store_update.block_exists(&header.hash()) {
            Ok(true) => {
                let head = self.chain_store_update.head()?;
                if header.height > 50 && header.height < head.height - 50 {
                    // We flag this as an "abusive peer" but only in the case
                    // where we have the full block in our store.
                    // So this is not a particularly exhaustive check.
                    Err(ErrorKind::OldBlock.into())
                } else {
                    Err(ErrorKind::Unfit("already known in store".to_string()).into())
                }
            }
            Ok(false) => {
                // Not yet processed this block, we can proceed.
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Check if header is known: head, orphan or in store.
    fn is_header_known(&self, header: &BlockHeader) -> Result<bool, Error> {
        let check = || {
            self.check_known_head(header)?;
            self.check_known_orphans(header)?;
            self.check_known_store(header)
        };
        match check() {
            Ok(()) => Ok(false),
            Err(err) => match err.kind() {
                ErrorKind::Unfit(_) => Ok(true),
                kind => Err(kind.into())
            }
        }
    }

    /// Check if block is known: head, orphan or in store.
    fn check_known(&self, block: &Block) -> Result<(), Error> {
        self.check_known_head(&block.header)?;
        self.check_known_orphans(&block.header)?;
        self.check_known_store(&block.header)?;
        Ok(())
    }
}
